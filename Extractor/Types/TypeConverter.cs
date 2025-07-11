using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using System.Xml;
using Cognite.Extensions.DataModels.QueryBuilder;
using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.FDM;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Types
{
    public enum JsonMode
    {
        Json,
        ReversibleJson
    }

    public class TypeConverter
    {
        private readonly IUAClientAccess client;
        private readonly ILogger log;
        private readonly FullConfig config;
        public TypeConverter(ILogger log, IUAClientAccess client, FullConfig config)
        {
            this.client = client;
            this.config = config;
            this.log = log;
            nodeIdConverter = new NodeIdConverter(client);
        }

        public Variant[] ExtractVariantArray(Variant value)
        {
            if (value.Value is null) return Array.Empty<Variant>();
            if (value.Value is Variant[] variants) return variants;
            if (value.Value is not IEnumerable enumerable) return new Variant[] { value };

            var typeInfo = new TypeInfo(value.TypeInfo.BuiltInType, ValueRanks.Scalar);
            // If the value is enumerable, we cast it to an array of variants,
            // reusing the outer type info.
            return enumerable.Cast<object>().Select(v => new Variant(
                v,
                typeInfo
            )).ToArray();
        }

        public string ConvertToString(
            Variant value,
            IDictionary<long, string>? enumValues = null,
            INodeIdConverter? context = null)
        {
            // Match on all possible variant types.
            if (value.Value is Variant nested) return ConvertToString(nested, enumValues, context);

            return VariantValueToString(value.Value, value.TypeInfo, enumValues, context) ?? string.Empty;
        }

        private static bool IsNumber(object value)
        {
            return value is sbyte
                || value is byte
                || value is short
                || value is ushort
                || value is int
                || value is uint
                || value is long
                || value is ulong
                || value is float
                || value is double
                || value is decimal;
        }

        private static string? TryConvertToEnumDiscriminant(object? value, IDictionary<long, string>? enumValues)
        {
            if (value is null || enumValues is null) return null;

            try
            {
                var longVal = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                if (enumValues.TryGetValue(longVal, out string enumVal))
                {
                    return enumVal;
                }
            }
            catch { }

            return null;
        }

        private JsonObject SerializeDiagnosticInfo(DiagnosticInfo diagInfo)
        {
            var obj = new JsonObject
            {
                ["LocalizedText"] = diagInfo.LocalizedText,
                ["InnerStatusCode"] = StatusCode.LookupSymbolicId(diagInfo.InnerStatusCode.Code)
            };
            if (diagInfo.AdditionalInfo != null)
            {
                obj["AdditionalInfo"] = diagInfo.AdditionalInfo;
            }
            if (diagInfo.InnerDiagnosticInfo != null)
            {
                obj["InnerDiagnosticInfo"] = SerializeDiagnosticInfo(diagInfo.InnerDiagnosticInfo);
            }

            return obj;
        }


        private string? VariantValueToString(object? value, TypeInfo typeInfo, IDictionary<long, string>? enumValues = null, INodeIdConverter? context = null)
        {
            if (value is null) return string.Empty;

            var enumDiscriminant = TryConvertToEnumDiscriminant(value, enumValues);
            if (enumDiscriminant != null) return enumDiscriminant;

            // This is based on the built-in types in OPC-UA, which are all the types a variant may take.
            // See OPC-UA Part 6, table 1.
            if (value is StatusCode statusCode) return StatusCode.LookupSymbolicId(statusCode.Code);
            if (typeInfo.BuiltInType == BuiltInType.StatusCode) return StatusCode.LookupSymbolicId(value as uint? ?? 0);
            if (IsNumber(value)) return value.ToString();
            if (value is bool boolValue) return boolValue ? "true" : "false";
            if (value is DateTime dateTimeValue) return dateTimeValue.ToISOString();
            if (value is DateTimeOffset dateTimeOffsetValue) return dateTimeOffsetValue.DateTime.ToISOString();
            if (value is Guid guidValue) return guidValue.ToString();
            if (value is string strValue) return strValue;
            if (value is XmlElement xmlElement) return Newtonsoft.Json.JsonConvert.SerializeXmlNode(xmlElement);
            // byte[] is used to represent both an array of bytes, and a bytestring.
            if (value is byte[] byteArray && typeInfo.BuiltInType == BuiltInType.ByteString) return Convert.ToBase64String(byteArray);
            if (value is NodeId nodeId)
            {
                if (context != null) return context.NodeIdToString(nodeId);
                return client.GetUniqueId(nodeId) ?? nodeId.ToString();
            }
            if (value is ExpandedNodeId expandedNodeId)
            {
                if (context != null) return context.NodeIdToString(client.Context.ToNodeId(expandedNodeId));
                return client.GetUniqueId(expandedNodeId) ?? expandedNodeId.ToString();
            }
            if (value is LocalizedText localizedText) return localizedText.Text ?? string.Empty;
            if (value is QualifiedName qualifiedName) return qualifiedName.Name ?? string.Empty;
            if (value is DataValue dataValue) return ConvertToString(dataValue.WrappedValue, enumValues, context);
            if (value is Uuid uuid) return uuid.GuidString;
            if (value is DiagnosticInfo diagInfo)
            {
                return SerializeDiagnosticInfo(diagInfo).ToJsonString();
            }
            if (value.GetType().IsEnum) return Enum.GetName(value.GetType(), value);

            if (value is ExtensionObject extObj)
            {
                return ExtensionObjectBodyToString(extObj.Body, context);
            }

            if (value is IEnumerable)
            {
                // Array elements are serialized as JSON...
                return ConvertToJson(new Variant(value, typeInfo), enumValues, context)?.ToJsonString();
            }
            if (value is Matrix matrix)
            {
                return ConvertToJson(new Variant(matrix, typeInfo), enumValues, context)?.ToJsonString();
            }

            log.LogWarning("Unknown variant contents: {Value} ({Type})", value, value.GetType().Name);
            return value.ToString();
        }

        private string ExtensionObjectBodyToString(object body, INodeIdConverter? context)
        {
            // For regular string conversion we special-case a few possible extension object bodies.
            // The body can be a byte-string, which technically means we haven't been able to encode it at all.
            // Just pass it through as a byte-string.
            if (body is byte[] byteArray) return Convert.ToBase64String(byteArray);
            if (body is XmlElement xmlElement) return Newtonsoft.Json.JsonConvert.SerializeXmlNode(xmlElement);

            // Speical case a few types
            if (body is Opc.Ua.KeyValuePair kvp) return $"{kvp.Key?.Name}: {ConvertToString(kvp.Value, null, context)}";
            if (body is EUInformation euInfo) return $"{euInfo.DisplayName?.Text}: {euInfo.Description?.Text}";
            if (body is Opc.Ua.Range range) return $"({range.Low}, {range.High})";
            if (body is Opc.Ua.EnumValueType enumType) return $"{enumType.DisplayName?.Text}: {enumType.Value}";

            // If we don't know the type, just serialize it to JSON.
            if (body is IEncodeable encodable)
            {
                try
                {
                    using var encoder = new JsonEncoder(client.Context.MessageContext, false);
                    encodable.Encode(encoder);
                    var result = encoder.CloseAndReturnText();
                    return result;
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to encode extension object body: {Body}", body);
                    return body.ToString();
                }
            }

            log.LogWarning("Unknown extension object body type: {Body} ({Type})", body, body.GetType().Name);
            return body.ToString();
        }

        private JsonNode? ExtensionObjectBodyToJson(object body, INodeIdConverter? context)
        {
            // For regular JSON conversion we special-case a few possible extension object bodies.
            // The body can be a byte-string, which technically means we haven't been able to encode it at all.
            // Just pass it through as a byte-string.
            if (body is byte[] byteArray) return Convert.ToBase64String(byteArray);
            if (body is XmlElement xmlElement) return JsonNode.Parse(Newtonsoft.Json.JsonConvert.SerializeXmlNode(xmlElement));

            // Speical case a few types
            if (body is Opc.Ua.KeyValuePair kvp) return new JsonObject { [kvp.Key?.Name ?? ""] = ConvertToJson(kvp.Value, null, context) };
            if (body is Opc.Ua.EnumValueType enumType) return new JsonObject { [enumType.DisplayName?.Text ?? ""] = ConvertToJson(enumType.Value, null, context) };

            return null;
        }

        private JsonNode? VariantValueToJson(object value, TypeInfo typeInfo, IDictionary<long, string>? enumValues, INodeIdConverter? context)
        {
            var enumDiscriminant = TryConvertToEnumDiscriminant(value, enumValues);
            if (enumDiscriminant != null) return enumDiscriminant;

            // This is based on the built-in types in OPC-UA, which are all the types a variant may take.
            // See OPC-UA Part 6, table 1.
            if (value is StatusCode statusCode) return StatusCode.LookupSymbolicId(statusCode.Code);
            if (typeInfo.BuiltInType == BuiltInType.StatusCode) return StatusCode.LookupSymbolicId(value as uint? ?? 0);

            if (value is sbyte sbyteval) return sbyteval;
            if (value is byte byteval) return byteval;
            if (value is short shortval) return shortval;
            if (value is ushort ushortval) return ushortval;
            if (value is int intval) return intval;
            if (value is uint uintval) return uintval;
            if (value is long longval) return longval;
            if (value is ulong ulongval) return ulongval;
            if (value is float floatval) return floatval;
            if (value is double doubleval) return doubleval;
            if (value is decimal decimalval) return decimalval;
            if (value is bool boolValue) return boolValue;
            if (value is DateTime dateTimeValue) return dateTimeValue.ToISOString();
            if (value is DateTimeOffset dateTimeOffsetValue) return dateTimeOffsetValue.DateTime.ToISOString();
            if (value is Guid guidValue) return guidValue.ToString();
            if (value is string strValue) return strValue;
            if (value is XmlElement xmlElement) return JsonNode.Parse(Newtonsoft.Json.JsonConvert.SerializeXmlNode(xmlElement));
            if (value is byte[] byteArray && typeInfo.BuiltInType == BuiltInType.ByteString) return Convert.ToBase64String(byteArray);
            if (value is NodeId nodeId)
            {
                if (context != null) return context.NodeIdToString(nodeId);
                return client.GetUniqueId(nodeId) ?? nodeId.ToString();
            }
            if (value is ExpandedNodeId expandedNodeId)
            {
                if (context != null) return context.NodeIdToString(client.Context.ToNodeId(expandedNodeId));
                return client.GetUniqueId(expandedNodeId) ?? expandedNodeId.ToString();
            }
            if (value.GetType().IsEnum) return Enum.GetName(value.GetType(), value);
            if (value is ExtensionObject extensionObject)
            {
                return ExtensionObjectBodyToJson(extensionObject.Body, context);
            }
            if (value is DiagnosticInfo diagInfo)
            {
                return SerializeDiagnosticInfo(diagInfo);
            }

            return null;
        }

        private JsonNode? SafeEncodeToJson(Variant value, bool reversible)
        {
            try
            {
                using var encoder = new JsonEncoder(client.Context.MessageContext, reversible);
                encoder.WriteVariant(null, value);
                var result = encoder.CloseAndReturnText();

                return JsonNode.Parse(result[1..^1]);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to encode variant contents: {Value}", value.Value);
                return null;
            }
        }

        public JsonNode? ConvertToJson(Variant value, IDictionary<long, string>? enumValues = null, INodeIdConverter? context = null, JsonMode mode = JsonMode.Json)
        {
            // Match on all possible variant types.
            if (value.Value is Variant nested) return ConvertToJson(nested, enumValues, context, mode);
            if (value.Value is null) return null;

            if (mode == JsonMode.ReversibleJson)
            {
                // For reversible JSON, we just always use the JSON encoder for now.
                return SafeEncodeToJson(value, true);
            }
            var specialCased = VariantValueToJson(value.Value, value.TypeInfo, enumValues, context);
            if (specialCased != null) return specialCased;
            if (value.Value is IEnumerable)
            {
                // If the value is enumerable, we cast it to an array of variants,
                // reusing the outer type info.
                var variants = ExtractVariantArray(value);
                var jsonArray = new JsonArray();
                foreach (var item in variants)
                {
                    var itemJson = ConvertToJson(item, enumValues, context, mode);
                    if (itemJson != null) jsonArray.Add(itemJson);
                }
                return jsonArray;
            }

            return SafeEncodeToJson(value, false);
        }

        public JsonObject? ConvertToJsonObject(Variant value, IDictionary<long, string>? enumValues = null, INodeIdConverter? context = null, JsonMode mode = JsonMode.Json)
        {
            var jsonNode = ConvertToJson(value, enumValues, context, mode);
            if (jsonNode is JsonObject jsonObject) return jsonObject;
            if (jsonNode is null) return null;

            // If we get here, we have a JSON array or a primitive value.
            // We wrap it in an object with a single property "value".
            return new JsonObject { ["Value"] = jsonNode };
        }

        private readonly ConcurrentDictionary<ConverterType, NodeSerializer> converters = new ConcurrentDictionary<ConverterType, NodeSerializer>();
        private readonly NodeIdConverter nodeIdConverter;
        public void AddConverters(JsonSerializerOptions options, ConverterType type)
        {
            options.Converters.Add(converters.GetOrAdd(type, key => new NodeSerializer(this, config, client.Context, key, log)));
            options.Converters.Add(nodeIdConverter);
        }
    }
}

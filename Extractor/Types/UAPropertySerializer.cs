/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.FDM;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cognite.OpcUa.Types
{
    public enum StringConverterMode
    {
        Simple,
        Json,
        ReversibleJson
    }

    class StringWrapper
    {
        [JsonPropertyName("value")]
        public string? Value { get; set; }
    }


    /// <summary>
    /// Class used for converting properties and property values to strings.
    /// Handles both conversion to metadata for Clean, and conversion to JSON for Raw.
    /// </summary>
    public class StringConverter
    {
        private readonly UAClient? uaClient;
        private readonly FullConfig? config;
        private readonly ILogger<StringConverter> log;

        public StringConverter(ILogger<StringConverter> log, UAClient? uaClient, FullConfig? config)
        {
            this.log = log;
            this.uaClient = uaClient;
            this.config = config;
            if (uaClient != null)
            {
                nodeIdConverter = new NodeIdConverter(uaClient);
            }
        }

        /// <summary>
        /// Convert a value from OPC-UA to a string, which may optionally be JSON.
        /// Gives better results if <paramref name="value"/> is Variant.
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="enumValues">Map from numeric to string values</param>
        /// <param name="typeInfo">TypeInfo for <paramref name="value"/></param>
        /// <param name="mode">String converter mode.</param>
        /// <returns></returns>
        public string ConvertToString(
            object? value,
            IDictionary<long, string>? enumValues = null,
            TypeInfo? typeInfo = null,
            StringConverterMode mode = StringConverterMode.Simple,
            NodeIdContext? context = null)
        {


            if (mode == StringConverterMode.ReversibleJson && uaClient != null && value is Variant variant)
            {
                using var encoder = new JsonEncoder(uaClient.MessageContext, true, null, false);
                encoder.WriteVariant(null, variant);
                var result = encoder.CloseAndReturnText();

                return result[1..^1];
            }

            var jsonMode = mode == StringConverterMode.ReversibleJson ? StringConverterMode.ReversibleJson : StringConverterMode.Json;
            if (value == null)
            {
                return mode != StringConverterMode.Simple ? "null" : "";
            }
            if (value is string strValue)
            {
                return mode != StringConverterMode.Simple ? JsonSerializer.Serialize(strValue) : strValue;
            }
            // If this is true, the value should be converted using the built-in JsonEncoder.
            if (ShouldUseJson(value, mode) && uaClient != null && value is Variant variant2)
            {
                try
                {
                    using var encoder = new JsonEncoder(uaClient.MessageContext, mode == StringConverterMode.ReversibleJson, null, false);
                    encoder.WriteVariant(null, variant2);
                    var result = encoder.CloseAndReturnText();

                    return result[1..^1];
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to serialize built in type: {Message}", ex.Message);
                }
            }

            if (value is Variant variantValue)
            {
                // This helps reduce code duplication, by making it possible to call ConvertToString with both variants and non-variants.
                return ConvertToString(variantValue.Value, enumValues, variantValue.TypeInfo, mode);
            }

            // If the type is enumerable we can write it to a JSON array.
            if (value is IEnumerable enumerableVal && value is not System.Xml.XmlElement)
            {
                var builder = new StringBuilder("[");
                int count = 0;
                foreach (var dvalue in enumerableVal)
                {
                    if (count++ > 0)
                    {
                        builder.Append(',');
                    }
                    builder.Append(ConvertToString(dvalue, enumValues, typeInfo, jsonMode));
                }
                builder.Append(']');
                return builder.ToString();
            }
            if (enumValues != null)
            {
                try
                {
                    var longVal = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                    if (enumValues.TryGetValue(longVal, out string enumVal))
                    {
                        if (mode != StringConverterMode.Simple)
                        {
                            return JsonSerializer.Serialize(enumVal);
                        }
                        else
                        {
                            return enumVal;
                        }
                    }
                }
                catch { }
            }
            string returnStr;

            if (value is NodeId nodeId)
            {
                if (context != null)
                {
                    returnStr = context.NodeIdToString(nodeId);
                }
                else
                {
                    returnStr = uaClient?.GetUniqueId(nodeId) ?? nodeId.ToString();
                }
            }
            else if (value is DataValue dv) return ConvertToString(dv.WrappedValue, enumValues, null, mode);
            else if (value is ExpandedNodeId expandedNodeId)
            {
                if (context != null && uaClient != null)
                {
                    returnStr = context.NodeIdToString(uaClient.Context.ToNodeId(expandedNodeId));
                }
                else
                {
                    returnStr = uaClient?.GetUniqueId(expandedNodeId) ?? expandedNodeId.ToString();
                }
            }
            else if (value is LocalizedText localizedText) returnStr = localizedText.Text;
            else if (value is QualifiedName qualifiedName) returnStr = qualifiedName.Name;
            else if (value is Opc.Ua.Range range) returnStr = $"({range.Low}, {range.High})";
            else if (value is EUInformation euInfo) returnStr = $"{euInfo.DisplayName?.Text}: {euInfo.Description?.Text}";
            else if (value is EnumValueType enumType)
            {
                if (mode != StringConverterMode.Simple) return $"{{{JsonSerializer.Serialize(enumType.DisplayName?.Text ?? "null")}:{enumType.Value}}}";
                return $"{enumType.DisplayName?.Text}: {enumType.Value}";
            }
            else if (value.GetType().IsEnum) returnStr = Enum.GetName(value.GetType(), value);
            else if (value is Opc.Ua.KeyValuePair kvp)
            {
                if (mode != StringConverterMode.Simple) return $"{{{JsonSerializer.Serialize(kvp.Key?.Name ?? "null")}:{ConvertToString(kvp.Value, enumValues, typeInfo, mode)}}}";
                return $"{kvp.Key?.Name}: {ConvertToString(kvp.Value, enumValues, typeInfo, mode)}";
            }
            else if (typeInfo != null && typeInfo.BuiltInType == BuiltInType.StatusCode && value is uint uintVal)
            {
                if (mode == StringConverterMode.ReversibleJson)
                {
                    var builder = new StringBuilder();
                    builder.Append("{");
                    builder.Append(@"""value"":");
                    builder.Append(uintVal);
                    builder.Append("}");
                    return builder.ToString();
                }
                else
                {
                    returnStr = StatusCode.LookupSymbolicId(uintVal);
                }
            }
            else if (value is System.Xml.XmlElement xml) return Newtonsoft.Json.JsonConvert.SerializeXmlNode(xml);
            else if (value is Uuid uuid) returnStr = uuid.GuidString;
            else if (value is DiagnosticInfo diagInfo)
            {
                var builder = new StringBuilder();
                builder.Append('{');
                builder.Append(@"""LocalizedText"":");
                builder.Append(diagInfo.LocalizedText);
                builder.Append(@",""AdditionalInfo"":");
                builder.Append(diagInfo.AdditionalInfo == null ? "null" : JsonSerializer.Serialize(diagInfo.AdditionalInfo));
                builder.Append(@",""InnerStatusCode"":");
                builder.Append(ConvertToString(diagInfo.InnerStatusCode, null, null, jsonMode));
                builder.Append(@",""InnerDiagnosticInfo"":");
                builder.Append(ConvertToString(diagInfo.InnerDiagnosticInfo, null, null, jsonMode));
                builder.Append('}');
                return builder.ToString();
            }
            else if (value is ExtensionObject extensionObject)
            {
                var body = extensionObject.Body;
                if (body == null)
                {
                    return mode != StringConverterMode.Simple ? "null" : "";
                }
                if (typeof(IEnumerable).IsAssignableFrom(body.GetType())
                    || customHandledTypes.Contains(body.GetType())
                    || typeInfo == null)
                {
                    return ConvertToString(extensionObject.Body, enumValues, null, mode);
                }
                returnStr = value.ToString();
            }
            else if (IsNumber(value)) return value.ToString();
            else returnStr = value.ToString();

            switch (mode)
            {
                case StringConverterMode.Simple:
                    return returnStr;
                case StringConverterMode.Json:
                    return JsonSerializer.Serialize(returnStr);
                case StringConverterMode.ReversibleJson:
                    if (returnStr.StartsWith("{"))
                    {
                        return JsonSerializer.Serialize(returnStr);
                    }
                    else
                    {
                        return JsonSerializer.Serialize(new StringWrapper { Value = returnStr });
                    }
                default:
                    // Should be impossible, but C# doesn't seem to realize.
                    return returnStr;
            }
        }

        /// <summary>
        /// Returns true if <paramref name="value"/> is a basic numeric type.
        /// </summary>
        /// <param name="value">Value to check</param>
        /// <returns>True if <paramref name="value"/> is a numeric type</returns>
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

        private static readonly HashSet<Type> customHandledTypes = new HashSet<Type>
        {
            typeof(NodeId), typeof(DataValue), typeof(ExpandedNodeId), typeof(LocalizedText),
            typeof(QualifiedName), typeof(Opc.Ua.Range), typeof(Opc.Ua.KeyValuePair), typeof(System.Xml.XmlElement),
            typeof(EUInformation), typeof(EnumValueType), typeof(DiagnosticInfo), typeof(Variant), typeof(Uuid), typeof(StatusCode)
        };

        /// <summary>
        /// Returns true if <paramref name="value"/> requires us to use the OPC-UA SDKs JsonEncoder.
        /// </summary>
        /// <param name="value">Value to check</param>
        /// <returns>True if this type is best handled by Opc.Ua.JsonEncoder</returns>
        private static bool ShouldUseJson(object value, StringConverterMode mode)
        {
            if (value == null) return false;
            if (value is Variant variantValue)
            {
                value = variantValue.Value;
                if (value == null) return false;
            }
            // Go through the value to check if we can parse it ourselves.
            // i.e. this is either an enumerable of a handled type, or an extensionobject
            // around a handled type.
            // If not, use the converter.
            var type = value.GetType();
            if (value is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                if (enumerator.MoveNext())
                {
                    return ShouldUseJson(enumerator.Current, mode);
                }
                return false;
            }
            if (value is ExtensionObject extensionObject)
            {
                return ShouldUseJson(extensionObject.Body, mode);
            }
            if (!type.Namespace.StartsWith("Opc.Ua", StringComparison.InvariantCulture)) return false;
            if (mode != StringConverterMode.ReversibleJson && customHandledTypes.Contains(type)) return false;
            if (type.IsEnum) return false;
            return true;
        }

        private readonly ConcurrentDictionary<ConverterType, NodeSerializer> converters = new ConcurrentDictionary<ConverterType, NodeSerializer>();
        private readonly NodeIdConverter? nodeIdConverter;
        public void AddConverters(JsonSerializerOptions options, ConverterType type)
        {
            if (config == null || uaClient == null || nodeIdConverter == null)
                throw new InvalidOperationException("Config and UAClient must be supplied to create converters");
            options.Converters.Add(converters.GetOrAdd(type, key => new NodeSerializer(this, config, uaClient.Context, key, log)));
            options.Converters.Add(nodeIdConverter);
        }
    }

    public enum ConverterType
    {
        Node,
        Variable
    }

    internal class NodeSerializer : JsonConverter<BaseUANode>
    {
        private readonly StringConverter converter;
        private readonly FullConfig config;
        private readonly SessionContext context;
        public ConverterType ConverterType { get; }
        private readonly ILogger log;
        public NodeSerializer(StringConverter converter, FullConfig config, SessionContext context, ConverterType type, ILogger log)
        {
            this.converter = converter;
            this.config = config;
            this.context = context;
            ConverterType = type;
            this.log = log;
        }

        public override BaseUANode? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }

        private void WriteValueSafe(Utf8JsonWriter writer, UAVariable variable)
        {
            var value = converter.ConvertToString(variable.Value, variable.FullAttributes.DataType?.EnumValues, null, StringConverterMode.Json);
            try
            {
                writer.WriteRawValue(value);
            }
            catch (Exception ex)
            {
                log.LogWarning("Serialization of value on variable: {Variable} resulted in invalid JSON: {Message}, {Json}",
                    variable.Name, ex.Message, value);
                writer.WriteNullValue();
            }
        }

        private void WriteProperties(Utf8JsonWriter writer, BaseUANode node, bool getExtras, bool writeValue)
        {
            Dictionary<string, string>? extras = null;

            if (getExtras)
            {
                extras = node.GetExtraMetadata(config, context, converter);
                if (extras != null) extras.Remove("Value");
            }
            // If we should treat this as a key/value pair, or write it as an object
            if (extras != null && extras.Count != 0 || node.Properties != null && node.Properties.Any())
            {
                writer.WriteStartObject();
            }
            else if (node is UAVariable variable && writeValue)
            {
                WriteValueSafe(writer, variable);
                return;
            }
            else
            {
                writer.WriteNullValue();
                return;
            }

            // Keep fields from being duplicated, resulting in illegal JSON.
            var fields = new HashSet<string>();
            if (node is UAVariable variable2 && writeValue)
            {
                writer.WritePropertyName("Value");
                WriteValueSafe(writer, variable2);
                fields.Add("Value");
            }
            if (extras != null)
            {
                foreach (var kvp in extras)
                {
                    writer.WriteString(kvp.Key, kvp.Value);
                    fields.Add(kvp.Key);
                }
            }
            if (node.Properties != null)
            {
                foreach (var child in node.Properties)
                {
                    var name = child.Name;
                    if (name == null) continue;
                    string safeName = name;
                    int idx = 0;
                    while (!fields.Add(safeName))
                    {
                        safeName = $"{name}{idx++}";
                    }

                    writer.WritePropertyName(safeName);
                    WriteProperties(writer, child, false, true);
                }
            }
            writer.WriteEndObject();
        }

        private void WriteBaseValues(Utf8JsonWriter writer, BaseUANode node)
        {
            var id = context.GetUniqueId(node.Id);
            writer.WriteString("externalId", id);
            writer.WriteString("name", string.IsNullOrEmpty(node.Name) ? id : node.Name);
            writer.WriteString("description", node.Attributes.Description);
            writer.WritePropertyName("metadata");
            WriteProperties(writer, node, true, node.NodeClass == NodeClass.VariableType);
            if (ConverterType == ConverterType.Variable && node is UAVariable variable)
            {
                writer.WriteString("assetExternalId", context.GetUniqueId(node.ParentId));
                writer.WriteBoolean("isString", variable.FullAttributes.DataType?.IsString ?? false);
                writer.WriteBoolean("isStep", variable.FullAttributes.DataType?.IsStep ?? false);
            }
            else
            {
                writer.WriteString("parentExternalId", context.GetUniqueId(node.ParentId));
            }
        }
        private void WriteNodeIds(Utf8JsonWriter writer, BaseUANode node, JsonSerializerOptions options)
        {
            writer.WritePropertyName("NodeId");
            JsonSerializer.Serialize(writer, node.Id, options);

            if (!node.ParentId.IsNullNodeId)
            {
                writer.WritePropertyName("ParentNodeId");
                JsonSerializer.Serialize(writer, node.ParentId, options);
            }
            if (node is UAObject obj)
            {
                if (obj.FullAttributes.TypeDefinition != null && !obj.FullAttributes.TypeDefinition.Id.IsNullNodeId)
                {
                    writer.WritePropertyName("TypeDefinitionId");
                    JsonSerializer.Serialize(writer, obj.FullAttributes.TypeDefinition.Id, options);
                }
            }

            if (node is UAVariable variable)
            {
                if (variable.FullAttributes.TypeDefinition != null && !variable.FullAttributes.TypeDefinition.Id.IsNullNodeId)
                {
                    writer.WritePropertyName("TypeDefinitionId");
                    JsonSerializer.Serialize(writer, variable.FullAttributes.TypeDefinition.Id, options);
                }
                if (ConverterType == ConverterType.Variable)
                {
                    if (variable.FullAttributes.DataType != null && !variable.FullAttributes.DataType.Id.IsNullNodeId)
                    {
                        writer.WritePropertyName("DataTypeId");
                        JsonSerializer.Serialize(writer, variable.FullAttributes.DataType.Id, options);
                    }
                }
            }
        }

        private void WriteInternalInfo(Utf8JsonWriter writer, BaseUANode node, JsonSerializerOptions options)
        {
            writer.WriteStartObject("InternalInfo");

            if (node is UAObject obj)
            {
                writer.WriteNumber("EventNotifier", obj.FullAttributes.EventNotifier);
                writer.WriteBoolean("ShouldSubscribeEvents", obj.FullAttributes.ShouldSubscribeToEvents(config));
                writer.WriteBoolean("ShouldReadHistoryEvents", obj.FullAttributes.ShouldReadEventHistory(config));
            }
            writer.WriteNumber("NodeClass", (int)node.NodeClass);
            if (ConverterType == ConverterType.Variable && node is UAVariable variable)
            {
                writer.WriteNumber("AccessLevel", variable.FullAttributes.AccessLevel);
                writer.WriteBoolean("Historizing", variable.FullAttributes.Historizing);
                writer.WriteNumber("ValueRank", variable.ValueRank);
                writer.WriteBoolean("ShouldSubscribeData", variable.FullAttributes.ShouldSubscribe(config));
                writer.WriteBoolean("ShouldReadHistoryData", variable.FullAttributes.ShouldReadHistory(config));
                if (variable.ArrayDimensions != null)
                {
                    writer.WritePropertyName("ArrayDimensions");
                    JsonSerializer.Serialize(writer, variable.ArrayDimensions, options);
                    writer.WriteNumber("Index", (variable as UAVariableMember)?.Index ?? -1);
                }
                if (variable.AsEvents)
                {
                    writer.WriteBoolean("AsEvents", variable.AsEvents);
                }
            }
            writer.WriteEndObject();
        }


        public override void Write(Utf8JsonWriter writer, BaseUANode value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }
            writer.WriteStartObject();
            WriteBaseValues(writer, value);
            if (config.Extraction.DataTypes.ExpandNodeIds)
            {
                WriteNodeIds(writer, value, options);
            }
            if (config.Extraction.DataTypes.AppendInternalValues)
            {
                WriteInternalInfo(writer, value, options);
            }
            writer.WriteEndObject();
        }
    }

    internal class NodeIdConverter : JsonConverter<NodeId>
    {
        private readonly UAClient uaClient;
        public NodeIdConverter(UAClient uaClient)
        {
            this.uaClient = uaClient;
        }

        public override NodeId? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (uaClient.NamespaceTable == null) throw new InvalidOperationException("Attempted to deserialize NodeId without initialized client");

            if (reader.TokenType != JsonTokenType.StartObject) return NodeId.Null;
            var obj = JsonElement.ParseValue(ref reader);
            if (obj.ValueKind != JsonValueKind.Object) return NodeId.Null;
            string? ns = null;
            if (obj.TryGetProperty("namespace", out var nsElem)) ns = nsElem.GetString();
            int? idType = null;
            if (obj.TryGetProperty("idType", out var idtElem)) idType = idtElem.GetInt32();

            if (idType == null || idType.Value > 3 || idType.Value < 0) return NodeId.Null;

            int nsIdx = 0;
            if (ns != null)
            {
                nsIdx = uaClient.NamespaceTable.GetIndex(ns);
                if (nsIdx < 0) return NodeId.Null;
            }

            try
            {
                switch ((IdType)idType.Value)
                {
                    case IdType.Numeric:
                        uint numIdf = obj.GetProperty("identifier").GetUInt32();
                        return new NodeId(numIdf, (ushort)nsIdx);
                    case IdType.String:
                        string? strIdf = obj.GetProperty("identifier").GetString();
                        if (strIdf == null) return NodeId.Null;
                        return new NodeId(strIdf, (ushort)nsIdx);
                    case IdType.Guid:
                        strIdf = obj.GetProperty("identifier").GetString();
                        if (strIdf == null) return NodeId.Null;
                        Guid guid = Guid.Parse(strIdf);
                        return new NodeId(guid, (ushort)nsIdx);
                    case IdType.Opaque:
                        var opqIdf = obj.GetProperty("identifier").GetBytesFromBase64();
                        return new NodeId(opqIdf, (ushort)nsIdx);
                }
            }
            catch { }
            return NodeId.Null;
        }

        public override void Write(Utf8JsonWriter writer, NodeId value, JsonSerializerOptions options)
        {
            if (uaClient.NamespaceTable == null) throw new InvalidOperationException("Attempted to serialize NodeId without initialized client");
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }
            writer.WriteStartObject();
            if (value.NamespaceIndex != 0)
            {
                var ns = uaClient.NamespaceTable.GetString(value.NamespaceIndex);
                writer.WriteString("namespace", ns);
            }

            writer.WriteNumber("idType", (int)value.IdType);
            switch (value.IdType)
            {
                case IdType.Numeric:
                    writer.WriteNumber("identifier", value.Identifier is uint iVal ? iVal : 0);
                    break;
                case IdType.Guid:
                    writer.WriteString("identifier", value.Identifier is Guid gVal ? gVal : Guid.Empty);
                    break;
                case IdType.Opaque:
                    writer.WriteBase64String("identifier", value.Identifier as byte[]);
                    break;
                case IdType.String:
                    writer.WriteString("identifier", value.Identifier as string);
                    break;
            }
            writer.WriteEndObject();
        }
    }
}

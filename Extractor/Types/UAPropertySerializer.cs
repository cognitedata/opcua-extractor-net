using Newtonsoft.Json;
using Opc.Ua;
using Serilog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Class used for converting properties and property values to strings.
    /// Handles both conversion to metadata for Clean, and conversion to JSON for Raw.
    /// </summary>
    public class StringConverter : JsonConverter<UANode>
    {
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        private readonly ILogger log = Log.Logger.ForContext(typeof(UAClient));

        public StringConverter(UAClient uaClient, FullConfig config)
        {
            this.uaClient = uaClient;
            this.config = config;
        }

        /// <summary>
        /// Convert a value from OPC-UA to a string, which may optionally be JSON.
        /// Gives better results if <paramref name="value"/> is Variant.
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="enumValues">Map from numeric to string values</param>
        /// <param name="typeInfo">TypeInfo for <paramref name="value"/></param>
        /// <param name="json">True to return valid JSON.</param>
        /// <returns></returns>
        public string ConvertToString(object value, IDictionary<long, string> enumValues = null, TypeInfo typeInfo = null, bool json = false)
        {
            if (value == null)
            {
                return json ? "null" : "";
            }
            if (value is Variant variantValue)
            {
                // This helps reduce code duplication, by making it possible to call ConvertToString with both variants and non-variants.
                return ConvertToString(variantValue.Value, enumValues, variantValue.TypeInfo, json);
            }
            if (value is string strValue)
            {
                return json ? JsonConvert.ToString(strValue) : strValue;
            }
            // If this is true, the value should be converted using the built-in JsonEncoder.
            if (typeInfo != null && ShouldUseJson(value) && uaClient != null)
            {
                try
                {
                    bool topLevelIsArray = typeInfo.ValueRank >= ValueRanks.OneDimension;

                    using var encoder = new JsonEncoder(uaClient.MessageContext, false, null, topLevelIsArray);
                    encoder.WriteVariantContents(value, typeInfo);
                    var result = encoder.CloseAndReturnText();

                    // JsonEncoder for some reason spits out {{ ... }} from WriteVariantContents.
                    if (topLevelIsArray)
                    {
                        return result[1..^1];
                    }

                    return result;
                }
                catch (Exception ex)
                {
                    log.Warning("Failed to serialize built in type: {err}", ex.Message);
                }
            }
            // If the type is enumerable we can write it to a JSON array.
            if (typeof(IEnumerable).IsAssignableFrom(value.GetType()) && !(value is System.Xml.XmlElement))
            {
                var builder = new StringBuilder("[");
                int count = 0;
                foreach (var dvalue in value as IEnumerable)
                {
                    if (count++ > 0)
                    {
                        builder.Append(',');
                    }
                    builder.Append(ConvertToString(dvalue, enumValues, typeInfo, true));
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
                        if (json)
                        {
                            return JsonConvert.ToString(enumVal);
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

            if (value is NodeId nodeId) returnStr = uaClient?.GetUniqueId(nodeId) ?? nodeId.ToString();
            else if (value is DataValue dv) return ConvertToString(dv.WrappedValue, enumValues, null, json);
            else if (value is ExpandedNodeId expandedNodeId) returnStr = uaClient?.GetUniqueId(expandedNodeId) ?? expandedNodeId.ToString();
            else if (value is LocalizedText localizedText) returnStr = localizedText.Text;
            else if (value is QualifiedName qualifiedName) returnStr = qualifiedName.Name;
            else if (value is Opc.Ua.Range range) returnStr = $"({range.Low}, {range.High})";
            else if (value is EUInformation euInfo) returnStr = $"{euInfo.DisplayName?.Text}: {euInfo.Description?.Text}";
            else if (value is EnumValueType enumType)
            {
                if (json) return $"{{{JsonConvert.ToString(enumType.DisplayName?.Text ?? "null")}:{enumType.Value}}}";
                return $"{enumType.DisplayName?.Text}: {enumType.Value}";
            }
            else if (value is Opc.Ua.KeyValuePair kvp)
            {
                if (json) return $"{{{JsonConvert.ToString(kvp.Key?.Name ?? "null")}:{ConvertToString(kvp.Value, enumValues, typeInfo, json)}}}";
                return $"{kvp.Key?.Name}: {ConvertToString(kvp.Value, enumValues, typeInfo, json)}";
            }
            else if (value is System.Xml.XmlElement xml) return JsonConvert.SerializeXmlNode(xml);
            else if (value is ExtensionObject extensionObject)
            {
                var body = extensionObject.Body;
                if (typeof(IEnumerable).IsAssignableFrom(body.GetType())
                    || customHandledTypes.Contains(body.GetType())
                    || typeInfo == null)
                {
                    return ConvertToString(extensionObject.Body, enumValues, null, json);
                }
                returnStr = value.ToString();
            }
            else if (IsNumber(value)) return value.ToString();
            else returnStr = value.ToString();

            return json ? JsonConvert.ToString(returnStr) : returnStr;
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
            typeof(EUInformation), typeof(EnumValueType), typeof(Variant)
        };

        /// <summary>
        /// Returns true if <paramref name="value"/> requires us to use the OPC-UA SDKs JsonEncoder.
        /// </summary>
        /// <param name="value">Value to check</param>
        /// <returns>True if this type is best handled by Opc.Ua.JsonEncoder</returns>
        private static bool ShouldUseJson(object value)
        {
            // Go through the value to check if we can parse it ourselves.
            // i.e. this is either an enumerable of a handled type, or an extensionobject
            // around a handled type.
            // If not, use the converter.
            var type = value.GetType();
            if (typeof(IEnumerable).IsAssignableFrom(type))
            {
                var enumerable = value as IEnumerable;
                var enumerator = enumerable.GetEnumerator();
                if (enumerator.MoveNext())
                {
                    return ShouldUseJson(enumerator.Current);
                }
                return false;
            }
            if (value is ExtensionObject extensionObject)
            {
                return ShouldUseJson(extensionObject.Body);
            }
            if (!type.Namespace.StartsWith("Opc.Ua", StringComparison.InvariantCulture)) return false;
            if (customHandledTypes.Contains(type)) return false;
            return true;
        }


        private void WriteProperties(JsonWriter writer, UANode node, bool getExtras, bool writeValue)
        {
            Dictionary<string, string> extras = null;

            if (getExtras)
            {
                extras = node.GetExtraMetadata(config.Extraction, uaClient.DataTypeManager, uaClient.StringConverter);
                if (extras != null) extras.Remove("Value");
            }
            // If we should treat this as a key/value pair, or write it as an object
            if (extras != null && extras.Any() || node.Properties != null && node.Properties.Any())
            {
                writer.WriteStartObject();
            }
            else if (node is UAVariable variable && writeValue)
            {
                writer.WriteRawValue(ConvertToString(variable.Value, variable.DataType?.EnumValues, null, true));
                return;
            }
            else
            {
                writer.WriteNull();
                return;
            }

            // Keep fields from being duplicated, resulting in illegal JSON.
            var fields = new HashSet<string>();
            if (node is UAVariable variable2 && writeValue)
            {
                writer.WritePropertyName("Value");
                writer.WriteRawValue(ConvertToString(variable2.Value, null, null, true));
                fields.Add("Value");
            }
            if (extras != null)
            {
                foreach (var kvp in extras)
                {
                    writer.WritePropertyName(kvp.Key);
                    writer.WriteValue(kvp.Value);
                    fields.Add(kvp.Key);
                }
            }
            if (node.Properties != null)
            {
                foreach (var child in node.Properties)
                {
                    var name = child.DisplayName;
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

        public override void WriteJson(JsonWriter writer, UANode value, Newtonsoft.Json.JsonSerializer serializer)
        {
            if (writer == null) throw new ArgumentNullException(nameof(writer));
            if (value == null)
            {
                writer.WriteNull();
                return;
            }
            var id = uaClient.GetUniqueId(value.Id);
            writer.WriteStartObject();
            writer.WritePropertyName("externalId");
            writer.WriteValue(id);
            writer.WritePropertyName("name");
            writer.WriteValue(string.IsNullOrEmpty(value.DisplayName) ? id : value.DisplayName);
            writer.WritePropertyName("description");
            writer.WriteValue(value.Description);
            writer.WritePropertyName("metadata");
            WriteProperties(writer, value, true, value.NodeClass == NodeClass.VariableType);
            if ((value.NodeClass == NodeClass.Variable) && value is UAVariable variable && (!variable.IsArray || variable.Index >= 0))
            {
                writer.WritePropertyName("assetExternalId");
                writer.WriteValue(uaClient.GetUniqueId(value.ParentId));
                writer.WritePropertyName("isString");
                writer.WriteValue(variable.DataType?.IsString ?? false);
                writer.WritePropertyName("isStep");
                writer.WriteValue(variable.DataType?.IsStep ?? false);
            }
            else
            {
                writer.WritePropertyName("parentExternalId");
                writer.WriteValue(uaClient.GetUniqueId(value.ParentId));
            }
            writer.WriteEndObject();
        }

        public override UANode ReadJson(JsonReader reader, Type objectType, UANode existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}

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
    public class StringConverter
    {
        private readonly UAClient uaClient;
        private readonly ILogger log = Log.Logger.ForContext(typeof(UAClient));

        public StringConverter(UAClient uaClient)
        {
            this.uaClient = uaClient;
        }
        /// <summary>
        /// Recursively converts the value and children of a node to JSON.
        /// If the node has no properties, simply returns a string representation of its value (which may be complex),
        /// if not, a JSON object is returned with a field "Value".
        /// </summary>
        /// <param name="builder">String builder to append to</param>
        /// <param name="node">Property to convert</param>
        private void PropertyToJson(StringBuilder builder, UANode node)
        {
            if (node.Properties == null || !node.Properties.Any())
            {
                if (node is UAVariable variable)
                {
                    builder.Append(ConvertToString(variable.Value, variable.DataType.EnumValues, null, true));
                }
                else
                {
                    builder.Append("{}");
                }
                return;
            }
            bool separator = false;
            var fields = new HashSet<string>();
            builder.Append('{');
            if (node is UAVariable variable2)
            {
                builder.Append(@"""Value"":");
                builder.Append(ConvertToString(variable2.Value, variable2.DataType.EnumValues, null, true));
                separator = true;
                fields.Add("Value");
            }

            foreach (var prop in node.Properties)
            {
                if (separator) builder.Append(',');
                var name = prop.DisplayName;
                string safeName = JsonConvert.ToString(name);
                int idx = 0;
                while (!fields.Add(safeName))
                {
                    safeName = JsonConvert.ToString($"{name}{idx++}");
                }
                builder.AppendFormat(@"{0}:", safeName);
                PropertyToJson(builder, prop);
                separator = true;
            }
            builder.Append('}');
            
        }

        /// <summary>
        /// Convert the full metadata of a node to JSON. Can take an optional list of extra fields.
        /// </summary>
        /// <param name="extraFields">Extra fields to add</param>
        /// <param name="properties">List of properties in result</param>
        /// <returns>A JSONDocument with the full serialized metadata</returns>
        public JsonDocument MetadataToJson(Dictionary<string, string> extraFields, IEnumerable<UANode> properties)
        {
            var builder = new StringBuilder("{");
            bool separator = false;
            var fields = new HashSet<string>();
            if (extraFields != null)
            {
                foreach (var field in extraFields)
                {
                    if (separator)
                    {
                        builder.Append(',');
                    }
                    // Using JsonConvert to escape values.
                    var name = JsonConvert.ToString(field.Key);
                    fields.Add(name);
                    builder.Append(name);
                    builder.Append(':');
                    builder.Append(JsonConvert.ToString(field.Value));
                    separator = true;
                }
            }

            if (properties != null)
            {
                foreach (var prop in properties)
                {
                    var name = prop.DisplayName;
                    if (name == null) continue;
                    if (separator)
                    {
                        builder.Append(',');
                    }
                    // Ensure that the field does not already exist.
                    string safeName = JsonConvert.ToString(name);
                    int idx = 0;
                    while (!fields.Add(safeName))
                    {
                        safeName = JsonConvert.ToString($"{name}{idx++}");
                    }
                    builder.AppendFormat("{0}:", safeName);
                    PropertyToJson(builder, prop);
                    separator = true;
                }
            }
            
            builder.Append('}');
            return JsonDocument.Parse(builder.ToString());
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
    }
}

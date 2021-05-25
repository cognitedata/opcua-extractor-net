using Newtonsoft.Json;
using Opc.Ua;
using Serilog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Types
{
    public class StringConverter
    {
        private readonly UAClient uaClient;
        private readonly ILogger log = Log.Logger.ForContext(typeof(UAClient));

        public StringConverter(UAClient uaClient)
        {
            this.uaClient = uaClient;
        }

        private void PropertyToJson(StringBuilder builder, UANode node, bool json)
        {
            if (node is UAVariable variable && !variable.Properties.Any())
            {
                builder.Append(ConvertToString(variable.Value, variable.DataType.EnumValues, null, json));
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
                string safeName = name;
                int idx = 0;
                while (!fields.Add(safeName))
                {
                    safeName = $"{name}{idx++}";
                }
                builder.AppendFormat(@"""{key}"":", safeName);
                PropertyToJson(builder, prop, true);
                separator = true;
            }
            builder.Append('}');
            
        }

        public Dictionary<string, string> MetadataToJson(IEnumerable<UANode> properties)
        {
            var result = new Dictionary<string, string>();
            foreach (var node in properties)
            {
                var builder = new StringBuilder();
                PropertyToJson(builder, node, false);
                result[node.DisplayName] = builder.ToString();
            }
            return result;
        }


        public string ConvertToString(object value, IDictionary<long, string> enumValues = null, TypeInfo typeInfo = null, bool json = false)
        {
            if (value == null) return "";
            if (value is Variant variantValue)
            {
                return ConvertToString(variantValue.Value, enumValues, variantValue.TypeInfo, json);
            }
            if (value is string strValue)
            {
                if (json)
                {
                    return JsonConvert.ToString(strValue);
                }
                else
                {
                    return strValue;
                }
            }
            if (typeInfo != null && ShouldUseJson(value) && uaClient != null)
            {
                try
                {
                    bool topLevelIsArray = typeInfo.ValueRank >= ValueRanks.OneDimension;

                    var encoder = new JsonEncoder(uaClient.MessageContext, false, null, topLevelIsArray);
                    encoder.WriteVariantContents(value, typeInfo);
                    var result = encoder.CloseAndReturnText();
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
            if (typeof(IEnumerable).IsAssignableFrom(value.GetType()))
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
            else if (value is EnumValueType enumType) returnStr = $"{enumType.DisplayName?.Text}: {enumType.Value}";
            else if (value is Opc.Ua.KeyValuePair kvp) returnStr = $"{kvp.Key?.Name}: {ConvertToString(kvp.Value, enumValues, typeInfo, json)}";
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

            if (json)
            {
                return JsonConvert.ToString(returnStr);
            }
            else
            {
                return returnStr;
            }
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

        private static readonly HashSet<Type> customHandledTypes = new HashSet<Type>
        {
            typeof(NodeId), typeof(DataValue), typeof(ExpandedNodeId), typeof(LocalizedText),
            typeof(QualifiedName), typeof(Opc.Ua.Range), typeof(Opc.Ua.KeyValuePair), typeof(System.Xml.XmlElement),
            typeof(EUInformation), typeof(EnumValueType), typeof(Variant)
        };

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
            if (!type.Namespace.StartsWith("Opc.Ua")) return false;
            if (customHandledTypes.Contains(type)) return false;
            return true;
        }
    }
}

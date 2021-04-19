using Newtonsoft.Json;
using Opc.Ua;
using Serilog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using JsonConverter = System.Text.Json.Serialization.JsonConverter;

namespace Cognite.OpcUa.Types
{
    public static class StringConversionUtils
    {
        
        
    }

    public class StringConverter
    {
        private UAClient uaClient;
        private bool useJson;
        private ILogger log = Log.Logger.ForContext(typeof(UAClient));

        public StringConverter(UAClient uaClient, bool useJson)
        {
            this.uaClient = uaClient;
            this.useJson = useJson;
        }

        public string ConvertToString(object value, IDictionary<long, string> enumValues = null, TypeInfo typeInfo = null)
        {
            if (value == null) return "";
            if (value is Variant variantValue)
            {
                return ConvertToString(variantValue.Value, enumValues, variantValue.TypeInfo);
            }
            if (value is string strValue)
            {
                return strValue;
            }
            if (typeInfo != null && ShouldUseJson(value))
            {
                Console.WriteLine("Trying to use json on type " + typeInfo + ", " + typeInfo.ValueRank);
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
                    builder.Append(((count++ > 0) ? ", " : "") + ConvertToString(dvalue, enumValues));
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
                        return enumVal;
                    }
                }
                catch { }
            }

            if (value is NodeId nodeId) return uaClient.GetUniqueId(nodeId);
            if (value is DataValue dv) return ConvertToString(dv.WrappedValue, enumValues);
            if (value is ExpandedNodeId expandedNodeId) return uaClient.GetUniqueId(expandedNodeId);
            if (value is LocalizedText localizedText) return localizedText.Text;
            if (value is QualifiedName qualifiedName) return qualifiedName.Name;
            if (value is Opc.Ua.Range range) return $"({range.Low}, {range.High})";
            if (value is EUInformation euInfo) return $"{euInfo.DisplayName?.Text}: {euInfo.Description?.Text}";
            if (value is EnumValueType enumType) return $"{enumType.DisplayName?.Text}: {enumType.Value}";
            if (value is Opc.Ua.KeyValuePair kvp) return $"{kvp.Key?.Name}: {ConvertToString(kvp.Value, enumValues)}";
            if (value is System.Xml.XmlElement xml) return JsonConvert.SerializeXmlNode(xml);
            if (value is ExtensionObject extensionObject)
            {
                var body = extensionObject.Body;
                if (typeof(IEnumerable).IsAssignableFrom(body.GetType())
                    || customHandledTypes.Contains(body.GetType())
                    || typeInfo == null)
                {
                    return ConvertToString(extensionObject.Body, enumValues);
                }
            }

            return value.ToString();
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

    class UATypeSerializerFactory : JsonConverterFactory
    {
        public override bool CanConvert(Type typeToConvert)
        {
            throw new NotImplementedException();
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }
    }

    class WrappedMetaValue
    {
        public object value 
    }



    class UATypeSerializer<T> : System.Text.Json.Serialization.JsonConverter<T> where T : class
    {
        private UAClient uaClient;
        public UATypeSerializer(UAClient client)
        {
            uaClient = client;
        }
        public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }

        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            if (value is NodeId nodeId) writer.WriteStringValue(uaClient.GetUniqueId(nodeId));
            if (value is DataValue dv) return ConvertToString(dv.WrappedValue, enumValues);
            if (value is ExpandedNodeId expandedNodeId) return uaClient.GetUniqueId(expandedNodeId);
            if (value is LocalizedText localizedText) return localizedText.Text;
            if (value is QualifiedName qualifiedName) return qualifiedName.Name;
            if (value is Opc.Ua.Range range) return $"({range.Low}, {range.High})";
            if (value is EUInformation euInfo) return $"{euInfo.DisplayName?.Text}: {euInfo.Description?.Text}";
            if (value is EnumValueType enumType) return $"{enumType.DisplayName?.Text}: {enumType.Value}";
            if (value is Opc.Ua.KeyValuePair kvp) return $"{kvp.Key?.Name}: {ConvertToString(kvp.Value, enumValues)}";
            if (value is System.Xml.XmlElement xml) return JsonConvert.SerializeXmlNode(xml);
            if (value is ExtensionObject extensionObject)
            {
                var body = extensionObject.Body;
                if (typeof(IEnumerable).IsAssignableFrom(body.GetType())
                    || customHandledTypes.Contains(body.GetType())
                    || typeInfo == null)
                {
                    return ConvertToString(extensionObject.Body, enumValues);
                }
            }
        }
    }


    class UAPropertySerializer : System.Text.Json.Serialization.JsonConverter<UANode>
    {
        public override UANode Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }

        public override void Write(Utf8JsonWriter writer, UANode value, JsonSerializerOptions options)
        {


            throw new NotImplementedException();
        }
    }
}

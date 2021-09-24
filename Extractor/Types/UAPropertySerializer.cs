﻿/* Cognite Extractor for OPC-UA
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

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Opc.Ua;
using Serilog;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Class used for converting properties and property values to strings.
    /// Handles both conversion to metadata for Clean, and conversion to JSON for Raw.
    /// </summary>
    public class StringConverter
    {
        private readonly UAClient? uaClient;
        private readonly FullConfig? config;
        private readonly ILogger log = Log.Logger.ForContext(typeof(UAClient));

        public StringConverter(UAClient? uaClient, FullConfig? config)
        {
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
        /// <param name="json">True to return valid JSON.</param>
        /// <returns></returns>
        public string ConvertToString(
            object? value,
            IDictionary<long, string>? enumValues = null,
            TypeInfo? typeInfo = null, bool json = false)
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
            if (value is IEnumerable enumerableVal && !(value is System.Xml.XmlElement))
            {
                var builder = new StringBuilder("[");
                int count = 0;
                foreach (var dvalue in enumerableVal)
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
            else if (value.GetType().IsEnum) returnStr = Enum.GetName(value.GetType(), value);
            else if (value is Opc.Ua.KeyValuePair kvp)
            {
                if (json) return $"{{{JsonConvert.ToString(kvp.Key?.Name ?? "null")}:{ConvertToString(kvp.Value, enumValues, typeInfo, json)}}}";
                return $"{kvp.Key?.Name}: {ConvertToString(kvp.Value, enumValues, typeInfo, json)}";
            }
            else if (typeInfo != null && typeInfo.BuiltInType == BuiltInType.StatusCode && value is uint uintVal)
            {
                returnStr = StatusCode.LookupSymbolicId(uintVal);
            }
            else if (value is System.Xml.XmlElement xml) return JsonConvert.SerializeXmlNode(xml);
            else if (value is Uuid uuid) returnStr = uuid.GuidString;
            else if (value is DiagnosticInfo diagInfo)
            {
                var builder = new StringBuilder();
                builder.Append('{');
                builder.Append(@"""LocalizedText"":");
                builder.Append(diagInfo.LocalizedText);
                builder.Append(@",""AdditionalInfo"":");
                builder.Append(diagInfo.AdditionalInfo == null ? "null" : JsonConvert.ToString(diagInfo.AdditionalInfo));
                builder.Append(@",""InnerStatusCode"":");
                builder.Append(ConvertToString(diagInfo.InnerStatusCode, null, null, true));
                builder.Append(@",""InnerDiagnosticInfo"":");
                builder.Append(ConvertToString(diagInfo.InnerDiagnosticInfo, null, null, true));
                builder.Append('}');
                return builder.ToString();
            }
            else if (value is ExtensionObject extensionObject)
            {
                var body = extensionObject.Body;
                if (body == null)
                {
                    return json ? "null" : "";
                }
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
            typeof(EUInformation), typeof(EnumValueType), typeof(Variant), typeof(Uuid), typeof(DiagnosticInfo)
        };

        /// <summary>
        /// Returns true if <paramref name="value"/> requires us to use the OPC-UA SDKs JsonEncoder.
        /// </summary>
        /// <param name="value">Value to check</param>
        /// <returns>True if this type is best handled by Opc.Ua.JsonEncoder</returns>
        private static bool ShouldUseJson(object value)
        {
            if (value == null) return false;
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
            if (type.IsEnum) return false;
            return true;
        }

        private readonly ConcurrentDictionary<ConverterType, NodeSerializer> converters = new ConcurrentDictionary<ConverterType, NodeSerializer>();
        private readonly NodeIdConverter? nodeIdConverter;
        public void AddConverters(Newtonsoft.Json.JsonSerializer serializer, ConverterType type)
        {
            if (config == null || uaClient == null || nodeIdConverter == null)
                throw new InvalidOperationException("Config and UAClient must be supplied to create converters");
            serializer.Converters.Add(converters.GetOrAdd(type, key => new NodeSerializer(this, config, uaClient, key)));
            serializer.Converters.Add(nodeIdConverter);
        }
    }

    public enum ConverterType
    {
        Node,
        Variable
    }

    class NodeSerializer : JsonConverter<UANode>
    {
        private readonly StringConverter converter;
        private readonly FullConfig config;
        private readonly UAClient uaClient;
        public ConverterType Type { get; }
        public NodeSerializer(StringConverter converter, FullConfig config, UAClient uaClient, ConverterType type)
        {
            this.converter = converter;
            this.config = config;
            this.uaClient = uaClient;
            Type = type;
        }

        private void WriteProperties(JsonWriter writer, UANode node, bool getExtras, bool writeValue)
        {
            Dictionary<string, string>? extras = null;

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
                writer.WriteRawValue(converter.ConvertToString(variable.Value, variable.DataType?.EnumValues, null, true));
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
                writer.WriteRawValue(converter.ConvertToString(variable2.Value, null, null, true));
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

        private void WriteBaseValues(JsonWriter writer, UANode node)
        {
            var id = uaClient.GetUniqueId(node.Id);
            writer.WritePropertyName("externalId");
            writer.WriteValue(id);
            writer.WritePropertyName("name");
            writer.WriteValue(string.IsNullOrEmpty(node.DisplayName) ? id : node.DisplayName);
            writer.WritePropertyName("description");
            writer.WriteValue(node.Description);
            writer.WritePropertyName("metadata");
            WriteProperties(writer, node, true, node.NodeClass == NodeClass.VariableType);
            if (Type == ConverterType.Variable && node is UAVariable variable)
            {
                writer.WritePropertyName("assetExternalId");
                writer.WriteValue(uaClient.GetUniqueId(node.ParentId));
                writer.WritePropertyName("isString");
                writer.WriteValue(variable.DataType?.IsString ?? false);
                writer.WritePropertyName("isStep");
                writer.WriteValue(variable.DataType?.IsStep ?? false);
            }
            else
            {
                writer.WritePropertyName("parentExternalId");
                writer.WriteValue(uaClient.GetUniqueId(node.ParentId));
            }
        }
        private void WriteNodeIds(JsonWriter writer, UANode node, Newtonsoft.Json.JsonSerializer serializer)
        {
            writer.WritePropertyName("NodeId");
            serializer.Serialize(writer, node.Id);
            if (node.ParentId != null && !node.ParentId.IsNullNodeId)
            {
                writer.WritePropertyName("ParentNodeId");
                serializer.Serialize(writer, node.ParentId);
            }
            if (node.NodeType != null && !node.NodeType.Id.IsNullNodeId)
            {
                writer.WritePropertyName("TypeDefinitionId");
                serializer.Serialize(writer, node.NodeType.Id);
            }
            if (Type == ConverterType.Variable && node is UAVariable variable)
            {
                if (variable.DataType != null && !variable.DataType.Raw.IsNullNodeId)
                {
                    writer.WritePropertyName("DataTypeId");
                    serializer.Serialize(writer, variable.DataType.Raw);
                }
            }
        }

        private void WriteInternalInfo(JsonWriter writer, UANode node, Newtonsoft.Json.JsonSerializer serializer)
        {
            writer.WritePropertyName("InternalInfo");
            writer.WriteStartObject();

            writer.WritePropertyName("EventNotifier");
            writer.WriteValue(node.EventNotifier);
            writer.WritePropertyName("ShouldSubscribe");
            writer.WriteValue(node.ShouldSubscribe);
            writer.WritePropertyName("NodeClass");
            writer.WriteValue(node.NodeClass);
            if (Type == ConverterType.Variable && node is UAVariable variable)
            {
                writer.WritePropertyName("AccessLevel");
                writer.WriteValue(variable.AccessLevel);
                writer.WritePropertyName("Historizing");
                writer.WriteValue(variable.VariableAttributes.Historizing);
                writer.WritePropertyName("ValueRank");
                writer.WriteValue(variable.ValueRank);
                if (variable.ArrayDimensions != null)
                {
                    writer.WritePropertyName("ArrayDimensions");
                    serializer.Serialize(writer, variable.ArrayDimensions.ToArray());
                    writer.WritePropertyName("Index");
                    writer.WriteValue(variable.Index);
                }
            }
            writer.WriteEndObject();
        }

        public override void WriteJson(
            JsonWriter writer,
            UANode? value,
            Newtonsoft.Json.JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }
            writer.WriteStartObject();
            WriteBaseValues(writer, value);
            if (config.Extraction.DataTypes.ExpandNodeIds)
            {
                WriteNodeIds(writer, value, serializer);
            }
            if (config.Extraction.DataTypes.AppendInternalValues)
            {
                WriteInternalInfo(writer, value, serializer);
            }
            writer.WriteEndObject();
        }

        public override UANode ReadJson(
            JsonReader reader,
            Type objectType,
            UANode? existingValue,
            bool hasExistingValue,
            Newtonsoft.Json.JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
    class NodeIdConverter : JsonConverter<NodeId>
    {
        private readonly UAClient uaClient;
        public NodeIdConverter(UAClient uaClient)
        {
            this.uaClient = uaClient;
        }
        public override NodeId ReadJson(
            JsonReader reader,
            Type objectType,
            NodeId? existingValue,
            bool hasExistingValue,
            Newtonsoft.Json.JsonSerializer serializer)
        {
            if (reader.TokenType != JsonToken.StartObject) return NodeId.Null;
            var obj = JToken.ReadFrom(reader);
            if (obj == null || obj.Type != JTokenType.Object) return NodeId.Null;
            string? ns = obj.Value<string>("namespace");
            int? idType = obj.Value<int?>("idType");
            if (idType == null || idType.Value > 3 || idType.Value < 0) return NodeId.Null;

            int nsIdx = 0;
            if (ns != null)
            {
                nsIdx = uaClient.NamespaceTable!.GetIndex(ns);
                if (nsIdx < 0) return NodeId.Null;
            }

            try
            {
                switch ((IdType)idType.Value)
                {
                    case IdType.Numeric:
                        uint? numIdf = obj.Value<uint?>("identifier");
                        if (numIdf == null) return NodeId.Null;
                        return new NodeId(numIdf, (ushort)nsIdx);
                    case IdType.String:
                        string? strIdf = obj.Value<string>("identifier");
                        if (strIdf == null) return NodeId.Null;
                        return new NodeId(strIdf, (ushort)nsIdx);
                    case IdType.Guid:
                        strIdf = obj.Value<string>("identifier");
                        if (strIdf == null) return NodeId.Null;
                        Guid guid = Guid.Parse(strIdf);
                        return new NodeId(guid, (ushort)nsIdx);
                    case IdType.Opaque:
                        strIdf = obj.Value<string>("identifier");
                        if (strIdf == null) return NodeId.Null;
                        var opqIdf = Convert.FromBase64String(strIdf);
                        return new NodeId(opqIdf, (ushort)nsIdx);
                }
            }
            catch { }
            return NodeId.Null;
        }

        public override void WriteJson(
            JsonWriter writer,
            NodeId? value,
            Newtonsoft.Json.JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }
            writer.WriteStartObject();
            if (value.NamespaceIndex != 0)
            {
                writer.WritePropertyName("namespace");
                var ns = uaClient.NamespaceTable!.GetString(value.NamespaceIndex);
                writer.WriteValue(ns);
            }

            writer.WritePropertyName("idType");
            writer.WriteValue((int)value.IdType);
            writer.WritePropertyName("identifier");
            switch (value.IdType)
            {
                case IdType.Numeric:
                    writer.WriteValue(value.Identifier is uint iVal ? iVal : 0);
                    break;
                case IdType.Guid:
                    writer.WriteValue(value.Identifier is Guid gVal ? gVal : Guid.Empty);
                    break;
                case IdType.Opaque:
                    writer.WriteValue(value.Identifier as byte[]);
                    break;
                case IdType.String:
                    writer.WriteValue(value.Identifier as string);
                    break;
            }
            writer.WriteEndObject();
        }
    }
}

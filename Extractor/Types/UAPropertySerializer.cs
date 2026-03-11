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
using System.Buffers.Text;
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
    public enum ConverterType
    {
        Node,
        Variable
    }

    internal class NodeSerializer : JsonConverter<BaseUANode>
    {
        private readonly TypeConverter converter;
        private readonly FullConfig config;
        private readonly SessionContext context;
        public ConverterType ConverterType { get; }
        private readonly ILogger log;
        public NodeSerializer(TypeConverter converter, FullConfig config, SessionContext context, ConverterType type, ILogger log)
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
            if (variable.Value == null)
            {
                writer.WriteNullValue();
                return;
            }
            var value = converter.ConvertToJson(variable.Value.Value, variable.FullAttributes.DataType?.EnumValues, null, JsonMode.Json);
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }
            try
            {
                writer.WriteRawValue(value.ToJsonString());
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
                extras?.Remove("Value");
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
            WriteNodeIds(writer, value, options);
            WriteInternalInfo(writer, value, options);
            writer.WriteEndObject();
        }
    }

    internal class NodeIdConverter : JsonConverter<NodeId>
    {
        private readonly IUAClientAccess uaClient;
        public NodeIdConverter(IUAClientAccess uaClient)
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

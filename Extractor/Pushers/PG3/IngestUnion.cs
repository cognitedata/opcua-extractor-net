using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cognite.OpcUa.Pushers.PG3
{
    internal class IngestUnion
    {
        public object Item { get; set; }
        public IngestType Type { get; }

        public IngestUnion(UANode node)
        {
            Item = node;
            Type = IngestType.Node;
        }

        public IngestUnion(UAReference reference)
        {
            Item = reference;
            Type = IngestType.Reference;
        }
    }

    internal class PG3IngestConverter : JsonConverter<IngestUnion>
    {
        private string? prefix;
        private UAExtractor extractor;
        private Dictionary<NodeId, bool> refTypeIsHierarchical;
        public PG3IngestConverter(string? prefix, UAExtractor extractor, Dictionary<NodeId, bool> refTypeIsHierarchical)
        {
            this.prefix = prefix;
            this.extractor = extractor;
            this.refTypeIsHierarchical = refTypeIsHierarchical;
        }

        private void WriteNodeId(Utf8JsonWriter writer, string name, NodeId id)
        {
            if (id.IsNullNodeId)
            {
                writer.WriteNull(name);
            }
            else
            {
                writer.WriteString(name, ToExtId(id));
            }
        }

        private string ToExtId(NodeId id)
        {
            return $"{prefix}{id}";
        }

        private void WriteMinimalNode(Utf8JsonWriter writer, UANode value)
        {
            writer.WriteStartObject();
            WriteNodeId(writer, "external_id", value.Id);
            writer.WriteString("type", "uanode");
            writer.WriteString("name", value.DisplayName);
            writer.WriteString("description", value.Description);
            writer.WriteEndObject();
        }

        private static void WriteCommonProperties(Utf8JsonWriter writer, UANode value)
        {
            writer.WriteStartObject();
            writer.WriteNumber("node_class", (byte)value.NodeClass);
            writer.WriteString("browse_name", value.BrowseName);
            writer.WriteEndObject();
        }

        private void WriteUANode(Utf8JsonWriter writer, UANode value, JsonSerializerOptions options)
        {
            writer.WritePropertyName("node");
            WriteMinimalNode(writer, value);
            writer.WritePropertyName("opcua.common");
            WriteCommonProperties(writer, value);

            switch (value.NodeClass)
            {
                case NodeClass.ReferenceType:
                    writer.WritePropertyName("opcua.reference_type");
                    writer.WriteStartObject();
                    writer.WriteBoolean("is_abstract", value.Attributes.TypeAttributes!.IsAbstract);
                    writer.WriteBoolean("symmetric", value.Attributes.TypeAttributes!.Symmetric);
                    writer.WriteString("inverse_name", value.Attributes.TypeAttributes!.InverseName);
                    writer.WriteEndObject();
                    break;
                case NodeClass.ObjectType:
                    writer.WritePropertyName("opcua.object_type");
                    writer.WriteStartObject();
                    writer.WriteBoolean("is_abstract", value.Attributes.TypeAttributes!.IsAbstract);
                    writer.WriteEndObject();
                    break;
                case NodeClass.Variable:
                    var variable = value as UAVariable;
                    if (variable == null) throw new InvalidOperationException("Node with class Variable passed as UANode");
                    writer.WritePropertyName("opcua.variable");
                    writer.WriteStartObject();
                    if (variable.IsProperty)
                    {
                        writer.WritePropertyName("value");
                        writer.WriteRawValue(extractor.StringConverter.ConvertToString(variable.Value, variable.DataType?.EnumValues, null, true));
                    }
                    else
                    {
                        writer.WriteString("time_series", extractor.GetUniqueId(value.Id));
                    }
                    WriteNodeId(writer, "data_type", variable.DataType?.Raw ?? NodeId.Null);
                    writer.WriteNumber("value_rank", variable.ValueRank);
                    writer.WritePropertyName("array_dimensions");
                    JsonSerializer.Serialize(writer, variable.ArrayDimensions);
                    writer.WriteBoolean("historizing", variable.VariableAttributes.Historizing);
                    writer.WriteEndObject();
                    break;
                case NodeClass.VariableType:
                    var variableType = value as UAVariable;
                    if (variableType == null) throw new InvalidOperationException("Node with class VariableType passed as UANode");
                    writer.WritePropertyName("opcua.variable_type");
                    writer.WriteStartObject();
                    writer.WritePropertyName("value");
                    writer.WriteRawValue(extractor.StringConverter.ConvertToString(variableType.Value, variableType.DataType?.EnumValues, null, true));
                    WriteNodeId(writer, "data_type", variableType.DataType?.Raw ?? NodeId.Null);
                    writer.WriteNumber("value_rank", variableType.ValueRank);
                    writer.WritePropertyName("array_dimensions");
                    JsonSerializer.Serialize(writer, variableType.ArrayDimensions);
                    writer.WriteBoolean("is_abstract", variableType.Attributes.TypeAttributes!.IsAbstract);
                    writer.WriteEndObject();
                    break;
                case NodeClass.DataType:
                    writer.WritePropertyName("opcua.data_type");
                    writer.WriteStartObject();
                    writer.WriteBoolean("is_abstract", value.Attributes.TypeAttributes!.IsAbstract);
                    writer.WriteEndObject();
                    // TODO: Handle data type definition
                    break;
            }
        }

        private void WriteUAReference(Utf8JsonWriter writer, UAReference reference, JsonSerializerOptions options)
        {
            writer.WritePropertyName("edge");
            writer.WriteStartObject();
            WriteNodeId(writer, "start_node", reference.Source.Id);
            WriteNodeId(writer, "end_node", reference.Target.Id);
            // writer.WriteString("type", ToExtId(reference.Type?.Id ?? NodeId.Null));
            writer.WriteString("type", "uareference");
            writer.WriteString("external_id", $"{ToExtId(reference.Source.Id)};{ToExtId(reference.Target.Id)};{ToExtId(reference.Type?.Id ?? NodeId.Null)}");
            writer.WriteEndObject();

            writer.WritePropertyName("opcua.reference");
            writer.WriteStartObject();
            writer.WriteString("reference_type_id", ToExtId(reference.Type?.Id ?? NodeId.Null));
            writer.WriteBoolean("is_hierarchical", refTypeIsHierarchical.GetValueOrDefault(reference.Type?.Id ?? NodeId.Null));
            writer.WriteEndObject();
        }


        public override IngestUnion? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }




        public override void Write(Utf8JsonWriter writer, IngestUnion value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            switch (value.Type)
            {
                case IngestType.Node:
                    WriteUANode(writer, (value.Item as UANode)!, options);
                    break;
                case IngestType.Reference:
                    WriteUAReference(writer, (value.Item as UAReference)!, options);
                    break;

            }
            writer.WriteEndObject();
        }
    }
}

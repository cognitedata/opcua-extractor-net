using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml.Linq;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class InstanceBuilder
    {
        public List<BaseInstanceWrite> Objects { get; } = new();
        public List<BaseInstanceWrite> Variables { get; } = new();
        public List<BaseInstanceWrite> ObjectTypes { get; } = new();
        public List<BaseInstanceWrite> VariableTypes { get; } = new();
        public List<BaseInstanceWrite> ReferenceTypes { get; } = new();
        public List<BaseInstanceWrite> DataTypes { get; } = new();
        public List<BaseInstanceWrite> References { get; } = new();

        private HashSet<NodeId> MappedNodes { get; } = new();
        private HashSet<NodeId> MappedAsProperty { get; } = new();

        private readonly NodeHierarchy nodes;
        private readonly string space;
        private readonly FDMTypeBatch types;
        private readonly DMSValueConverter converter;
        private readonly IUAClientAccess client;
        private readonly ILogger log;
        public InstanceBuilder(
            NodeHierarchy nodes,
            FDMTypeBatch types,
            DMSValueConverter converter,
            IUAClientAccess client,
            string space,
            ILogger log)
        {
            this.nodes = nodes;
            this.space = space;
            this.types = types;
            this.converter = converter;
            this.client = client;
            this.log = log;
        }

        private InstanceData<BaseNodeData> GetBaseNodeData(UANode node)
        {
            return new InstanceData<BaseNodeData>
            {
                Source = new ContainerIdentifier(space, "BaseNode"),
                Properties = new BaseNodeData(node)
            };
        }

        private InstanceData<VariableData> GetVariableData(UAVariable? node)
        {
            if (node == null) throw new InvalidOperationException("Got Variable node that was not a variable");

            return new InstanceData<VariableData>
            {
                Source = new ContainerIdentifier(space, "BaseVariableType"),
                Properties = new VariableData(node, client, converter, space)
            };
        }

        private InstanceData<ObjectData> GetObjectData(UANode node)
        {
            return new InstanceData<ObjectData>
            {
                Source = new ContainerIdentifier(space, "BaseObjectType"),
                Properties = new ObjectData(node)
            };
        }

        private InstanceData<ObjectTypeData> GetObjectTypeData(UANode node)
        {
            return new InstanceData<ObjectTypeData>
            {
                Source = new ContainerIdentifier(space, "ObjectType"),
                Properties = new ObjectTypeData(node)
            };
        }

        private InstanceData<VariableTypeData> GetVariableTypeData(UAVariable? node)
        {
            if (node == null) throw new InvalidOperationException("Got VariableType node that was not a variable");

            return new InstanceData<VariableTypeData>
            {
                Source = new ContainerIdentifier(space, "VariableType"),
                Properties = new VariableTypeData(node, converter, space)
            };
        }

        private InstanceData<ReferenceTypeData> GetReferenceTypeData(UANode node)
        {
            return new InstanceData<ReferenceTypeData>
            {
                Source = new ContainerIdentifier(space, "ReferenceType"),
                Properties = new ReferenceTypeData(node)
            };
        }

        private InstanceData<DataTypeData> GetDataTypeData(UANode node)
        {
            return new InstanceData<DataTypeData>
            {
                Source = new ContainerIdentifier(space, "DataType"),
                Properties = new DataTypeData(node, converter)
            };
        }

        private bool CollectProperties(
            UANode node,
            Dictionary<string, FullChildNode> currentChildren,
            IEnumerable<string> path,
            Dictionary<string, IDMSValue?> properties,
            FullUANodeType type,
            bool first)
        {
            bool collected = false;

            IEnumerable<string> nextPath;
            if (!first)
            {
                nextPath = path.Append(node.BrowseName);
                var name = string.Join('_', nextPath);
                if (type.Properties.TryGetValue(name, out var property) && node is UAVariable variable)
                {
                    IDMSValue? value;
                    if (variable.IsProperty)
                    {
                        value = converter.ConvertVariant(property.TypeVariant, variable.Value);
                    }
                    else
                    {
                        value = new RawPropertyValue<string>(client.GetUniqueId(variable.Id)!);
                    }
                    properties[name] = value;
                    collected = true;
                }
            }
            else
            {
                nextPath = path;
            }

            
            var refs = nodes.BySource(node.Id);
            foreach (var rf in refs)
            {
                if (nodes.NodeMap.TryGetValue(rf.Target.Id, out var child))
                {
                    var name = child.BrowseName;
                    if (name == null)
                    {
                        if (child.DisplayName != null)
                        {
                            log.LogWarning("Node is missing BrowseName, falling back to DisplayName: {Id}, {Name}", child.Id, child.DisplayName);
                            name = child.DisplayName;
                        }
                        else
                        {
                            log.LogWarning("Node is missing BrowseName and DisplayName, cannot be mapped: {Id}", child.Id);
                            continue;
                        }
                    }
                    if (currentChildren.TryGetValue(name, out var nextChild))
                    {
                        collected |= CollectProperties(child, nextChild.Children, nextPath, properties, type, false);
                    }
                }
            }

            if (collected && !first)
            {
                MappedAsProperty.Add(node.Id);
            }
            return collected;
        }

        public IEnumerable<InstanceData> BuildNode(UANode node, FullUANodeType? type)
        {
            var data = new List<InstanceData>();
            data.Add(GetBaseNodeData(node));
            switch (node.NodeClass)
            {
                case NodeClass.Object: data.Add(GetObjectData(node)); break;
                case NodeClass.Variable: data.Add(GetVariableData(node as UAVariable)); break;
                case NodeClass.ObjectType: data.Add(GetObjectTypeData(node)); return data;
                case NodeClass.ReferenceType: data.Add(GetReferenceTypeData(node)); return data;
                case NodeClass.VariableType: data.Add(GetVariableTypeData(node as UAVariable)); return data;
                case NodeClass.DataType: data.Add(GetDataTypeData(node)); return data;
                default:
                    throw new InvalidOperationException("Unknwon NodeClass received");
            }


            var currentType = type;
            while (currentType != null)
            {
                if (currentType.RawChildren.Any()) continue;

                var props = new Dictionary<string, IDMSValue?>();
                CollectProperties(node, currentType.RawChildren, Enumerable.Empty<string>(), props, currentType, true);
                if (props.Any())
                {
                    data.Add(new InstanceData<Dictionary<string, IDMSValue?>>
                    {
                        Properties = props,
                        Source = new ContainerIdentifier(space, currentType.ExternalId)
                    });
                }

                currentType = currentType.Parent;
            }

            return data;
        }

        private void AddToCollections(UANode node, IEnumerable<InstanceData> data)
        {
            var instance = new NodeWrite
            {
                ExternalId = node.Id.ToString(),
                Sources = data,
                Space = space,
            };
            switch (node.NodeClass)
            {
                case NodeClass.Object: Objects.Add(instance); break;
                case NodeClass.Variable: Variables.Add(instance); break;
                case NodeClass.ObjectType: ObjectTypes.Add(instance); break;
                case NodeClass.ReferenceType: ReferenceTypes.Add(instance); break;
                case NodeClass.VariableType: VariableTypes.Add(instance); break;
                case NodeClass.DataType: DataTypes.Add(instance); break;
            }
        }

        public void Build()
        {
            // First, build all nodes with complex types.
            foreach (var node in nodes.NodeMap.Values)
            {
                if (MappedAsProperty.Contains(node.Id)) continue;

                FullUANodeType? type = null;
                if (node.NodeType != null)
                {
                    type = types.Types[node.NodeType.Id];
                    if (type.IsSimple()) continue;
                }
                MappedNodes.Add(node.Id);
                AddToCollections(node, BuildNode(node, type));
            }

            // Next, all remaining nodes
            foreach (var node in nodes.NodeMap.Values)
            {
                if (MappedNodes.Contains(node.Id) || MappedAsProperty.Contains(node.Id)) continue;

                FullUANodeType? type = null;
                if (node.NodeType != null)
                {
                    type = types.Types[node.NodeType.Id];
                }
                MappedNodes.Add(node.Id);
                AddToCollections(node, BuildNode(node, type));
            }

            // Finally, add any reference between two mapped nodes
            foreach (var rf in nodes.ReferencesBySourceId.Values.SelectMany(v => v))
            {
                if (!MappedNodes.Contains(rf.Source.Id) || !MappedNodes.Contains(rf.Target.Id)) continue;

                var edge = new EdgeWrite
                {
                    StartNode = new DirectRelationIdentifier(space, rf.Source.Id.ToString()),
                    EndNode = new DirectRelationIdentifier(space, rf.Target.Id.ToString()),
                    ExternalId = $"{rf.Source.Id}{rf.Type.GetName(false)}{rf.Target.Id}",
                    Space = space,
                    Type = new DirectRelationIdentifier(space, rf.Type.Id.ToString())
                };
                References.Add(edge);
            }
        }

        public void DebugLog(ILogger log)
        {
            var options = new JsonSerializerOptions(Oryx.Cognite.Common.jsonOptions) { WriteIndented = true };
            log.LogDebug("Objects: ");
            foreach (var obj in Objects)
            {
                log.LogDebug(JsonSerializer.Serialize(obj, options));
            }
            log.LogDebug("Variables: ");
            foreach (var obj in Variables)
            {
                log.LogDebug(JsonSerializer.Serialize(obj, options));
            }
            log.LogDebug("ObjectTypes: ");
            foreach (var obj in ObjectTypes)
            {
                log.LogDebug(JsonSerializer.Serialize(obj, options));
            }
            log.LogDebug("VariableTypes: ");
            foreach (var obj in VariableTypes)
            {
                log.LogDebug(JsonSerializer.Serialize(obj, options));
            }
            log.LogDebug("ReferenceTypes: ");
            foreach (var obj in ReferenceTypes)
            {
                log.LogDebug(JsonSerializer.Serialize(obj, options));
            }
            log.LogDebug("DataTypes: ");
            foreach (var obj in DataTypes)
            {
                log.LogDebug(JsonSerializer.Serialize(obj, options));
            }
            log.LogDebug("Edges: ");
            foreach (var obj in References)
            {
                log.LogDebug(JsonSerializer.Serialize(obj, options));
            }
        }
    }

    class BaseNodeData
    {
        [JsonPropertyName("NodeClass")]
        public int NodeClass { get; }
        public BaseNodeData(UANode node)
        {
            NodeClass = (int)node.NodeClass;
        }
    }

    class VariableData
    {
        [JsonPropertyName("DataType")]
        public DirectRelationIdentifier DataType { get; }
        [JsonPropertyName("ArrayDimensions")]
        public int[]? ArrayDimensions { get; }
        [JsonPropertyName("ValueRank")]
        public int ValueRank { get; }
        [JsonPropertyName("Value")]
        public JsonElement? Value { get; }
        [JsonPropertyName("ValueTimeseries")]
        public string? ValueTimeseries { get; }
        [JsonPropertyName("MinimumSamplingInterval")]
        public double MinimumSamplingInterval { get; }

        public VariableData(UAVariable variable, IUAClientAccess client, DMSValueConverter converter, string space)
        {
            if (variable.IsProperty)
            {
                var json = converter.Converter.ConvertToString(variable.Value, null, null, StringConverterMode.ReversibleJson);
                Value = JsonDocument.Parse(json).RootElement;
            }
            else
            {
                ValueTimeseries = client.GetUniqueId(variable.Id);
            }
            if (!variable.DataType.Raw.IsNullNodeId) DataType = new DirectRelationIdentifier(space, variable.DataType.Raw.ToString());
            ArrayDimensions = variable.ArrayDimensions;
            ValueRank = variable.ValueRank;
        }
    }

    class ObjectData
    {
        [JsonPropertyName("EventNotifier")]
        public int EventNotifier { get; }
        public ObjectData(UANode node)
        {
            EventNotifier = node.EventNotifier;
        }
    }

    class ObjectTypeData
    {
        [JsonPropertyName("IsAbstract")]
        public bool IsAbstract { get; }
        public ObjectTypeData(UANode node)
        {
            IsAbstract = node.Attributes.TypeAttributes?.IsAbstract ?? false;
        }
    }

    class VariableTypeData
    {
        [JsonPropertyName("IsAbstract")]
        public bool IsAbstract { get; }
        [JsonPropertyName("DataType")]
        public DirectRelationIdentifier? DataType { get; }
        [JsonPropertyName("ArrayDimensions")]
        public int[]? ArrayDimensions { get; }
        [JsonPropertyName("ValueRank")]
        public int ValueRank { get; }
        [JsonPropertyName("Value")]
        public JsonElement Value { get; }

        public VariableTypeData(UAVariable variable, DMSValueConverter converter, string space)
        {
            IsAbstract = variable.Attributes.TypeAttributes?.IsAbstract ?? false;
            if (!variable.DataType.Raw.IsNullNodeId) DataType = new DirectRelationIdentifier(space, variable.DataType.Raw.ToString());
            ArrayDimensions = variable.ArrayDimensions;
            ValueRank = variable.ValueRank;
            var json = converter.Converter.ConvertToString(variable.Value, null, null, StringConverterMode.ReversibleJson);
            Value = JsonDocument.Parse(json).RootElement;
        }
    }

    class ReferenceTypeData
    {
        [JsonPropertyName("IsAbstract")]
        public bool IsAbstract { get; }
        [JsonPropertyName("InverseName")]
        public string? InverseName { get; }

        public ReferenceTypeData(UANode node)
        {
            IsAbstract = node.Attributes.TypeAttributes?.IsAbstract ?? false;
            InverseName = node.Attributes.TypeAttributes?.InverseName;
        }
    }

    class DataTypeData
    {
        [JsonPropertyName("IsAbstract")]
        public bool IsAbstract { get; }
        [JsonPropertyName("DataTypeDefinition")]
        public JsonElement? DataTypeDefinition { get; }

        public DataTypeData(UANode node, DMSValueConverter converter)
        {
            var def = node.Attributes.TypeAttributes?.DataTypeDefinition;
            if (def != null)
            {
                var json = converter.Converter.ConvertToString(def, null, null, StringConverterMode.ReversibleJson);
                DataTypeDefinition = JsonDocument.Parse(json).RootElement;
            }
        }
    }
}

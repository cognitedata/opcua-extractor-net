﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml.Linq;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.FDM.Types;
using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Microsoft.Extensions.Logging;
using Opc.Ua;

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

        private InstanceData<BaseNodeData> GetBaseNodeData(BaseUANode node, Dictionary<string, string> knownProperties)
        {
            return new InstanceData<BaseNodeData>
            {
                Source = new ContainerIdentifier(space, "BaseNode"),
                Properties = new BaseNodeData(node, knownProperties)
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

        private InstanceData<ObjectData> GetObjectData(UAObject node)
        {
            return new InstanceData<ObjectData>
            {
                Source = new ContainerIdentifier(space, "BaseObjectType"),
                Properties = new ObjectData(node, space)
            };
        }

        private InstanceData<TypeData> GetTypeData(BaseUAType type)
        {
            return new InstanceData<TypeData>
            {
                Source = new ContainerIdentifier(space, "BaseType"),
                Properties = new TypeData(type)
            };
        }

        private InstanceData<ObjectTypeData> GetObjectTypeData(UAObjectType node)
        {
            return new InstanceData<ObjectTypeData>
            {
                Source = new ContainerIdentifier(space, "ObjectType"),
                Properties = new ObjectTypeData(node)
            };
        }

        private InstanceData<VariableTypeData> GetVariableTypeData(UAVariableType? node)
        {
            if (node == null) throw new InvalidOperationException("Got VariableType node that was not a variable");

            return new InstanceData<VariableTypeData>
            {
                Source = new ContainerIdentifier(space, "VariableType"),
                Properties = new VariableTypeData(node, converter, space)
            };
        }

        private InstanceData<ReferenceTypeData> GetReferenceTypeData(UAReferenceType node)
        {
            return new InstanceData<ReferenceTypeData>
            {
                Source = new ContainerIdentifier(space, "ReferenceType"),
                Properties = new ReferenceTypeData(node)
            };
        }

        private InstanceData<DataTypeData> GetDataTypeData(UADataType node)
        {
            return new InstanceData<DataTypeData>
            {
                Source = new ContainerIdentifier(space, "DataType"),
                Properties = new DataTypeData(node, converter)
            };
        }

        private bool CollectProperties(
            BaseUANode node,
            NodeId rootId,
            Dictionary<string, ChildNode> currentChildren,
            IEnumerable<string> path,
            Dictionary<string, IDMSValue?> properties,
            Dictionary<string, string> knownPropertyIds,
            FullUANodeType type,
            bool first)
        {
            bool collected = false;

            IEnumerable<string> nextPath;
            if (!first)
            {
                nextPath = path.Append(node.Attributes.BrowseName?.Name ?? node.Name ?? "");
                var name = string.Join('_', nextPath);
                if (type.Properties.TryGetValue(name, out var property) && node is UAVariable variable)
                {
                    IDMSValue? value;
                    if (variable.IsProperty)
                    {
                        value = converter.ConvertVariant(property.DMSType, variable.Value);
                    }
                    else
                    {
                        if (variable.IsArray)
                        {
                            value = new RawPropertyValue<string[]>(variable.ArrayChildren.Select(v => v.GetUniqueId(client)!).ToArray());
                        }
                        else
                        {
                            value = new RawPropertyValue<string>(client.GetUniqueId(variable.Id)!);
                        }
                    }
                    properties[name] = value;
                    collected = true;
                    knownPropertyIds[name] = node.Id.ToString();
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
                    var name = child.Attributes.BrowseName?.Name;
                    if (name == null)
                    {
                        if (child.Name != null)
                        {
                            log.LogWarning("Node is missing BrowseName, falling back to DisplayName: {Id}, {Name}", child.Id, child.Name);
                            name = child.Name;
                        }
                        else
                        {
                            log.LogWarning("Node is missing BrowseName and DisplayName, cannot be mapped: {Id}", child.Id);
                            continue;
                        }
                    }
                    if (currentChildren.TryGetValue(name, out var nextChild))
                    {
                        collected |= CollectProperties(child, rootId, nextChild.Children, nextPath, properties, knownPropertyIds, type, false);
                    }
                }
            }

            if (collected && !first)
            {
                MappedAsProperty.Add(node.Id);
                knownPropertyIds[string.Join('_', nextPath)] = node.Id.ToString();
            }

            if (first)
            {
                foreach (var prop in type.Properties)
                {
                    if (properties.ContainsKey(prop.Key)) continue;
                    properties[prop.Key] = null;
                }
            }

            return collected;
        }

        public IEnumerable<InstanceData> BuildNode(BaseUANode node, FullUANodeType? type)
        {
            var data = new List<InstanceData>();

            if (node is UAObject obj)
            {
                data.Add(GetObjectData(obj));
            }
            else if (node is UAVariable vr)
            {
                data.Add(GetVariableData(vr));
            }
            else if (node is UAObjectType objType)
            {
                data.Add(GetTypeData(objType));
                data.Add(GetObjectTypeData(objType));
            }
            else if (node is UAVariableType varType)
            {
                data.Add(GetTypeData(varType));
                data.Add(GetVariableTypeData(varType));
            }
            else if (node is UAReferenceType rfType)
            {
                data.Add(GetTypeData(rfType));
                data.Add(GetReferenceTypeData(rfType));
            }
            else if (node is UADataType dtType)
            {
                data.Add(GetTypeData(dtType));
                data.Add(GetDataTypeData(dtType));
            }

            var knownProperties = new Dictionary<string, string>();
            var currentType = type;
            while (currentType != null)
            {
                if (!currentType.Children.Any())
                {
                    currentType = currentType.Parent;
                    continue;
                }

                var props = new Dictionary<string, IDMSValue?>();
                CollectProperties(node, node.Id, currentType.Children, Enumerable.Empty<string>(), props, knownProperties, currentType, true);
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

            data.Add(GetBaseNodeData(node, knownProperties));
            return data;
        }

        private void AddToCollections(BaseUANode node, IEnumerable<InstanceData> data)
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
                if (node.TypeDefinition != null && !node.TypeDefinition.IsNullNodeId)
                {
                    if (!types.Types.TryGetValue(node.TypeDefinition, out var typ))
                    {
                        log.LogWarning("Failed to retrieve type {Id} for node {Node} {Name}", node.TypeDefinition, node.Id, node.Name);
                        MappedNodes.Add(node.Id);
                        continue;
                    }
                    type = types.Types[node.TypeDefinition];
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
                if (node.TypeDefinition != null && !node.TypeDefinition.IsNullNodeId)
                {
                    type = types.Types[node.TypeDefinition];
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

        private string SerializeStoreSize(BaseInstanceWrite data, JsonSerializerOptions options, ref long size)
        {
            var res = JsonSerializer.Serialize(data, options);
            if (res.Length > size)
            {
                size = res.Length;
            }
            return res;
        }

        public void DebugLog(ILogger log)
        {
            var options = new JsonSerializerOptions(Oryx.Cognite.Common.jsonOptions) { WriteIndented = true };
            long maxSize = 0;
            log.LogTrace("Objects: ");
            foreach (var obj in Objects)
            {
                log.LogTrace(SerializeStoreSize(obj, options, ref maxSize));
            }
            log.LogTrace("Variables: ");
            foreach (var obj in Variables)
            {
                log.LogTrace(SerializeStoreSize(obj, options, ref maxSize));
            }
            log.LogTrace("ObjectTypes: ");
            foreach (var obj in ObjectTypes)
            {
                log.LogTrace(SerializeStoreSize(obj, options, ref maxSize));
            }
            log.LogTrace("VariableTypes: ");
            foreach (var obj in VariableTypes)
            {
                log.LogTrace(SerializeStoreSize(obj, options, ref maxSize));
            }
            log.LogTrace("ReferenceTypes: ");
            foreach (var obj in ReferenceTypes)
            {
                log.LogTrace(SerializeStoreSize(obj, options, ref maxSize));
            }
            log.LogTrace("DataTypes: ");
            foreach (var obj in DataTypes)
            {
                log.LogTrace(SerializeStoreSize(obj, options, ref maxSize));
            }
            log.LogTrace("Edges: ");
            foreach (var obj in References)
            {
                log.LogTrace(SerializeStoreSize(obj, options, ref maxSize));
            }
            log.LogInformation("Max size is {Size}", maxSize);
        }
    }

    class BaseNodeData
    {
        [JsonPropertyName("NodeClass")]
        public int NodeClass { get; }
        [JsonPropertyName("DisplayName")]
        public string? DisplayName { get; }
        [JsonPropertyName("Description")]
        public string? Description { get; }
        [JsonPropertyName("BrowseName")]
        public string? BrowseName { get; }
        [JsonPropertyName("NodeMeta")]
        public Dictionary<string, string>? NodeMeta { get; }
        public BaseNodeData(BaseUANode node, Dictionary<string, string> knownProperties)
        {
            NodeClass = (int)node.NodeClass;
            DisplayName = node.Attributes.DisplayName;
            Description = node.Attributes.Description;
            if (knownProperties.Any())
            {
                NodeMeta = knownProperties;
            }
            if (node.Attributes.BrowseName != null)
            {
                BrowseName = $"{node.Attributes.BrowseName.NamespaceIndex}:{node.Attributes.BrowseName.Name}";
            }
        }
    }

    class VariableData
    {
        [JsonPropertyName("DataType")]
        public DirectRelationIdentifier? DataType { get; }
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
        [JsonPropertyName("TypeDefinition")]
        public DirectRelationIdentifier? TypeDefinition { get; }

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
            if (!variable.FullAttributes.DataType.Id.IsNullNodeId) DataType = new DirectRelationIdentifier(space, variable.FullAttributes.DataType.Id.ToString());
            ArrayDimensions = variable.ArrayDimensions;
            ValueRank = variable.ValueRank;
            if (variable.TypeDefinition != null && !variable.TypeDefinition.IsNullNodeId)
            {
                TypeDefinition = new DirectRelationIdentifier(space, variable.TypeDefinition.ToString());
            }
        }
    }

    class ObjectData
    {
        [JsonPropertyName("EventNotifier")]
        public int EventNotifier { get; }
        [JsonPropertyName("TypeDefinition")]
        public DirectRelationIdentifier? TypeDefinition { get; }
        public ObjectData(UAObject node, string space)
        {
            EventNotifier = node.FullAttributes.EventNotifier;
            if (node.TypeDefinition != null && !node.TypeDefinition.IsNullNodeId)
            {
                TypeDefinition = new DirectRelationIdentifier(space, node.TypeDefinition.ToString());
            }
        }
    }

    class TypeData
    {
        [JsonPropertyName("TypeHierarchy")]
        public IEnumerable<string> TypeHierarchy { get; }
        public TypeData(BaseUAType node)
        {
            var current = node;
            var items = new List<string>();
            while (current != null && !current.Id.IsNullNodeId)
            {
                items.Add(current.Id.ToString());
                current = current.Parent as BaseUAType;
            }
            TypeHierarchy = items;
        }
    }

    class ObjectTypeData
    {
        [JsonPropertyName("IsAbstract")]
        public bool IsAbstract { get; }
        public ObjectTypeData(UAObjectType node)
        {
            IsAbstract = node.FullAttributes.IsAbstract;
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

        public VariableTypeData(UAVariableType variable, DMSValueConverter converter, string space)
        {
            IsAbstract = variable.FullAttributes.IsAbstract;
            if (!variable.FullAttributes.DataType.Id.IsNullNodeId) DataType = new DirectRelationIdentifier(space, variable.FullAttributes.DataType.Id.ToString());
            ArrayDimensions = variable.FullAttributes.ArrayDimensions;
            ValueRank = variable.FullAttributes.ValueRank;
            var json = converter.Converter.ConvertToString(variable.FullAttributes.Value, null, null, StringConverterMode.ReversibleJson);
            Value = JsonDocument.Parse(json).RootElement;
        }
    }

    class ReferenceTypeData
    {
        [JsonPropertyName("InverseName")]
        [JsonIgnore(Condition = JsonIgnoreCondition.Never)]
        public string? InverseName { get; }

        public ReferenceTypeData(UAReferenceType node)
        {
            InverseName = node.FullAttributes.InverseName;
        }
    }

    class DataTypeData
    {
        [JsonPropertyName("IsAbstract")]
        public bool IsAbstract { get; }
        [JsonPropertyName("DataTypeDefinition")]
        public JsonElement? DataTypeDefinition { get; }

        public DataTypeData(UADataType node, DMSValueConverter converter)
        {
            var def = node.FullAttributes.DataTypeDefinition;
            if (def != null)
            {
                var json = converter.Converter.ConvertToString(def, null, null, StringConverterMode.ReversibleJson);
                DataTypeDefinition = JsonDocument.Parse(json).RootElement;
            }
        }
    }
}

﻿using Cognite.OpcUa.Nodes;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using Cognite.OpcUa.Config;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa.TypeCollectors
{
    public class TypeManager
    {
        public Dictionary<NodeId, BaseUANode> NodeMap { get; } = new();
        public Dictionary<NodeId, HashSet<NodeId>> NodeChildren { get; } = new();



        private readonly ILogger log;
        private readonly FullConfig config;
        private readonly UAClient client;

        private bool eventTypesRead = false;
        private bool dataTypesRead = false;
        private bool typesBuilt = false;
        public TypeManager(FullConfig config, UAClient client, ILogger log)
        {
            this.log = log;
            this.config = config;
            this.client = client;
        }

        public bool IsTypeNodeClass(NodeClass nodeClass)
        {
            return nodeClass == NodeClass.ObjectType
                || nodeClass == NodeClass.VariableType
                || nodeClass == NodeClass.ReferenceType
                || nodeClass == NodeClass.DataType;
        }

        public void Reset()
        {
            eventTypesRead = false;
            dataTypesRead = false;
            NodeMap.Clear();
            NodeChildren.Clear();
            EventFields.Clear();
        }

        private async Task ReadTypeData(CancellationToken token)
        {
            var toRead = new List<BaseUANode>();
            var toReadValues = new List<UAVariable>();
            foreach (var tp in NodeMap.Values)
            {
                if (tp.Attributes.IsDataRead) continue;
                if (tp.NodeClass == NodeClass.ObjectType && !config.Extraction.NodeTypes.Metadata
                    && (tp is not UAObjectType otp || !otp.IsEventType())) continue;
                if (tp.NodeClass == NodeClass.VariableType && !config.Extraction.NodeTypes.Metadata) continue;
                if (tp.NodeClass == NodeClass.DataType && !config.Extraction.DataTypes.DataTypeMetadata
                    && !config.Extraction.DataTypes.AutoIdentifyTypes) continue;
                if (tp.NodeClass == NodeClass.ReferenceType && !config.Extraction.Relationships.Enabled) continue;
                toRead.Add(tp);
                if (tp.NodeClass == NodeClass.Variable && tp is UAVariable variable) toReadValues.Add(variable);
            }
            await client.ReadNodeData(toRead, this, token);
            await client.ReadNodeValues(toReadValues, this, token);
        }

        private async Task ReadTypeHiearchies(CancellationToken token)
        {
            var rootNodes = new List<NodeId>();
            var mask = (uint)NodeClass.Variable | (uint)NodeClass.Object;
            if (config.Extraction.DataTypes.AutoIdentifyTypes && !dataTypesRead)
            {
                mask |= (uint)NodeClass.DataType;
                rootNodes.Add(DataTypeIds.BaseDataType);
                dataTypesRead = true;
            }
            if (config.Events.Enabled && !eventTypesRead)
            {
                mask |= (uint)NodeClass.ObjectType;
                rootNodes.Add(ObjectTypeIds.BaseEventType);
                eventTypesRead = true;
            }
            if (!rootNodes.Any()) return;
            await client.Browser.BrowseDirectory(rootNodes, HandleNode, token, ReferenceTypeIds.HierarchicalReferences,
                mask, doFilter: false, purpose: "the type hierarchy");
        }

        public async Task LoadTypeData(CancellationToken token)
        {
            await ReadTypeHiearchies(token);
            await ReadTypeData(token);
        }

        public void BuildTypeInfo()
        {
            if (typesBuilt) return;
            typesBuilt = true;
            BuildNodeChildren();
            if (config.Events.Enabled) CollectTypes();
            BuildDataTypes();
        }

        private void BuildNodeChildren()
        {
            foreach (var node in NodeMap.Values)
            {
                if (node.ParentId == null) continue;

                if (NodeChildren.TryGetValue(node.ParentId, out var children))
                {
                    children.Add(node.Id);
                }
                else
                {
                    NodeChildren[node.ParentId] = new HashSet<NodeId> { node.Id };
                }
            }
        }

        private void HandleNode(ReferenceDescription node, NodeId parentId, bool visited)
        {
            if (visited) return;

            var parent = NodeMap.GetValueOrDefault(parentId);

            var result = BaseUANode.Create(node, parentId, parent, client, this);

            if (result == null)
            {
                log.LogWarning("Node of unexpected type received: {Type}, {Id}", node.NodeClass, node.NodeId);
                return;
            }

            NodeMap[result.Id] = result;
            log.LogTrace("Handle node {Name}, {Id}: {Class}", result.Attributes.DisplayName, result.Id, result.NodeClass);
        }

        #region dataTypes
        private void BuildDataTypes()
        {
            var ignoreDataTypes = new HashSet<NodeId>();
            if (config.Extraction.DataTypes.IgnoreDataTypes != null)
            {
                foreach (var type in config.Extraction.DataTypes.IgnoreDataTypes)
                {
                    var id = type.ToNodeId(client);
                    if (id == null || id.IsNullNodeId)
                    {
                        log.LogWarning("Invalid ignore datatype nodeId: {NameSpace}: {Identifier}", type.NamespaceUri, type.NodeId);
                        continue;
                    }
                    ignoreDataTypes.Add(id);
                }
            }
            var customDataTypes = new Dictionary<NodeId, ProtoDataType>();
            if (config.Extraction.DataTypes.CustomNumericTypes != null)
            {
                foreach (var type in config.Extraction.DataTypes.CustomNumericTypes)
                {
                    if (type.NodeId == null) continue;
                    var id = type.NodeId.ToNodeId(client);
                    if (id == null || id.IsNullNodeId)
                    {
                        log.LogWarning("Invalid datatype nodeId: {NameSpace}: {Identifier}", type.NodeId.NamespaceUri, type.NodeId.NodeId);
                        continue;
                    }
                    customDataTypes[id] = type;
                    log.LogInformation("Add custom datatype: {Id}", id);
                }
            }

            foreach (var type in NodeMap.Values.OfType<UADataType>())
            {
                type.ShouldIgnore = ignoreDataTypes.Contains(type.Id);
                if (customDataTypes.TryGetValue(type.Id, out var protoType))
                {
                    type.Initialize(protoType, config.Extraction.DataTypes);
                }
                if (!config.Extraction.DataTypes.AutoIdentifyTypes) continue;
                type.UpdateFromParent(config.Extraction.DataTypes);
                if (NodeChildren.TryGetValue(type.Id, out var children))
                {
                    var enumValues = children.SelectNonNull(child => NodeMap.GetValueOrDefault(child))
                        .OfType<UAVariable>().Where(v =>
                            v.Attributes.BrowseName!.Name == "EnumStrings"
                            || v.Attributes.BrowseName!.Name == "EnumValues")
                        .FirstOrDefault();
                    if (enumValues != null)
                    {
                        type.SetEnumStrings(log, enumValues.Value);
                    }
                }

            }
        }
        #endregion

        #region events
        public Dictionary<NodeId, UAObjectType> EventFields { get; } = new();

        private void CollectTypes()
        {
            Regex? ignoreFilter = null;
            if (!string.IsNullOrEmpty(config.Events.ExcludeEventFilter))
            {
                ignoreFilter = new Regex(config.Events.ExcludeEventFilter, RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.CultureInvariant);
            }
            var excludeProperties = new HashSet<string>(config.Events.ExcludeProperties);
            var baseExcludeProperties = new HashSet<string>(config.Events.BaseExcludeProperties);
            var whitelist = config.Events.GetWhitelist(client, log);

            foreach (var type in NodeMap.Values.OfType<UAObjectType>())
            {
                if (type.IsEventType()) continue;
                if (ignoreFilter != null && ignoreFilter.IsMatch(type.Attributes.DisplayName)) continue;
                if (whitelist != null && whitelist.Any())
                {
                    if (!whitelist.Contains(type.Id)) continue;
                }
                else if (!config.Events.AllEvents && type.Id.NamespaceIndex == 0) continue;
                EventFields[type.Id] = type;
                CollectType(type, baseExcludeProperties, excludeProperties);
            }
        }

        private void CollectType(BaseUAType type, HashSet<string> baseExcludeProperties, HashSet<string> excludeProperties)
        {
            if (type.IsCollected) return;

            // Initialize with any children in parent, or collect parent if it has not yet been collected
            if (type.Parent is BaseUAType parentType)
            {
                CollectType(parentType, baseExcludeProperties, excludeProperties);

                foreach (var field in parentType.AllCollectedFields)
                {
                    type.AllCollectedFields.Add(field);
                }
            }

            if (NodeChildren.TryGetValue(type.Id, out var children))
            {
                foreach (var child in children)
                {
                    foreach (var field in CollectNormalNode(child))
                    {
                        if (type.Id == ObjectTypeIds.BaseEventType && baseExcludeProperties.Contains(field.Node.Attributes.BrowseName!.Name)
                            || excludeProperties.Contains(field.Node.Attributes.BrowseName!.Name)) continue;
                        type.AllCollectedFields.Add(field);
                    }
                }
            }
            type.IsCollected = true;
        }

        private IEnumerable<TypeField> CollectNormalNode(NodeId id)
        {
            if (!NodeMap.TryGetValue(id, out var node)) yield break;
            if (node is BaseUAType) yield break;

            yield return new TypeField(node);
            
            if (NodeChildren.TryGetValue(id, out var children))
            {
                foreach (var child in children)
                {
                    var childFields = CollectNormalNode(child);
                    foreach (var childField in childFields)
                    {
                        childField.BrowsePath.Insert(0, node.Attributes.BrowseName);
                        yield return childField;
                    }
                }
            }
        }
        #endregion

        #region typeGetters
        public UADataType GetDataType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UADataType dt)
                {
                    log.LogWarning("Requested data type {Type}, but it was not a data type: {Class} {Name}",
                        nodeId, node.NodeClass, node.Attributes.DisplayName);
                    // This is a bug in the server, but instead of crashing we return a fresh node. The extracted data may be incomplete,
                    // but not incorrect.
                    return new UADataType(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = new UADataType(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }

        public UAReferenceType GetReferenceType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UAReferenceType dt)
                {
                    log.LogWarning("Requested reference type {Type}, but it was not a reference type: {Class} {Name}",
                        nodeId, node.NodeClass, node.Attributes.DisplayName);
                    return new UAReferenceType(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = new UAReferenceType(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }

        public UAObjectType GetObjectType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UAObjectType dt)
                {
                    log.LogWarning("Requested object type {Type}, but it was not an object type: {Class} {Name}",
                        nodeId, node.NodeClass, node.Attributes.DisplayName);
                    return new UAObjectType(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = new UAObjectType(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }

        public UAVariableType GetVariableType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UAVariableType dt)
                {
                    log.LogWarning("Requested variable type {Type}, but it was not a variable type: {Class} {Name}",
                        nodeId, node.NodeClass, node.Attributes.DisplayName);
                    return new UAVariableType(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = new UAVariableType(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }
        #endregion
    }
}
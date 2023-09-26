/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Microsoft.Extensions.Logging;
using Opc.Ua;

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


        private readonly HashSet<NodeId> ignoreDataTypes = new();
        private readonly Dictionary<NodeId, ProtoDataType> customDataTypes = new();


        public TypeManager(FullConfig config, UAClient client, ILogger log)
        {
            this.log = log;
            this.config = config;
            this.client = client;

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

        private async Task ReadTypeData(ITypeAndNodeSource source, CancellationToken token)
        {
            var toRead = new List<BaseUANode>();
            foreach (var tp in NodeMap.Values)
            {
                if (tp.Attributes.IsDataRead) continue;
                if (tp.Id.IsNullNodeId) continue;
                if (tp.NodeClass == NodeClass.ObjectType && !config.Extraction.NodeTypes.Metadata
                    && (tp is not UAObjectType otp || !otp.IsEventType())) continue;
                if (tp.NodeClass == NodeClass.VariableType && !config.Extraction.NodeTypes.Metadata) continue;
                if (tp.NodeClass == NodeClass.DataType && !config.Extraction.DataTypes.DataTypeMetadata
                    && !config.Extraction.DataTypes.AutoIdentifyTypes) continue;
                if (tp.NodeClass == NodeClass.ReferenceType && !config.Extraction.Relationships.Enabled) continue;
                toRead.Add(tp);
            }
            await source.LoadTypeMetadata(toRead, config.Extraction.DataTypes, token);
        }

        private async Task ReadTypeHiearchies(ITypeAndNodeSource source, CancellationToken token)
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

            var result = await source.LoadNodes(rootNodes, mask, HierarchicalReferenceMode.Disabled, token);
            foreach (var kvp in result.Nodes)
            {
                NodeMap[kvp.Key] = kvp.Value;
            }
        }

        public async Task LoadTypeData(ITypeAndNodeSource source, CancellationToken token)
        {
            await ReadTypeHiearchies(source, token);
            await ReadTypeData(source, token);
            BuildTypeInfo();
        }

        public void BuildTypeInfo()
        {
            log.LogInformation("Building type information from nodes in memory");
            BuildNodeChildren();
            if (config.Events.Enabled) CollectEventTypes();
            BuildDataTypes();
        }

        public void BuildNodeChildren()
        {
            foreach (var node in NodeMap.Values)
            {
                if (node.ParentId.IsNullNodeId) continue;

                if (NodeChildren.TryGetValue(node.ParentId, out var children))
                {
                    children.Add(node.Id);
                }
                else
                {
                    NodeChildren[node.ParentId] = new HashSet<NodeId> { node.Id };
                }

                if (node.Parent == null && NodeMap.TryGetValue(node.ParentId, out var parent))
                {
                    node.Parent = parent;
                    log.LogTrace("Add parent to node {Id}: {P}", node.Id, node.ParentId);
                    if (node is BaseUAType t)
                    {
                        log.LogTrace("Node is hierarchical: {T}", t.IsChildOf(ReferenceTypeIds.HierarchicalReferences));
                    }
                }
            }
        }

        public void AddTypeHierarchyNode(BaseUANode node)
        {
            NodeMap[node.Id] = node;
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
            log.LogTrace("Handle node {Name}, {Id}: {Class}", result.Name, result.Id, result.NodeClass);
        }

        #region dataTypes
        private void BuildDataTypes()
        {
            foreach (var type in NodeMap.Values.OfType<UADataType>())
            {
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

        private void CollectEventTypes()
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
                if (!type.IsEventType()) continue;
                if (ignoreFilter != null && ignoreFilter.IsMatch(type.Name)) continue;
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

            type.AllCollectedFields = new HashSet<TypeField>();
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
        private T GetType<T>(NodeId nodeId, Func<NodeId, T> constructor, string typeName, bool allowNullNodeId = false) where T : BaseUAType
        {
            if (nodeId == null || (nodeId.IsNullNodeId && !allowNullNodeId))
            {
                return constructor(NodeId.Null);
            }

            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not T dt)
                {
                    log.LogWarning($"Requested {typeName} type {nodeId}, but it was not a {typeName} type: {node.NodeClass} {node.Name}");
                    // This is a bug in the server, but instead of crashing we return a fresh node. The extracted data may be incomplete,
                    // but not incorrect.
                    return constructor(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = constructor(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }
        public UADataType GetDataType(NodeId nodeId)
        {
            return GetType<UADataType>(nodeId, x => {
                UADataType dt;
                if (customDataTypes.TryGetValue(x, out var protoDataType))
                {
                    dt = new UADataType(protoDataType, nodeId, config.Extraction.DataTypes);
                }
                else
                {
                    dt = new UADataType(x);
                }
                if (ignoreDataTypes.Contains(x)) dt.ShouldIgnore = true;
                if (dt.Id.IsNullNodeId)
                {
                    dt.IsString = !config.Extraction.DataTypes.NullAsNumeric;
                }
                return dt;
            }, "data", true);
        }

        public UAReferenceType GetReferenceType(NodeId nodeId)
        {
            return GetType<UAReferenceType>(nodeId, x => new UAReferenceType(x), "reference");
        }

        public UAObjectType GetObjectType(NodeId nodeId)
        {
            return GetType<UAObjectType>(nodeId, x => new UAObjectType(x), "object");
        }

        public UAVariableType GetVariableType(NodeId nodeId)
        {
            return GetType<UAVariableType>(nodeId, x => new UAVariableType(x), "variable");
        }
        #endregion
    }
}

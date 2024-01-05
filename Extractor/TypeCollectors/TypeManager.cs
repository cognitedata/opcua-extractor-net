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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.TypeCollectors
{
    public class TypeManager
    {
        public Dictionary<NodeId, BaseUANode> NodeMap { get; } = new();
        public Dictionary<NodeId, HashSet<NodeId>> NodeChildren { get; } = new();
        public HashSet<UAReference> References { get; } = new();

        private readonly ILogger log;
        private readonly FullConfig config;
        private readonly UAClient client;

        private bool eventTypesLoaded = false;
        private bool dataTypesLoaded = false;
        private bool typeDefsLoaded = false;
        private bool referenceTypesLoaded = false;

        private readonly HashSet<NodeId> ignoreDataTypes = new();
        private readonly Dictionary<NodeId, ProtoDataType> customDataTypes = new();

        public TypeManager(FullConfig config, UAClient client, ILogger log)
        {
            this.log = log;
            this.config = config;
            this.client = client;
        }

        public async Task Initialize(ITypeAndNodeSource source, CancellationToken token)
        {
            InitDataTypeConfig();
            await source.Initialize(token);
            await ReadTypeHiearchies(source, token);
            BuildTypeInfo();
            if (config.Events.Enabled) CollectEventTypes();
        }

        public void InitDataTypeConfig()
        {
            ignoreDataTypes.Clear();
            customDataTypes.Clear();
            if (config.Extraction.DataTypes.IgnoreDataTypes != null)
            {
                foreach (var type in config.Extraction.DataTypes.IgnoreDataTypes)
                {
                    var id = type.ToNodeId(client.Context);
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
                    var id = type.NodeId.ToNodeId(client.Context);
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
            eventTypesLoaded = false;
            dataTypesLoaded = false;
            typeDefsLoaded = false;
            referenceTypesLoaded = false;
            NodeMap.Clear();
            NodeChildren.Clear();
            EventFields.Clear();
            ignoreDataTypes.Clear();
            customDataTypes.Clear();
        }

        private async Task ReadTypeData(ITypeAndNodeSource source, CancellationToken token)
        {
            var toRead = new List<BaseUANode>();
            foreach (var tp in NodeMap.Values)
            {
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
            var referenceMode = HierarchicalReferenceMode.Disabled;
            bool loadReferences = false;
            var rootNodes = new List<NodeId>();
            var mask = (uint)NodeClass.Variable | (uint)NodeClass.Object;
            if (config.Toggles.LoadDataTypes && !dataTypesLoaded)
            {
                mask |= (uint)NodeClass.DataType;
                rootNodes.Add(DataTypeIds.BaseDataType);
                dataTypesLoaded = true;
                log.LogInformation("Loading data type hierarchy to map out custom data types");
            }
            if (config.Toggles.LoadEventTypes && !eventTypesLoaded)
            {
                mask |= (uint)NodeClass.ObjectType;
                // Avoid adding the BaseEventType node if we are also adding the BaseObjectType node, since
                // it's a parent. The browser doesn't really like if you have loops, though it will work.
                if (!config.Toggles.LoadTypeDefinitions) rootNodes.Add(ObjectTypeIds.BaseEventType);
                eventTypesLoaded = true;
                log.LogInformation("Loading event type hierarchy");
            }
            if (config.Toggles.LoadReferenceTypes && !referenceTypesLoaded)
            {
                mask |= (uint)NodeClass.ReferenceType;
                rootNodes.Add(ReferenceTypeIds.References);
                referenceTypesLoaded = true;
                log.LogInformation("Loading reference type hierarchy");
            }
            if (config.Toggles.LoadTypeDefinitions && !typeDefsLoaded)
            {
                mask |= (uint)NodeClass.VariableType | (uint)NodeClass.ObjectType;
                rootNodes.Add(ObjectTypeIds.BaseObjectType);
                rootNodes.Add(VariableTypeIds.BaseVariableType);
                typeDefsLoaded = true;
                log.LogInformation("Loading object type and variable type hierarchies");
            }
            if (config.Toggles.LoadTypeReferences)
            {
                referenceMode = HierarchicalReferenceMode.Forward;
                loadReferences = true;
            }

            if (rootNodes.Count == 0) return;


            var result = await source.LoadNodes(rootNodes, mask, referenceMode, "the type hierarchy", token);

            foreach (var node in result.Nodes)
            {
                NodeMap.TryAdd(node.Id, node);
                // Any nodes discovered here will be in the type hierarchy.
                node.IsChildOfType = true;
            }

            if (loadReferences)
            {
                foreach (var rf in result.References) References.Add(rf);

                var refResult = await source.LoadNonHierarchicalReferences(result.Nodes.ToDictionary(n => n.Id), true, true, "the type hierarchy", token);

                log.LogInformation("Found {Count} hierarchical and {Count2} non-hierarchical references in the type hierarchy",
                    result.References.Count(), refResult.References.Count());

                foreach (var node in refResult.Nodes)
                {
                    // These might not be in the type hierarchy. Still map them out here,
                    // but don't set IsChildOfType
                    NodeMap.TryAdd(node.Id, node);
                }
                foreach (var rf in refResult.References) References.Add(rf);
            }
        }

        public async Task LoadTypeData(ITypeAndNodeSource source, CancellationToken token)
        {
            await ReadTypeData(source, token);
            BuildTypeInfo();
        }

        public void BuildTypeInfo()
        {
            log.LogInformation("Building type information from nodes in memory");
            BuildNodeChildren();
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
                }
            }
        }

        public void AddTypeHierarchyNode(BaseUANode node)
        {
            NodeMap[node.Id] = node;
        }

        #region dataTypes
        private void BuildDataTypes()
        {
            foreach (var type in NodeMap.Values.OfType<UADataType>())
            {
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
            var whitelist = config.Events.GetWhitelist(client.Context, log);

            foreach (var type in NodeMap.Values.OfType<UAObjectType>())
            {
                if (!type.IsEventType()) continue;
                if (ignoreFilter != null && ignoreFilter.IsMatch(type.Name)) continue;
                if (whitelist != null && whitelist.Count != 0)
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
            return GetType(nodeId, x =>
            {
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
            return GetType(nodeId, x => new UAReferenceType(x), "reference");
        }

        public UAObjectType GetObjectType(NodeId nodeId)
        {
            return GetType(nodeId, x => new UAObjectType(x), "object");
        }

        public UAVariableType GetVariableType(NodeId nodeId)
        {
            return GetType(nodeId, x => new UAVariableType(x), "variable");
        }

        public UAObject GetModellingRule(NodeId nodeId, string name)
        {
            if (NodeMap.TryGetValue(nodeId, out var rule))
            {
                // This really shouldn't happen, we don't really know what to do if it does.
                if (rule is not UAObject obj) throw new ExtractorFailureException(
                    "Modelling rule is not an object. This is likely caused by an invalid server or incorrect NodeSet file"
                );
                return obj;
            }

            var res = new UAObject(nodeId, name, name, null, NodeId.Null, GetObjectType(ObjectTypeIds.ModellingRuleType));
            NodeMap.Add(nodeId, res);
            return res;
        }
        #endregion
    }
}

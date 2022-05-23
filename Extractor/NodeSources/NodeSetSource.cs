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

using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    internal class BasicReference : IReference
    {
        public NodeId? ReferenceTypeId { get; set; }
        public bool IsInverse { get; set; }
        public ExpandedNodeId? TargetId { get; set; }
    }

    internal class PlainType
    {
        public NodeId NodeId { get; set; }
        public PlainType? Parent { get; set; }
        public NodeClass NodeClass { get; set; }
        public string? DisplayName { get; set; }

        public PlainType(NodeId id, string? displayName)
        {
            NodeId = id;
            DisplayName = displayName;
        }

        public bool IsOfType(NodeId type)
        {
            PlainType? node = this;
            do
            {
                if (node.NodeId == type) return true;
                node = node.Parent;
            } while (node != null);
            return false;
        }
    }

    internal class PlainEventType : PlainType
    {
        public IList<NodeState> Properties { get; } = new List<NodeState>();
        public IEnumerable<EventField>? Fields { get; set; }
        public PlainEventType(PlainType other) : base(other.NodeId, other.DisplayName)
        {
            Parent = other.Parent;
            NodeClass = other.NodeClass;
        }
    }

    public class NodeSetSource : BaseNodeSource, IEventFieldSource
    {
        private readonly NodeStateCollection nodes = new NodeStateCollection();
        private readonly Dictionary<NodeId, NodeState> nodeDict = new Dictionary<NodeId, NodeState>();

        private readonly Dictionary<NodeId, PlainType> types = new Dictionary<NodeId, PlainType>();

        private readonly Dictionary<NodeId, IList<IReference>> references = new Dictionary<NodeId, IList<IReference>>();
        private readonly object buildLock = new object();
        private bool built;
        public NodeSetSource(ILogger<NodeSetSource> log, FullConfig config, UAExtractor extractor, UAClient client)
            : base(log, config, extractor, client)
        {
        }

        #region build
        private void LoadNodeSet(NodeSetConfig set)
        {
            if (set.Url != null)
            {
                string fileName = set.FileName ?? set.Url.Segments.Last();
                if (!File.Exists(fileName))
                {
                    using var client = new WebClient();
                    client.DownloadFile(set.Url, fileName);
                }
                set.FileName = fileName;
            }
            Log.LogInformation("Loading nodeset from {File}", set.FileName);
            LoadNodeSet(set.FileName!);
        }

        private void LoadNodeSet(string file)
        {
            using var stream = new FileStream(file, FileMode.Open, FileAccess.Read);
            var set = Opc.Ua.Export.UANodeSet.Read(stream);
            if (Config.Source.EndpointUrl == null)
            {
                Client.AddExternalNamespaces(set.NamespaceUris);
            }
            set.Import(Client.SystemContext, nodes);
        }

        private void LoadReferences()
        {
            int cnt = 0;
            // First, extract all references and group them by nodeId
            foreach (var node in nodes)
            {
                if (!references.TryGetValue(node.NodeId, out var refs))
                {
                    references[node.NodeId] = refs = new List<IReference>();
                }
                node.GetReferences(Client.SystemContext, refs);
                if (node is BaseTypeState type) refs.Add(new BasicReference
                {
                    IsInverse = true,
                    ReferenceTypeId = ReferenceTypeIds.HasSubtype,
                    TargetId = type.SuperTypeId
                });
                if (node is BaseInstanceState instance)
                {
                    if (instance.ModellingRuleId != null && !instance.ModellingRuleId.IsNullNodeId) refs.Add(new BasicReference
                    {
                        IsInverse = false,
                        ReferenceTypeId = ReferenceTypeIds.HasModellingRule,
                        TargetId = instance.ModellingRuleId
                    });
                    if (instance.TypeDefinitionId != null && !instance.TypeDefinitionId.IsNullNodeId) refs.Add(new BasicReference
                    {
                        IsInverse = false,
                        ReferenceTypeId = ReferenceTypeIds.HasTypeDefinition,
                        TargetId = instance.TypeDefinitionId
                    });
                }
                cnt += refs.Count;
            }
            // Create all inverse references
            foreach (var node in nodes)
            {
                foreach (var reference in references[node.NodeId])
                {
                    var targetId = Client.ToNodeId(reference.TargetId);
                    if (!references.TryGetValue(targetId, out var targetRefs))
                    {

                        references[targetId] = targetRefs = new List<IReference>();
                    }
                    if (!targetRefs.Any(targetRef =>
                        Client.ToNodeId(targetRef.TargetId) == node.NodeId
                        && targetRef.ReferenceTypeId == reference.ReferenceTypeId
                        && targetRef.IsInverse == !reference.IsInverse))
                    {
                        targetRefs.Add(new BasicReference
                        {
                            IsInverse = !reference.IsInverse,
                            TargetId = node.NodeId,
                            ReferenceTypeId = reference.ReferenceTypeId
                        });
                        cnt++;
                    }
                }
            }
            Log.LogInformation("Found or created {Count} references in nodeset files", cnt);
        }

        private void LoadTypeTree()
        {
            // Needs two passes since the order is not guaranteed.
            foreach (var node in nodes)
            {
                if (node.NodeClass != NodeClass.VariableType
                    && node.NodeClass != NodeClass.ObjectType
                    && node.NodeClass != NodeClass.ReferenceType
                    && node.NodeClass != NodeClass.DataType) continue;
                types[node.NodeId] = new PlainType(node.NodeId, node.DisplayName?.Text)
                {
                    NodeClass = node.NodeClass,
                    NodeId = node.NodeId,
                    DisplayName = node.DisplayName?.Text
                };

            }
            foreach (var (id, type) in types)
            {
                var parentRef = references[id].FirstOrDefault(rf =>
                    rf.ReferenceTypeId == ReferenceTypeIds.HasSubtype
                    && rf.IsInverse);
                if (parentRef != null)
                {
                    type.Parent = types.GetValueOrDefault(Client.ToNodeId(parentRef.TargetId));
                }
                if (type.NodeClass == NodeClass.DataType)
                {
                    PropertyState? enumVarNode = null;
                    foreach (var rf in references[id])
                    {
                        if (rf.ReferenceTypeId != ReferenceTypeIds.HasProperty || rf.IsInverse) continue;
                        if (!nodeDict.TryGetValue(Client.ToNodeId(rf.TargetId), out var node)) continue;
                        if (node.BrowseName?.Name != "EnumStrings" && node.BrowseName?.Name != "EnumValues") continue;
                        enumVarNode = node as PropertyState;
                        break;
                    }


                    Client.DataTypeManager.RegisterType(type.NodeId,
                        type.Parent?.NodeId ?? NodeId.Null, type.DisplayName);

                    if (enumVarNode != null)
                    {
                        Client.DataTypeManager.SetEnumStrings(type.NodeId, enumVarNode.Value);
                    }
                }
                else if ((type.NodeClass == NodeClass.ObjectType || type.NodeClass == NodeClass.VariableType)
                        && !type.IsOfType(ObjectTypeIds.BaseEventType))
                {
                    var nodeType = Client.ObjectTypeManager.GetObjectType(type.NodeId, type.NodeClass == NodeClass.VariableType);
                    nodeType.Name = type.DisplayName;
                }
                else if (type.NodeClass == NodeClass.ReferenceType && Extractor.ReferenceTypeManager != null)
                {
                    var refType = Extractor.ReferenceTypeManager.GetReferenceType(type.NodeId);
                    var state = nodeDict[type.NodeId];
                    if (state is ReferenceTypeState refState)
                    {
                        refType.SetNames(refState.DisplayName?.Text, refState.InverseName?.Text);
                    }
                }
            }
        }

        private bool IsOfType(NodeId source, NodeId parent)
        {
            if (!types.TryGetValue(source, out var type)) return false;
            return type.IsOfType(parent);
        }

        private IEnumerable<IReference> Browse(NodeId node, NodeId referenceTypeId, BrowseDirection direction, bool allowSubTypes)
        {
            var refs = references[node];
            foreach (var reference in refs)
            {
                if (!allowSubTypes && referenceTypeId != reference.ReferenceTypeId) continue;
                else if (allowSubTypes && !IsOfType(reference.ReferenceTypeId, referenceTypeId)) continue;

                if (reference.IsInverse && direction != BrowseDirection.Inverse
                    && direction != BrowseDirection.Both) continue;
                else if (!reference.IsInverse && direction != BrowseDirection.Forward
                    && direction != BrowseDirection.Both) continue;

                yield return reference;
            }
        }

        private bool BuildNode(NodeId id, NodeId parent)
        {
            var node = nodeDict[id];
            if (node.NodeClass == NodeClass.Variable
                || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.VariableType)
            {
                var variable = new UAVariable(id, node.DisplayName?.Text ?? "", parent, node.NodeClass);
                if (node is BaseVariableState varState)
                {
                    variable.VariableAttributes.AccessLevel = varState.AccessLevel;
                    variable.VariableAttributes.ArrayDimensions =
                        varState.ArrayDimensions == null || !varState.ArrayDimensions.Any()
                        ? null
                        : varState.ArrayDimensions.Select(val => (int)val).ToArray();
                    variable.VariableAttributes.ValueRank = varState.ValueRank;
                    variable.VariableAttributes.Description = varState.Description?.Text;
                    variable.VariableAttributes.Historizing = varState.Historizing;
                    variable.SetNodeType(Client, varState.TypeDefinitionId);
                    variable.VariableAttributes.DataType = Client.DataTypeManager.GetDataType(varState.DataType);
                    variable.SetDataPoint(new Variant(varState.Value));
                    if (Config.History.Enabled && Config.History.Data)
                    {
                        if (Config.Subscriptions.IgnoreAccessLevel)
                        {
                            variable.VariableAttributes.ReadHistory = varState.Historizing;
                        }
                        else
                        {
                            variable.VariableAttributes.ReadHistory = (varState.AccessLevel & AccessLevels.HistoryRead) != 0;
                        }
                    }
                    variable.VariableAttributes.ShouldSubscribeData = Config.Subscriptions.DataPoints && (
                        Config.Subscriptions.IgnoreAccessLevel
                        || (varState.AccessLevel & AccessLevels.CurrentRead) != 0);
                }
                else if (node is BaseVariableTypeState typeState)
                {
                    variable.VariableAttributes.ArrayDimensions =
                        typeState.ArrayDimensions == null || !typeState.ArrayDimensions.Any()
                        ? null
                        : typeState.ArrayDimensions.Select(val => (int)val).ToArray();
                    variable.VariableAttributes.ValueRank = typeState.ValueRank;
                    variable.VariableAttributes.Description = typeState.Description?.Text;
                    variable.SetDataPoint(new Variant(typeState.Value));
                    variable.ValueRead = true;
                    variable.VariableAttributes.DataType = Client.DataTypeManager.GetDataType(typeState.DataType);
                    variable.VariableAttributes.ShouldSubscribeData = false;
                }
                NodeMap[id] = variable;
                return true;
            }
            else if (node.NodeClass == NodeClass.Object
                || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.ObjectType)
            {
                if (id == ObjectIds.Server || id == ObjectIds.Aliases) return false;
                var obj = new UANode(id, node.DisplayName?.Text ?? "", parent, node.NodeClass);
                if (node is BaseObjectState objState)
                {
                    obj.Attributes.Description = objState.Description?.Text;
                    obj.Attributes.EventNotifier = objState.EventNotifier;
                    obj.SetNodeType(Client, objState.TypeDefinitionId);
                }
                else if (node is BaseObjectTypeState typeState)
                {
                    obj.Attributes.Description = typeState.Description?.Text;
                }
                NodeMap[id] = obj;
                return true;
            }
            return false;
        }

        public void Build()
        {
            lock (buildLock)
            {
                if (built) return;
                foreach (var set in Config.Source.NodeSetSource!.NodeSets!)
                {
                    LoadNodeSet(set);
                }
                foreach (var node in nodes)
                {
                    nodeDict[node.NodeId] = node;
                }
                LoadReferences();
                LoadTypeTree();
                built = true;
            }
        }


        /// <summary>
        /// Construct 
        /// </summary>
        public void BuildNodes(IEnumerable<NodeId> rootNodes)
        {
            Build();

            NodeMap.Clear();
            var visitedNodes = new HashSet<NodeId>();

            // Simulate browsing the node hierarchy. We do it this way to ensure that we visit the correct nodes.
            var nextIds = new HashSet<NodeId>();

            foreach (var id in rootNodes)
            {
                visitedNodes.Add(id);
                if (BuildNode(id, NodeId.Null))
                {
                    nextIds.Add(id);
                }
            }

            while (nextIds.Any())
            {
                var refs = new List<(IReference Node, NodeId ParentId)>();

                foreach (var id in nextIds)
                {
                    var children = Browse(id, ReferenceTypeIds.HierarchicalReferences, BrowseDirection.Forward, true);
                    refs.AddRange(children.Select(child => (child, id)));
                }

                nextIds.Clear();
                foreach (var (child, parent) in refs)
                {
                    var childId = Client.ToNodeId(child.TargetId);
                    if (visitedNodes.Add(childId) && BuildNode(childId, parent))
                    {
                        nextIds.Add(childId);
                    }
                }
            }
        }
        #endregion

        #region parse

        public override async Task<NodeSourceResult?> ParseResults(CancellationToken token)
        {
            if (!NodeMap.Any()) return null;

            RawObjects.Clear();
            RawVariables.Clear();
            FinalDestinationObjects.Clear();
            FinalDestinationVariables.Clear();
            FinalReferences.Clear();
            FinalSourceObjects.Clear();
            FinalSourceVariables.Clear();

            var properties = new List<UAVariable>();

            foreach (var node in NodeMap.Values)
            {
                SortNode(node);
                node.Attributes.DataRead = true;
                if ((node.IsProperty || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.VariableType)
                    && (node is UAVariable variable))
                {
                    properties.Add(variable);
                }
            }
            if (Config.Source.EndpointUrl != null) await Client.ReadNodeValues(properties, token);

            if (Config.Extraction.DataTypes.MaxArraySize != 0 && Config.Extraction.DataTypes.EstimateArraySizes == true)
            {
                await EstimateArraySizes(RawVariables, token);
            }

            var update = Config.Extraction.Update;
            var mappedObjects = RawObjects.Where(obj => FilterObject(update.Objects, obj)).ToList();
            FinalDestinationObjects.AddRange(mappedObjects);
            FinalSourceObjects.AddRange(mappedObjects);
            foreach (var variable in RawVariables)
            {
                SortVariable(update.Variables, variable);
            }

            foreach (var node in FinalSourceObjects.Concat(FinalSourceVariables))
            {
                InitNodeState(update, node);
            }

            if (Config.Extraction.Relationships.Enabled)
            {
                GetRelationshipData(FinalSourceObjects.Concat(FinalSourceVariables));
            }

            NodeMap.Clear();

            if (!FinalDestinationObjects.Any() && !FinalDestinationVariables.Any() && !FinalSourceVariables.Any() && !FinalReferences.Any())
            {
                Log.LogInformation("Mapping resulted in no new nodes");
                return null;
            }

            Log.LogInformation("Mapping resulted in {ObjCount} destination objects and {TsCount} destination timeseries," +
                " {SourceObj} objects and {SourceVar} variables.",
                FinalDestinationObjects.Count, FinalDestinationVariables.Count,
                FinalSourceObjects.Count, FinalSourceVariables.Count);

            if (FinalReferences.Any())
            {
                Log.LogInformation("Found a total of {Count} references", FinalReferences.Count);
            }

            return new NodeSourceResult(
                FinalSourceObjects,
                FinalSourceVariables,
                FinalDestinationObjects,
                FinalDestinationVariables,
                FinalReferences);
        }

        private void GetRelationshipData(IEnumerable<UANode> mappedNodes)
        {
            var nodeMap = mappedNodes.ToDictionary(node => node.Id);

            foreach (var (id, refs) in references)
            {
                if (!nodeMap.TryGetValue(id, out var node)) continue;
                bool sourceIsTs = node is UAVariable variable && !variable.IsObject;
                foreach (var rf in refs)
                {
                    bool isHierarchical = IsOfType(rf.ReferenceTypeId, ReferenceTypeIds.HierarchicalReferences);

                    if (!nodeMap.TryGetValue(Client.ToNodeId(rf.TargetId), out var target)) continue;
                    bool targetIsTs = target is UAVariable targetVariable && !targetVariable.IsObject;

                    var reference = new UAReference(
                        type: Client.ToNodeId(rf.ReferenceTypeId),
                        isForward: !rf.IsInverse,
                        source: id,
                        target: target.Id,
                        sourceIsTs,
                        targetIsTs,
                        isHierarchical,
                        manager: Extractor.ReferenceTypeManager!);

                    if (!FilterReference(nodeMap, reference, true)) continue;

                    FinalReferences.Add(reference);
                }
            }
        }

        #endregion

        #region event-types

        private IEnumerable<EventField> ToFields(NodeId parent, NodeState state)
        {
            if (parent == ObjectTypeIds.BaseEventType && baseExcludeProperties!.Contains(state.BrowseName.Name)
                        || excludeProperties!.Contains(state.BrowseName.Name)) yield break;

            var refs = references[state.NodeId];
            var children = refs
                .Where(rf => !rf.IsInverse && IsOfType(rf.ReferenceTypeId, ReferenceTypeIds.HierarchicalReferences))
                .Select(rf => nodeDict.GetValueOrDefault(Client.ToNodeId(rf.TargetId)))
                .Where(node => node != null && (node.NodeClass == NodeClass.Object || node.NodeClass == NodeClass.Variable))
                .ToList();
            if (state.NodeClass == NodeClass.Object && !children.Any()) yield break;
            else if (state.NodeClass != NodeClass.Variable && state.NodeClass != NodeClass.Object) yield break;

            if (state.NodeClass == NodeClass.Variable)
            {
                yield return new EventField(state.BrowseName);
            }
            foreach (var child in children)
            {

                var childFields = ToFields(state.NodeId, child);
                foreach (var childField in childFields)
                {
                    childField.BrowsePath.Insert(0, state.BrowseName);
                    yield return childField;
                }
            }
        }

        private IEnumerable<EventField> CollectFields(PlainEventType type)
        {
            if (type.Fields != null) return type.Fields;

            var fields = new List<EventField>();

            if (type.Parent is PlainEventType parent)
            {
                fields.AddRange(CollectFields(parent));
            }
            foreach (var child in type.Properties)
            {
                fields.AddRange(ToFields(type.NodeId, child));
            }
            type.Fields = fields;
            return fields;
        }

        private HashSet<string>? excludeProperties;
        private HashSet<string>? baseExcludeProperties;

        public Task<Dictionary<NodeId, UAEventType>> GetEventIdFields(CancellationToken token)
        {
            Build();

            excludeProperties = new HashSet<string>(Config.Events.ExcludeProperties);
            baseExcludeProperties = new HashSet<string>(Config.Events.BaseExcludeProperties);

            var evtTypes = types.Where(type => type.Value.NodeClass == NodeClass.ObjectType
                && IsOfType(type.Key, ObjectTypeIds.BaseEventType)).ToDictionary(kvp => kvp.Key, kvp => new PlainEventType(kvp.Value));

            foreach (var (id, type) in evtTypes)
            {
                if (type.NodeId != ObjectTypeIds.BaseEventType)
                {
                    type.Parent = evtTypes[type.Parent!.NodeId];
                }
                else
                {
                    type.Parent = null;
                }

                var refs = references[id];
                var children = refs
                    .Where(rf => !rf.IsInverse && IsOfType(rf.ReferenceTypeId, ReferenceTypeIds.HierarchicalReferences))
                    .Select(rf => nodeDict.GetValueOrDefault(Client.ToNodeId(rf.TargetId)))
                    .Where(node => node != null && (node.NodeClass == NodeClass.Object || node.NodeClass == NodeClass.Variable))
                    .ToList();

                foreach (var child in children) type.Properties.Add(child);
            }

            HashSet<NodeId>? whitelist = null;
            if (Config.Events.EventIds != null && Config.Events.EventIds.Any())
            {
                whitelist = new HashSet<NodeId>(Config.Events.EventIds.Select(proto => proto.ToNodeId(Client, ObjectTypeIds.BaseEventType)));
            }
            Regex? ignoreFilter = Config.Events.ExcludeEventFilter == null ? null : new Regex(Config.Events.ExcludeEventFilter);

            var result = new Dictionary<NodeId, UAEventType>();

            foreach (var (id, type) in evtTypes)
            {
                if (type.DisplayName != null && ignoreFilter != null && ignoreFilter.IsMatch(type.DisplayName)) continue;
                if (whitelist != null && whitelist.Any())
                {
                    if (!whitelist.Contains(type.NodeId)) continue;
                }
                else if (!Config.Events.AllEvents && type.NodeId.NamespaceIndex == 0) continue;
                result[type.NodeId] = new UAEventType(type.NodeId, type.DisplayName, CollectFields(type));
            }

            return Task.FromResult(result);
        }
        #endregion
    }
}

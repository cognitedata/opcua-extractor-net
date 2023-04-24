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
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
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

    public class NodeSetSource : BaseNodeSource
    {
        private readonly NodeStateCollection nodes = new NodeStateCollection();
        private readonly Dictionary<NodeId, NodeState> nodeDict = new Dictionary<NodeId, NodeState>();

        private readonly Dictionary<NodeId, Dictionary<(NodeId, NodeId, bool), IReference>> references = new Dictionary<NodeId, Dictionary<(NodeId, NodeId, bool), IReference>>();
        private readonly object buildLock = new object();
        private bool built;
        private bool isFullBrowse;
        public NodeSetSource(ILogger<NodeSetSource> log, FullConfig config, UAExtractor extractor, UAClient client, TypeManager typeManager)
            : base(log, config, extractor, client, typeManager)
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
            Log.LogDebug("Imported nodeset from file {File}, buiding internal data structures", file);
        }

        private void AddReference(Dictionary<(NodeId, NodeId, bool), IReference> dict, IReference refr)
        {
            dict[(Client.ToNodeId(refr.TargetId), refr.ReferenceTypeId, refr.IsInverse)] = refr;
        }

        private void LoadReferences()
        {
            int cnt = 0;
            // First, extract all references and group them by nodeId
            foreach (var node in nodes)
            {
                if (!references.TryGetValue(node.NodeId, out var refs))
                {
                    references[node.NodeId] = refs = new Dictionary<(NodeId, NodeId, bool), IReference>();
                }
                var rawRefs = new List<IReference>();
                node.GetReferences(Client.SystemContext, rawRefs);
                foreach (var rf in rawRefs)
                {
                    AddReference(refs, rf);
                    // if (!rf.IsInverse) Log.LogTrace("Add regular reference {Parent} -> {Child}", node.NodeId, rf.TargetId);
                    // else Log.LogTrace("Add regular reference {Parent} -> {Child}", rf.TargetId, node.NodeId);
                }

                if (node is BaseTypeState type && type.SuperTypeId != null && !type.SuperTypeId.IsNullNodeId)
                {
                    AddReference(refs, new BasicReference
                    {
                        IsInverse = true,
                        ReferenceTypeId = ReferenceTypeIds.HasSubtype,
                        TargetId = type.SuperTypeId
                    });
                    // Log.LogTrace("Add supertype reference from {Parent} -> {Child}", type.SuperTypeId, node.NodeId);
                }
                if (node is BaseInstanceState instance)
                {
                    if (instance.ModellingRuleId != null && !instance.ModellingRuleId.IsNullNodeId) AddReference(refs, new BasicReference
                    {
                        IsInverse = false,
                        ReferenceTypeId = ReferenceTypeIds.HasModellingRule,
                        TargetId = instance.ModellingRuleId
                    });
                    if (instance.TypeDefinitionId != null && !instance.TypeDefinitionId.IsNullNodeId) AddReference(refs, new BasicReference
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
                foreach (var reference in references[node.NodeId].Values)
                {
                    var targetId = Client.ToNodeId(reference.TargetId);
                    if (!references.TryGetValue(targetId, out var targetRefs))
                    {
                        references[targetId] = targetRefs = new Dictionary<(NodeId, NodeId, bool), IReference>();
                    }
                    if (!targetRefs.ContainsKey((node.NodeId, reference.ReferenceTypeId, !reference.IsInverse)))
                    {
                        if (reference.IsInverse)
                        {
                            // Log.LogTrace("Add reference {P} -> {T} of type {Typ}", targetId, node.NodeId, reference.ReferenceTypeId);
                        }
                        AddReference(targetRefs, new BasicReference
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

        private bool IsOfType(NodeId source, NodeId parent)
        {
            if (!TypeManager.NodeMap.TryGetValue(source, out var node)) return false;
            if (node is not BaseUAType type) return false;

            return type.IsChildOf(parent);
        }

        private IEnumerable<IReference> Browse(NodeId node, NodeId referenceTypeId, BrowseDirection direction, bool allowSubTypes)
        {
            var refs = references[node].Values;
            foreach (var reference in refs)
            {
                if (!allowSubTypes && referenceTypeId != reference.ReferenceTypeId) continue;
                else if (allowSubTypes && !IsOfType(reference.ReferenceTypeId, referenceTypeId))
                {
                    // Log.LogTrace("Skipping reference from {N} to {T} due to bad reference type {Typ}", node, reference.TargetId, reference.ReferenceTypeId);
                    continue;
                }

                if (reference.IsInverse && direction != BrowseDirection.Inverse
                    && direction != BrowseDirection.Both) continue;
                else if (!reference.IsInverse && direction != BrowseDirection.Forward
                    && direction != BrowseDirection.Both) continue;

                // Log.LogTrace("Discovered reference from {N} to {T} of type {Typ}", node, reference.TargetId, reference.ReferenceTypeId);
                yield return reference;
            }
        }

        private bool BuildNode(NodeId id, NodeId parent)
        {
            var node = nodeDict[id];

            var res = BaseUANode.FromNodeState(node, parent, TypeManager);
            if (res != null)
            {
                return TryAdd(res);
            }
            return false;
        }

        private bool BuildType(NodeId id, NodeId parent)
        {
            var node = nodeDict[id];
            var res = BaseUANode.FromNodeState(node, parent, TypeManager);
            if (res != null) TypeManager.AddTypeHierarchyNode(res);
            return true;
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
                Log.LogInformation("Loading references into internal data structure");
                LoadReferences();
                Log.LogInformation("Server built, resulted in a total of {Nodes} nodes", nodes.Count);
                built = true;
            }
        }

        private void BrowseHierarchy(IEnumerable<NodeId> rootIds, Func<NodeId, NodeId, bool> callback)
        {
            var visitedNodes = new HashSet<NodeId>();
            visitedNodes.Add(ObjectIds.Server);

            var nextIds = new HashSet<NodeId>();
            foreach (var id in rootIds)
            {
                visitedNodes.Add(id);
                if (callback(id, NodeId.Null))
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
                    if (visitedNodes.Add(childId) && callback(childId, parent))
                    {
                        nextIds.Add(childId);
                    }
                }
            }
        }

        private void LoadReferenceTypes()
        {
            foreach (var node in nodeDict.Values.OfType<ReferenceTypeState>())
            {
                BuildType(node.NodeId, node.SuperTypeId);
            }
        }

        /// <summary>
        /// Construct 
        /// </summary>
        public void BuildNodes(IEnumerable<NodeId> rootNodes, bool isFullBrowse)
        {
            Build();
            this.isFullBrowse = isFullBrowse;

            ClearRaw();
            // Build full type hierarchy
            LoadReferenceTypes();
            // First pass builds reference types, second builds the remaining types.
            // We can't properly browse the type hierarchy without loading the reference types first.
            TypeManager.BuildTypeInfo();

            Log.LogInformation("Browse types folder");
            BrowseHierarchy(new[] { ObjectIds.TypesFolder }, BuildType);
            TypeManager.BuildTypeInfo();

            Log.LogInformation("Browse root nodes {Nodes}", string.Join(", ", rootNodes));
            BrowseHierarchy(rootNodes, BuildNode);

            if (Config.Source.NodeSetSource!.Types)
            {
                TypeManager.SetTypesRead();
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

            foreach (var node in NodeList)
            {
                SortNode(node);
                node.Attributes.IsDataRead = true;
                if (node.IsProperty && (node is UAVariable variable))
                {
                    properties.Add(variable);
                }
            }
            if (Config.Source.EndpointUrl != null) await Client.ReadNodeValues(properties, token);

            if (Config.Extraction.DataTypes.MaxArraySize != 0 && Config.Extraction.DataTypes.EstimateArraySizes)
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

            var usesFdm = Config.Cognite?.FlexibleDataModels?.Enabled ?? false;

            if (Config.Extraction.Relationships.Enabled)
            {
                GetRelationshipData(usesFdm, usesFdm ? ModellingRules : new HashSet<NodeId>());
            }

            ClearRaw();

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
                FinalReferences,
                isFullBrowse);
        }

        private void GetRelationshipData(bool getPropertyReferences, HashSet<NodeId> additionalKnownNodes)
        {
            foreach (var (id, refs) in references)
            {
                var parentNode = Extractor.State.GetMappedNode(id);
                if (parentNode == null) continue;

                foreach (var rf in refs.Values)
                {
                    bool isHierarchical = IsOfType(rf.ReferenceTypeId, ReferenceTypeIds.HierarchicalReferences);

                    var childNode = Extractor.State.GetMappedNode(Client.ToNodeId(rf.TargetId));
                    if (childNode == null) continue;

                    var reference = new UAReference(
                        type: Client.ToNodeId(rf.ReferenceTypeId),
                        isForward: !rf.IsInverse,
                        source: id,
                        target: childNode.Id,
                        sourceTs: !parentNode.IsObject,
                        targetTs: !childNode.IsObject,
                        isHierarchical,
                        manager: TypeManager);

                    if (!FilterReference(reference, getPropertyReferences, additionalKnownNodes)) continue;

                    FinalReferences.Add(reference);
                }
            }
        }

        #endregion
    }
}

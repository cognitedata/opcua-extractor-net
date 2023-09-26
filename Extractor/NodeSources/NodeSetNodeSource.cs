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

using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
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

    public class NodeSetNodeSource : ITypeAndNodeSource
    {
        private readonly ILogger logger;
        private readonly FullConfig config;
        private readonly UAClient client;
        private readonly TypeManager typeManager;

        private readonly NodeStateCollection nodes = new NodeStateCollection();
        private readonly Dictionary<NodeId, NodeState> nodeDict = new Dictionary<NodeId, NodeState>();
        private readonly Dictionary<NodeId, Dictionary<(NodeId, NodeId, bool), IReference>> references = new Dictionary<NodeId, Dictionary<(NodeId, NodeId, bool), IReference>>();
        private readonly object buildLock = new object();

        private Dictionary<NodeId, BaseUANode> nodeMap = new();
        private List<UAReference> finalReferences = new();

        private bool isInitialized;

        public NodeSetNodeSource(ILogger logger, FullConfig config, UAClient client, TypeManager typeManager)
        {
            this.logger = logger;
            this.config = config;
            this.client = client;
            this.typeManager = typeManager;
        }


        public Task Initialize(CancellationToken token)
        {
            return Task.Run(() =>
            {
                lock (buildLock)
                {
                    if (isInitialized) return;
                    foreach (var set in config.Source.NodeSetSource!.NodeSets!)
                    {
                        LoadNodeSet(set);
                    }
                    foreach (var node in nodes)
                    {
                        nodeDict[node.NodeId] = node;
                    }
                    logger.LogInformation("Loading references into internal data structure");
                    LoadReferences();
                    logger.LogInformation("Server built, resulted in a total of {Nodes} nodes", nodes.Count);
                    isInitialized = true;
                }
            });
        }

        public Task<NodeLoadResult> LoadNodes(IEnumerable<NodeId> nodesToBrowse, uint nodeClassMask, HierarchicalReferenceMode hierarchicalReferences, CancellationToken token)
        {
            // First we need to build reference types, otherwise we can't browse.
            // Under normal browse the server keeps track of all the reference types, but in this case we
            // are the server.
            return Task.Run(() =>
            {
                LoadReferenceTypes();

                BrowseHierarchy(nodesToBrowse);

                return TakeResults(false);
            });
        }

        public Task<NodeLoadResult> LoadNonHierarchicalReferences(
            IReadOnlyDictionary<NodeId, BaseUANode> parentNodes,
            bool getTypeReferences,
            bool initUnknownNodes,
            CancellationToken token)
        {
            return Task.Run(() =>
            {
                foreach (var node in parentNodes.Values)
                {
                    if (!references.TryGetValue(node.Id, out var outReferences)) continue;
                    foreach (var rf in outReferences.Values)
                    {
                        var childId = client.ToNodeId(rf.TargetId);
                        var childNode = nodeMap.GetValueOrDefault(childId);
                        if (childNode == null)
                        {
                            if (initUnknownNodes)
                            {
                                if (!nodeDict.ContainsKey(childId)) continue;
                                BuildNode(childId, NodeId.Null);
                                childNode = nodeMap.GetValueOrDefault(childId);

                                if (childNode == null) continue;
                            }
                            else
                            {
                                logger.LogTrace("Skipping reference from {Parent} to {Child} due to missing child node", node.Id, rf.TargetId);
                                continue;
                            }
                        }

                        var reference = new UAReference(
                            typeManager.GetReferenceType(rf.ReferenceTypeId),
                            !rf.IsInverse,
                            node,
                            childNode);

                        finalReferences.Add(reference);
                    }
                }

                return TakeResults(false);
            });
        }

        public async Task LoadTypeMetadata(IEnumerable<BaseUANode> nodes, DataTypeConfig config, CancellationToken token)
        {
            foreach (var node in nodes)
            {
                if (nodeDict.TryGetValue(node.Id, out var nodeState))
                {
                    if (!node.UpdateFromNodeState(nodeState, typeManager))
                    {
                        logger.LogWarning("Mismatched node class when initializing type {Id} from node set. NodeSet state was {Type}, expected {Expected}",
                            node.Id, nodeState.NodeClass, node.NodeClass);
                    }
                }
            }

            if (this.config.Source.EndpointUrl != null)
            {
                await client.ReadNodeValues(nodes.Where(n => n.AllowValueRead(logger, config)), token);
            }
        }

        private NodeLoadResult TakeResults(bool assumeFullyTransformed)
        {
            var ret = new NodeLoadResult(nodeMap, finalReferences, assumeFullyTransformed, config.Source.AltSourceBackgroundBrowse);
            nodeMap = new();
            finalReferences = new();
            return ret;
        }

        #region initialization
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
            logger.LogInformation("Loading nodeset from {File}", set.FileName);
            LoadNodeSet(set.FileName!);
        }

        private void LoadNodeSet(string file)
        {
            using var stream = new FileStream(file, FileMode.Open, FileAccess.Read);
            var set = Opc.Ua.Export.UANodeSet.Read(stream);
            if (config.Source.EndpointUrl == null)
            {
                client.AddExternalNamespaces(set.NamespaceUris);
            }
            logger.LogDebug("Import nodeset into common node collection");
            set.Import(client.SystemContext, nodes);
            logger.LogDebug("Imported nodeset from file {File}, buiding internal data structures", file);
        }

        private void AddReference(Dictionary<(NodeId, NodeId, bool), IReference> dict, IReference refr)
        {
            dict[(client.ToNodeId(refr.TargetId), refr.ReferenceTypeId, refr.IsInverse)] = refr;
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
                node.GetReferences(client.SystemContext, rawRefs);
                foreach (var rf in rawRefs)
                {
                    AddReference(refs, rf);
                }

                if (node is BaseTypeState type && type.SuperTypeId != null && !type.SuperTypeId.IsNullNodeId)
                {
                    AddReference(refs, new BasicReference
                    {
                        IsInverse = true,
                        ReferenceTypeId = ReferenceTypeIds.HasSubtype,
                        TargetId = type.SuperTypeId
                    });
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
                    var targetId = client.ToNodeId(reference.TargetId);
                    if (!references.TryGetValue(targetId, out var targetRefs))
                    {
                        references[targetId] = targetRefs = new Dictionary<(NodeId, NodeId, bool), IReference>();
                    }
                    if (!targetRefs.ContainsKey((node.NodeId, reference.ReferenceTypeId, !reference.IsInverse)))
                    {
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
            logger.LogInformation("Found or created {Count} references in nodeset files", cnt);
        }
        #endregion

        #region browse
        private bool IsOfType(NodeId source, NodeId parent)
        {
            if (!typeManager.NodeMap.TryGetValue(source, out var node)) return false;
            if (node is not BaseUAType type) return false;

            return type.IsChildOf(parent);
        }
        private IEnumerable<IReference> Browse(NodeId node, NodeId referenceTypeId, BrowseDirection direction, bool allowSubTypes)
        {
            var refs = references[node].Values;
            foreach (var reference in refs)
            {
                if (!allowSubTypes && referenceTypeId != reference.ReferenceTypeId) continue;
                if (allowSubTypes && !IsOfType(reference.ReferenceTypeId, referenceTypeId)) continue;

                if (reference.IsInverse && direction != BrowseDirection.Inverse
                    && direction != BrowseDirection.Both) continue;
                else if (!reference.IsInverse && direction != BrowseDirection.Forward
                    && direction != BrowseDirection.Both) continue;

                yield return reference;
            }
        }
        private void BrowseHierarchy(IEnumerable<NodeId> rootIds)
        {
            var visitedNodes = new HashSet<NodeId>();
            visitedNodes.Add(ObjectIds.Server);

            var nextIds = new HashSet<NodeId>();
            foreach (var id in rootIds)
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
                    var childId = client.ToNodeId(child.TargetId);
                    if (visitedNodes.Add(childId) && BuildNode(childId, parent))
                    {
                        nextIds.Add(childId);
                    }
                }
            }
        }

        private bool referenceTypesLoaded = false;

        private void LoadReferenceTypes()
        {
            if (referenceTypesLoaded) return;
            referenceTypesLoaded = true;
            foreach (var node in nodeDict.Values.OfType<ReferenceTypeState>())
            {
                var res = BaseUANode.FromNodeState(node, node.SuperTypeId, typeManager);
                if (res != null) typeManager.AddTypeHierarchyNode(res);
            }

            typeManager.BuildNodeChildren();
        }

        private bool BuildNode(NodeId id, NodeId parentId)
        {
            var nodeState = nodeDict[id];

            var node = BaseUANode.FromNodeState(nodeState, parentId, typeManager);
            var added = false;
            if (node != null)
            {
                added = TryAdd(node);

                if (node.Parent == null)
                {
                    node.Parent = nodeMap.GetValueOrDefault(parentId);
                }
            }

            return added;
        }

        private bool TryAdd(BaseUANode node)
        {
            if (!config.Extraction.NodeTypes.AsNodes && node.IsType) return false;

            return nodeMap.TryAdd(node.Id, node);
        }
        #endregion
    }
}

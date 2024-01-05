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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    public class UANodeSource : ITypeAndNodeSource
    {
        private readonly ILogger logger;
        private readonly UAExtractor extractor;
        private readonly UAClient client;
        private readonly TypeManager typeManager;

        private UANodeCollection nodeMap = new();
        private HashSet<UAReference> references = new();

        private HierarchicalReferenceMode hierarchicalReferences = HierarchicalReferenceMode.Disabled;

        public UANodeSource(ILogger logger, UAExtractor extractor, UAClient client, TypeManager typeManager)
        {
            this.logger = logger;
            this.extractor = extractor;
            this.client = client;
            this.typeManager = typeManager;
        }

        private NodeLoadResult TakeResults(bool assumeFullyTransformed)
        {
            var ret = new NodeLoadResult(nodeMap, references, assumeFullyTransformed, false);
            nodeMap = new();
            references = new();
            return ret;
        }

        public Task Initialize(CancellationToken token)
        {
            return Task.CompletedTask;
        }


        public async Task<NodeLoadResult> LoadNodes(
            IEnumerable<NodeId> nodesToBrowse,
            uint nodeClassMask,
            HierarchicalReferenceMode hierarchicalReferences,
            string purpose,
            CancellationToken token)
        {
            this.hierarchicalReferences = hierarchicalReferences;

            await client.Browser.BrowseNodeHierarchy(nodesToBrowse, HandleNode, token, purpose, nodeClassMask);

            if (nodeMap.Count != 0) await client.ReadNodeData(nodeMap, token, purpose);

            return TakeResults(false);
        }

        public async Task<NodeLoadResult> LoadNonHierarchicalReferences(
            IReadOnlyDictionary<NodeId, BaseUANode> knownNodes,
            bool getTypeReferences,
            bool initUnknownNodes,
            string purpose,
            CancellationToken token)
        {
            await LoadNonHierarchicalReferencesInternal(knownNodes, getTypeReferences, initUnknownNodes, purpose, token);

            if (nodeMap.Count != 0) await client.ReadNodeData(nodeMap, token, "new non-hierarchical instances");

            logger.LogDebug("Is mandatory in nodemap? {Yes}", nodeMap.FirstOrDefault(n => n.Id == ObjectIds.ModellingRule_Mandatory));

            return TakeResults(false);
        }

        public async Task LoadTypeMetadata(IEnumerable<BaseUANode> nodes, DataTypeConfig config, CancellationToken token)
        {
            await client.ReadNodeData(nodes, token, "the type hierarchy");
            await client.ReadNodeValues(nodes.Where(n => n.AllowValueRead(logger, config, true)), token);
        }

        #region Support

        private async Task LoadNonHierarchicalReferencesInternal(
            IReadOnlyDictionary<NodeId, BaseUANode> knownNodes,
            bool getTypeReferences,
            bool initUnknownNodes,
            string purpose,
            CancellationToken token)
        {
            if (!knownNodes.Any()) return;

            var nodesToQuery = knownNodes.Keys.Select(n => new BrowseNode(n)).ToDictionary(n => n.Id);
            var classMask = NodeClass.Object | NodeClass.Variable;
            if (getTypeReferences)
            {
                classMask |= NodeClass.ObjectType | NodeClass.VariableType | NodeClass.DataType | NodeClass.ReferenceType;
            }

            var baseParams = new BrowseParams
            {
                BrowseDirection = BrowseDirection.Both,
                NodeClassMask = (uint)classMask,
                ReferenceTypeId = ReferenceTypeIds.NonHierarchicalReferences,
                Nodes = nodesToQuery
            };

            var foundReferences = await client.Browser.BrowseLevel(baseParams, token, purpose: purpose);

            int count = 0;
            foreach (var (parentId, children) in foundReferences)
            {
                var parentNode = knownNodes.GetValueOrDefault(parentId);

                if (parentNode == null)
                {
                    logger.LogWarning("Got reference from unknown node: {Id}", parentId);
                    continue;
                }

                foreach (var child in children)
                {
                    var childId = client.ToNodeId(child.NodeId);
                    var childNode = knownNodes.GetValueOrDefault(childId) ?? nodeMap.GetValueOrDefault(childId);

                    if (childNode == null)
                    {
                        if (extractor.State.IsMappedNode(childId))
                        {
                            // Corner case, this happens if we are browsing a subset of the node hierarchy
                            // and find a reference to a node outside of the subset we're looking at.
                            logger.LogTrace("Found reference to previously mapped node: {Id}, {Name}", childId, child.DisplayName);
                            childNode = BaseUANode.Create(child, NodeId.Null, null, client, typeManager);
                        }
                        else if (initUnknownNodes)
                        {
                            // Do not create nodes from inverse references.
                            if (!child.IsForward) continue;

                            logger.LogTrace("Found reference to unknown node, adding: {Id}, {Name}", childId, child.DisplayName);
                            childNode = BaseUANode.Create(child, NodeId.Null, null, client, typeManager);
                            if (childNode != null) nodeMap.Add(childNode);
                        }
                        else
                        {
                            logger.LogTrace("Skipping reference from {Parent} to {Child} due to missing child node", parentId, childId);
                            continue;
                        }

                        if (childNode == null) continue;
                    }

                    var rf = new UAReference(
                        typeManager.GetReferenceType(child.ReferenceTypeId),
                        child.IsForward,
                        parentNode,
                        childNode);

                    references.Add(rf);
                    count++;
                }
            }

            logger.LogInformation("Found {Count} non-hierarchical references", count);
        }

        private void HandleNode(ReferenceDescription node, NodeId parentId, bool visited)
        {
            BaseUANode? mapped;

            if (!visited)
            {
                var parent = nodeMap.GetValueOrDefault(parentId);
                var result = BaseUANode.Create(node, parentId, parent, client, typeManager);
                if (result == null)
                {
                    logger.LogWarning("Node of unexpected type received: {Type}, {Id}", node.NodeClass, node.NodeId);
                    return;
                }
                mapped = result;

                logger.LogTrace("Handle node {Name}, {Id}: {Class}", mapped.Name, mapped.Id, mapped.NodeClass);
                nodeMap.TryAdd(mapped);
            }
            else
            {
                mapped = nodeMap.GetValueOrDefault(client.ToNodeId(node.NodeId));
                if (mapped != null && mapped.ParentId.IsNullNodeId)
                {
                    mapped.Parent = nodeMap.GetValueOrDefault(parentId);
                }
            }

            if (mapped != null && (
                hierarchicalReferences == HierarchicalReferenceMode.Forward
                || hierarchicalReferences == HierarchicalReferenceMode.Both))
            {
                if (parentId == null || parentId.IsNullNodeId || !nodeMap.TryGetValue(parentId, out var parent)) return;

                var rf = new UAReference(typeManager.GetReferenceType(node.ReferenceTypeId), true, parent, mapped);

                references.Add(rf);
                if (hierarchicalReferences == HierarchicalReferenceMode.Both)
                {
                    references.Add(rf.CreateInverse());
                }
            }
        }
        #endregion
    }
}

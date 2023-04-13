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

using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    /// <summary>
    /// Contains the results of a browse operation, and parses the nodes to produce
    /// lists of nodes that should be mapped to destinations.
    /// </summary>
    public class UANodeSource : BaseNodeSource
    {
        private bool parsed;

        private readonly List<(ReferenceDescription Node, NodeId ParentId)> references = new List<(ReferenceDescription, NodeId)>();
        public Action<ReferenceDescription, NodeId, bool> Callback => HandleNode;
        private readonly bool isFullBrowse;

        public UANodeSource(ILogger<UANodeSource> log, FullConfig config, UAExtractor extractor, UAClient client, bool isFullBrowse, TypeManager typeManager)
            : base(log, config, extractor, client, typeManager)
        {
            this.isFullBrowse = isFullBrowse;
        }

        /// <summary>
        /// Called after mapping has completed.
        /// Transforms the list of raw nodes into five collections:
        /// Source variables, source objects,
        /// destination variables, destination objects, and references.
        /// This reads necessary information from the state and the server.
        /// </summary>
        /// <returns>Resulting lists of populated and sorted nodes.</returns>
        public override async Task<NodeSourceResult?> ParseResults(CancellationToken token)
        {
            if (parsed) throw new InvalidOperationException("Browse result has already been parsed");
            if (!NodeMap.Any()) return null;
            await Client.ReadNodeData(NodeList, token);

            var properties = new HashSet<UAVariable>();
            foreach (var node in NodeList)
            {
                SortNode(node);
                if ((node.IsProperty || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.VariableType)
                    && (node is UAVariable variable))
                {
                    properties.Add(variable);
                }
            }
            parsed = true;
            ClearRaw();

            await TypeManager.LoadTypeData(token);
            TypeManager.BuildTypeInfo();

            await Client.ReadNodeData(properties, token);
            var propsToReadValues = properties.Where(prop => prop.AllowTSMap(Log, Config.Extraction.DataTypes, 10, true)).ToList();
            await Client.ReadNodeValues(propsToReadValues, token);

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
                await GetRelationshipData(false, token);
            }

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

        /// <summary>
        /// Get references for the mapped nodes.
        /// </summary>
        /// <returns>A list of references.</returns>
        private async Task GetRelationshipData(bool getPropertyReferences, CancellationToken token)
        {
            var nodes = FinalSourceObjects.Concat(FinalSourceVariables);

            if (!getPropertyReferences)
            {
                nodes = nodes.Where(node => !node.IsProperty);
            }
            else
            {
                nodes = nodes.Concat(nodes.SelectMany(node => node.GetAllProperties())).DistinctBy(node => node.Id);
            }

            var nonHierarchicalReferences = await GetReferencesAsync(
                nodes.Select(node => node.Id).ToList(),
                ReferenceTypeIds.NonHierarchicalReferences,
                token);

            Log.LogInformation("Found {Count} non-hierarchical references", nonHierarchicalReferences.Count());

            var hierarchicalReferences = new List<UAReference>();

            if (Config.Extraction.Relationships.Hierarchical)
            {
                Log.LogInformation("Mapping {Count} hierarchical references", references.Count);

                foreach (var pair in references)
                {
                    // The child should always be in the list of mapped nodes here
                    var nodeId = Client.ToNodeId(pair.Node.NodeId);
                    var childNode = Extractor.State.GetMappedNode(nodeId);
                    if (childNode == null) continue;
                    var parentNode = Extractor.State.GetMappedNode(pair.ParentId);
                    if (parentNode == null) continue;

                    hierarchicalReferences.Add(new UAReference(
                        type: pair.Node.ReferenceTypeId,
                        isForward: true,
                        source: pair.ParentId,
                        target: childNode.Id,
                        sourceTs: !parentNode.IsObject,
                        targetTs: !childNode.IsObject,
                        isHierarchical: true,
                        manager: TypeManager));

                    if (Config.Extraction.Relationships.InverseHierarchical)
                    {
                        hierarchicalReferences.Add(new UAReference(
                            type: pair.Node.ReferenceTypeId,
                            isForward: false,
                            source: childNode.Id,
                            target: pair.ParentId,
                            sourceTs: !childNode.IsObject,
                            targetTs: !parentNode.IsObject,
                            isHierarchical: true,
                            manager: TypeManager));
                    }
                }
                Log.LogInformation("Found {Count} hierarchical references", hierarchicalReferences.Count);
            }

            foreach (var reference in nonHierarchicalReferences.Concat(hierarchicalReferences))
            {
                if (!FilterReference(reference)) continue;
                FinalReferences.Add(reference);
            }
        }

        /// <summary>
        /// Callback for the browse operation, creates <see cref="UANode"/>s and adds them to stored nodes.
        /// </summary>
        /// <param name="node">Description of the node to be handled</param>
        /// <param name="parentId">Id of the parent node</param>
        private void HandleNode(ReferenceDescription node, NodeId parentId, bool visited)
        {
            bool mapped;

            if (!visited)
            {
                var parent = NodeMap.GetValueOrDefault(parentId);

                var result = BaseUANode.Create(node, parentId, parent, Client, TypeManager);

                if (result == null)
                {
                    Log.LogWarning("Node of unexpected type received: {Type}, {Id}", node.NodeClass, node.NodeId);
                    return;
                }

                Log.LogTrace("Handle node {Name}, {Id}: {Class}", result.Name, result.Id, result.NodeClass);
                Extractor.State.RegisterNode(result.Id, result.GetUniqueId(Extractor));
                mapped = TryAdd(result);
            }
            else
            {
                mapped = NodeMap.ContainsKey(Client.ToNodeId(node.NodeId));
            }

            if (mapped && Config.Extraction.Relationships.Enabled && Config.Extraction.Relationships.Hierarchical)
            {
                if (parentId == null || parentId.IsNullNodeId) return;
                references.Add((node, parentId));
            }
        }

        /// <summary>
        /// Get all references between nodes in <paramref name="nodes"/> with reference type as subtype of
        /// <paramref name="referenceTypes"/>.
        /// </summary>
        /// <param name="nodes">Nodes to fetch references for</param>
        /// <param name="referenceTypes">ReferenceType filter</param>
        /// <returns>List of found references</returns>
        private async Task<IEnumerable<UAReference>> GetReferencesAsync(
            IEnumerable<NodeId> nodes,
            NodeId referenceTypes,
            CancellationToken token)
        {
            if (!nodes.Any()) return Enumerable.Empty<UAReference>();

            Log.LogInformation("Get extra references from the server for {Count} nodes", nodes.Count());

            var browseNodes = nodes.Select(node => new BrowseNode(node)).ToDictionary(node => node.Id);

            var classMask = NodeClass.Object | NodeClass.Variable;
            if (Config.Extraction.NodeTypes?.AsNodes ?? false)
            {
                classMask |= NodeClass.ObjectType | NodeClass.VariableType;
            }

            var baseParams = new BrowseParams
            {
                BrowseDirection = BrowseDirection.Both,
                NodeClassMask = (uint)classMask,
                ReferenceTypeId = referenceTypes,
                Nodes = browseNodes
            };

            var references = await Client.Browser.BrowseLevel(baseParams, token, purpose: "references");

            var results = new List<UAReference>();
            foreach (var (parentId, children) in references)
            {
                var parentNode = Extractor.State.GetMappedNode(parentId);
                if (parentNode == null) continue;
                foreach (var child in children)
                {
                    var childId = Client.ToNodeId(child.NodeId);
                    var childNode = Extractor.State.GetMappedNode(childId);

                    results.Add(new UAReference(
                        type: child.ReferenceTypeId,
                        isForward: child.IsForward,
                        source: parentId,
                        target: childId,
                        sourceTs: !parentNode.IsObject,
                        targetTs: childNode != null && !childNode.IsObject,
                        isHierarchical: false,
                        manager: TypeManager));
                }
            }

            Log.LogInformation("Found {Count} extra references", results.Count);

            return results;
        }
    }
}

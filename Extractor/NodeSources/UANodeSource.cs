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

using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
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
        protected override ILogger Log { get; set; } = Serilog.Log.Logger.ForContext(typeof(UAExtractor));

        private bool parsed;

        private List<(ReferenceDescription Node, NodeId ParentId)> references = new List<(ReferenceDescription, NodeId)>();
        public Action<ReferenceDescription, NodeId> Callback => HandleNode;

        public UANodeSource(FullConfig config, UAExtractor extractor, UAClient client)
            : base(config, extractor, client)
        {
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
            await Client.ReadNodeData(NodeMap.Values, token);
            
            foreach (var node in NodeMap.Values)
            {
                SortNode(node);
            }
            parsed = true;
            NodeMap.Clear();

            if (Config.Extraction.DataTypes.MaxArraySize != 0 && Config.Extraction.DataTypes.EstimateArraySizes == true)
            {
                await EstimateArraySizes(RawVariables, token);
            }

            var update = Config.Extraction.Update;
            await GetExtraNodeData(update, token);

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
                await GetRelationshipData(token);
            }

            if (!FinalDestinationObjects.Any() && !FinalDestinationVariables.Any() && !FinalSourceVariables.Any() && !FinalReferences.Any())
            {
                Log.Information("Mapping resulted in no new nodes");
                return null;
            }

            Log.Information("Mapping resulted in {obj} destination objects and {ts} destination timeseries," +
                " {robj} objects and {var} variables.",
                FinalDestinationObjects.Count, FinalDestinationVariables.Count,
                FinalSourceObjects.Count, FinalSourceVariables.Count);
            if (FinalReferences.Any())
            {
                Log.Information("Found a total of {cnt} references", FinalReferences.Count);
            }

            return new NodeSourceResult(
                FinalSourceObjects,
                FinalSourceVariables,
                FinalDestinationObjects,
                FinalDestinationVariables,
                FinalReferences);
        }

        /// <summary>
        /// Retrieve extra node data for the sorted raw variables and objects.
        /// </summary>
        /// <param name="update">UpdateConfig used to determine what should be fetched</param>
        private async Task GetExtraNodeData(UpdateConfig update, CancellationToken token)
        {
            Log.Information("Getting data for {NumVariables} variables and {NumObjects} objects",
                RawVariables.Count, RawObjects.Count);

            var nodes = RawObjects.Concat(RawVariables);

            if (update.Objects.Metadata || update.Variables.Metadata)
            {
                var toReadProperties = nodes
                    .Where(node => Extractor.State.IsMappedNode(node.Id)
                        && (update.Objects.Metadata && !(node is UAVariable)
                            || update.Variables.Metadata && (node is UAVariable)))
                    .ToList();
                if (toReadProperties.Any())
                {
                    await Extractor.ReadProperties(toReadProperties);
                }
            }

            var extraMetaTasks = new List<Task>();

            var distinctDataTypes = RawVariables.Select(variable => variable.DataType.Raw).ToHashSet();
            extraMetaTasks.Add(Extractor.DataTypeManager.GetDataTypeMetadataAsync(distinctDataTypes, token));

            if (Config.Extraction.NodeTypes.Metadata)
            {
                extraMetaTasks.Add(Client.ObjectTypeManager.GetObjectTypeMetadataAsync(token));
            }

            if (Config.Extraction.NodeTypes.AsNodes)
            {
                var toRead = nodes.Where(node => node.NodeClass == NodeClass.VariableType)
                    .SelectNonNull(node => node as UAVariable)
                    .ToList();
                extraMetaTasks.Add(Client.ReadNodeValues(toRead, token));
            }

            await Task.WhenAll(extraMetaTasks);
        }

        /// <summary>
        /// Get references for the mapped nodes.
        /// </summary>
        /// <returns>A list of references.</returns>
        private async Task GetRelationshipData(CancellationToken token)
        {
            if (Extractor.ReferenceTypeManager == null) return;
            var nodes = RawObjects.Concat(RawVariables);

            var nonHierarchicalReferences = await Extractor.ReferenceTypeManager.GetReferencesAsync(
                nodes,
                ReferenceTypeIds.NonHierarchicalReferences,
                token);

            foreach (var reference in nonHierarchicalReferences)
            {
                FinalReferences.Add(reference);
            }
            Log.Information("Found {cnt} non-hierarchical references", FinalReferences.Count);

            if (Config.Extraction.Relationships.Hierarchical)
            {
                var nodeMap = FinalSourceObjects.Concat(FinalSourceVariables)
                    .ToDictionary(node => node.Id);

                Log.Information("Mapping {cnt} hierarchical references", references.Count);

                foreach (var pair in references)
                {
                    // The child should always be in the list of mapped nodes here
                    var nodeId = Client.ToNodeId(pair.Node.NodeId);
                    if (!nodeMap.TryGetValue(nodeId, out var childNode)) continue;
                    if (childNode == null || childNode is UAVariable childVar && childVar.IsProperty) continue;

                    bool childIsTs = childNode is UAVariable cVar && !cVar.IsArray && cVar.NodeClass == NodeClass.Variable;

                    FinalReferences.Add(new UAReference(
                        pair.Node.ReferenceTypeId,
                        true,
                        pair.ParentId,
                        childNode.Id,
                        false,
                        childIsTs,
                        Extractor.ReferenceTypeManager));

                    if (Config.Extraction.Relationships.InverseHierarchical)
                    {
                        FinalReferences.Add(new UAReference(
                            pair.Node.ReferenceTypeId,
                            false,
                            childNode.Id,
                            pair.ParentId,
                            childIsTs,
                            false,
                            Extractor.ReferenceTypeManager));
                    }
                }
            }

            await Extractor.ReferenceTypeManager.GetReferenceTypeDataAsync(token);
        }

        /// <summary>
        /// Callback for the browse operation, creates <see cref="UANode"/>s and adds them to stored nodes.
        /// </summary>
        /// <param name="node">Description of the node to be handled</param>
        /// <param name="parentId">Id of the parent node</param>
        private void HandleNode(ReferenceDescription node, NodeId parentId)
        {
            bool mapped = false;

            if (node.NodeClass == NodeClass.Object || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.ObjectType)
            {
                var uaNode = new UANode(Client.ToNodeId(node.NodeId), node.DisplayName.Text, parentId, node.NodeClass);
                uaNode.SetNodeType(Client, node.TypeDefinition);

                mapped = !uaNode.IsProperty;

                Extractor.State.RegisterNode(uaNode.Id, Extractor.GetUniqueId(uaNode.Id));
                Log.Verbose("HandleNode {class} {name}", uaNode.NodeClass, uaNode.DisplayName);

                NodeMap[uaNode.Id] = uaNode;
            }
            else if (node.NodeClass == NodeClass.Variable || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.VariableType)
            {
                var variable = new UAVariable(Client.ToNodeId(node.NodeId), node.DisplayName.Text, parentId, node.NodeClass);
                variable.SetNodeType(Client, node.TypeDefinition);

                mapped = !variable.IsProperty;

                Extractor.State.RegisterNode(variable.Id, Extractor.GetUniqueId(variable.Id));
                Log.Verbose("HandleNode Variable {name}", variable.DisplayName);

                NodeMap[variable.Id] = variable;
            }
            else
            {
                Log.Warning("Node of unknown type received: {type}, {id}", node.NodeClass, node.NodeId);
            }

            if (mapped && Config.Extraction.Relationships.Enabled && Config.Extraction.Relationships.Hierarchical)
            {
                if (parentId == null || parentId.IsNullNodeId) return;
                references.Add((node, parentId));
            }
        }


    }
}

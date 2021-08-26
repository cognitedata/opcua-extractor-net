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

using Cognite.OpcUa.HistoryStates;
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
    public class BrowseResultHandler : BaseNodeSource
    {
        private readonly ILogger log = Log.Logger.ForContext(typeof(UAExtractor));

        private Dictionary<NodeId, UANode> nodeMap = new Dictionary<NodeId, UANode>();
        private bool parsed = false;

        private List<(ReferenceDescription Node, NodeId ParentId)> references = new List<(ReferenceDescription, NodeId)>();
        public Action<ReferenceDescription, NodeId> Callback => HandleNode;

        private List<UAVariable> rawVariables = new List<UAVariable>();
        private List<UANode> rawObjects = new List<UANode>();

        public BrowseResultHandler(FullConfig config, UAExtractor extractor, UAClient client)
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
        public override async Task<BrowseResult?> ParseResults(CancellationToken token)
        {
            if (parsed) throw new InvalidOperationException("Browse result has already been parsed");
            if (!nodeMap.Any()) return null;
            await Task.Run(() => Client.ReadNodeData(nodeMap.Values, token), CancellationToken.None);

            foreach (var node in nodeMap.Values)
            {
                SortNode(node);
            }
            parsed = true;
            nodeMap.Clear();

            var update = Config.Extraction.Update;
            await GetExtraNodeData(update, token);

            var mappedObjects = rawObjects.Where(obj => FilterObject(update, obj)).ToList();
            finalDestinationObjects.AddRange(mappedObjects);
            finalSourceObjects.AddRange(mappedObjects);
            foreach (var variable in rawVariables)
            {
                SortVariable(update, variable);
            }

            foreach (var node in finalSourceObjects.Concat(finalSourceVariables))
            {
                InitNodeState(update, node);
            }

            if (Config.Extraction.Relationships.Enabled)
            {
                await GetRelationshipData(token);
            }

            if (!finalDestinationObjects.Any() && !finalDestinationVariables.Any() && !finalSourceVariables.Any() && !finalReferences.Any())
            {
                log.Information("Mapping resulted in no new nodes");
                return null;
            }

            log.Information("Mapping resulted in {obj} destination objects and {ts} destination timeseries," +
                " {robj} objects and {var} variables.",
                finalDestinationObjects.Count, finalDestinationVariables.Count,
                finalSourceObjects.Count, finalSourceVariables.Count);
            if (finalReferences.Any())
            {
                log.Information("Found a total of {cnt} references", finalReferences.Count);
            }

            return new BrowseResult(
                finalSourceObjects,
                finalSourceVariables,
                finalDestinationObjects,
                finalDestinationVariables,
                finalReferences);
        }

        /// <summary>
        /// Apply transformations and sort the given node as variable, object or property.
        /// </summary>
        /// <param name="node"></param>
        private void SortNode(UANode node)
        {
            if (node.ParentId != null && !node.ParentId.IsNullNodeId && nodeMap.TryGetValue(node.ParentId, out var parent))
            {
                node.Parent = parent;
            }

            bool initialProperty = node.IsProperty;
            if (Extractor.Transformations != null)
            {
                foreach (var trns in Extractor.Transformations)
                {
                    trns.ApplyTransformation(node, Client.NamespaceTable);
                    if (node.Ignore) return;
                }
            }

            if (node.IsProperty)
            {
                if (node.Parent == null) return;
                node.Parent.AddProperty(node);
                // Edge-case, since attributes are read before transformations, if transformations cause a node to become a property,
                // ArrayDimensions won't be read. We can just read them later at minimal cost.
                if (!initialProperty && Config.Extraction.DataTypes.MaxArraySize == 0 && (node is UAVariable variable) && variable.ValueRank >= 0)
                {
                    node.Attributes.DataRead = false;
                }
            }
            else if (node is UAVariable variable)
            {
                rawVariables.Add(variable);
            }
            else
            {
                rawObjects.Add(node);
            }
        }

        /// <summary>
        /// Retrieve extra node data for the sorted raw variables and objects.
        /// </summary>
        /// <param name="update">UpdateConfig used to determine what should be fetched</param>
        private async Task GetExtraNodeData(UpdateConfig update, CancellationToken token)
        {
            log.Information("Getting data for {NumVariables} variables and {NumObjects} objects",
                rawVariables.Count, rawObjects.Count);

            var nodes = rawObjects.Concat(rawVariables);

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

            var distinctDataTypes = rawVariables.Select(variable => variable.DataType.Raw).ToHashSet();
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
                extraMetaTasks.Add(Task.Run(() => Client.ReadNodeValues(toRead, token), CancellationToken.None));
            }

            await Task.WhenAll(extraMetaTasks);
        }

        /// <summary>
        /// Returns true if the raw object should be added to the final list of objects.
        /// </summary>
        /// <param name="update">Update configuration used to determine what nodes are changed.</param>
        /// <param name="node">Node to be filtered.</param>
        /// <returns>True if node should be considered for mapping, false otherwise.</returns>
        private bool FilterObject(UpdateConfig update, UANode node)
        {
            if (update.AnyUpdate)
            {
                var oldChecksum = Extractor.State.GetNodeChecksum(node.Id);
                if (oldChecksum != null)
                {
                    node.Changed |= oldChecksum != node.GetUpdateChecksum(
                        update.Objects,
                        Config.Extraction.DataTypes.DataTypeMetadata,
                        Config.Extraction.NodeTypes.Metadata);
                    return node.Changed;
                }
            }
            log.Verbose(node.ToString());

            return true;
        }

        /// <summary>
        /// Filter a node, creating new objects and variables based on attributes and config.
        /// </summary>
        /// <param name="update">Configuration used to determine what nodes have changed.</param>
        /// <param name="node">Node to sort.</param>
        private void SortVariable(UpdateConfig update, UAVariable node)
        {
            if (!Extractor.DataTypeManager.AllowTSMap(node)) return;
            if (update.AnyUpdate)
            {
                var oldChecksum = Extractor.State.GetNodeChecksum(node.Id);
                if (oldChecksum != null)
                {
                    node.Changed |= oldChecksum != node.GetUpdateChecksum(
                        update.Variables,
                        Config.Extraction.DataTypes.DataTypeMetadata,
                        Config.Extraction.NodeTypes.Metadata);

                    if (node.Changed)
                    {
                        AddVariableToLists(node);
                    }
                    return;
                }
            }

            log.Verbose(node.ToString());
            AddVariableToLists(node);
        }

        /// <summary>
        /// Get references for the mapped nodes.
        /// </summary>
        /// <returns>A list of references.</returns>
        private async Task GetRelationshipData(CancellationToken token)
        {
            if (extractor.ReferenceTypeManager == null) return;
            var nodes = rawObjects.Concat(rawVariables);

            var nonHierarchicalReferences = await Extractor.ReferenceTypeManager.GetReferencesAsync(
                nodes,
                ReferenceTypeIds.NonHierarchicalReferences,
                token);

            foreach (var reference in nonHierarchicalReferences)
            {
                finalReferences.Add(reference);
            }
            log.Information("Found {cnt} non-hierarchical references", finalReferences.Count);

            if (Config.Extraction.Relationships.Hierarchical)
            {
                var nodeMap = finalSourceObjects.Concat(finalSourceVariables)
                    .ToDictionary(node => node.Id);

                log.Information("Mapping {cnt} hierarchical references", references.Count);

                foreach (var pair in references)
                {
                    // The child should always be in the list of mapped nodes here
                    var nodeId = Client.ToNodeId(pair.Node.NodeId);
                    if (!nodeMap.TryGetValue(nodeId, out var childNode)) continue;
                    if (childNode == null || childNode is UAVariable childVar && childVar.IsProperty) continue;

                    bool childIsTs = childNode is UAVariable cVar && !cVar.IsArray && cVar.NodeClass == NodeClass.Variable;

                    finalReferences.Add(new UAReference(
                        pair.Node.ReferenceTypeId,
                        true,
                        pair.ParentId,
                        childNode.Id,
                        false,
                        childIsTs,
                        Extractor.ReferenceTypeManager));

                    if (Config.Extraction.Relationships.InverseHierarchical)
                    {
                        finalReferences.Add(new UAReference(
                            pair.Node.ReferenceTypeId,
                            false,
                            childNode.Id,
                            pair.ParentId,
                            childIsTs,
                            true,
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
                log.Verbose("HandleNode {class} {name}", uaNode.NodeClass, uaNode.DisplayName);

                nodeMap[uaNode.Id] = uaNode;
            }
            else if (node.NodeClass == NodeClass.Variable || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.VariableType)
            {
                var variable = new UAVariable(Client.ToNodeId(node.NodeId), node.DisplayName.Text, parentId, node.NodeClass);
                variable.SetNodeType(Client, node.TypeDefinition);

                mapped = !variable.IsProperty;

                Extractor.State.RegisterNode(variable.Id, Extractor.GetUniqueId(variable.Id));
                log.Verbose("HandleNode Variable {name}", variable.DisplayName);

                nodeMap[variable.Id] = variable;
            }
            else
            {
                log.Warning("Node of unknown type received: {type}, {id}", node.NodeClass, node.NodeId);
            }

            if (mapped && Config.Extraction.Relationships.Enabled && Config.Extraction.Relationships.Hierarchical)
            {
                if (parentId == null || parentId.IsNullNodeId) return;
                references.Add((node, parentId));
            }
        }


    }
}

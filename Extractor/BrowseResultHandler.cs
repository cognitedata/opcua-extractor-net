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

namespace Cognite.OpcUa
{
    /// <summary>
    /// Contains the result of a round of browsing the server node hierarchy.
    /// </summary>
    public class BrowseResult
    {
        public BrowseResult(
            IEnumerable<UANode> sourceObjects,
            IEnumerable<UAVariable> sourceVariables,
            IEnumerable<UANode> destinationObjects,
            IEnumerable<UAVariable> destinationVariables,
            IEnumerable<UAReference> destinationReferences)
        {
            SourceObjects = sourceObjects;
            SourceVariables = sourceVariables;
            DestinationObjects = destinationObjects;
            DestinationVariables = destinationVariables;
            DestinationReferences = destinationReferences;
        }
        public IEnumerable<UANode> SourceObjects { get; }
        public IEnumerable<UAVariable> SourceVariables { get; }
        public IEnumerable<UANode> DestinationObjects { get; }
        public IEnumerable<UAVariable> DestinationVariables { get; }
        public IEnumerable<UAReference> DestinationReferences { get; }
    }
    /// <summary>
    /// Contains the results of a browse operation, and parses the nodes to produce
    /// lists of nodes that should be mapped to destinations.
    /// </summary>
    public class BrowseResultHandler
    {
        private readonly ILogger log = Log.Logger.ForContext(typeof(UAExtractor));

        private readonly FullConfig config;
        private readonly UAExtractor extractor;
        private readonly UAClient client;

        private Dictionary<NodeId, UANode> nodeMap = new Dictionary<NodeId, UANode>();

        private List<(ReferenceDescription Node, NodeId ParentId)> references = new List<(ReferenceDescription, NodeId)>();
        public Action<ReferenceDescription, NodeId> Callback => HandleNode;

        private List<UAVariable> rawVariables = new List<UAVariable>();
        private List<UANode> rawObjects = new List<UANode>();

        // Nodes that are treated as variables (and synchronized) in the source system
        private readonly List<UAVariable> finalSourceVariables = new List<UAVariable>();
        // Nodes that are treated as objects (so not synchronized) in the source system.
        // finalSourceVariables and finalSourceObjects should together contain all mapped nodes
        // in the source system.
        private readonly List<UANode> finalSourceObjects = new List<UANode>();

        // Nodes that are treated as objects in the destination systems (i.e. mapped to assets)
        private readonly List<UANode> finalDestinationObjects = new List<UANode>();
        // Nodes that are treated as variables in the destination systems (i.e. mapped to timeseries)
        // May contain duplicate NodeIds, but all should produce distinct UniqueIds.
        private readonly List<UAVariable> finalDestinationVariables = new List<UAVariable>();

        private readonly HashSet<UAReference> finalReferences = new HashSet<UAReference>();


        public BrowseResultHandler(FullConfig config, UAExtractor extractor, UAClient client)
        {
            this.config = config;
            this.extractor = extractor;
            this.client = client;
        }

        /// <summary>
        /// Called after mapping has completed.
        /// Transforms the list of raw nodes into five collections:
        /// Source variables, source objects,
        /// destination variables, destination objects, and references.
        /// This reads necessary information from the state and the server.
        /// </summary>
        /// <returns>Resulting lists of populated and sorted nodes.</returns>
        public async Task<BrowseResult> ParseResults(CancellationToken token)
        {
            if (nodeMap == null) throw new InvalidOperationException("Browse result has already been parsed");
            if (!nodeMap.Any()) return null;
            await Task.Run(() => client.ReadNodeData(nodeMap.Values, token));

            foreach (var node in nodeMap.Values)
            {
                SortNode(node);
            }
            nodeMap = null;

            var update = config.Extraction.Update;
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

            if (config.Extraction.Relationships.Enabled)
            {
                await GetRelationshipData(token);
            }

            if (!finalDestinationObjects.Any() && !finalDestinationVariables.Any() && !finalSourceVariables.Any() && !finalReferences.Any())
            {
                log.Information("Mapping resulted in no new nodes");
                return null;
            }

            log.Information("Mapping resulted in {obj} destination objects and {ts} destination timeseries," +
                " representing {var} variables and {robj} objects.",
                finalDestinationObjects.Count, finalDestinationVariables.Count,
                finalSourceVariables.Count, finalSourceObjects.Count);
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

            if (extractor.Transformations != null)
            {
                foreach (var trns in extractor.Transformations)
                {
                    trns.ApplyTransformation(node, client.NamespaceTable);
                    if (node.Ignore) return;
                }
            }

            if (node.IsProperty)
            {
                if (node.Parent == null) return;
                node.Parent.AddProperty(node);
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
                    .Where(node => extractor.State.IsMappedNode(node.Id)
                        && (update.Objects.Metadata && !(node is UAVariable)
                            || update.Variables.Metadata && (node is UAVariable)))
                    .ToList();
                if (toReadProperties.Any())
                {
                    await extractor.ReadProperties(toReadProperties);
                }
            }

            var extraMetaTasks = new List<Task>();

            var distinctDataTypes = rawVariables.Select(variable => variable.DataType.Raw).ToHashSet();
            extraMetaTasks.Add(extractor.DataTypeManager.GetDataTypeMetadataAsync(distinctDataTypes, token));

            if (config.Extraction.NodeTypes.Metadata)
            {
                extraMetaTasks.Add(client.ObjectTypeManager.GetObjectTypeMetadataAsync(token));
            }

            if (config.Extraction.NodeTypes.AsNodes)
            {
                var toRead = nodes.Where(node => node.NodeClass == NodeClass.VariableType)
                    .Select(node => node as UAVariable)
                    .Where(node => node != null)
                    .ToList();
                extraMetaTasks.Add(Task.Run(() => client.ReadNodeValues(toRead, token)));
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
                var oldChecksum = extractor.State.GetNodeChecksum(node.Id);
                if (oldChecksum != null)
                {
                    node.Changed |= oldChecksum != node.GetUpdateChecksum(
                        update.Objects,
                        config.Extraction.DataTypes.DataTypeMetadata,
                        config.Extraction.NodeTypes.Metadata);
                    return node.Changed;
                }
            }
            log.Verbose(node.ToString());

            return true;
        }

        /// <summary>
        /// Write a variable to the correct output lists. This assumes the variable should be mapped.
        /// </summary>
        /// <param name="node">Variable to write</param>
        private void AddVariableToLists(UAVariable node)
        {
            if (node.IsArray)
            {
                finalDestinationVariables.AddRange(node.CreateArrayChildren());
            }

            if (node.IsArray || node.NodeClass != NodeClass.Variable)
            {
                finalDestinationObjects.Add(node);
            }
            else
            {
                finalDestinationVariables.Add(node);
            }

            if (node.NodeClass == NodeClass.Variable)
            {
                finalSourceVariables.Add(node);
            }
            else
            {
                finalSourceObjects.Add(node);
            }
        }

        /// <summary>
        /// Filter a node, creating new objects and variables based on attributes and config.
        /// </summary>
        /// <param name="update">Configuration used to determine what nodes have changed.</param>
        /// <param name="node">Node to sort.</param>
        private void SortVariable(UpdateConfig update, UAVariable node)
        {
            if (!extractor.DataTypeManager.AllowTSMap(node)) return;
            if (update.AnyUpdate)
            {
                var oldChecksum = extractor.State.GetNodeChecksum(node.Id);
                if (oldChecksum != null)
                {
                    node.Changed |= oldChecksum != node.GetUpdateChecksum(
                        update.Variables,
                        config.Extraction.DataTypes.DataTypeMetadata,
                        config.Extraction.NodeTypes.Metadata);

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
            var nodes = rawObjects.Concat(rawVariables);

            var nonHierarchicalReferences = await extractor.ReferenceTypeManager.GetReferencesAsync(
                nodes,
                ReferenceTypeIds.NonHierarchicalReferences,
                token);

            foreach (var reference in nonHierarchicalReferences)
            {
                finalReferences.Add(reference);
            }
            log.Information("Found {cnt} non-hierarchical references", finalReferences.Count);

            if (config.Extraction.Relationships.Hierarchical)
            {
                var nodeMap = finalSourceObjects.Concat(finalSourceVariables)
                    .ToDictionary(node => node.Id);

                log.Information("Mapping {cnt} hierarchical references", references.Count);

                foreach (var pair in references)
                {
                    // The child should always be in the list of mapped nodes here
                    var nodeId = client.ToNodeId(pair.Node.NodeId);
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
                        extractor.ReferenceTypeManager));

                    if (config.Extraction.Relationships.InverseHierarchical)
                    {
                        finalReferences.Add(new UAReference(
                            pair.Node.ReferenceTypeId,
                            false,
                            childNode.Id,
                            pair.ParentId,
                            childIsTs,
                            true,
                            extractor.ReferenceTypeManager));
                    }
                }
            }

            await extractor.ReferenceTypeManager.GetReferenceTypeDataAsync(token);
        }

        /// <summary>
        /// Write the node to the extractor state
        /// </summary>
        /// <param name="update">Update configuration</param>
        /// <param name="node">Node to store</param>
        private void InitNodeState(UpdateConfig update, UANode node)
        {
            var updateConfig = node is UAVariable ? update.Variables : update.Objects;

            extractor.State.AddActiveNode(
                node,
                updateConfig,
                config.Extraction.DataTypes.DataTypeMetadata,
                config.Extraction.NodeTypes.Metadata);

            if (config.Events.Enabled
                && (node.EventNotifier & EventNotifiers.SubscribeToEvents) != 0
                && extractor.State.GetEmitterState(node.Id) != null)
            {
                bool history = (node.EventNotifier & EventNotifiers.HistoryRead) != 0 && config.Events.History;
                var eventState = new EventExtractionState(extractor, node.Id, history, history && config.History.Backfill);
                extractor.State.SetEmitterState(eventState);
            }

            if (node is UAVariable variable && variable.NodeClass == NodeClass.Variable)
            {
                var state = extractor.State.GetNodeState(node.Id);
                if (state != null) return;

                state = new VariableExtractionState(
                            extractor,
                            variable,
                            variable.Historizing,
                            variable.Historizing && config.History.Backfill);

                if (variable.IsArray)
                {
                    foreach (var child in variable.ArrayChildren)
                    {
                        var uniqueId = extractor.GetUniqueId(child.Id, child.Index);
                        extractor.State.SetNodeState(state, uniqueId);
                        extractor.State.RegisterNode(node.Id, uniqueId);
                    }
                }
                else
                {
                    extractor.State.SetNodeState(state);
                }
            }
        }

        /// <summary>
        /// Callback for the browse operation, creates <see cref="UANode"/>s and adds them to stored nodes.
        /// </summary>
        /// <param name="node">Description of the node to be handled</param>
        /// <param name="parentId">Id of the parent node</param>
        private void HandleNode(ReferenceDescription node, NodeId parentId)
        {
            bool mapped = false;

            if (node.NodeClass == NodeClass.Object || config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.ObjectType)
            {
                var uaNode = new UANode(client.ToNodeId(node.NodeId), node.DisplayName.Text, parentId, node.NodeClass);
                uaNode.SetNodeType(client, node.TypeDefinition);

                mapped = !uaNode.IsProperty;

                extractor.State.RegisterNode(uaNode.Id, extractor.GetUniqueId(uaNode.Id));
                log.Verbose("HandleNode {class} {name}", uaNode.NodeClass, uaNode.DisplayName);

                nodeMap[uaNode.Id] = uaNode;
            }
            else if (node.NodeClass == NodeClass.Variable || config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.VariableType)
            {
                var variable = new UAVariable(client.ToNodeId(node.NodeId), node.DisplayName.Text, parentId, node.NodeClass);
                variable.SetNodeType(client, node.TypeDefinition);

                mapped = !variable.IsProperty;

                extractor.State.RegisterNode(variable.Id, extractor.GetUniqueId(variable.Id));
                log.Verbose("HandleNode Variable {name}", variable.DisplayName);

                nodeMap[variable.Id] = variable;
            }
            else
            {
                log.Warning("Node of unknown type received: {type}, {id}", node.NodeClass, node.NodeId);
            }

            if (mapped && config.Extraction.Relationships.Enabled && config.Extraction.Relationships.Hierarchical)
            {
                if (parentId == null || parentId.IsNullNodeId) return;
                references.Add((node, parentId));
            }
        }


    }
}

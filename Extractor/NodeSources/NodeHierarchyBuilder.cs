using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    public class NodeHierarchyBuilder
    {
        private readonly INodeSource nodeSource;
        private readonly ITypeAndNodeSource typeSource;
        private readonly FullConfig config;
        private readonly ILogger logger;
        private readonly IEnumerable<NodeId> nodesToRead;
        private readonly UAClient client;
        private readonly TransformationCollection? transformations;
        private readonly UAExtractor extractor;

        // Nodes that are treated as variables (and synchronized) in the source system
        private List<UAVariable> FinalSourceVariables { get; } = new List<UAVariable>();
        // Nodes that are treated as objects (so not synchronized) in the source system.
        // finalSourceVariables and finalSourceObjects should together contain all mapped nodes
        // in the source system.
        private List<BaseUANode> FinalSourceObjects { get; } = new List<BaseUANode>();

        // Nodes that are treated as objects in the destination systems (i.e. mapped to assets)
        private List<BaseUANode> FinalDestinationObjects { get; } = new List<BaseUANode>();
        // Nodes that are treated as variables in the destination systems (i.e. mapped to timeseries)
        // May contain duplicate NodeIds, but all should produce distinct UniqueIds.
        private List<UAVariable> FinalDestinationVariables { get; } = new List<UAVariable>();
        private HashSet<UAReference> FinalReferences { get; } = new HashSet<UAReference>();

        private bool hasRun = false;

        public NodeHierarchyBuilder(
            INodeSource nodeSource,
            ITypeAndNodeSource typeSource,
            FullConfig config,
            IEnumerable<NodeId> nodesToRead,
            UAClient client,
            UAExtractor extractor,
            TransformationCollection? transformations,
            ILogger logger)
        {
            this.nodeSource = nodeSource;
            this.typeSource = typeSource;
            this.config = config;
            this.logger = logger;
            this.nodesToRead = nodesToRead;
            this.client = client;
            this.extractor = extractor;
            this.transformations = transformations;
        }

        public async Task<NodeSourceResult> LoadNodeHierarchy(bool isFullBrowse, CancellationToken token)
        {
            if (hasRun) throw new InvalidOperationException("NodeHierarchyBuilder has already been started");
            hasRun = true;

            // If enabled, load the type hierarchies, we do this early to handle custom data types
            await client.TypeManager.Initialize(typeSource, token);
            extractor.State.PopulateActiveEventTypes(client.TypeManager.EventFields);

            // Perform any pre-load initialization steps
            await nodeSource.Initialize(token);


            var classMask = (uint)NodeClass.Object | (uint)NodeClass.Variable;
            if (config.Toggles.LoadTypesAsNodes)
            {
                classMask |= (uint)NodeClass.ObjectType | (uint)NodeClass.VariableType | (uint)NodeClass.ReferenceType | (uint)NodeClass.DataType;
            }

            // Load nodes from the source, this is the main operation
            var initialNodes = await nodeSource.LoadNodes(nodesToRead, classMask, config.Extraction.Relationships.Mode, "the main instance hierarchy", token);
            // Refresh any newly loaded type data. Since TypeManager is lazy, we need to do this after loading nodes,
            // but before processing them.
            await client.TypeManager.LoadTypeData(typeSource, token);
            await ProcessNodeLoadResult(initialNodes, token);

            // If enabled, load non-hierarchical references from the source, this may
            // also load nodes.
            if (config.Extraction.Relationships.Enabled)
            {
                IEnumerable<BaseUANode> knownNodes = FinalSourceObjects.Concat(FinalSourceVariables);
                if (config.Toggles.MapPropertyReferences)
                {
                    knownNodes = knownNodes.Concat(knownNodes.SelectMany(n => n.GetAllProperties()));
                }

                var referenceNodes = await nodeSource.LoadNonHierarchicalReferences(
                    knownNodes.DistinctBy(n => n.Id).ToDictionary(n => n.Id),
                    config.Toggles.GetNonHierarchicalTypeReferences,
                    config.Toggles.CreateUnknownReferencedNodes,
                    "non-hierarchical relationships in the main instance hierarchy",
                    token);
                // Refresh type data. This may have discovered new types.
                await client.TypeManager.LoadTypeData(typeSource, token);
                await ProcessNodeLoadResult(referenceNodes, token);
            }

            // Finally, with all the type information in place, we can build node states
            foreach (var node in FinalSourceObjects.Concat(FinalSourceVariables))
            {
                InitNodeState(node);
            }

            return new NodeSourceResult(
                FinalSourceObjects,
                FinalSourceVariables,
                FinalDestinationObjects,
                FinalDestinationVariables,
                FinalReferences,
                isFullBrowse,
                initialNodes.ShouldBackgroundBrowse);
        }

        private async Task ProcessNodeLoadResult(NodeLoadResult res, CancellationToken token)
        {
            var rawObjects = new List<BaseUANode>();
            var rawVariables = new List<UAVariable>();
            var toReadValues = new List<BaseUANode>();

            logger.LogInformation("Got {Count1} nodes and {Count2} references from source",
                res.Nodes.Count, res.References.Count());

            int propCount = 0;
            // First, build the nodes given the full context.
            foreach (var node in res.Nodes)
            {
                if (!res.AssumeFullyTransformed)
                {
                    // Set properties inherited from parents, if present
                    UpdateFromParent(res.Nodes, node);
                    // Apply transformations
                    transformations?.ApplyTransformations(logger, node, client.NamespaceTable, config);
                    // Check if we should ignore it, need to do it here before we modify the parent.
                    if (node.Ignore) continue;
                    // Check if the parent node should be converted to an object.
                    CheckParentShouldBeObject(node);
                }


                // Add to the intermediate lists.
                if (node.IsProperty)
                {
                    propCount++;
                    node.Parent?.Attributes?.AddProperty(node);
                    if (node is UAVariable variable) toReadValues.Add(variable);
                }
                else if (node is UAVariable variable)
                {
                    rawVariables.Add(variable);
                }
                else if (config.Toggles.LoadTypesAsNodes && node is UAVariableType variableType)
                {
                    rawObjects.Add(variableType);
                    toReadValues.Add(variableType);
                }
                else
                {
                    rawObjects.Add(node);
                }
            }

            // Read values of properties, this is added as metadata in CDF.
            if (config.Source.EndpointUrl != null) await client.ReadNodeValues(
                toReadValues.Where(v => v.AllowValueRead(logger, config.Extraction.DataTypes, false)),
                token);

            // Estimate array sizes if enabled.
            if (config.Extraction.DataTypes.MaxArraySize != 0
                && config.Extraction.DataTypes.EstimateArraySizes
                && !res.AssumeFullyTransformed)
            {
                await EstimateArraySizes(rawVariables, token);
            }

            // Add to final collections.
            var update = config.Extraction.Update;

            int objCount = 0, varCount = 0;
            // Add variables and objects to final lists and cache
            foreach (var obj in rawObjects)
            {
                if (FilterBaseNode(update.Objects, obj))
                {
                    FinalDestinationObjects.Add(obj);
                    FinalSourceObjects.Add(obj);
                    objCount++;
                    AddManagedNode(update.Objects, obj);
                    if (obj.Id == ObjectIds.ModellingRule_Mandatory)
                    {
                        logger.LogDebug("Add managed object mandatory");
                    }
                }
            }

            foreach (var variable in rawVariables)
            {
                if (FilterBaseNode(update.Variables, variable) && AddVariableToLists(variable))
                {
                    varCount++;
                    AddManagedNode(update.Variables, variable);
                }
            }

            logger.LogInformation("Mapped {Count1} nodes to objects, {Count2} to variables, and {Count3} to properties",
                objCount, varCount, propCount);

            // Finally, filter references. This uses the knowledge of which nodes were added in the
            // end to avoid extracting references that point to an unmapped node.
            foreach (var reference in res.References)
            {
                if (!FilterReference(reference, config.Toggles.MapPropertyReferences)) continue;
                FinalReferences.Add(reference);
            }

            if (FinalReferences.Count != 0)
            {
                logger.LogInformation("Mapped {Count} references", FinalReferences.Count);
            }
        }

        private void AddManagedNode(TypeUpdateConfig update, BaseUANode node)
        {
            extractor.State.RegisterNode(node.Id, node.GetUniqueId(client.Context));
            extractor.State.AddActiveNode(
                node,
                config.Extraction.Update.Objects,
                config.Extraction.DataTypes.DataTypeMetadata,
                config.Extraction.NodeTypes.Metadata);
            foreach (var prop in node.GetAllProperties())
            {
                extractor.State.AddActiveNode(
                prop,
                update,
                config.Extraction.DataTypes.DataTypeMetadata,
                config.Extraction.NodeTypes.Metadata);
            }
        }


        #region result-processing
        private void UpdateFromParent(UANodeCollection nodeMap, BaseUANode node)
        {
            if (node.Parent == null && !node.ParentId.IsNullNodeId)
            {
                node.Parent = nodeMap.GetValueOrDefault(node.ParentId);
            }

            if (node.Parent != null)
            {
                node.Ignore |= node.Parent.Ignore;
                node.IsRawProperty |= node.Parent.IsRawProperty;
                if (node.Parent.NodeClass == NodeClass.Variable && !config.Extraction.MapVariableChildren)
                {
                    node.IsRawProperty = true;
                }
                node.IsChildOfType = IsDescendantOfType(node);
            }

            if (!node.IsRawProperty && node is UAVariable vb)
            {
                if (vb.FullAttributes.TypeDefinition?.Id == VariableTypeIds.PropertyType)
                {
                    vb.IsRawProperty = true;
                }
            }
        }

        private void CheckParentShouldBeObject(BaseUANode node)
        {
            if (node.Parent == null) return;
            if (node.Parent.NodeClass != NodeClass.Variable) return;
            if (node.IsProperty) return;
            if (node.Parent is not UAVariable varParent) return;

            varParent.IsObject = true;
        }

        private bool IsDescendantOfType(BaseUANode node)
        {
            return node.Parent != null && (node.Parent.IsType || node.Parent.IsChildOfType);
        }

        private (int Length, bool IsCollection) GetLengthOfCollection(object value)
        {
            int size = 0;
            if (value is ICollection coll)
            {
                size = coll.Count;
            }
            else if (value is IEnumerable enumVal)
            {
                var e = enumVal.GetEnumerator();
                while (e.MoveNext()) size++;
            }
            else
            {
                return (1, false);
            }
            return (size, true);
        }

        private async Task EstimateArraySizes(IEnumerable<UAVariable> nodes, CancellationToken token)
        {
            nodes = nodes.Where(node =>
                (node.ArrayDimensions == null || !node.ArrayDimensions.Any() || node.ArrayDimensions[0] == 0)
                && (node.ValueRank == ValueRanks.OneDimension
                    || node.ValueRank == ValueRanks.ScalarOrOneDimension
                    || node.ValueRank == ValueRanks.OneOrMoreDimensions
                    || node.ValueRank == ValueRanks.Any));
            // Start by looking for "MaxArrayLength" standard property. This is defined in OPC-UA 5/6.3.2
            if (!nodes.Any()) return;

            logger.LogInformation("Estimating array length for {Count} nodes", nodes.Count());

            var toReadValues = new List<UAVariable>();

            var maxLengthProperties = nodes
                .SelectNonNull(node => node.Properties?.FirstOrDefault(prop => prop.Name == "MaxArrayLength") as UAVariable);

            foreach (var node in nodes)
            {
                var maxLengthProp = node.Properties?.FirstOrDefault(prop => prop.Name == "MaxArrayLength");
                if (maxLengthProp != null && maxLengthProp is UAVariable varProp && varProp.Value != null)
                {
                    try
                    {
                        int size = Convert.ToInt32(varProp.Value.Value.Value);
                        if (size > 0)
                        {
                            node.FullAttributes.ArrayDimensions = new[] { size };
                        }
                        continue;
                    }
                    catch (Exception ex)
                    {
                        logger.LogDebug("Failed to convert value of MaxArrayLength property for {Id}: {Message}", node.Id, ex.Message);
                    }
                }
                if (node.Value?.Value != null)
                {
                    var (length, isCollection) = GetLengthOfCollection(node.Value.Value.Value);
                    if (length > 0 && isCollection)
                    {
                        node.FullAttributes.ArrayDimensions = new[] { length };
                    }
                    else
                    {
                        logger.LogDebug("Unable to estimate length of node {Id}, value is scalar", node.Id);
                    }
                    continue;
                }
                toReadValues.Add(node);
            }
            if (toReadValues.Count == 0) return;

            if (config.Source.EndpointUrl == null)
            {
                logger.LogDebug("Unable to estimate array length for {Count} nodes, since no server is configured", toReadValues.Count);
                return;
            }

            logger.LogDebug("Fetch values for {Count} nodes to estimate array lengths", toReadValues.Count);
            await client.ReadNodeValues(toReadValues, token);

            foreach (var node in toReadValues)
            {
                if (node.Value == null) continue;
                var (length, isCollection) = GetLengthOfCollection(node.Value.Value.Value);
                if (length > 0 && isCollection)
                {
                    node.FullAttributes.ArrayDimensions = new[] { length };
                }
            }
        }

        private bool FilterBaseNode(TypeUpdateConfig update, BaseUANode node)
        {
            // If deletes are enabled, we are not able to filter any nodes, even if that has a real cost in terms of memory usage.
            if (update.AnyUpdate && !config.Extraction.Deletes.Enabled)
            {
                var oldChecksum = extractor.State.GetMappedNode(node.Id)?.Checksum;
                if (oldChecksum != null)
                {
                    node.Changed |= oldChecksum != node.GetUpdateChecksum(
                        update,
                        config.Extraction.DataTypes.DataTypeMetadata,
                        config.Extraction.NodeTypes.Metadata);
                    return node.Changed;
                }
            }

            return true;
        }

        /// <summary>
        /// Write a variable to the correct output lists. This assumes the variable should be mapped.
        /// </summary>
        /// <param name="node">Variable to write</param>
        private bool AddVariableToLists(UAVariable node)
        {
            var map = node.GetVariableGroups(logger, config.Extraction.DataTypes);
            if (map.IsDestinationVariable) FinalDestinationVariables.AddRange(node.CreateTimeseries());
            if (map.IsDestinationObject) FinalDestinationObjects.Add(node);
            if (map.IsSourceVariable) FinalSourceVariables.Add(node);
            if (map.IsSourceObject) FinalSourceObjects.Add(node);

            return map.Any();
        }

        private bool FilterReference(UAReference reference, bool mapProperties)
        {
            var source = extractor.State.GetMappedNode(reference.Source.Id);
            if (source == null || source.IsProperty && !mapProperties)
            {
                if (source == null) logger.LogTrace("Skipping relationship from {Src} to {Trg} due to missing source",
                    reference.Source.Id, reference.Target.Id);
                else logger.LogTrace("Skipping relationship from {Src} to {Trg} since the source is a property",
                    reference.Source.Id, reference.Target.Id);
                return false;
            }

            var target = extractor.State.GetMappedNode(reference.Target.Id);
            if (target == null || target.IsProperty && !mapProperties)
            {
                if (target == null) logger.LogTrace("Skipping relationship from {Src} to {Trg} due to missing target",
                    reference.Source.Id, reference.Target.Id);
                else logger.LogTrace("Skipping relationship from {Src} to {Trg} since the target is a property",
                    reference.Source.Id, reference.Target.Id);
                return false;
            }

            if (reference.IsHierarchical && !config.Extraction.Relationships.Hierarchical)
            {
                logger.LogTrace("Skipping relationship from {Src} to {Trg} since it is hierarchical, and hierarchical references are disabled",
                    reference.Source.Id, reference.Target.Id);
                return false;
            }
            if (reference.IsHierarchical && !reference.IsForward && !config.Extraction.Relationships.InverseHierarchical)
            {
                logger.LogTrace("Skipping relationship from {Src} to {Trg} since it is inverse hierarchical, and inverse hierarchical references are disabled",
                    reference.Source.Id, reference.Target.Id);
                return false;
            }

            return true;
        }
        #endregion

        #region states
        private void InitNodeState(BaseUANode node)
        {
            if (config.Events.Enabled
                && node is UAObject objNode
                && extractor.State.GetEmitterState(node.Id) == null)
            {
                bool subscribe = objNode.FullAttributes.ShouldSubscribeToEvents(config);
                bool history = objNode.FullAttributes.ShouldReadEventHistory(config);

                if (subscribe || history)
                {
                    var eventState = new EventExtractionState(extractor, node.Id, history, history && config.History.Backfill, subscribe);
                    extractor.State.SetEmitterState(eventState);
                }
            }

            if (node is UAVariable variable)
            {
                bool subscribe = variable.FullAttributes.ShouldSubscribe(config);
                bool history = variable.FullAttributes.ShouldReadHistory(config);

                VariableExtractionState? state = null;
                if ((subscribe || history) && extractor.State.GetNodeState(node.Id) == null)
                {
                    state = new VariableExtractionState(
                        extractor,
                        variable,
                        history,
                        history && config.History.Backfill,
                        subscribe);
                    extractor.State.SetNodeState(state);
                }


                if (variable.IsObject)
                {
                    foreach (var child in variable.CreateTimeseries())
                    {
                        var uniqueId = child.GetUniqueId(extractor.Context);
                        if (state != null) extractor.State.SetNodeState(state, uniqueId);
                        extractor.State.RegisterNode(node.Id, uniqueId);
                    }
                }
            }
        }
        #endregion
    }
}

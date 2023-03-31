/* Cognite Extractor for OPC-UA
Copyright (C) 2022 Cognite AS

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
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using CogniteSdk.Beta.DataModels;
using System.Text.Json;
using System;

namespace Cognite.OpcUa.Pushers.FDM
{
    internal class FDMWriter
    {
        private CogniteDestination destination;
        private FullConfig config;
        private ILogger<FDMWriter> log;
        private string instSpace;
        public UAExtractor Extractor { get; set; } = null!;
        public FDMWriter(FullConfig config, CogniteDestination destination, ILogger<FDMWriter> log)
        {
            this.config = config;
            this.destination = destination;
            this.log = log;
            instSpace = config.Cognite!.FlexibleDataModels!.Space;
        }

        /* private async Task IngestNodes<T>(string model, IEnumerable<T> items, CancellationToken token) where T : BaseNode
        {
            if (!items.Any()) return;

            foreach (var chunk in items.ChunkBy(1000))
            {
                var request = new NodeIngestRequest<T>
                {
                    Items = chunk,
                    Model = new ModelIdentifier(FDMDataModel.Space, model),
                    Overwrite = true,
                    SpaceExternalId = instSpace
                };
                try
                {
                    await destination.CogniteClient.Beta.DataModels.IngestNodes(request, token);
                }
                catch (ResponseException ex)
                {
                    log.LogError("Failed to write to FDM: {Extras}", ex.Extra);
                    throw;
                }
            }
        }

        private async Task IngestEdges<T>(string model, IEnumerable<T> items, CancellationToken token) where T : BaseEdge
        {
            if (!items.Any()) return;

            foreach (var chunk in items.ChunkBy(1000))
            {
                var request = new EdgeIngestRequest<T>
                {
                    Items = chunk,
                    AutoCreateEndNodes = true,
                    AutoCreateStartNodes = true,
                    Model = new ModelIdentifier(FDMDataModel.Space, model),
                    Overwrite = true,
                    SpaceExternalId = instSpace
                };
                try
                {
                    await destination.CogniteClient.Beta.DataModels.IngestEdges(request, token);
                }
                catch (ResponseException ex)
                {
                    log.LogError("Failed to write to FDM: {Extras}", ex.Extra);
                    throw;
                }
            }
        } */

        private async Task IngestInstances(IEnumerable<BaseInstanceWrite> instances, CancellationToken token)
        {
            var chunks = instances.ChunkBy(1000).ToList();
            var results = new IEnumerable<SlimInstance>[chunks.Count];
            var generators = chunks
                .Select<IEnumerable<BaseInstanceWrite>, Func<Task>>((c, idx) => async () =>
                {
                    var req = new InstanceWriteRequest
                    {
                        AutoCreateEndNodes = true,
                        AutoCreateStartNodes = true,
                        Items = instances,
                        Replace = true
                    };
                    results[idx] = await destination.CogniteClient.Beta.DataModels.UpsertInstances(req, token);
                });

            int taskNum = 0;
            await generators.RunThrottled(
                10,
                (_) =>
                {
                    if (chunks.Count > 1)
                        log.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(IngestInstances), ++taskNum, chunks.Count);
                },
                token);
        }

        private async Task Initialize(FDMTypeBatch types, CancellationToken token)
        {
            if (config.Cognite!.Debug)
            {
                var options = new JsonSerializerOptions(Oryx.Cognite.Common.jsonOptions) { WriteIndented = true };
                foreach (var type in types.Containers)
                {
                    log.LogDebug("Build container: {Type}", JsonSerializer.Serialize(type.Value, options));
                }
                foreach (var type in types.Views)
                {
                    log.LogDebug("Build view: {Type}", JsonSerializer.Serialize(type.Value, options));
                }
                return;
            }

            await destination.CogniteClient.Beta.DataModels.UpsertSpaces(new[]
            {
                new SpaceCreate
                {
                    Space = instSpace
                }
            }, token);

            foreach (var chunk in types.Containers.Values.ChunkBy(100))
            {
                await destination.CogniteClient.Beta.DataModels.UpsertContainers(chunk, token);
            }

            foreach (var level in types.Views.Values.ChunkByHierarchy(100, v => v.ExternalId, v => v.Implements?.FirstOrDefault()?.ExternalId))
            {
                foreach (var item in level)
                {
                    log.LogTrace("Create type {Id} parent: {Parent}", item.ExternalId, item.Implements?.FirstOrDefault()?.ExternalId);
                }

                log.LogInformation("Inserting level with {Count} items", level.Count());
                foreach (var chunk in level.ChunkBy(100))
                {
                    log.LogInformation("Inserting {Count} items", chunk.Count());
                    await destination.CogniteClient.Beta.DataModels.UpsertViews(chunk, token);
                }
            }

            var model = new DataModelCreate
            {
                Name = "OPC-UA",
                ExternalId = "OPC_UA_DM",
                Space = instSpace,
                Version = "1",
                Views = types.Views.Select(v => new ViewIdentifier(instSpace, v.Value.ExternalId, v.Value.Version))
            };
            await destination.CogniteClient.Beta.DataModels.UpsertDataModels(new[] { model }, token);
        }

        private IEnumerable<UANode> GetModellingRules()
        {
            return new[] {
                new UANode(ObjectIds.ModellingRule_Mandatory, "Mandatory", NodeId.Null, NodeClass.Object),
                new UANode(ObjectIds.ModellingRule_Optional, "Optional", NodeId.Null, NodeClass.Object),
                new UANode(ObjectIds.ModellingRule_MandatoryPlaceholder, "MandatoryPlaceholder", NodeId.Null, NodeClass.Object),
                new UANode(ObjectIds.ModellingRule_OptionalPlaceholder, "OptionalPlaceholder", NodeId.Null, NodeClass.Object),
                new UANode(ObjectIds.ModellingRule_ExposesItsArray, "ExposesItsArray", NodeId.Null, NodeClass.Object),
            };
        }

        public async Task<bool> PushNodes(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            IUAClientAccess client,
            CancellationToken token)
        {
            var converter = new DMSValueConverter(client.StringConverter, instSpace);
            var builder = new TypeHierarchyBuilder(log, client, converter, config);
            // First, collect all nodes, including properties.
            var nodes = objects
                .SelectMany(obj => obj.GetAllProperties())
                .Concat(objects)
                .Concat(variables.SelectMany(variable => variable.GetAllProperties()))
                .Concat(variables)
                .Concat(GetModellingRules())
                // This has the additional effect of deduplicating array variables
                // Which means we get a single node referencing a non-existing timeseries (missing array indexer)
                // which is currently fine, but might be problematic in the future.
                .DistinctBy(node => node.Id)
                .Where(node => node.Id != null && !node.Id.IsNullNodeId)
                .ToList();

            // Inject modelling rule nodes


            var nodeHierarchy = new NodeHierarchy(references, nodes);

            foreach (var node in nodeHierarchy.NodeMap.Values)
            {
                log.LogTrace("Map node {Id}", node.Id);
            }

            foreach (var rf in nodeHierarchy.ReferencesBySourceId.Values.SelectMany(r => r))
            {
                log.LogTrace("Map reference {Source} to {Target}. Type: {Type}",
                    rf.Source.Id, rf.Target.Id, rf.Type.Id);
            }

            var types = builder.ConstructTypes(nodeHierarchy);

            // Initialize if needed
            await Initialize(types, token);

            var nodeIds = nodes.Select(node => node.Id).ToHashSet();

            var finalReferences = new List<UAReference>();

            // Iterate over references and identify any that are missing source or target nodes
            // Some servers are not in the node hierarchy, we currently don't map those, but we might in the future.
            var skipped = new HashSet<NodeId>();
            foreach (var refr in references)
            {
                if (!refr.IsForward) continue;

                if (!nodeIds.Contains(refr.Source.Id))
                {
                    log.LogTrace("Missing source node {Node} ({Target})", refr.Source.Id, refr.Target.Id);
                    skipped.Add(refr.Source.Id);
                    continue;
                }
                if (!nodeIds.Contains(refr.Target.Id))
                {
                    log.LogTrace("Missing target node {Node} ({Source})", refr.Target.Id, refr.Source.Id);
                    skipped.Add(refr.Target.Id);
                    continue;
                }
                if (!nodeIds.Contains(refr.Type?.Id ?? NodeId.Null))
                {
                    log.LogTrace("Missing type {Node} ({Source}, {Target})", refr.Type?.Id ?? NodeId.Null, refr.Source.Id, refr.Target.Id);
                    skipped.Add(refr.Type?.Id ?? NodeId.Null);
                    continue;
                }

                finalReferences.Add(refr);
            }

            if (skipped.Count > 0) log.LogWarning("Skipped {Count} references due to missing type, source, or target. This may not be an issue, as servers often have nodes outside the main hierarchy", skipped.Count);

            log.LogInformation("Mapped out {Nodes} nodes and {Edges} edges to write to PG3", nodes.Count, finalReferences.Count);

            // Run the node filter unless we are writing everything.
            if (config.Cognite!.FlexibleDataModels!.ExcludeNonReferenced)
            {
                var trimmer = new NodeTrimmer(nodeHierarchy, config, log);
                nodeHierarchy = trimmer.Filter();
                log.LogInformation("After filtering {Nodes} nodes are left", nodeHierarchy.NodeMap.Count);
            }

            var instanceBuilder = new InstanceBuilder(nodeHierarchy, types, converter, client, instSpace, log);
            log.LogInformation("Begin building instances");
            instanceBuilder.Build();

            if (config.Cognite!.Debug)
            {
                instanceBuilder.DebugLog(log);
                return true;
            }

            // Ingest types first
            log.LogInformation("Ingesting {Count1} object types, {Count2} reference types, {Count3} dataTypes",
                instanceBuilder.ObjectTypes.Count,
                instanceBuilder.ReferenceTypes.Count,
                instanceBuilder.DataTypes.Count);
            await IngestInstances(instanceBuilder.ObjectTypes
                .Concat(instanceBuilder.ReferenceTypes)
                .Concat(instanceBuilder.DataTypes), token);

            // Then ingest variable types
            log.LogInformation("Ingesting {Count} variable types", instanceBuilder.VariableTypes.Count);
            await IngestInstances(instanceBuilder.VariableTypes, token);

            // Ingest instances
            log.LogInformation("Ingesting {Count1} objects, {Count2} variables", instanceBuilder.Objects.Count, instanceBuilder.Variables.Count);
            await IngestInstances(instanceBuilder.Objects.Concat(instanceBuilder.Variables), token);

            // Finally, ingest edges
            log.LogInformation("Ingesting {Count} references", instanceBuilder.References.Count);
            await IngestInstances(instanceBuilder.References, token);

            /* var serverMetadata = new FDMServer(instSpace, config.Extraction.IdPrefix ?? "", uaClient, ObjectIds.RootFolder, uaClient.NamespaceTable!, config.Extraction.NamespaceMap);

            var fdmObjects = new List<FDMBaseInstance>();
            var fdmVariables = new List<FDMVariable>();
            var fdmObjectTypes = new List<FDMBaseType>();
            var fdmVariableTypes = new List<FDMVariableType>();
            var fdmReferences = new List<FDMReference>();
            var fdmReferenceTypes = new List<FDMReferenceType>();
            var fdmDataTypes = new List<FDMDataType>();

            foreach (var node in nodes)
            {
                switch (node.NodeClass) {
                    case NodeClass.ReferenceType:
                        fdmReferenceTypes.Add(new FDMReferenceType(uaClient, ObjectIds.RootFolder, node));
                        break;
                    case NodeClass.Object:
                        fdmObjects.Add(new FDMBaseInstance(instSpace, uaClient, node));
                        break;
                    case NodeClass.ObjectType:
                        fdmObjectTypes.Add(new FDMBaseType(uaClient, ObjectIds.RootFolder, node));
                        break;
                    case NodeClass.Variable:
                        fdmVariables.Add(new FDMVariable(instSpace, uaClient, node));
                        break;
                    case NodeClass.VariableType:
                        fdmVariableTypes.Add(new FDMVariableType(instSpace, uaClient, ObjectIds.RootFolder, node));
                        break;
                    case NodeClass.DataType:
                        fdmDataTypes.Add(new FDMDataType(uaClient, ObjectIds.RootFolder, node));
                        break;
                    default:
                        break;
                }
            }

            foreach (var rf in finalReferences)
            {
                fdmReferences.Add(new FDMReference(config.Extraction.IdPrefix ?? "", instSpace, uaClient, rf));
            }

            // Ingest types first
            await Task.WhenAll(
                IngestNodes("BaseType", fdmObjectTypes, token),
                IngestNodes("ReferenceType", fdmReferenceTypes, token),
                IngestNodes("DataType", fdmDataTypes, token)
            );

            // Need to ingest variable types after data types.
            await IngestNodes("VariableType", fdmVariableTypes, token);

            // Next ingest instances
            await Task.WhenAll(
                IngestNodes("BaseInstance", fdmObjects, token),
                IngestNodes("Variable", fdmVariables, token)
            );

            // Finally, ingest references and the server object
            await Task.WhenAll(
                IngestEdges("Reference", fdmReferences, token),
                IngestNodes("Server", new[] { serverMetadata }, token)
            );   */

            return true;
        }
    }
}

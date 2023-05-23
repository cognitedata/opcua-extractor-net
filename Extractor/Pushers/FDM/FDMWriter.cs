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
using Cognite.OpcUa.Nodes;
using CogniteSdk;
using System.Text.Json.Serialization;

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
            instSpace = config.Cognite!.FlexibleDataModels!.Space!;
        }

        private async Task IngestInstances(IEnumerable<BaseInstanceWrite> instances, int chunkSize, CancellationToken token)
        {
            var chunks = instances.ChunkBy(chunkSize).ToList();
            var results = new IEnumerable<SlimInstance>[chunks.Count];
            var generators = chunks
                .Select<IEnumerable<BaseInstanceWrite>, Func<Task>>((c, idx) => async () =>
                {
                    var req = new InstanceWriteRequest
                    {
                        AutoCreateEndNodes = true,
                        AutoCreateStartNodes = true,
                        Items = c,
                        Replace = true
                    };
                    try
                    {
                        results[idx] = await destination.CogniteClient.Beta.DataModels.UpsertInstances(req, token);
                    }
                    catch (ResponseException rex)
                    {
                        log.LogError("Response exception: {Err}, {ReqId}", rex.Message, rex.RequestId);
                        throw;
                    }
                });

            int taskNum = 0;
            await generators.RunThrottled(
                5,
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
            var options = new JsonSerializerOptions(Oryx.Cognite.Common.jsonOptions) { WriteIndented = true };

            var viewsToInsert = types.Views.Values.ToList();
            if (config.Cognite!.FlexibleDataModels!.SkipSimpleTypes)
            {
                viewsToInsert = viewsToInsert.Where(v => v.Properties.Any() || types.ViewIsReferenced.GetValueOrDefault(v.ExternalId)).ToList();
            }

            log.LogInformation("Building {Count} containers, and {Count2} views", types.Containers.Count, viewsToInsert.Count);
            foreach (var type in types.Containers.Values)
            {
                log.LogTrace("Build container: {Type}", JsonSerializer.Serialize(type, options));
            }
            foreach (var type in viewsToInsert)
            {
                log.LogTrace("Build view: {Type}", JsonSerializer.Serialize(type, options));
            }
            if (config.DryRun) return;

            // Check if the data model exists
            if (config.Cognite!.FlexibleDataModels!.SkipTypesOnEqualCount)
            {
                try
                {
                    var existingModels = await destination.CogniteClient.Beta.DataModels.RetrieveDataModels(new[] { new FDMExternalId("OPC_UA", instSpace, "1") }, false, token);
                    if (existingModels.Any())
                    {
                        var existingModel = existingModels.First();
                        var viewCount = existingModel.Views.Count();
                        if (viewCount == viewsToInsert.Count)
                        {
                            log.LogInformation("Number of views in model is the same, not updating");
                            return;
                        }
                    }

                }
                catch { }
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
                log.LogDebug("Creating {Count} containers", chunk.Count());
                await destination.CogniteClient.Beta.DataModels.UpsertContainers(chunk, token);
            }

            foreach (var level in viewsToInsert.ChunkByHierarchy(100, v => v.ExternalId, v => v.Implements?.FirstOrDefault()?.ExternalId!))
            {
                foreach (var chunk in level.ChunkBy(100))
                {
                    log.LogDebug("Creating {Count} views", chunk.Count());
                    await destination.CogniteClient.Beta.DataModels.UpsertViews(chunk, token);
                }
            }

            var model = new DataModelCreate
            {
                Name = "OPC-UA",
                ExternalId = "OPC_UA",
                Space = instSpace,
                Version = "1",
                Views = viewsToInsert.Select(v => new ViewIdentifier(instSpace, v.ExternalId, v.Version))
            };
            await destination.CogniteClient.Beta.DataModels.UpsertDataModels(new[] { model }, token);
        }

        private IEnumerable<BaseUANode> GetModellingRules()
        {
            var typeDef = Extractor.TypeManager.GetObjectType(ObjectTypeIds.ModellingRuleType);
            return new BaseUANode[] {
                new UAObject(ObjectIds.ModellingRule_Mandatory, "Mandatory", "Mandatory", null, NodeId.Null, typeDef),
                new UAObject(ObjectIds.ModellingRule_Optional, "Optional", "Optional", null, NodeId.Null, typeDef),
                new UAObject(ObjectIds.ModellingRule_MandatoryPlaceholder, "MandatoryPlaceholder", "MandatoryPlaceholder", null, NodeId.Null, typeDef),
                new UAObject(ObjectIds.ModellingRule_OptionalPlaceholder, "OptionalPlaceholder", "OptionalPlaceholder", null, NodeId.Null, typeDef),
                new UAObject(ObjectIds.ModellingRule_ExposesItsArray, "ExposesItsArray", "ExposesItsArray", null, NodeId.Null, typeDef),
            };
        }

        public async Task<bool> PushNodes(
            IEnumerable<BaseUANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            IUAClientAccess client,
            CancellationToken token)
        {
            var converter = new DMSValueConverter(client.StringConverter, instSpace);
            var builder = new TypeHierarchyBuilder(log, converter, config);
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

            var nodeHierarchy = new NodeHierarchy(references, nodes);

            var finalReferences = new List<UAReference>();
            var nodeIds = nodes.Select(node => node.Id).ToHashSet();

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

            var types = builder.ConstructTypes(nodeHierarchy);

            // Initialize if needed
            await Initialize(types, token);

            var instanceBuilder = new InstanceBuilder(nodeHierarchy, types, converter, client, instSpace, log);
            log.LogInformation("Begin building instances");
            instanceBuilder.Build();
            log.LogInformation("Finish building instances");

            if (config.DryRun)
            {
                instanceBuilder.DebugLog(log);
                return true;
            }

            var serverMeta = new NodeWrite
            {
                ExternalId = $"{config.Extraction.IdPrefix}Server",
                Sources = new[]
                {
                    new InstanceData<ServerMeta>
                    {
                        Source = new ContainerIdentifier(instSpace, "ServerMeta"),
                        Properties = new ServerMeta
                        {
                            Namespaces = client.NamespaceTable!.ToArray()
                        }
                    }
                },
                Space = instSpace
            };

            // Ingest types first
            log.LogInformation("Ingesting {Count1} object types, {Count2} reference types, {Count3} dataTypes",
                instanceBuilder.ObjectTypes.Count,
                instanceBuilder.ReferenceTypes.Count,
                instanceBuilder.DataTypes.Count);
            await IngestInstances(instanceBuilder.ObjectTypes
                .Concat(instanceBuilder.ReferenceTypes)
                .Concat(instanceBuilder.DataTypes)
                .Append(serverMeta), 1000, token);

            // Then ingest variable types
            log.LogInformation("Ingesting {Count} variable types", instanceBuilder.VariableTypes.Count);
            await IngestInstances(instanceBuilder.VariableTypes, 1000, token);

            // Ingest instances
            log.LogInformation("Ingesting {Count1} objects, {Count2} variables", instanceBuilder.Objects.Count, instanceBuilder.Variables.Count);
            await IngestInstances(instanceBuilder.Objects.Concat(instanceBuilder.Variables), 1000, token);

            // Finally, ingest edges
            log.LogInformation("Ingesting {Count} references", instanceBuilder.References.Count);
            await IngestInstances(instanceBuilder.References, 1000, token);

            return true;
        }
    }

    class ServerMeta
    {
        [JsonPropertyName("Namespaces")]
        public IEnumerable<string>? Namespaces { get; set; }
    }
}

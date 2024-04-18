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
using Cognite.Extensions.DataModels;
using System.Text.Json.Nodes;
using System.ComponentModel.Design;
using Cognite.Extensions.DataModels.QueryBuilder;
using System.Net;
using Microsoft.Extensions.ObjectPool;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class FDMWriter
    {
        private CogniteDestination destination;
        private FullConfig config;
        private ILogger<FDMWriter> log;
        private NodeIdContext? context;
        private FdmDestinationConfig.ModelInfo modelInfo;
        private FdmDestinationConfig fdmConfig;
        public FDMWriter(FullConfig config, CogniteDestination destination, ILogger<FDMWriter> log)
        {
            this.config = config;
            this.destination = destination;
            this.log = log;
            modelInfo = new FdmDestinationConfig.ModelInfo(config.Cognite!.MetadataTargets!.DataModels!);
            fdmConfig = config.Cognite!.MetadataTargets!.DataModels!;
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
                fdmConfig.InstanceParallelism,
                (_) =>
                {
                    if (chunks.Count > 1)
                        log.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(IngestInstances), ++taskNum, chunks.Count);
                },
                token);
        }

        private async Task InitializeSpaceAndServer(CancellationToken token)
        {
            if (config.DryRun) return;

            var spaces = new[] { modelInfo.InstanceSpace, modelInfo.ModelSpace }.Distinct();

            await destination.CogniteClient.Beta.DataModels.UpsertSpaces(spaces.Select(s => new SpaceCreate { Space = s, Name = s }), token);

            var serverMetaContainer = BaseDataModelDefinitions.ServerMeta(modelInfo.ModelSpace);
            await destination.CogniteClient.Beta.DataModels.UpsertContainers(new[] { serverMetaContainer }, token);
            await destination.CogniteClient.Beta.DataModels.UpsertViews(new[] { serverMetaContainer.ToView(modelInfo.ModelVersion) }, token);
        }

        private bool IsConnectionDefEqual(ConnectionDefinition lh, ConnectionDefinition rh)
        {
            if (lh.Name != rh.Name) return false;
            if (lh.Description != rh.Description) return false;
            if (lh.Source?.ExternalId != rh.Source?.ExternalId) return false;
            if (lh.Source?.Version != rh.Source?.Version) return false;
            if (lh.Source?.Space != rh.Source?.Space) return false;
            if (lh.Type?.ExternalId != rh.Type?.ExternalId) return false;
            if (lh.Type?.Space != rh.Type?.Space) return false;
            if (lh.Direction != rh.Direction) return false;
            return true;
        }

        private bool IsViewPropertyEqual(ViewPropertyDefinition lh, ViewPropertyCreate rh)
        {
            if (lh.Name != rh.Name) return false;
            if (lh.Container.Space != rh.Container.Space
                || lh.Container.ExternalId != rh.Container.ExternalId) return false;
            if (lh.ContainerPropertyIdentifier != rh.ContainerPropertyIdentifier) return false;
            if (lh.Description != rh.Description) return false;
            return true;
        }

        private bool IsViewEqual(View lh, ViewCreate rh)
        {
            if (lh.Version != rh.Version) return false;
            if (lh.Space != rh.Space) return false;
            if (lh.Name != rh.Name) return false;
            foreach (var kvp in rh.Properties)
            {
                var rhProp = kvp.Value;
                var lhProp = lh.Properties.GetValueOrDefault(kvp.Key);
                if (lhProp == null) return false;

                if (lhProp is ConnectionDefinition lhConn)
                {
                    if (rhProp is not ConnectionDefinition rhConn) return false;
                    if (!IsConnectionDefEqual(lhConn, rhConn)) return false;
                }
                else
                {
                    if (lhProp is not ViewPropertyDefinition lhP
                        || rhProp is not ViewPropertyCreate rhP)
                    {
                        log.LogWarning("Invalid view retrieved from CDF, this is a bug: {Name}", lh.Name);
                        return false;
                    }
                    if (!IsViewPropertyEqual(lhP, rhP)) return false;
                }
            }
            if (lh.Description != rh.Description) return false;
            if (lh.Implements.Count() != rh.Implements.Count()) return false;
            var lhImp = lh.Implements.ToArray();
            var rhImp = rh.Implements.ToArray();
            for (int i = 0; i < lhImp.Length; i++)
            {
                var lhI = lhImp[i];
                var rhI = rhImp[i];
                if (lhI.Version != rhI.Version) return false;
                if (lhI.Space != rhI.Space) return false;
                if (lhI.ExternalId != rhI.ExternalId) return false;
            }
            return true;
        }

        private IEnumerable<ViewCreate> FilterModifiedViews(IEnumerable<ViewCreate> toCreate, DataModel model)
        {
            var views = model.Views.OfType<View>();
            var oldViews = views.ToDictionary(v => v.ExternalId);
            var newViews = toCreate.ToDictionary(v => v.ExternalId);

            foreach (var newView in newViews)
            {
                if (!oldViews.TryGetValue(newView.Key, out var oldView))
                {
                    yield return newView.Value;
                    continue;
                }
                if (!IsViewEqual(oldView, newView.Value)) yield return newView.Value;
            }
        }

        private async Task Initialize(FDMTypeBatch types, CancellationToken token)
        {
            var options = new JsonSerializerOptions(Oryx.Cognite.Common.jsonOptions) { WriteIndented = true };

            var viewsToInsert = types.Views.Values.ToList();
            if (fdmConfig.SkipSimpleTypes)
            {
                viewsToInsert = viewsToInsert.Where(v => v.Properties.Count != 0 || types.ViewIsReferenced.GetValueOrDefault(v.ExternalId)).ToList();
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
            try
            {
                var existingModels = await destination.CogniteClient.Beta.DataModels.RetrieveDataModels(new[] { modelInfo.FDMExternalId("OPC_UA") }, true, token);

                if (existingModels.Any())
                {
                    var existingModel = existingModels.First();
                    viewsToInsert = FilterModifiedViews(viewsToInsert, existingModel).ToList();
                }
            }
            catch { }

            if (viewsToInsert.Count == 0)
            {
                log.LogDebug("No views have changed, not updating");
                return;
            }

            var containersToInsert = new List<ContainerCreate>();

            var containersByExtId = types.Containers.Values.ToDictionary(c => c.ExternalId);

            foreach (var view in viewsToInsert)
            {
                if (containersByExtId.TryGetValue(view.ExternalId, out var container))
                {
                    containersToInsert.Add(container);
                }
            }

            log.LogDebug("Creating {Count} containers and {Count2} views", containersToInsert.Count, viewsToInsert.Count);

            foreach (var chunk in containersToInsert.ChunkBy(fdmConfig.ModelChunk))
            {
                log.LogDebug("Creating {Count} containers", chunk.Count());
                await destination.CogniteClient.Beta.DataModels.UpsertContainers(chunk, token);
            }

            foreach (var level in viewsToInsert.ChunkByHierarchy(fdmConfig.ModelChunk, v => v.ExternalId, v => v.Implements?.FirstOrDefault()?.ExternalId!))
            {
                foreach (var chunk in level.ChunkBy(fdmConfig.ModelChunk))
                {
                    log.LogDebug("Creating {Count} views", chunk.Count());
                    await destination.CogniteClient.Beta.DataModels.UpsertViews(chunk, token);
                }
            }

            var model = new DataModelCreate
            {
                Name = "OPC-UA",
                ExternalId = "OPC_UA",
                Space = modelInfo.ModelSpace,
                Version = modelInfo.ModelVersion,
                Views = viewsToInsert
                    .Select(v => modelInfo.ViewIdentifier(v.ExternalId))
                    .Append(modelInfo.ViewIdentifier("ServerMeta"))
            };
            await destination.CogniteClient.Beta.DataModels.UpsertDataModels(new[] { model }, token);
        }

        public async Task<bool> PushNodes(
            IEnumerable<BaseUANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            UAExtractor extractor,
            CancellationToken token)
        {
            await InitializeSpaceAndServer(token);
            context = await SyncServerMeta(extractor.NamespaceTable!, token);

            var converter = new DMSValueConverter(extractor.StringConverter, modelInfo);
            var builder = new TypeHierarchyBuilder(log, converter, config, modelInfo, context);

            // First, collect all nodes, including properties.
            var nodes = objects
                .SelectMany(obj => obj.GetAllProperties())
                .Concat(objects)
                .Concat(variables.SelectMany(variable => variable.GetAllProperties()))
                .Concat(variables)
                .ToList();

            // Hierarchy of all known type-hierarchy nodes
            var typeHierarchy = new NodeHierarchy(extractor.TypeManager.References, extractor.TypeManager.NodeMap);

            // We also need to collect any types, and any nodes referenced by those types.
            var typeCollector = new NodeTypeCollector(log,
                nodes.SelectNonNull(n => n.TypeDefinition).Where(id => !id.IsNullNodeId).ToHashSet(),
                typeHierarchy,
                fdmConfig.TypesToMap);

            var typeResult = typeCollector.MapTypes();

            nodes.AddRange(typeResult.MappedNodes.Values);
            references = references.Concat(typeResult.MappedReferences);

            // Collect any data types and reference types
            var simpleTypeCollector = new SimpleTypeCollector(log, nodes, references, typeHierarchy);
            var simpleTypeResult = simpleTypeCollector.CollectReferencedTypes();

            nodes.AddRange(simpleTypeResult.MappedNodes.Values);
            references = references.Concat(simpleTypeResult.MappedReferences).ToList();

            nodes = nodes.DistinctBy(n => n.Id).Where(node => node.Id != null && !node.Id.IsNullNodeId).ToList();

            var finalReferences = new List<UAReference>();
            var nodeIds = nodes.Select(node => node.Id).ToHashSet();

            // Iterate over references and identify any that are missing source or target nodes
            // Some servers have nodes that are not in the node hierarchy, we currently don't map those,
            // but we might in the future.
            var skipped = new HashSet<NodeId>();
            int skippedCount = 0;
            log.LogInformation("Filtering {Count} references, removing any non-referenced", references.Count());
            var seen = new HashSet<(NodeId, NodeId, NodeId)>();
            foreach (var refr in references)
            {
                if (!refr.IsForward)
                {
                    log.LogTrace("Reference from {S} to {T} is inverse",
                        refr.Source.Id, refr.Target.Id);
                    skippedCount++;
                    continue;
                }



                if (!nodeIds.Contains(refr.Source.Id))
                {
                    log.LogTrace("Missing source node {Node} ({Target})", refr.Source.Id, refr.Target.Id);
                    skipped.Add(refr.Source.Id);
                    skippedCount++;
                    continue;
                }
                if (!nodeIds.Contains(refr.Target.Id))
                {
                    log.LogTrace("Missing target node {Node} ({Source})", refr.Target.Id, refr.Source.Id);
                    skipped.Add(refr.Target.Id);
                    skippedCount++;
                    continue;
                }
                if (refr.Type == null || !nodeIds.Contains(refr.Type.Id))
                {
                    log.LogTrace("Missing type {Node} ({Source}, {Target})", refr.Type?.Id ?? NodeId.Null, refr.Source.Id, refr.Target.Id);
                    skipped.Add(refr.Type?.Id ?? NodeId.Null);
                    skippedCount++;
                    continue;
                }

                if (!seen.Add((refr.Source.Id, refr.Type.Id, refr.Target.Id)))
                {
                    log.LogTrace("Skipping reference {Source} {Type} {Target} since it has already been added",
                        refr.Source.Id, refr.Type.Id, refr.Target.Id);
                    skippedCount++;
                    continue;
                }

                finalReferences.Add(refr);
            }

            var nodeHierarchy = new NodeHierarchy(finalReferences, nodes);


            if (skipped.Count > 0) log.LogWarning("Skipped {Count} references due to missing type, source, or target. " +
                "This may not be an issue, as servers often have nodes outside the main hierarchy. " +
                "{Count2} distinct nodes were missing.", skippedCount, skipped.Count);

            log.LogInformation("Mapped out {Nodes} nodes and {Edges} edges to write to PG3", nodes.Count, finalReferences.Count);

            var types = builder.ConstructTypes(typeResult.Types);

            // Initialize if needed
            await Initialize(types, token);

            var instanceBuilder = new InstanceBuilder(nodeHierarchy, types, converter, context, extractor, modelInfo, log);

            log.LogInformation("Begin building instances");
            instanceBuilder.Build();
            log.LogInformation("Finish building instances");

            if (config.DryRun)
            {
                instanceBuilder.DebugLog(log);
                return true;
            }

            var typeMeta = types.Types.Values.Select(v => (BaseInstanceWrite)new NodeWrite
            {
                ExternalId = $"{v.ExternalId}_TypeMetadata",
                Space = modelInfo.InstanceSpace,
                Sources = new[]
                {
                    new InstanceData<TypeMetadata>
                    {
                        Source = modelInfo.ContainerIdentifier("TypeMeta"),
                        Properties = v.GetTypeMetadata(context)
                    }
                }
            });

            // Ingest types first
            log.LogInformation("Ingesting {Count1} object types, {Count2} reference types, {Count3} dataTypes",
                instanceBuilder.ObjectTypes.Count,
                instanceBuilder.ReferenceTypes.Count,
                instanceBuilder.DataTypes.Count);
            await IngestInstances(instanceBuilder.ObjectTypes
                .Concat(instanceBuilder.ReferenceTypes)
                .Concat(instanceBuilder.DataTypes)
                .Concat(typeMeta), fdmConfig.InstanceChunk, token);

            // Then ingest variable types
            log.LogInformation("Ingesting {Count} variable types", instanceBuilder.VariableTypes.Count);
            await IngestInstances(instanceBuilder.VariableTypes, fdmConfig.InstanceChunk, token);

            // Ingest instances
            log.LogInformation("Ingesting {Count1} objects, {Count2} variables", instanceBuilder.Objects.Count, instanceBuilder.Variables.Count);
            await IngestInstances(instanceBuilder.Objects.Concat(instanceBuilder.Variables), fdmConfig.InstanceChunk, token);

            // Finally, ingest edges
            log.LogInformation("Ingesting {Count} references", instanceBuilder.References.Count);
            await IngestInstances(instanceBuilder.References, fdmConfig.InstanceChunk, token);

            return true;
        }

        private async Task<NodeIdContext> SyncServerMeta(NamespaceTable namespaces, CancellationToken token)
        {
            var nss = namespaces.ToArray();

            var namespacesIfNew = new List<string>
            {
                Namespaces.OpcUa,
                "RESERVED",
                "RESERVED"
            };
            foreach (var ns in nss)
            {
                if (ns == Namespaces.OpcUa) continue;
                namespacesIfNew.Add(ns);
            }

            if (config.DryRun)
            {
                return new NodeIdContext(namespacesIfNew, nss);
            }

            var externalId = "Server";

            List<string>? finalNamespaces = null;

            await destination.CogniteClient.Beta.DataModels.UpsertAtomic<Dictionary<string, Dictionary<string, ServerMeta>>>(
                new[] { externalId },
                modelInfo.InstanceSpace,
                InstanceType.node,
                new[]
                {
                    new InstanceSource
                    {
                        Source = modelInfo.ViewIdentifier("ServerMeta")
                    }
                }, old =>
                {
                    ServerMeta meta;
                    if (!old.Any())
                    {
                        finalNamespaces = namespacesIfNew;
                    }
                    else
                    {
                        var oldNamespaces = old.First().Properties
                            .First()
                            .Value
                            .First()
                            .Value
                            .Namespaces;

                        var newNamespaces = oldNamespaces.ToList();
                        foreach (var ns in nss)
                        {
                            if (ns == Namespaces.OpcUa) continue;
                            if (newNamespaces.Contains(ns)) continue;

                            newNamespaces.Add(ns.ToString());
                        }
                        finalNamespaces = newNamespaces;
                    }
                    meta = new ServerMeta
                    {
                        Namespaces = finalNamespaces
                    };
                    return new[]
                    {
                        new NodeWrite
                        {
                            ExternalId = externalId,
                            Space = modelInfo.InstanceSpace,
                            Sources = new[]
                            {
                                new InstanceData<ServerMeta>
                                {
                                    Source = modelInfo.ContainerIdentifier("ServerMeta"),
                                    Properties = meta,
                                }
                            }
                        }
                    };
                },
                token);

            if (finalNamespaces == null) throw new InvalidOperationException("Namespaces were not successfully assigned");

            return new NodeIdContext(finalNamespaces, nss);
        }


        private async Task DeleteInstances(IEnumerable<InstanceIdentifier> instances, int chunkSize, CancellationToken token)
        {
            var chunks = instances.ChunkBy(chunkSize).ToList();
            var generators = chunks
                .Select<IEnumerable<InstanceIdentifier>, Func<Task>>(c => async () =>
                {
                    await destination.CogniteClient.Beta.DataModels.DeleteInstances(
                        instances,
                        token
                    );
                });

            int taskNum = 0;
            await generators.RunThrottled(
                    4,
                (_) =>
                    {
                        if (chunks.Count > 1)
                            log.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(DeleteInstances), ++taskNum, chunks.Count);
                    },
                    token
                );
        }

        private async Task<IEnumerable<InstanceIdentifier>> GetAllReferencingEdges(IEnumerable<NodeId> nodes, NodeIdContext context, CancellationToken token)
        {
            var chunks = nodes.Select(n => context.NodeIdToString(n)).ChunkBy(1000).ToList();
            if (chunks.Count == 0) return Enumerable.Empty<InstanceIdentifier>();

            var edgeIds = new IEnumerable<InstanceIdentifier>[chunks.Count];
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>((c, idx) => async () =>
                {
                    var values = c.Select(id => Value.Raw(new DirectRelationIdentifier(modelInfo.InstanceSpace, id)));
                    var filter = Filter.Or(
                        Filter.In(values, "edge", "startNode"),
                        Filter.In(values, "edge", "endNode")
                    );
                    var res = new List<InstanceIdentifier>();
                    string? cursor = null;
                    do
                    {
                        var r = await destination.CogniteClient.Beta.DataModels.FilterInstances<JsonElement>(new InstancesFilter
                        {
                            IncludeTyping = false,
                            InstanceType = InstanceType.edge,
                            Filter = filter,
                            Limit = 1000,
                            Cursor = cursor
                        }, token);
                        res.AddRange(r.Items.Select(it => new InstanceIdentifier(InstanceType.edge, it.Space, it.ExternalId)));
                        cursor = r.NextCursor;
                    } while (cursor != null);

                    edgeIds[idx] = res;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                4,
                (_) =>
                {
                    if (chunks.Count > 1)
                        log.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetAllReferencingEdges), ++taskNum, chunks.Count);
                },
                token
            );

            return edgeIds.Aggregate((seed, r) => seed.Concat(r)).ToList();
        }

        public async Task DeleteInFdm(DeletedNodes deletes, SessionContext sessionContext, CancellationToken token)
        {
            if (!fdmConfig.EnableDeletes) return;

            // First find all edges pointing to or from the nodes we are deleting.
            // We pretty much need to do this, since we don't know if anyone has added edges to the nodes.
            var nodes = deletes.Objects
                .Concat(deletes.Variables)
                .Select(d => d.GetNodeId(sessionContext, log))
                .Where(id => !id.IsNullNodeId)
                .Distinct()
                .ToList();
            log.LogInformation("Deleting edges for {Count} nodes", nodes.Count);
            var edges = await GetAllReferencingEdges(nodes, context!, token);
            log.LogInformation("Deleting {Count} edges", edges.Count());
            await DeleteInstances(edges, 1000, token);
            log.LogInformation("Deleting {Count} nodes", nodes.Count);
            await DeleteInstances(nodes.Select(id =>
                new InstanceIdentifier(InstanceType.node, modelInfo.InstanceSpace, context!.NodeIdToString(id))
            ), 1000, token);
        }
    }

    class ServerMeta
    {
        [JsonPropertyName("Namespaces")]
        public IEnumerable<string>? Namespaces { get; set; }
    }
}

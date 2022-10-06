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
using CogniteSdk;
using CogniteSdk.Beta;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Pushers.FDM
{
    internal class FDMWriter
    {
        private CogniteDestination destination;
        private FullConfig config;
        private ILogger<FDMWriter> log;
        private string instSpace;
        private bool initialized;
        public FDMWriter(FullConfig config, CogniteDestination destination, ILogger<FDMWriter> log)
        {
            this.config = config;
            this.destination = destination;
            this.log = log;
            instSpace = config.Cognite!.FlexibleDataModels!.InstanceSpace;
        }

        private async Task IngestNodes<T>(string model, IEnumerable<T> items, CancellationToken token) where T : BaseNode
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
        }

        class SelectAllTemp : IDMSFilter, ICompositeDMSFilter
        {
        }

        private async Task Initialize(CancellationToken token)
        {
            if (initialized) return;

            await destination.CogniteClient.Beta.DataModels.CreateSpaces(new[]
            {
                new Space { ExternalId = FDMDataModel.Space },
                new Space { ExternalId = instSpace }
            }.DistinctBy(s => s.ExternalId), token);

            while (!token.IsCancellationRequested)
            {
                var res = await destination.CogniteClient.Beta.DataModels.GraphQuery<Dictionary<string, IEnumerable<Dictionary<string, Dictionary<string, JsonElement>>>>>(new GraphQuery
                {
                    Select = new Dictionary<string, SelectModelProperties>
                    {
                        { "externalId", new SelectModelProperties { Limit = 1000, Models = new Dictionary<string, ModelQueryProperties>
                        {
                            { "node", new ModelQueryProperties { Model = ModelIdentifier.Node, Properties = new [] { "externalId" } } }
                        } } },
                        { "edgeExternalId", new SelectModelProperties { Limit = 1000, Models = new Dictionary<string, ModelQueryProperties>
                        {
                            { "edge", new ModelQueryProperties { Model = ModelIdentifier.Edge, Properties = new [] { "externalId" } } }
                        } } }
                    },
                    With = new Dictionary<string, QueryExpression>
                    {
                        { "externalId", new QueryExpression
                        {
                            Nodes = new NodeQueryExpression
                            {
                                Filter = new SelectAllTemp()
                            },
                        } },
                        { "edgeExternalId", new QueryExpression
                        {
                            Edges = new EdgeQueryExpression
                            {
                                Filter = new SelectAllTemp()
                            },
                        } }
                    }
                }, token);

                var nodes = res["externalId"].Where(r => r["node"]["spaceExternalId"].Deserialize<string>() == instSpace).Select(r => r["node"]["externalId"]).Select(el => el.Deserialize<string>()).ToList();
                var edges = res["edgeExternalId"].Where(r => r["edge"]["spaceExternalId"].Deserialize<string>() == instSpace).Select(r => r["edge"]["externalId"]).Select(el => el.Deserialize<string>()).ToList();
                if (nodes.Any())
                {
                    log.LogInformation("Delete {Count} nodes: {Nodes}", nodes.Count, string.Join(", ", nodes));
                    await destination.CogniteClient.Beta.DataModels.DeleteNodes(nodes, instSpace, token);
                }

                if (edges.Any())
                {
                    log.LogInformation("Delete {Count} edges: {Edges}", edges.Count, string.Join(", ", edges));
                    await destination.CogniteClient.Beta.DataModels.DeleteNodes(edges, instSpace, token);
                }

                if (!nodes.Any() && !edges.Any()) break;
            }

            await destination.CogniteClient.Beta.DataModels.ApplyModels(FDMDataModel.GetModels(), FDMDataModel.Space, token);
            initialized = true;


        }

        public async Task<bool> PushNodes(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            IUAClientAccess uaClient,
            CancellationToken token)
        {
            // Initialize if needed
            await Initialize(token);

            // First, collect all nodes, including properties.
            var nodes = objects
                .SelectMany(obj => obj.GetAllProperties())
                .Concat(objects)
                .Concat(variables.SelectMany(variable => variable.GetAllProperties()))
                .Concat(variables)
                // This has the additional effect of deduplicating array variables
                // Which means we get a single node referencing a non-existing timeseries (missing array indexer)
                // which is currently fine, but might be problematic in the future.
                .DistinctBy(node => node.Id)
                .Where(node => node.Id != null && !node.Id.IsNullNodeId)
                .ToList();

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
                var trimmer = new NodeTrimmer(nodes, finalReferences, config, log);
                (nodes, finalReferences) = trimmer.Filter();
                log.LogInformation("After filtering {Nodes} nodes and {Edges} edges are left", nodes.Count, finalReferences.Count);
            }

            var namespaces = new Dictionary<string, string>();
            foreach (var ns in uaClient.NamespaceTable?.ToArray() ?? Enumerable.Empty<string>())
            {
                namespaces[ns] = config.Extraction.NamespaceMap.GetValueOrDefault(ns) ?? ns;
            }

            var serverMetadata = new FDMServer(instSpace, config.Extraction.IdPrefix ?? "", uaClient, ObjectIds.RootFolder, namespaces);

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
            );

            return true;
        }
    }
}

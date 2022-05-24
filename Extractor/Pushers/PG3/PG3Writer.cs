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
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Pushers.PG3
{
    public class PG3Writer
    {
        private HttpClient client;
        private FullConfig config;
        private ILogger<PG3Writer> log;

        private Dictionary<NodeId, bool> referenceTypeIsHierarchical = new Dictionary<NodeId, bool>();

        public PG3Writer(FullConfig config, HttpClient client, ILogger<PG3Writer> log)
        {
            this.client = client;
            this.config = config;
            this.log = log;
        }

        private async Task SendBulkInsertRequest(BulkRequestWrapper request, IUAClientAccess uaClient, CancellationToken token)
        {
            // Temporary code for writing to a local PG3 instance,
            // we will want to add this to the SDK/utils at some point.
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
            options.Converters.Add(new PG3IngestConverter(config.Extraction.IdPrefix, uaClient, referenceTypeIsHierarchical));

            using var stream = new MemoryStream();
            await JsonSerializer.SerializeAsync(stream, request, options, token);
            stream.Seek(0, SeekOrigin.Begin);

            using var content = new StreamContent(stream);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            using var requestMessage = new HttpRequestMessage(HttpMethod.Post, "http://localhost:8000/api/v0/graph/opcua/_bulk");
            requestMessage.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            requestMessage.Content = content;

            var result = await client.SendAsync(requestMessage, token);

            var response = await result.Content.ReadAsStringAsync();
            File.WriteAllText("out.txt", response);
            File.WriteAllText("in.txt", JsonSerializer.Serialize(request, options));

            result.EnsureSuccessStatusCode();
        }

        public async Task<bool> PushNodes(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            IUAClientAccess uaClient,
            CancellationToken token)
        {
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
                if (!nodeIds.Contains(refr.Type?.Id ?? NodeId.Null)) {
                    log.LogTrace("Missing type {Node} ({Source}, {Target})", refr.Type?.Id ?? NodeId.Null, refr.Source.Id, refr.Target.Id);
                    skipped.Add(refr.Type?.Id ?? NodeId.Null);
                    continue;
                }

                finalReferences.Add(refr);
            }

            if (skipped.Count > 0) log.LogWarning("Skipped {Count} references due to missing type, source, or target. This may not be an issue, as servers often have nodes outside the main hierarchy", skipped.Count);

            // At this point, we need to identify whether each reference type is hierarchical.
            foreach (var node in nodes.Where(nd => nd.NodeClass == NodeClass.ReferenceType))
            {
                if (node.Id == ReferenceTypeIds.NonHierarchicalReferences || node.Id == ReferenceTypeIds.References)
                {
                    referenceTypeIsHierarchical[node.Id] = false;
                }
                else if (node.Id == ReferenceTypeIds.HierarchicalReferences)
                {
                    referenceTypeIsHierarchical[node.Id] = true;
                }
                else
                {
                    referenceTypeIsHierarchical[node.Id] = referenceTypeIsHierarchical[node.ParentId];
                }
            }

            log.LogInformation("Mapped out {Nodes} nodes and {Edges} edges to write to PG3", nodes.Count, finalReferences.Count);

            // Run the node filter unless we are writing everything.
            if (config.Cognite!.FlexibleDataModels!.ExcludeNonReferenced)
            {
                var trimmer = new NodeTrimmer(referenceTypeIsHierarchical, nodes, finalReferences, config, log);
                (nodes, finalReferences) = trimmer.Filter();
                log.LogInformation("After filtering {Nodes} nodes and {Edges} edges are left", nodes.Count, finalReferences.Count);
            }

            var namespaces = new Dictionary<string, string>();
            foreach (var ns in uaClient.NamespaceTable?.ToArray() ?? Enumerable.Empty<string>()) 
            {
                namespaces[ns] = config.Extraction.NamespaceMap.GetValueOrDefault(ns) ?? ns;
            }

            var serverMetadata = new ServerMetadata
            {
                HierarchyUpdateTimestamp = CogniteTime.ToUnixTimeMilliseconds(DateTime.UtcNow),
                Namespaces = namespaces,
                Prefix = config.Extraction.IdPrefix,
                Root = $"{config.Extraction.IdPrefix}{ObjectIds.RootFolder}"
            };

            var request = new BulkRequestWrapper(nodes, finalReferences, serverMetadata);
            await SendBulkInsertRequest(request, uaClient, token);

            return true;
        }
    }

    internal class BulkRequestWrapper
    {
        public string? Space { get; set; } = "opcua";
        public bool Overwrite { get; set; } = true;
        public IEnumerable<IngestUnion> Bulk { get; }
        public BulkRequestWrapper(IEnumerable<UANode> nodes, IEnumerable<UAReference> references, ServerMetadata server)
        {
            Bulk = nodes
                .Select(node => new IngestUnion(node))
                .Concat(references.Select(reference => new IngestUnion(reference)))
                .Append(new IngestUnion(server));
        }
    }

    internal class ServerMetadata
    {
        [JsonPropertyName("root")]
        public string? Root { get; set; }
        [JsonPropertyName("prefix")]
        public string? Prefix { get; set; }
        [JsonPropertyName("namespaces")]
        public Dictionary<string, string>? Namespaces { get; set; }
        [JsonPropertyName("hierarchy_update_timestamp")]
        public long HierarchyUpdateTimestamp { get; set; }
    }
}

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
using System.Text;
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

        private async Task SendBulkInsertRequest(BulkRequestWrapper request, UAExtractor extractor, CancellationToken token)
        {
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
            options.Converters.Add(new PG3IngestConverter(config.Extraction.IdPrefix, extractor, referenceTypeIsHierarchical));

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
            UAExtractor extractor,
            CancellationToken token)
        {
            var nodes = objects
                .SelectMany(obj => obj.GetAllProperties())
                .Concat(objects)
                .Concat(variables.SelectMany(variable => variable.GetAllProperties()))
                .Concat(variables)
                .DistinctBy(node => node.Id)
                .ToList();

            var nodeIds = nodes.Select(node => node.Id).ToHashSet();

            var finalReferences = new List<UAReference>();

            int numSkipped = 0;
            foreach (var refr in references)
            {
                if (!refr.IsForward) continue;

                if (!nodeIds.Contains(refr.Source.Id))
                {
                    log.LogWarning("Missing source node {Node} ({Target})", refr.Source.Id, refr.Target.Id);
                    numSkipped++;
                    continue;
                }
                if (!nodeIds.Contains(refr.Target.Id))
                {
                    log.LogWarning("Missing target node {Node} ({Source})", refr.Target.Id, refr.Source.Id);
                    numSkipped++;
                    continue;
                }
                if (!nodeIds.Contains(refr.Type?.Id ?? NodeId.Null)) {
                    log.LogWarning("Missing type {Node} ({Source}, {Target})", refr.Type?.Id ?? NodeId.Null, refr.Source.Id, refr.Target.Id);
                    numSkipped++;
                    continue;
                }

                finalReferences.Add(refr);
            }

            // We need to map out hierarchical references here, but this is should be safe at this point.
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

            if (config.Cognite!.FlexibleDataModels!.ExcludeNonReferenced)
            {
                var trimmer = new NodeTrimmer(referenceTypeIsHierarchical, nodes, finalReferences, config, log);
                (nodes, finalReferences) = trimmer.Filter();
                log.LogInformation("After filtering {Nodes} nodes and {Edges} edges are left", nodes.Count, finalReferences.Count);
            }

            nodeIds = nodes.Select(node => node.Id).ToHashSet();

            var request = new BulkRequestWrapper(nodes, finalReferences);

            await SendBulkInsertRequest(request, extractor, token);

            return true;
        }
    }

    internal class BulkRequestWrapper
    {
        public string? Space { get; set; } = "opcua";
        public bool Overwrite { get; set; } = true;
        public IEnumerable<IngestUnion> Bulk { get; }
        public BulkRequestWrapper(IEnumerable<UANode> nodes, IEnumerable<UAReference> references)
        {
            Bulk = nodes.Select(node => new IngestUnion(node)).Concat(references.Select(reference => new IngestUnion(reference)));
        }
    }

    internal enum IngestType
    {
        Node,
        Reference
    }


}

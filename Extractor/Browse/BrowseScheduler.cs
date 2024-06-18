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

using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    internal class DirectoryBrowseParams
    {
        public TransformationCollection? Transformations { get; set; }
        public Action<ReferenceDescription, NodeId, bool>? Callback { get; set; }
        public int NodesChunk { get; set; }
        public int MaxNodeParallelism { get; set; }
        public BrowseParams? InitialParams { get; set; }
        public int MaxDepth { get; set; } = -1;
    }

    internal class BrowseScheduler : SharedResourceScheduler<BrowseNode>
    {
        private readonly UAClient client;
        private readonly DirectoryBrowseParams options;

        private readonly TransformationCollection? transformations;

        private readonly Action<ReferenceDescription, NodeId, bool>? callback;
        private readonly ISet<NodeId> localVisitedNodes = new HashSet<NodeId>();

        private readonly ILogger log;

        private readonly BrowseParams baseParams;

        private readonly List<int> depthCounts = new List<int>();

        private readonly List<Exception> exceptions = new List<Exception>();

        private int numReads;

        private bool failed;

        private readonly string purpose = "";

        private static readonly Gauge depth = Metrics
            .CreateGauge("opcua_tree_depth", "Depth of node tree from rootnode");

        public BrowseScheduler(
            ILogger log,
            TaskThrottler throttler,
            UAClient client,
            IResourceCounter resource,
            DirectoryBrowseParams options,
            CancellationToken token,
            string purpose = ""
            ) : base(options.InitialParams!.Items, throttler, options.NodesChunk, resource, token)
        {
            this.log = log;
            this.client = client;
            this.options = options;
            callback = options.Callback;

            if (options == null) throw new ArgumentNullException(nameof(options));
            if (options.InitialParams?.Nodes == null) throw new ArgumentException("options.InitialParams.Nodes is required");

            baseParams = options.InitialParams;

            transformations = options.Transformations;
            if (baseParams.Nodes.Count != 0)
            {
                foreach (var node in baseParams.Nodes)
                {
                    localVisitedNodes.Add(node.Value.Id);
                }
            }
            depthCounts.Add(baseParams.Nodes.Count);

            if (!string.IsNullOrEmpty(purpose)) this.purpose = $" for {purpose}";
        }



        protected override async Task AbortChunk(IChunk<BrowseNode> chunk, CancellationToken token)
        {
            try
            {
                await client.AbortBrowse(chunk.Items);
            }
            catch (Exception e)
            {
                ExtractorUtils.LogException(log, e, $"Failed to abort browse chunk{purpose}");
            }
            foreach (var item in chunk.Items)
            {
                item.ContinuationPoint = null;
            }
        }

        protected override async Task ConsumeChunk(IChunk<BrowseNode> chunk, CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
            numReads++;
            var browseChunk = (BrowseParams)chunk;
            await client.GetReferences(browseChunk, false, token);
        }

        protected override IChunk<BrowseNode> GetChunk(IEnumerable<BrowseNode> items)
        {
            return new BrowseParams(baseParams) { Nodes = items.ToDictionary(item => item.Id) };
        }

        /// <summary>
        /// Apply ignore filters, if any are set.
        /// </summary>
        /// <param name="displayName">DisplayName of node to filter</param>
        /// <param name="id">NodeId of node to filter</param>
        /// <param name="typeDefinition">TypeDefinition of node to filter</param>
        /// <param name="nc">NodeClass of node to filter</param>
        /// <returns>True if the node should be kept</returns>
        public bool NodeFilter(string displayName, NodeId id, NodeId typeDefinition, NodeClass nc)
        {
            if (transformations == null) return true;
            return transformations.ShouldIncludeBasic(displayName, id, typeDefinition, client.NamespaceTable, nc);
        }

        protected override async Task<IEnumerable<BrowseNode>> HandleTaskResult(IChunk<BrowseNode> chunk, CancellationToken token)
        {
            var result = new List<BrowseNode>();

            if (chunk.Exception != null)
            {
                ExtractorUtils.LogException(log, chunk.Exception, $"Unexpected failure during browse{purpose}");
                failed = true;
                exceptions.Add(chunk.Exception);
                await AbortChunk(chunk, token);
                return Enumerable.Empty<BrowseNode>();
            }

            if (failed) return Enumerable.Empty<BrowseNode>();

            foreach (var node in chunk.Items)
            {
                var refs = node.GetNextReferences();
                if (!refs.Any()) continue;

                log.LogTrace("Read {Count} children from node {Id}{Purpose}", refs.Count(), node.Id, purpose);
                foreach (var rd in refs)
                {
                    if (rd.NodeId.ServerIndex != 0) continue;
                    var nodeId = client.ToNodeId(rd.NodeId);
                    if (nodeId == ObjectIds.Server || nodeId == ObjectIds.Aliases || nodeId == ObjectIds.Locations) continue;
                    if (!NodeFilter(rd.DisplayName.Text, client.ToNodeId(rd.TypeDefinition), nodeId, rd.NodeClass))
                    {
                        log.LogTrace("Ignoring filtered {NodeId}", nodeId);
                        continue;
                    }

                    bool visited = !localVisitedNodes.Add(nodeId);
                    callback?.Invoke(rd, node.Id, visited);

                    if (node.Depth + 1 == depthCounts.Count) depthCounts.Add(1);
                    else depthCounts[node.Depth + 1]++;

                    if (rd.TypeDefinition == VariableTypeIds.PropertyType) continue;
                    if ((options.MaxDepth < 0 || node.Depth < options.MaxDepth) && !visited)
                    {
                        result.Add(new BrowseNode(nodeId, node));
                    }
                }
            }
            return result;
        }

        public new async Task RunAsync()
        {
            if (options.MaxDepth < 0)
            {
                log.LogInformation("Begin browsing {Count} nodes{Purpose}", baseParams.Nodes!.Count, purpose);
            }
            else
            {
                log.LogInformation("Begin browsing {Count} nodes to depth {Depth}{Purpose}",
                    baseParams.Nodes!.Count, options.MaxDepth + 1, purpose);
            }

            // If there is a reasonably low number of nodes...
            if (baseParams.Nodes!.Count < 40)
            {
                log.LogDebug("Browse node hierarchy{Purpose} for {Nodes}",
                    purpose, string.Join(", ", baseParams.Nodes!.Select(node => node.Key)));
            }
            await base.RunAsync();
            LogBrowseResult();
            if (exceptions.Count != 0)
            {
                throw new AggregateException(exceptions);
            }
        }

        private int currentFinished;

        private void LogBrowseResult()
        {
            int total = depthCounts.Sum();
            log.LogInformation("Browsed a total of {FinishedCount} nodes in {ReadCount} operations, and found {TotalCount} nodes total{Purpose}",
                currentFinished, numReads, total, purpose);

            var builder = new StringBuilder();
            for (int i = 0; i < depthCounts.Count; i++)
            {
                builder.AppendFormat("    {0}: {1}", i, depthCounts[i]);
                builder.Append(Environment.NewLine);
            }
            log.LogDebug("Total results{Purpose} by depth:{NewLine}{Results}", purpose, Environment.NewLine, builder);
            depth.IncTo(depthCounts.Count);
        }

        protected override void OnIteration(int pending, int operations, int finished, int total)
        {
            currentFinished = finished;
            log.LogDebug("Browse node children{Purpose}: {Pending} pending, {OpCount} total operations. {Finished}/{Total}",
                purpose, pending, operations, finished, total);
        }
    }
}

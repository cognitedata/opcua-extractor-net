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
using Opc.Ua;
using Prometheus;
using Serilog;
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
        public IEnumerable<NodeFilter>? Filters { get; set; }
        public Action<ReferenceDescription, NodeId>? Callback { get; set; }
        public bool ReadVariableChildren { get; set; }
        public int NodesChunk { get; set; }
        public int MaxNodeParallelism { get; set; }
        public BrowseParams? InitialParams { get; set; }
        public ISet<NodeId>? VisitedNodes { get; set; }
        public int MaxDepth { get; set; } = -1;
    }

    internal class BrowseScheduler : SharedResourceScheduler<BrowseNode>
    {
        private readonly UAClient client;
        private readonly DirectoryBrowseParams options;

        private readonly IEnumerable<NodeFilter>? filters;
        private readonly ISet<NodeId> visitedNodes;
        private readonly Action<ReferenceDescription, NodeId>? callback;
        private readonly ISet<NodeId> localVisitedNodes = new HashSet<NodeId>();

        private readonly ILogger log = Log.Logger.ForContext(typeof(BrowseScheduler));

        private readonly BrowseParams baseParams;

        private readonly List<int> depthCounts = new List<int>();

        private readonly List<Exception> exceptions = new List<Exception>();

        private int numReads;

        private bool failed;

        private static readonly Gauge depth = Metrics
            .CreateGauge("opcua_tree_depth", "Depth of node tree from rootnode");

        public BrowseScheduler(
            TaskThrottler throttler,
            UAClient client,
            IResourceCounter resource,
            DirectoryBrowseParams options,
            CancellationToken token
            ) : base(options.InitialParams!.Items, throttler, options.NodesChunk, resource, token)
        {
            this.client = client;
            this.options = options;
            visitedNodes = options.VisitedNodes ?? new HashSet<NodeId>();
            callback = options.Callback;

            if (options == null) throw new ArgumentNullException(nameof(options));
            if (options.InitialParams?.Nodes == null) throw new ArgumentException("options.InitialParams.Nodes is required");

            baseParams = options.InitialParams;

            filters = options.Filters;
            if (baseParams.Nodes.Any())
            {
                foreach (var node in baseParams.Nodes)
                {
                    localVisitedNodes.Add(node.Value.Id);
                    visitedNodes.Add(node.Value.Id);
                }
            }
            depthCounts.Add(baseParams.Nodes.Count);
        }



        protected override void AbortChunk(IChunk<BrowseNode> chunk, CancellationToken token)
        {
            try
            {
                client.AbortBrowse(chunk.Items).Wait(CancellationToken.None);
            }
            catch (Exception e)
            {
                ExtractorUtils.LogException(log, e, "Failed to abort browse chunk", "Failed to abort browse chunk");
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
            if (filters == null) return true;
            if (filters.Any(filter => filter.IsBasicMatch(displayName, id, typeDefinition, client.NamespaceTable!, nc))) return false;
            return true;
        }

        protected override IEnumerable<BrowseNode> HandleTaskResult(IChunk<BrowseNode> chunk, CancellationToken token)
        {
            var result = new List<BrowseNode>();

            if (failed) return Enumerable.Empty<BrowseNode>();

            if (chunk.Exception != null)
            {
                ExtractorUtils.LogException(log, chunk.Exception, "Unexpected failure during browse", "Unexpected failure during browse");
                failed = true;
                exceptions.Add(chunk.Exception);
                return Enumerable.Empty<BrowseNode>();
            }

            foreach (var node in chunk.Items)
            {
                var refs = node.GetNextReferences();
                if (!refs.Any()) continue;

                log.Verbose("Read {cnt} children from node {id}", refs.Count(), node.Id);
                foreach (var rd in refs)
                {
                    var nodeId = client.ToNodeId(rd.NodeId);
                    if (nodeId == ObjectIds.Server || nodeId == ObjectIds.Aliases) continue;
                    if (!NodeFilter(rd.DisplayName.Text, client.ToNodeId(rd.TypeDefinition), nodeId, rd.NodeClass))
                    {
                        log.Verbose("Ignoring filtered {nodeId}", nodeId);
                        continue;
                    }

                    bool docb = true;
                    if (visitedNodes != null && !visitedNodes.Add(nodeId))
                    {
                        docb = false;
                        log.Verbose("Ignoring visited {nodeId}", nodeId);
                    }
                    if (docb)
                    {
                        log.Verbose("Discovered new node {nodeId}", nodeId);
                        callback?.Invoke(rd, node.Id);
                    }

                    if (node.Depth + 1 == depthCounts.Count) depthCounts.Add(1);
                    else depthCounts[node.Depth + 1]++;

                    if (!options.ReadVariableChildren && rd.NodeClass == NodeClass.Variable) continue;
                    if (options.ReadVariableChildren && rd.TypeDefinition == VariableTypeIds.PropertyType) continue;
                    if ((options.MaxDepth < 0 || node.Depth < options.MaxDepth) && localVisitedNodes.Add(nodeId))
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
                log.Information("Begin browsing {cnt} nodes", baseParams.Nodes!.Count);
            }
            else
            {
                log.Information("Begin browsing {cnt} nodes to depth {depth}", baseParams.Nodes!.Count, options.MaxDepth + 1);
            }

            // If there is a reasonably low number of nodes...
            if (baseParams.Nodes!.Count < 40)
            {
                log.Debug("Browse node hierarchy for {nodes}", string.Join(',', baseParams.Nodes!.Select(node => node.Key)));
            }
            await base.RunAsync();
            LogBrowseResult();
            if (exceptions.Any())
            {
                throw new AggregateException(exceptions);
            }
        }

        private int currentFinished;

        private void LogBrowseResult()
        {
            int total = depthCounts.Sum();
            log.Information("Browsed a total of {cnt} nodes in {cnt2} operations, and found {cnt3} nodes total",
                currentFinished, numReads, total);

            var builder = new StringBuilder("Total results by depth: \n");
            for (int i = 0; i < depthCounts.Count; i++)
            {
                builder.AppendFormat("    {0}: {1}\n", i, depthCounts[i]);
            }
            log.Debug(builder.ToString());
            depth.IncTo(depthCounts.Count);
        }

        protected override void OnIteration(int pending, int operations, int finished, int total)
        {
            currentFinished = finished;
            log.Debug("Browse node children: {pend} pending, {op} total operations. {fin}/{tot}", pending, operations, finished, total);
        }
    }
}

using Opc.Ua.Client;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Cognite.Extractor.Common;
using Prometheus;
using System.Collections.Concurrent;

namespace Cognite.OpcUa
{
    internal sealed class BrowseScheduler : IDisposable
    {
        private readonly TaskThrottler throttler;
        private readonly BrowseParams baseParams;
        private int numActiveNodes;
        private readonly UAClient uaClient;
        private readonly int chunkSize;
        private readonly int maxNodeParallelism;
        private ILogger log = Log.Logger.ForContext(typeof(BrowseScheduler));

        private Dictionary<NodeId, ReferenceDescriptionCollection> results = new Dictionary<NodeId, ReferenceDescriptionCollection>();
        private object dictLock = new object();
        public BrowseScheduler(TaskThrottler throttler, BrowseParams baseParams, UAClient client, int nodesChunk, int maxNodeParallelism)
        {
            this.throttler = throttler;
            uaClient = client;
            chunkSize = nodesChunk;
            this.maxNodeParallelism = maxNodeParallelism;
            this.baseParams = baseParams;
        }

        private readonly BlockingCollection<BrowseParams> finishedReads = new BlockingCollection<BrowseParams>();

        private void BrowseOp(BrowseParams browseParams, CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
            try
            {
                var references = uaClient.GetReferences(browseParams, token);
                int found = 0;
                lock (dictLock)
                {
                    foreach (var kvp in references)
                    {
                        found += kvp.Value.Count;
                        results[kvp.Key] = kvp.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, "Unexpected failure during browse: {msg}", ex.Message);
            }
            finally
            {
                finishedReads.Add(browseParams, token);
            }
        }

        private IEnumerable<BrowseNode> GetBrowseChunk(List<BrowseNode> nodes, int idx)
        {
            if (idx >= nodes.Count) yield break;
            for (int i = idx; i < nodes.Count; i++)
            {
                if (chunkSize > 0 && i - idx + 1 > chunkSize) yield break;
                if (maxNodeParallelism > 0 && i - idx + numActiveNodes >= maxNodeParallelism) yield break;
                yield return nodes[i];
            }
        }

        private List<BrowseParams> GetNextChunks(List<BrowseNode> nodes, ref int index)
        {
            List<BrowseParams> chunks = new List<BrowseParams>();
            List<BrowseNode> chunk;
            do
            {
                chunk = GetBrowseChunk(nodes, index).ToList();
                numActiveNodes += chunk.Count;
                index += chunk.Count;
                if (chunk.Any())
                {
                    var browseParams = new BrowseParams(baseParams) { Nodes = chunk };
                    chunks.Add(browseParams);
                }
            } while (chunk.Any());
            return chunks;
        }


        public Dictionary<NodeId, ReferenceDescriptionCollection> BrowseLevel(CancellationToken token)
        {
            var nodes = baseParams.Nodes.ToList();
            int index = 0;
            int totalRead = 0;
            int toRead = nodes.Count;
            List<BrowseParams> chunks = GetNextChunks(nodes, ref index);

            while (numActiveNodes > 0 || chunks.Any())
            {
                var generators = chunks
                    .Select<BrowseParams, Func<Task>>(
                        chunk => () => {
                            return Task.Run(() => BrowseOp(chunk, token));
                        })
                    .ToList();
                chunks.Clear();

                foreach (var generator in generators)
                {
                    throttler.EnqueueTask(generator);
                }
                var finished = new List<BrowseParams>();
                try
                {
                    finished.Add(finishedReads.Take(token));
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                while (finishedReads.TryTake(out var finishedRead))
                {
                    finished.Add(finishedRead);
                }

                foreach (var chunk in finished)
                {
                    int numRead = chunk.Nodes.Count();
                    numActiveNodes -= numRead;
                    totalRead += numRead;

                    log.Debug("Read children for {cnt}/{total} nodes", totalRead, toRead);
                }

                chunks.AddRange(GetNextChunks(nodes, ref index));
            }
            return results;
        }

        public void Dispose()
        {
            finishedReads.Dispose();
        }
    }



    public sealed class Browser : IDisposable
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(Browser));
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        private readonly object visitedNodesLock = new object();
        private readonly ISet<NodeId> visitedNodes = new HashSet<NodeId>();
        private static readonly Gauge depth = Metrics
            .CreateGauge("opcua_tree_depth", "Depth of node tree from rootnode");

        private readonly ContinuationPointThrottlingConfig throttling;
        private readonly TaskThrottler throttler;

        public IEnumerable<NodeFilter> IgnoreFilters { get; set; }

        public Browser(UAClient client, FullConfig config)
        {
            uaClient = client ?? throw new ArgumentNullException(nameof(client));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            if (config.Source.BrowseThrottling == null)
            {
                throttling = new ContinuationPointThrottlingConfig { MaxNodeParallelism = 0, MaxParallelism = 0, MaxPerMinute = 0 };
            }
            else
            {
                throttling = config.Source.BrowseThrottling;
            }
            throttler = new TaskThrottler(throttling.MaxParallelism, false, throttling.MaxPerMinute, TimeSpan.FromMinutes(1));
        }


        /// <summary>
        /// Browse node hierarchy for single root node
        /// </summary>
        /// <param name="root">Root node to browse for</param>
        /// <param name="callback">Callback to call for each found node</param>
        /// <param name="ignoreVisited">Default true, do not call callback for previously visited nodes</param>
        public Task BrowseNodeHierarchy(NodeId root,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            bool ignoreVisited = true)
        {
            return BrowseNodeHierarchy(new[] { root }, callback, token, ignoreVisited);
        }
        /// <summary>
        /// Browse an opcua directory, calling callback for all relevant nodes found.
        /// </summary>
        /// <param name="roots">Initial nodes to start mapping.</param>
        /// <param name="callback">Callback for each mapped node, takes a description of a single node, and its parent id</param>
        /// <param name="ignoreVisited">Default true, do not call callback for previously visited nodes</param>
        public async Task BrowseNodeHierarchy(IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            bool ignoreVisited = true)
        {
            if (roots == null) throw new ArgumentNullException(nameof(roots));
            log.Debug("Browse node tree for nodes {nodes}", string.Join(", ", roots));
            foreach (var root in roots)
            {
                bool docb = true;
                lock (visitedNodesLock)
                {
                    if (!visitedNodes.Add(root) && ignoreVisited)
                    {
                        docb = false;
                    }
                }
                if (docb)
                {
                    var rootNode = GetRootNode(root, token);
                    if (rootNode == null) throw new ExtractorFailureException($"Root node does not exist: {root}");
                    callback?.Invoke(rootNode, null);
                }
            }
            uint classMask = (uint)NodeClass.Variable | (uint)NodeClass.Object;
            if (config.Extraction.NodeTypes.AsNodes)
            {
                classMask |= (uint)NodeClass.VariableType | (uint)NodeClass.ObjectType;
            }
            await Task.Run(() => BrowseDirectory(roots, callback, token, null,
                classMask, ignoreVisited), token);
        }
        /// <summary>
        /// Get the root node and return it as a reference description.
        /// </summary>
        /// <param name="nodeId">Id of the root node</param>
        /// <returns>A partial description of the root node</returns>
        public ReferenceDescription GetRootNode(NodeId nodeId, CancellationToken token)
        {
            var attributes = new List<uint>
            {
                Attributes.NodeId,
                Attributes.BrowseName,
                Attributes.DisplayName,
                Attributes.NodeClass
            };
            var readValueIds = attributes.Select(attr => new ReadValueId { NodeId = nodeId, AttributeId = attr }).ToList();
            IList<DataValue> results;
            try
            {
                results = uaClient.ReadAttributes(new ReadValueIdCollection(readValueIds), 1, token);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadRootNode);
            }
            var refd = new ReferenceDescription();
            refd.NodeId = results[0].GetValue(NodeId.Null);
            if (refd.NodeId == NodeId.Null) return null;
            refd.BrowseName = results[1].GetValue(QualifiedName.Null);
            refd.DisplayName = results[2].GetValue(LocalizedText.Null);
            refd.NodeClass = (NodeClass)results[3].GetValue(0);

            if (config.Extraction.NodeTypes.Metadata)
            {
                try
                {
                    var children = uaClient.GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.ObjectType | (uint)NodeClass.VariableType,
                        ReferenceTypeId = ReferenceTypeIds.HasTypeDefinition,
                        IncludeSubTypes = false,
                        Nodes = new [] { new BrowseNode(nodeId) },
                        MaxPerNode = 1
                    }, token);
                    var references = children[nodeId];
                    if (references?.Any() ?? false)
                    {
                        refd.TypeDefinition = references.First().NodeId;
                    }
                }
                catch (ServiceResultException ex)
                {
                    throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadRootNode);
                }
            }

            refd.ReferenceTypeId = null;
            refd.IsForward = true;
            return refd;
        }

        
        public Dictionary<NodeId, ReferenceDescriptionCollection> BrowseLevel(BrowseParams baseParams, CancellationToken token)
        {
            using var scheduler = new BrowseScheduler(throttler, baseParams, uaClient, config.Source.BrowseNodesChunk, throttling.MaxNodeParallelism);
            return scheduler.BrowseLevel(token);
        }

        /// <summary>
        /// Get all children of root nodes recursively and invoke the callback for each.
        /// </summary>
        /// <param name="roots">Root nodes to browse</param>
        /// <param name="callback">Callback for each node</param>
        /// <param name="referenceTypes">Permitted reference types, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes as described in the OPC-UA specification</param>
        /// <param name="doFilter">True to apply the node filter to discovered nodes</param>
        /// <param name="ignoreVisited">True to not call callback on already visited nodes.</param>
        /// <param name="readVariableChildren">Read the children of variables.</param>
        public void BrowseDirectory(
            IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            NodeId referenceTypes = null,
            uint nodeClassMask = (uint)NodeClass.Variable | (uint)NodeClass.Object,
            bool ignoreVisited = true,
            bool doFilter = true,
            bool readVariableChildren = false)
        {
            if (roots == null) throw new ArgumentNullException(nameof(roots));
            var nextIds = roots.ToList();
            int levelCnt = 0;
            int nodeCnt = 0;
            var localVisitedNodes = new HashSet<NodeId>();
            var baseParams = new BrowseParams
            {
                NodeClassMask = nodeClassMask,
                ReferenceTypeId = referenceTypes ?? ReferenceTypeIds.HierarchicalReferences,
                MaxPerNode = (uint)config.Source.BrowseChunk
            };

            foreach (var root in roots)
            {
                localVisitedNodes.Add(root);
            }
            do
            {
                var total = nextIds.Count;
                int count = 0;
                int countChildren = 0;

                baseParams.Nodes = nextIds.Select(id => new BrowseNode(id)).ToList();

                var references = BrowseLevel(baseParams, token);

                foreach (var res in references)
                {
                    countChildren += res.Value.Count;
                }
                count += references.Count;

                log.Information("Found {cnt} children of {cnt2} nodes", countChildren, count);

                nextIds.Clear();
                levelCnt++;
                foreach (var (parentId, children) in references)
                {
                    nodeCnt += children.Count;
                    foreach (var rd in children)
                    {
                        var nodeId = uaClient.ToNodeId(rd.NodeId);
                        if (rd.NodeId == ObjectIds.Server || rd.NodeId == ObjectIds.Aliases) continue;
                        if (doFilter && !NodeFilter(rd.DisplayName.Text, uaClient.ToNodeId(rd.TypeDefinition), uaClient.ToNodeId(rd.NodeId), rd.NodeClass))
                        {
                            log.Verbose("Ignoring filtered {nodeId}", nodeId);
                            continue;
                        }

                        bool docb = true;
                        lock (visitedNodesLock)
                        {
                            if (ignoreVisited && !visitedNodes.Add(nodeId))
                            {
                                docb = false;
                                log.Verbose("Ignoring visited {nodeId}", nodeId);
                            }
                        }
                        if (docb)
                        {
                            log.Verbose("Discovered new node {nodeid}", nodeId);
                            callback?.Invoke(rd, parentId);
                        }
                        if (!readVariableChildren && rd.NodeClass == NodeClass.Variable) continue;
                        if (readVariableChildren && rd.TypeDefinition == VariableTypeIds.PropertyType) continue;
                        if (localVisitedNodes.Add(nodeId))
                        {
                            nextIds.Add(nodeId);
                        }
                    }
                }
            } while (nextIds.Any());
            log.Information("Found {NumUANodes} nodes in {NumNodeLevels} levels", nodeCnt, levelCnt);
            depth.Set(levelCnt);
        }
        /// <summary>
        /// Clear internal list of visited nodes, allowing callbacks to be called for visited nodes again.
        /// </summary>
        public void ResetVisitedNodes()
        {
            lock (visitedNodesLock)
            {
                visitedNodes.Clear();
            }
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
            if (IgnoreFilters == null) return true;
            if (IgnoreFilters.Any(filter => filter.IsBasicMatch(displayName, id, typeDefinition, uaClient.NamespaceTable, nc))) return false;
            return true;
        }

        public void Dispose()
        {
            throttler.Dispose();
        }
    }
    public class BrowseParams
    {
        public IEnumerable<BrowseNode> Nodes { get; set; }
        public NodeId ReferenceTypeId { get; set; } = ReferenceTypeIds.HierarchicalReferences;
        public uint NodeClassMask { get; set; }
        public bool IncludeSubTypes { get; set; } = true;
        public BrowseDirection BrowseDirection { get; set; } = BrowseDirection.Forward;
        public uint MaxPerNode { get; set; }
        public uint ResultMask { get; set; } =
            (uint)BrowseResultMask.NodeClass | (uint)BrowseResultMask.DisplayName | (uint)BrowseResultMask.IsForward
            | (uint)BrowseResultMask.ReferenceTypeId | (uint)BrowseResultMask.TypeDefinition | (uint)BrowseResultMask.BrowseName;
        public BrowseDescription ToDescription(BrowseNode node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (node.ContinuationPoint != null) throw new ArgumentException("Node has already been read");
            return new BrowseDescription
            {
                BrowseDirection = BrowseDirection,
                NodeClassMask = NodeClassMask,
                IncludeSubtypes = IncludeSubTypes,
                NodeId = node.Id,
                ReferenceTypeId = ReferenceTypeId,
                ResultMask = ResultMask
            };
        }

        public BrowseParams() { }
        public BrowseParams(BrowseParams other)
        {
            if (other == null) return;
            ReferenceTypeId = other.ReferenceTypeId;
            NodeClassMask = other.NodeClassMask;
            IncludeSubTypes = other.IncludeSubTypes;
            BrowseDirection = other.BrowseDirection;
            MaxPerNode = other.MaxPerNode;
            ResultMask = other.ResultMask;
        }
    }
    public class BrowseNode
    {
        public BrowseNode(NodeId id)
        {
            Id = id;
        }
        public NodeId Id { get; }
        public byte[] ContinuationPoint { get; set; }
    }
}

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
    internal class DirectoryBrowseParams
    {
        public IEnumerable<NodeFilter> Filters { get; set; }
        public Action<ReferenceDescription, NodeId> Callback { get; set; }
        public bool ReadVariableChildren { get; set; }
        public int NodesChunk { get; set; }
        public int MaxNodeParallelism { get; set; }
        public BrowseParams InitialParams { get; set; }
        public HashSet<NodeId> VisitedNodes { get; set; }
        public int MaxDepth { get; set; } = -1;
    }


    internal sealed class BrowseScheduler : IDisposable
    {
        private readonly TaskThrottler throttler;
        private readonly BrowseParams baseParams;
        private int numActiveNodes;
        private readonly UAClient uaClient;
        private ILogger log = Log.Logger.ForContext(typeof(BrowseScheduler));

        private Dictionary<NodeId, ReferenceDescriptionCollection> results = new Dictionary<NodeId, ReferenceDescriptionCollection>();
        private object dictLock = new object();

        private readonly DirectoryBrowseParams options;

        private readonly IEnumerable<NodeFilter> filters;
        private readonly HashSet<NodeId> visitedNodes;
        private readonly Action<ReferenceDescription, NodeId> callback;
        private readonly HashSet<NodeId> localVisitedNodes = new HashSet<NodeId>();
        public BrowseScheduler(
            TaskThrottler throttler,
            UAClient client,
            DirectoryBrowseParams options)
        {
            this.throttler = throttler;
            uaClient = client;
            this.options = options;
            visitedNodes = options.VisitedNodes ?? new HashSet<NodeId>();
            callback = options.Callback;
            baseParams = options.InitialParams;
        }

        private readonly BlockingCollection<BrowseParams> finishedReads = new BlockingCollection<BrowseParams>();

        private void BrowseOp(BrowseParams browseParams, CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
            try
            {
                uaClient.GetReferences(browseParams, false, token);
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
            if (filters.Any(filter => filter.IsBasicMatch(displayName, id, typeDefinition, uaClient.NamespaceTable, nc))) return false;
            return true;
        }

        private ICollection<BrowseNode> HandleReadResult(ICollection<BrowseNode> nextIds, BrowseNode node)
        {
            if (node.Result == null || !node.Result.References.Any()) return nextIds;
            log.Verbose("Read {cnt} children from node {id}", node.Result.References.Count);

            foreach (var rd in node.Result.References)
            {
                var nodeId = uaClient.ToNodeId(rd.NodeId);
                if (nodeId == ObjectIds.Server || nodeId == ObjectIds.Aliases) continue;
                if (!NodeFilter(rd.DisplayName.Text, uaClient.ToNodeId(rd.TypeDefinition), nodeId, rd.NodeClass))
                {
                    log.Verbose("Ignoring filtered {nodeId}", nodeId);
                    continue;
                }

                bool docb = true;
                if (visitedNodes != null && visitedNodes.Contains(nodeId))
                {
                    docb = false;
                    log.Verbose("Ignoring visited {nodeId}", nodeId);
                }
                if (docb)
                {
                    log.Verbose("Discovered new node {nodeId}", nodeId);
                    callback?.Invoke(rd, node.Id);
                }
                if (!options.ReadVariableChildren && rd.NodeClass == NodeClass.Variable) continue;
                if (options.ReadVariableChildren && rd.TypeDefinition == VariableTypeIds.PropertyType) continue;
                if ((options.MaxDepth < 0 || node.Depth < options.MaxDepth) && localVisitedNodes.Add(nodeId))
                {
                    nextIds.Add(new BrowseNode(nodeId, node));
                }
            }

            return nextIds;
        }

        private List<BrowseNode> GetBrowseChunk(LinkedListNode<BrowseNode> first, out LinkedListNode<BrowseNode> current)
        {
            var result = new List<BrowseNode>();
            int cnt = 0;
            current = first;
            do
            {
                if (options.NodesChunk > 0 && cnt >= options.NodesChunk) return result;
                if (options.MaxNodeParallelism > 0 && cnt + numActiveNodes >= options.MaxNodeParallelism) return result;
                result.Add(current.Value);
                current = current.Next;
            } while (current != null);
            return result;
        }

        private List<BrowseParams> GetNextChunks(LinkedListNode<BrowseNode> first, out LinkedListNode<BrowseNode> current)
        {
            List<BrowseParams> chunks = new List<BrowseParams>();
            List<BrowseNode> chunk;
            current = first;
            do
            {
                chunk = GetBrowseChunk(current, out current);
                numActiveNodes += chunk.Count;
                if (chunk.Any())
                {
                    var browseParams = new BrowseParams(baseParams) { Nodes = chunk.ToDictionary(node => node.Id) };
                    chunks.Add(browseParams);
                }
            } while (chunk.Any());
            return chunks;
        }

        public Dictionary<NodeId, ReferenceDescriptionCollection> Browse(CancellationToken token)
        {
            var nodes = new LinkedList<BrowseNode>(baseParams.Nodes.Values);
            int totalRead = 0;
            int toRead = nodes.Count;
            var continued = new List<BrowseNode>();
            List<BrowseParams> chunks = GetNextChunks(nodes.First, out var current);

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
                    foreach (var node in chunk.Nodes.Values)
                    {
                        HandleReadResult(nodes, node);
                        if (node.ContinuationPoint != null)
                        {
                            nodes.AddBefore(current, node);
                        }
                    }
                }
                log.Debug("Read children for {cnt}/{total} nodes", totalRead, nodes.Count);

                chunks.AddRange(GetNextChunks(current, out current));
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
                    var node = new BrowseNode(nodeId);
                    uaClient.GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.ObjectType | (uint)NodeClass.VariableType,
                        ReferenceTypeId = ReferenceTypeIds.HasTypeDefinition,
                        IncludeSubTypes = false,
                        Nodes = new Dictionary<NodeId, BrowseNode> { { nodeId, node } },
                        MaxPerNode = 1
                    }, true, token);
                    var result = node.Result;
                    if (result.References.Any())
                    {
                        refd.TypeDefinition = result.References.First().NodeId;
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

        private Action<ReferenceDescription, NodeId> GetDictWriteCallback(Dictionary<NodeId, ReferenceDescriptionCollection> dict)
        {
            object lck = new object();
            return (rd, nodeId) =>
            {
                lock(lck)
                {
                    if (!dict.TryGetValue(nodeId, out var refs))
                    {
                        dict[nodeId] = refs = new ReferenceDescriptionCollection();
                    }
                    refs.Add(rd);
                }
            };
        }




        public Dictionary<NodeId, ReferenceDescriptionCollection> BrowseLevel(
            BrowseParams baseParams,
            CancellationToken token,
            bool doFilter = true,
            bool ignoreVisited = false)
        {
            var result = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            var options = new DirectoryBrowseParams
            {
                Callback = GetDictWriteCallback(result),
                Filters = doFilter ? IgnoreFilters : null,
                InitialParams = baseParams,
                MaxNodeParallelism = throttling.MaxNodeParallelism,
                NodesChunk = config.Source.BrowseNodesChunk,
                ReadVariableChildren = false,
                VisitedNodes = null,
                MaxDepth = 0
            };

            using var scheduler = new BrowseScheduler(throttler, uaClient, options);
            return scheduler.Browse(token);
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

                baseParams.Nodes = nextIds.Select(id => new BrowseNode(id)).ToDictionary(node => node.Id);

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
        

        public void Dispose()
        {
            throttler.Dispose();
        }
    }
    public class BrowseParams
    {
        public Dictionary<NodeId, BrowseNode> Nodes { get; set; }
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
            Depth = 0;
        }
        public BrowseNode(NodeId id, BrowseNode parent)
        {
            Id = id;
            Depth = parent.Depth + 1;
        }
        public int Depth { get; }
        public NodeId Id { get; }
        public byte[] ContinuationPoint { get; set; }
        public BrowseResult Result { get; private set; }
        public void AddReferences(ReferenceDescriptionCollection references)
        {
            if (references == null) references = new ReferenceDescriptionCollection();
            if (Result == null)
            {
                Result = new BrowseResult(this, references);
            }
            else
            {
                Result.References.AddRange(references);
            }
        }
    }
    public class BrowseResult
    {
        public BrowseNode Parent { get; }
        public ReferenceDescriptionCollection References { get; }
        public BrowseResult(BrowseNode parent, ReferenceDescriptionCollection references)
        {
            Parent = parent;
            References = references;
        }
    }
}

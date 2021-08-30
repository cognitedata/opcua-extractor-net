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
        public ISet<NodeId> VisitedNodes { get; set; }
        public int MaxDepth { get; set; } = -1;
    }


    internal sealed class BrowseScheduler : IDisposable
    {
        private readonly TaskThrottler throttler;
        private readonly BrowseParams baseParams;
        private int numActiveNodes;
        private readonly UAClient uaClient;
        private ILogger log = Log.Logger.ForContext(typeof(BrowseScheduler));

        private static readonly Gauge depth = Metrics
            .CreateGauge("opcua_tree_depth", "Depth of node tree from rootnode");

        private readonly DirectoryBrowseParams options;

        private readonly IEnumerable<NodeFilter> filters;
        private readonly ISet<NodeId> visitedNodes;
        private readonly Action<ReferenceDescription, NodeId> callback;
        private readonly ISet<NodeId> localVisitedNodes = new HashSet<NodeId>();

        private List<int> depthCounts = new List<int>();
        private int numOperations;

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
            filters = options.Filters;
            if (baseParams.Nodes?.Any() ?? false)
            {
                foreach (var node in baseParams.Nodes)
                {
                    localVisitedNodes.Add(node.Value.Id);
                    visitedNodes.Add(node.Value.Id);
                }
            }
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

        private int HandleReadResult(ICollection<BrowseNode> nextIds, BrowseNode node)
        {
            if (node.Result == null || !node.Result.References.Any()) return 0;
            int cnt = 0;
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
                    nextIds.Add(new BrowseNode(nodeId, node));
                    cnt++;
                }
                
            }

            return cnt;
        }

        private List<BrowseNode> GetBrowseChunk(LinkedListNode<BrowseNode> first, out LinkedListNode<BrowseNode> current)
        {
            var result = new List<BrowseNode>();
            int cnt = 0;

            current = first;
            while (current != null)
            {
                if (options.NodesChunk > 0 && cnt >= options.NodesChunk) break;
                if (options.MaxNodeParallelism > 0 && cnt + numActiveNodes >= options.MaxNodeParallelism) break;
                result.Add(current.Value);
                cnt++;
                current = current.Next;
            }

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
            } while (chunk.Any() && current != null);
            return chunks;
        }

        private void LogBrowseResult(IEnumerable<BrowseNode> nodes)
        {
            int total = depthCounts.Sum();
            log.Information("Browsed a total of {cnt} nodes in {cnt2} operations, and found {cnt3} nodes total",
                nodes.Count(), numOperations, total);

            var builder = new StringBuilder("Total results by depth: \n");
            for (int i = 0; i < depthCounts.Count; i++)
            {
                builder.AppendFormat("    {0}: {1}\n", i, depthCounts[i]);
            }
            log.Debug(builder.ToString());
            depth.IncTo(depthCounts.Count);
        }

        public void Browse(CancellationToken token)
        {
            var nodes = new LinkedList<BrowseNode>(baseParams.Nodes.Values);
            depthCounts.Add(nodes.Count);
            log.Information("Browse node hierarchy for {cnt} nodes", nodes.Count);
            int totalRead = 0;
            int totalToRead = nodes.Count;
            int toRead = nodes.Count;
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

                if (current == null) nodes.Clear();

                foreach (var chunk in finished)
                {
                    numOperations++;
                    foreach (var node in chunk.Nodes.Values)
                    {
                        totalToRead += HandleReadResult(nodes, node);
                        numActiveNodes--;
                        if (node.ContinuationPoint != null)
                        {
                            if (current == null)
                            {
                                nodes.AddFirst(node);
                            }
                            else
                            {
                                nodes.AddAfter(current, node);
                            }
                        }
                        else
                        {
                            totalRead++;
                        }
                    }
                }
                log.Debug("Read children for {cnt}/{total} nodes", totalRead, totalToRead);

                if (current == null) current = nodes.First;

                chunks.AddRange(GetNextChunks(current, out current));
            }
            LogBrowseResult(nodes);
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

        private readonly ContinuationPointThrottlingConfig throttling;
        private readonly TaskThrottler throttler;

        public IEnumerable<NodeFilter> IgnoreFilters { get; set; }

        public Browser(UAClient client, FullConfig config)
        {
            uaClient = client ?? throw new ArgumentNullException(nameof(client));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            throttling = config.Source.BrowseThrottling;
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
            var rootRefs = GetRootNodes(roots, token);
            foreach (var root in rootRefs)
            {
                bool docb = true;
                lock (visitedNodesLock)
                {
                    if (!visitedNodes.Add(uaClient.ToNodeId(root.NodeId)) && ignoreVisited)
                    {
                        docb = false;
                    }
                }
                if (docb) callback?.Invoke(root, null);
            }
            uint classMask = (uint)NodeClass.Variable | (uint)NodeClass.Object;
            if (config.Extraction.NodeTypes.AsNodes)
            {
                classMask |= (uint)NodeClass.VariableType | (uint)NodeClass.ObjectType;
            }
            await Task.Run(() => BrowseDirectory(roots, callback, token, null,
                classMask, ignoreVisited), token);
        }
        public IEnumerable<ReferenceDescription> GetRootNodes(IEnumerable<NodeId> ids, CancellationToken token)
        {
            var attributes = new uint[]
            {
                Attributes.NodeId,
                Attributes.BrowseName,
                Attributes.DisplayName,
                Attributes.NodeClass
            };
            var readValueIds = ids
                .SelectMany(id => attributes.Select(attr => new ReadValueId { NodeId = id, AttributeId = attr }))
                .ToList();

            IList<DataValue> results;
            try
            {
                results = uaClient.ReadAttributes(new ReadValueIdCollection(readValueIds), 1, token);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadRootNode);
            }

            var roots = new Dictionary<NodeId, ReferenceDescription>();

            for (int i = 0; i < ids.Count(); i++)
            {
                var id = results[i * attributes.Length].GetValue(NodeId.Null);
                if (id == NodeId.Null) throw new ExtractorFailureException($"Root node does not exist: {ids.ElementAt(i)}");
                var refd = new ReferenceDescription
                {
                    NodeId = id,
                    BrowseName = results[i * attributes.Length + 1].GetValue(QualifiedName.Null),
                    DisplayName = results[i * attributes.Length + 2].GetValue(LocalizedText.Null),
                    NodeClass = (NodeClass)results[i * attributes.Length + 3].GetValue(0),
                    ReferenceTypeId = null,
                    IsForward = true
                };
                roots[id] = refd;
            }

            if (config.Extraction.NodeTypes.Metadata)
            {
                try
                {
                    var nodes = ids.Select(id => new BrowseNode(id)).ToDictionary(node => node.Id);

                    uaClient.GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.ObjectType | (uint)NodeClass.VariableType,
                        ReferenceTypeId = ReferenceTypeIds.HasTypeDefinition,
                        IncludeSubTypes = false,
                        Nodes = nodes,
                        MaxPerNode = 1
                    }, true, token);
                    foreach (var node in nodes.Values)
                    {
                        var result = node.Result;
                        if (result.References.Any())
                        {
                            roots[node.Id].TypeDefinition = result.References.First().NodeId;
                        }
                    }
                }
                catch (ServiceResultException ex)
                {
                    throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadRootNode);
                }
            }

            return roots.Values;
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
                VisitedNodes = ignoreVisited ? null : visitedNodes,
                MaxDepth = 0
            };

            using var scheduler = new BrowseScheduler(throttler, uaClient, options);
            scheduler.Browse(token);
            foreach (var node in baseParams.Nodes)
            {
                result[node.Key] = node.Value.Result.References;
            }
            return result;
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

            var baseParams = new BrowseParams
            {
                NodeClassMask = nodeClassMask,
                ReferenceTypeId = referenceTypes ?? ReferenceTypeIds.HierarchicalReferences,
                MaxPerNode = (uint)config.Source.BrowseChunk,
                Nodes = roots.ToDictionary(id => id, id => new BrowseNode(id))
            };
            var options = new DirectoryBrowseParams
            {
                Callback = callback,
                NodesChunk = config.Source.BrowseNodesChunk,
                ReadVariableChildren = readVariableChildren,
                MaxDepth = -1,
                Filters = doFilter ? IgnoreFilters : null,
                InitialParams = baseParams,
                MaxNodeParallelism = throttling.MaxNodeParallelism,
                VisitedNodes = ignoreVisited ? visitedNodes : null
            };

            using var scheduler = new BrowseScheduler(throttler, uaClient, options);
            scheduler.Browse(token);
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

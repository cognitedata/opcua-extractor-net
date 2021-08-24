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

namespace Cognite.OpcUa
{
    public class Browser
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(Browser));
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        private readonly object visitedNodesLock = new object();
        private readonly ISet<NodeId> visitedNodes = new HashSet<NodeId>();
        private static readonly Gauge depth = Metrics
            .CreateGauge("opcua_tree_depth", "Depth of node tree from rootnode");

        public IEnumerable<NodeFilter> IgnoreFilters { get; set; }

        public Browser(UAClient client, FullConfig config)
        {
            uaClient = client ?? throw new ArgumentNullException(nameof(client));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
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
                    var children = uaClient.GetNodeChildren(new[] { nodeId }, ReferenceTypeIds.HasTypeDefinition, (uint)NodeClass.ObjectType | (uint)NodeClass.VariableType, token);
                    var references = children.GetValueOrDefault(nodeId);
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
            foreach (var root in roots)
            {
                localVisitedNodes.Add(root);
            }
            do
            {
                var references = new Dictionary<NodeId, ReferenceDescriptionCollection>();
                var total = nextIds.Count;
                int count = 0;
                int countChildren = 0;
                foreach (var chunk in nextIds.ChunkBy(config.Source.BrowseNodesChunk))
                {
                    if (token.IsCancellationRequested) return;
                    var result = uaClient.GetNodeChildren(chunk, referenceTypes, nodeClassMask, token);
                    foreach (var res in result)
                    {
                        references[res.Key] = res.Value;
                        countChildren += res.Value.Count;
                    }
                    count += result.Count;
                    log.Debug("Read node children {cnt} / {total}. Children: {childcnt}", count, total, countChildren);
                }

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
    }
    public class BrowseParams
    {
        public IEnumerable<BrowseNode> Nodes { get; set; }
        public NodeId ReferenceTypeId { get; set; } = ReferenceTypeIds.HierarchicalReferences;
        public uint NodeClassMask { get; set; }
        public bool IncludeSubTypes { get; set; } = true;
        public BrowseDirection BrowseDirection { get; set; } = BrowseDirection.Forward;
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
    }
    public class BrowseNode
    {
        public NodeId Id { get; }
        public byte[] ContinuationPoint { get; set; }
    }
}

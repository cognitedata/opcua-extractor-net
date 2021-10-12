﻿/* Cognite Extractor for OPC-UA
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

using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Cognite.Extractor.Common;

namespace Cognite.OpcUa
{
    public sealed class Browser : IDisposable
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(Browser));
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        private readonly object visitedNodesLock = new object();
        private readonly ISet<NodeId> visitedNodes = new HashSet<NodeId>();

        private readonly ContinuationPointThrottlingConfig throttling;
        private readonly TaskThrottler throttler;

        private readonly BlockingResourceCounter continuationPoints;

        public IEnumerable<NodeFilter>? IgnoreFilters { get; set; }

        public Browser(UAClient client, FullConfig config)
        {
            uaClient = client ?? throw new ArgumentNullException(nameof(client));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            throttling = config.Source.BrowseThrottling;
            throttler = new TaskThrottler(throttling.MaxParallelism, false, throttling.MaxPerMinute, TimeSpan.FromMinutes(1));
            continuationPoints = new BlockingResourceCounter(throttling.MaxNodeParallelism <= 0 ? 100_000 : throttling.MaxNodeParallelism);
        }

        /// <summary>
        /// Browse node hierarchy for single root node
        /// </summary>
        /// <param name="root">Root node to browse for</param>
        /// <param name="callback">Callback to call for each found node</param>
        /// <param name="ignoreVisited">Default true, do not call callback for previously visited nodes</param>
        public Task BrowseNodeHierarchy(NodeId root,
            Action<ReferenceDescription, NodeId?> callback,
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
                if (docb) callback?.Invoke(root, NodeId.Null);
            }
            uint classMask = (uint)NodeClass.Variable | (uint)NodeClass.Object;
            if (config.Extraction.NodeTypes.AsNodes)
            {
                classMask |= (uint)NodeClass.VariableType | (uint)NodeClass.ObjectType;
            }
            await BrowseDirectory(roots, callback, token, null, classMask, ignoreVisited);
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
                        if (result != null && result.References.Any())
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

        private static Action<ReferenceDescription, NodeId> GetDictWriteCallback(Dictionary<NodeId, ReferenceDescriptionCollection> dict)
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

        public async Task<Dictionary<NodeId, ReferenceDescriptionCollection>> BrowseLevel(
            BrowseParams baseParams,
            CancellationToken token,
            bool doFilter = true,
            bool ignoreVisited = false)
        {
            var result = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            if (baseParams == null || baseParams.Nodes == null || !baseParams.Nodes.Any()) return result;
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

            using var scheduler = new BrowseScheduler(throttler, uaClient, continuationPoints, options, token);
            await scheduler.RunAsync();

            foreach (var node in baseParams.Nodes)
            {
                if (node.Value.Result == null) continue;
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
        public async Task BrowseDirectory(
            IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId>? callback,
            CancellationToken token,
            NodeId? referenceTypes = null,
            uint nodeClassMask = (uint)NodeClass.Variable | (uint)NodeClass.Object,
            bool ignoreVisited = true,
            bool doFilter = true,
            bool readVariableChildren = false,
            int maxDepth = -1)
        {
            if (roots == null) throw new ArgumentNullException(nameof(roots));
            var nextIds = roots.ToList();

            var baseParams = new BrowseParams
            {
                NodeClassMask = nodeClassMask,
                ReferenceTypeId = referenceTypes ?? ReferenceTypeIds.HierarchicalReferences,
                MaxPerNode = (uint)config.Source.BrowseChunk,
                Nodes = roots.ToDictionary(id => id, id => new BrowseNode(id)),
            };
            var options = new DirectoryBrowseParams
            {
                Callback = callback,
                NodesChunk = config.Source.BrowseNodesChunk,
                ReadVariableChildren = readVariableChildren,
                MaxDepth = maxDepth,
                Filters = doFilter ? IgnoreFilters : null,
                InitialParams = baseParams,
                MaxNodeParallelism = throttling.MaxNodeParallelism,
                VisitedNodes = ignoreVisited ? visitedNodes : null
            };

            using var scheduler = new BrowseScheduler(throttler, uaClient, continuationPoints, options, token);
            await scheduler.RunAsync();
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
}
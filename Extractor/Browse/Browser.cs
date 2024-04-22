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
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Browse
{
    public sealed class Browser : IDisposable
    {
        private readonly ILogger<Browser> log;
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        private readonly object visitedNodesLock = new object();

        private readonly ContinuationPointThrottlingConfig throttling;
        private readonly TaskThrottler throttler;

        private readonly BlockingResourceCounter continuationPoints;

        public TransformationCollection? Transformations { get; set; }

        public Browser(ILogger<Browser> log, UAClient client, FullConfig config)
        {
            this.log = log;
            uaClient = client ?? throw new ArgumentNullException(nameof(client));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            throttling = config.Source.BrowseThrottling;
            throttler = new TaskThrottler(throttling.MaxParallelism, false, throttling.MaxPerMinute, TimeSpan.FromMinutes(1));
            continuationPoints = new BlockingResourceCounter(throttling.MaxNodeParallelism <= 0 ? 100_000 : throttling.MaxNodeParallelism);
        }

        public void MaxNodeParallelismChanged()
        {
            continuationPoints.SetCapacity(
                config.Source.BrowseThrottling.MaxNodeParallelism > 0 ? config.Source.BrowseThrottling.MaxNodeParallelism : 100_000
            );
        }

        /// <summary>
        /// Browse node hierarchy for single root node
        /// </summary>
        /// <param name="root">Root node to browse for</param>
        /// <param name="callback">Callback to call for each found node</param>
        /// <param name="purpose">Purpose of the browse, for logging</param>
        public Task BrowseNodeHierarchy(NodeId root,
            Action<ReferenceDescription, NodeId?, bool> callback,
            CancellationToken token,
            string purpose = "")
        {
            return BrowseNodeHierarchy(new[] { root }, callback, token, purpose);
        }
        /// <summary>
        /// Browse an opcua directory, calling callback for all relevant nodes found.
        /// </summary>
        /// <param name="roots">Initial nodes to start mapping.</param>
        /// <param name="callback">Callback for each mapped node, takes a description of a single node, and its parent id</param>
        /// <param name="purpose">Purpose of the browse, for logging</param>
        public async Task BrowseNodeHierarchy(IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId, bool> callback,
            CancellationToken token,
            string purpose = "",
            uint? nodeClassMask = null)
        {
            if (roots == null) throw new ArgumentNullException(nameof(roots));

            log.LogDebug("Browse node tree for nodes {Nodes}", string.Join(", ", roots));
            var rootRefs = await GetRootNodes(roots, token);

            foreach (var root in rootRefs)
            {
                callback?.Invoke(root, NodeId.Null, false);
            }

            if (nodeClassMask == null)
            {
                nodeClassMask = (uint)NodeClass.Variable | (uint)NodeClass.Object;
                if (config.Extraction.NodeTypes.AsNodes)
                {
                    nodeClassMask |= (uint)NodeClass.VariableType | (uint)NodeClass.ObjectType | (uint)NodeClass.ReferenceType | (uint)NodeClass.DataType;
                }
            }

            await BrowseDirectory(roots, callback, token, null, nodeClassMask.Value, true, purpose: purpose);
        }

        /// <summary>
        /// Get reference descriptions for a list of NodeIds, used to fetch root nodes when browsing.
        /// </summary>
        /// <param name="ids">NodeIds to retrieve</param>
        /// <param name="token"></param>
        /// <returns>A list of reference descriptions, order is not guaranteed to be the same as the input.</returns>
        /// <exception cref="ExtractorFailureException"></exception>
        public async Task<IEnumerable<ReferenceDescription>> GetRootNodes(IEnumerable<NodeId> ids, CancellationToken token)
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
                results = await uaClient.ReadAttributes(new ReadValueIdCollection(readValueIds), 1, token, "root nodes");
            }
            catch (Exception ex)
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

            try
            {
                var toBrowseForTypeDef = roots.Values
                    .Where(r => r.NodeClass == NodeClass.Object || r.NodeClass == NodeClass.Variable)
                    .Select(r => uaClient.ToNodeId(r.NodeId))
                    .ToList();

                if (toBrowseForTypeDef.Count != 0)
                {
                    var nodes = toBrowseForTypeDef.Select(id => new BrowseNode(id)).ToDictionary(node => node.Id);

                    await uaClient.GetReferences(new BrowseParams
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
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadRootNode);
            }

            return roots.Values;
        }

        public async Task GetRootNodes(IEnumerable<NodeId> ids, Action<ReferenceDescription, NodeId, bool> callback, CancellationToken token, string purpose = "")
        {
            var refs = await GetRootNodes(ids, token);
            foreach (var rf in refs)
            {
                callback(rf, NodeId.Null, false);
            }
        }

        private static Action<ReferenceDescription, NodeId, bool> GetDictWriteCallback(Dictionary<NodeId, ReferenceDescriptionCollection> dict)
        {
            object lck = new object();
            return (rd, nodeId, visited) =>
            {
                if (visited) return;
                lock (lck)
                {
                    if (!dict.TryGetValue(nodeId, out var refs))
                    {
                        dict[nodeId] = refs = new ReferenceDescriptionCollection();
                    }
                    refs.Add(rd);
                }
            };
        }

        /// <summary>
        /// Browse a list of nodes, returning their children.
        /// </summary>
        /// <param name="baseParams">Browse parameters</param>
        /// <param name="doFilter">True to apply the node filter to discovered nodes</param>
        /// <param name="purpose">Purpose of the browse, for logging</param>
        /// <returns>Map from given nodes to list of references. Nodes with no children are not guaranteed to be in the result.</returns>
        public async Task<Dictionary<NodeId, ReferenceDescriptionCollection>> BrowseLevel(
            BrowseParams baseParams,
            CancellationToken token,
            bool doFilter = true,
            string purpose = "")
        {
            var result = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            if (baseParams == null || baseParams.Nodes == null || baseParams.Nodes.Count == 0) return result;
            var options = new DirectoryBrowseParams
            {
                Callback = GetDictWriteCallback(result),
                Transformations = doFilter ? Transformations : null,
                InitialParams = baseParams,
                MaxNodeParallelism = throttling.MaxNodeParallelism,
                NodesChunk = config.Source.BrowseNodesChunk,
                MaxDepth = 0
            };

            using var scheduler = new BrowseScheduler(log, throttler, uaClient, continuationPoints, options, token, purpose);
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
        /// <param name="maxDepth">Maximum depth to browse to. 0 will browse children of the given roots only.</param>
        /// <param name="purpose">Purpose of the browse, for logging.</param>
        public async Task BrowseDirectory(
            IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId, bool>? callback,
            CancellationToken token,
            NodeId? referenceTypes = null,
            uint nodeClassMask = (uint)NodeClass.Variable | (uint)NodeClass.Object,
            bool doFilter = true,
            int maxDepth = -1,
            string purpose = "")
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
                MaxDepth = maxDepth,
                Transformations = doFilter ? Transformations : null,
                InitialParams = baseParams,
                MaxNodeParallelism = throttling.MaxNodeParallelism
            };

            using var scheduler = new BrowseScheduler(log, throttler, uaClient, continuationPoints, options, token, purpose);
            await scheduler.RunAsync();
        }

        public void Dispose()
        {
            throttler.Dispose();
        }
    }
}

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
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Config
{
    public partial class UAServerExplorer : UAClient
    {
        private readonly ICollection<int> testBrowseNodesChunkSizes = new[]
        {
            1000,
            100,
            10,
            1
        };

        private readonly ICollection<int> testBrowseChunkSizes = new[]
        {
            10000,
            1000,
            100,
            10,
            1
        };

        /// <summary>
        /// Try to get at least 10k nodes using the given node chunk when browsing.
        /// </summary>
        /// <param name="nodesChunk">Chunk size to use when browsing</param>
        /// <returns>A list of discovered UANodes</returns>
        private async Task<IEnumerable<UANode>> GetTestNodeChunk(int nodesChunk, CancellationToken token)
        {
            var root = ObjectIds.ObjectsFolder;

            // Try to find at least 10000 nodes
            var nodes = new List<UANode>();
            var callback = ToolUtil.GetSimpleListWriterCallback(nodes, this, log);

            var nextIds = new List<NodeId> { root };

            var localVisitedNodes = new HashSet<NodeId>
            {
                root
            };

            int totalChildCount = 0;

            log.LogInformation("Get test node chunk with BrowseNodesChunk {Count}", nodesChunk);

            do
            {
                // Recursively browse the node hierarchy until we get at least 10k nodes, or there are no more nodes to browse.
                var references = new Dictionary<NodeId, ReferenceDescriptionCollection>();
                var total = nextIds.Count;
                int count = 0;
                int countChildren = 0;
                foreach (var chunk in nextIds.ChunkBy(nodesChunk))
                {
                    if (token.IsCancellationRequested) return nodes;
                    var browseNodes = chunk.Select(node => new BrowseNode(node)).ToDictionary(node => node.Id);
                    await GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable,
                        Nodes = browseNodes
                    }, true, token);

                    foreach (var node in browseNodes.Values)
                    {
                        if (node.Result == null) continue;
                        references[node.Id] = node.Result.References;
                        countChildren += node.Result.References.Count;
                    }
                    count += browseNodes.Count;
                    log.LogDebug("Read node children {Count} / {Total}. Children: {ChildCount}", count, total, countChildren);
                    totalChildCount += countChildren;
                    if (totalChildCount >= 10000) break;
                }

                nextIds.Clear();
                foreach (var (parentId, children) in references)
                {
                    foreach (var rd in children)
                    {
                        var nodeId = ToNodeId(rd.NodeId);
                        bool docb = true;
                        if (docb)
                        {
                            log.LogTrace("Discovered new node {NodeId}", nodeId);
                            callback?.Invoke(rd, parentId, false);
                        }
                        if (rd.NodeClass == NodeClass.Variable) continue;
                        if (localVisitedNodes.Add(nodeId))
                        {
                            nextIds.Add(nodeId);
                        }
                    }
                }
            } while (totalChildCount < 10001 && nextIds.Any());

            return nodes;
        }
        /// <summary>
        /// Try to get the optimal values for the browse-chunk and browse-nodes-chunk config options.
        /// This defaults to just using as large values as possible, then performs an extra test
        /// to check for issues caused by lack of BrowseNext support.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task GetBrowseChunkSizes(CancellationToken token)
        {
            if (Session == null || !Session.Connected)
            {
                await Run(token, 0);
                await LimitConfigValues(token);
            }

            IEnumerable<UANode>? testNodes = null;

            int browseChunkSize = 0;

            // First try to find a chunk size that works
            foreach (int chunkSize in testBrowseNodesChunkSizes.Where(chunk => chunk <= Config.Source.BrowseNodesChunk))
            {
                try
                {
                    testNodes = await ToolUtil.RunWithTimeout(Task.Run(() => GetTestNodeChunk(chunkSize, token)), 60);
                    browseChunkSize = chunkSize;
                    break;
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to browse node hierarchy");
                    log.LogDebug(ex, "Failed to browse nodes");
                    if (ex is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        throw new FatalException(
                            "Browse unsupported by server, the extractor does not support servers without support for" +
                            " the \"Browse\" service");
                    }
                }
            }
            var parents = testNodes.Select(node => node.ParentId).Distinct().ToList();
            log.LogInformation("Found {Count} nodes over {ParentCount} parents", testNodes.Count(), parents.Count);

            // Test tolerance for large chunks
            log.LogInformation("Testing browseNodesChunk tolerance");
            // We got some indication of max legal size before, here we choose a chunkSize that is reasonable
            // for the discovered server size
            var validSizes = testBrowseChunkSizes.Where(size => (browseChunkSize == 1000 || size <= browseChunkSize) && size <= testNodes.Count()).ToList();
            foreach (int chunkSize in validSizes)
            {
                var ids = testNodes.Select(node => node.Id).Take(chunkSize).ToList();
                try
                {
                    log.LogInformation("Try to get the children of {Count} nodes", ids.Count);
                    var browseNodes = ids.Select(node => new BrowseNode(node)).ToDictionary(node => node.Id);
                    await ToolUtil.RunWithTimeout(GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable,
                        Nodes = browseNodes
                    }, true, token), 30);
                    break;
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to browse node hierarchy");
                    log.LogDebug(ex, "Failed to browse nodes");
                }
                browseChunkSize = chunkSize;
            }
            log.LogInformation("Settled on a BrowseNodesChunk of {ChunkSize}", browseChunkSize);
            Summary.Browse.BrowseNodesChunk = browseChunkSize;

            // Test if there are issues with BrowseNext.
            int originalChunkSize = Config.Source.BrowseChunk;
            foreach (int chunkSize in testBrowseChunkSizes)
            {
                var nodesByParent = testNodes.GroupBy(node => node.ParentId).OrderByDescending(group => group.Count()).Take(browseChunkSize);
                int total = 0;
                var toBrowse = nodesByParent.TakeWhile(chunk =>
                {
                    bool pass = total <= chunkSize * 2;
                    if (pass)
                    {
                        total += chunk.Count();
                    }
                    return pass;
                }).ToList();

                if (total < chunkSize) continue;

                Config.Source.BrowseChunk = chunkSize;
                Dictionary<NodeId, BrowseResult?> children;
                try
                {
                    log.LogInformation("Try to get the children of the {Count} largest parent nodes, with return chunk size {Size}",
                        toBrowse.Count, chunkSize);
                    var nodes = toBrowse.Select(group => new BrowseNode(group.Key)).ToDictionary(node => node.Id);
                    await ToolUtil.RunWithTimeout(GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable,
                        Nodes = nodes
                    }, true, token), 60);
                    children = nodes.ToDictionary(node => node.Key, node => node.Value.Result);
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to browse node hierarchy");
                    log.LogDebug(ex, "Failed to browse nodes");
                    continue;
                }
                int childCount = children.Aggregate(0, (seed, kvp) => seed + kvp.Value?.References?.Count ?? 0);
                if (childCount < total)
                {
                    log.LogWarning("Expected to receive {Count} nodes but only got {ChildCount}!", total, childCount);
                    log.LogWarning("There is likely an issue with returning large numbers of nodes from the server");
                    Summary.Browse.BrowseNextWarning = true;

                    var largestNode = toBrowse.First();

                    var largestDict = new Dictionary<NodeId, BrowseNode> {
                        { largestNode.Key, new BrowseNode(largestNode.Key) }
                    };
                    await GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable,
                        Nodes = largestDict
                    }, true, token);

                    int largest = largestDict[largestNode.Key].Result?.References?.Count ?? throw new FatalException(
                        "Largest node has 0 children, this should not be possible at this stage, and indicates a server failure");

                    log.LogInformation("The largest discovered node has {Count} children", largest);
                    // Usually we will have found the largest parent by this point, unless the server is extremely large
                    // So we can try to choose a BrowseNodesChunk that lets us avoid the issue
                    Summary.Browse.BrowseNodesChunk = Math.Max(1, (int)Math.Floor((double)chunkSize / largest));
                }
                Summary.Browse.BrowseChunk = Math.Min(chunkSize, originalChunkSize);
                break;
            }
            Config.Source.BrowseChunk = Summary.Browse.BrowseChunk;
            Config.Source.BrowseNodesChunk = Summary.Browse.BrowseNodesChunk;
            baseConfig.Source.BrowseChunk = Summary.Browse.BrowseChunk;
            baseConfig.Source.BrowseNodesChunk = Summary.Browse.BrowseNodesChunk;
        }
    }
}

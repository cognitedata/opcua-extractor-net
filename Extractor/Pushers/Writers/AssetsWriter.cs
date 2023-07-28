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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class AssetsWriter : IAssetsWriter
    {
        private readonly ILogger<AssetsWriter> log;
        private readonly FullConfig config;
        private readonly CogniteDestination destination;

        public AssetsWriter(ILogger<AssetsWriter> logger, CogniteDestination destination, FullConfig config)
        {
            this.log = logger;
            this.config = config;
            this.destination = destination;
        }

        /// <summary>
        /// Synchronizes all BaseUANode to CDF assets
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="nodes">Dictionary of mapping of variables to keys</param>
        /// <param name="nodeToAssetIds">Node to assets to ids</param>
        /// <param name="update">Type update configuration</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Operation result</returns>
        public async Task<Result> PushNodes(UAExtractor extractor, IDictionary<string, BaseUANode> nodes,
                IDictionary<NodeId, long> nodeToAssetIds, TypeUpdateConfig update, CancellationToken token)
        {
            var result = new Result { Created = 0, Updated = 0 };
            var assets = await CreateAssets(extractor, nodes, nodeToAssetIds, result, token);

            if (update.AnyUpdate)
            {
                await UpdateAssets(extractor, nodes, assets, update, result, token);
            }
            return result;
        }

        /// <summary>
        /// Create all BaseUANode to CDF assets
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="assetMap">Dictionary of mapping of variables to keys</param>
        /// <param name="nodeToAssetIds">Node to assets to ids</param>
        /// <param name="result">Operation result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Future list of assets</returns>
        private async Task<IEnumerable<Asset>> CreateAssets(UAExtractor extractor,
                IDictionary<string, BaseUANode> assetMap, IDictionary<NodeId, long> nodeToAssetIds, Result result, CancellationToken token)
        {
            var assets = new List<Asset>();
            var maxSize = config.Cognite?.CdfChunking.Assets ?? 1000;
            foreach (var chunk in Chunking.ChunkByHierarchy(assetMap.Values, maxSize, node => node.Id, node => node.ParentId))
            {
                var assetChunk = await destination.GetOrCreateAssetsAsync(chunk.Select(node => extractor.GetUniqueId(node.Id)!), ids =>
                {
                    var assets = ids.Select(id => assetMap[id]);
                    var creates = assets
                        .Select(node => node.ToCDFAsset(
                            config,
                            extractor,
                            config.Cognite?.DataSet?.Id,
                            config.Cognite?.MetadataMapping?.Assets))
                        .Where(asset => asset != null);
                    result.Created += creates.Count();
                    return creates;
                }, RetryMode.None, SanitationMode.Clean, token);

                log.LogResult(assetChunk, RequestType.CreateAssets, true);

                assetChunk.ThrowOnFatal();

                if (assetChunk.Results == null) continue;

                foreach (var asset in assetChunk.Results)
                {
                    nodeToAssetIds[assetMap[asset.ExternalId].Id] = asset.Id;
                }
                assets.AddRange(assetChunk.Results);
            }
            return assets;
        }

        /// <summary>
        /// Update all BaseUANode to CDF assets
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="assetMap">Dictionary of mapping of variables to keys</param>
        /// <param name="assets">List of assets</param>
        /// <param name="update">Type update configuration</param>
        /// <param name="result">Operation result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Future list of assets</returns>
        private async Task UpdateAssets(UAExtractor extractor, IDictionary<string, BaseUANode> assetMap,
                IEnumerable<Asset> assets, TypeUpdateConfig update, Result result, CancellationToken token)
        {
            var updates = new List<AssetUpdateItem>();
            var existing = assets.ToDictionary(asset => asset.ExternalId);
            foreach (var kvp in assetMap)
            {
                if (existing.TryGetValue(kvp.Key, out var asset))
                {
                    var assetUpdate = PusherUtils.GetAssetUpdate(config, asset, kvp.Value, extractor, update);

                    if (assetUpdate == null)
                        continue;
                    if (
                        assetUpdate.ParentExternalId != null
                        || assetUpdate.Description != null
                        || assetUpdate.Name != null
                        || assetUpdate.Metadata != null
                    )
                    {
                        updates.Add(new AssetUpdateItem(asset.ExternalId) { Update = assetUpdate });
                    }
                }
            }
            if (updates.Any())
            {
                var res = await destination.UpdateAssetsAsync(updates, RetryMode.OnError, SanitationMode.Clean, token);

                log.LogResult(res, RequestType.UpdateAssets, false);

                res.ThrowOnFatal();

                result.Updated += res.Results?.Count() ?? 0;
            }
        }
    }
}

using System.Collections.Concurrent;
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

        public async Task<Result>  PushNodes(UAExtractor extractor, ConcurrentDictionary<string, BaseUANode> nodes, IDictionary<NodeId, long> nodeToAssetIds, TypeUpdateConfig update, CancellationToken token)
        {
            var result = new Result { Created = 0, Updated = 0 };
            var assets = await CreateAssets(extractor, nodes, nodeToAssetIds, result, token);

            if (update.AnyUpdate)
            {
                await UpdateAssets(extractor, nodes, assets, update, result, token);
            }
            return result;
        }
        
        private async Task<IEnumerable<Asset>> CreateAssets( UAExtractor extractor, IDictionary<string, BaseUANode> assetMap, IDictionary<NodeId, long> nodeToAssetIds, Result result, CancellationToken token)
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

        private async Task UpdateAssets(UAExtractor extractor, IDictionary<string, BaseUANode> assetMap, IEnumerable<Asset> assets, TypeUpdateConfig update, Result result, CancellationToken token)
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

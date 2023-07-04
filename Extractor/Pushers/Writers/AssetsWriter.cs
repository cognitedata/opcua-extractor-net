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
        private readonly CancellationToken token;

        public AssetsWriter(
            ILogger<AssetsWriter> logger,
            CancellationToken token,
            CogniteDestination destination,
            FullConfig config)
        {
            this.log = logger;
            this.config = config;
            this.destination = destination;
            this.token = token;
        }

        public async Task PushNodes(
            UAExtractor extractor,
            ConcurrentDictionary<string, BaseUANode> nodes,
            IDictionary<NodeId, long> nodeToAssetIds,
            TypeUpdateConfig update,
            BrowseReport report
        )
        {
            var assets = await CreateAssets(extractor, nodes, nodeToAssetIds, report);

            if (update.AnyUpdate)
            {
                await UpdateAssets(extractor, nodes, assets, update, report);
            }
        }
        
        private async Task<IEnumerable<Asset>> CreateAssets(
            UAExtractor extractor,
            IDictionary<string, BaseUANode> assetMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            BrowseReport report)
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
                    report.AssetsCreated += creates.Count();
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

        private async Task UpdateAssets(
            UAExtractor extractor,
            IDictionary<string, BaseUANode> assetMap,
            IEnumerable<Asset> assets,
            TypeUpdateConfig update,
            BrowseReport report
        )
        {
            var updates = new List<AssetUpdateItem>();
            var existing = assets.ToDictionary(asset => asset.ExternalId);
            foreach (var kvp in assetMap)
            {
                if (existing.TryGetValue(kvp.Key, out var asset))
                {
                    var assetUpdate = PusherUtils.GetAssetUpdate(
                        config,
                        asset,
                        kvp.Value,
                        extractor,
                        update
                    );

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
                var res = await destination.UpdateAssetsAsync(
                    updates,
                    RetryMode.OnError,
                    SanitationMode.Clean,
                    token
                );

                log.LogResult(res, RequestType.UpdateAssets, false);

                res.ThrowOnFatal();

                report.AssetsUpdated += res.Results?.Count() ?? 0;
            }
        }    }
}

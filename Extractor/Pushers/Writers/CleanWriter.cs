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

using System;
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
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class CleanWriter
    {
        private readonly ILogger<CleanWriter> log;
        private readonly FullConfig config;
        private readonly CogniteDestination destination;
        private readonly CleanMetadataTargetConfig cleanConfig;

        public CleanWriter(ILogger<CleanWriter> logger, CogniteDestination destination, FullConfig config)
        {
            log = logger;
            this.config = config;
            this.destination = destination;
            cleanConfig = config.Cognite?.MetadataTargets?.Clean ?? throw new ArgumentException("Initialize clean writer without clean config");
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
        public async Task<bool> PushAssets(
            UAExtractor extractor,
            IDictionary<string,
            BaseUANode> nodes,
            IDictionary<NodeId, long> nodeToAssetIds,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token)
        {
            if (!cleanConfig.Assets) return true;

            try
            {
                var result = new Result { Created = 0, Updated = 0 };
                var assets = await CreateAssets(extractor, nodes, nodeToAssetIds, result, token);

                if (update.AnyUpdate)
                {
                    await UpdateAssets(extractor, nodes, assets, update, result, token);
                }

                report.AssetsUpdated += result.Updated;
                report.AssetsCreated += result.Created;
                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push assets to CDF Clean: {Message}", ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Push all refernces to CDF relationship
        /// </summary>
        /// <param name="relationships">List of sanitized references</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A result reporting items created/updated</returns>
        public async Task<bool> PushReferences(IEnumerable<RelationshipCreate> relationships, BrowseReport report, CancellationToken token)
        {
            if (!cleanConfig.Relationships) return true;

            try
            {
                var counts = await Task.WhenAll(
                    relationships.ChunkBy(1000).Select(chunk => PushReferencesChunk(chunk, token))
                );
                report.RelationshipsCreated += counts.Sum();
                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push relationships to CDF Clean: {Message}", ex.Message);
                return false;
            }
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
            bool isFirstChunk = true;
            foreach (var chunk in Chunking.ChunkByHierarchy(assetMap.Values, maxSize, node => node.Id, node => node.ParentId))
            {
                var extIds = new HashSet<string>(chunk.Select(node => extractor.GetUniqueId(node.Id)!));
                var assetChunk = await destination.GetOrCreateAssetsAsync(extIds, ids =>
                {
                    var assets = ids.Select(id => assetMap[id]);
                    var creates = assets
                        .Select(node => node.ToCDFAsset(
                            config,
                            extractor,
                            config.Cognite?.DataSet?.Id,
                            config.Cognite?.MetadataMapping?.Assets))
                        .Where(asset => asset != null)
                        .ToList();
                    if (isFirstChunk)
                    {
                        foreach (var asset in creates)
                        {
                            if (asset.ParentExternalId != null && !assetMap.ContainsKey(asset.ParentExternalId) && !extIds.Contains(asset.ParentExternalId))
                            {
                                log.LogWarning("Parent asset {Id} not found for asset {CId}", asset.ParentExternalId, asset.ExternalId);
                                asset.ParentExternalId = null;
                            }
                        }
                    }

                    result.Created += creates.Count();
                    return creates;
                }, RetryMode.OnError, SanitationMode.Clean, token);

                isFirstChunk = false;

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
                    if (assetUpdate.ParentExternalId != null && !assetMap.ContainsKey(assetUpdate.ParentExternalId.Set))
                    {
                        log.LogWarning("Parent asset {Id} not found for asset {CId}", assetUpdate.ParentExternalId.Set, asset.ExternalId);
                        assetUpdate.ParentExternalId = null;
                    }
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
            if (updates.Count != 0)
            {
                var res = await destination.UpdateAssetsAsync(updates, RetryMode.OnError, SanitationMode.Clean, token);

                log.LogResult(res, RequestType.UpdateAssets, false);

                res.ThrowOnFatal();

                result.Updated += res.Results?.Count() ?? 0;
            }
        }

        /// <summary>
        /// Push all references in chunks
        /// </summary>
        /// <param name="relationships">List of sanitized references</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task<int> PushReferencesChunk(IEnumerable<RelationshipCreate> relationships, CancellationToken token)
        {
            if (!relationships.Any())
                return 0;
            try
            {
                await destination.CogniteClient.Relationships.CreateAsync(relationships, token);
                return relationships.Count();
            }
            catch (ResponseException ex)
            {
                if (ex.Duplicated != null && ex.Duplicated.Any())
                {
                    var existing = new HashSet<string>();
                    foreach (var dict in ex.Duplicated)
                    {
                        if (dict.TryGetValue("externalId", out var value))
                        {
                            if (value is MultiValue.String strValue)
                            {
                                existing.Add(strValue.Value);
                            }
                        }
                    }
                    if (existing.Count == 0)
                        throw;

                    relationships = relationships
                        .Where(rel => !existing.Contains(rel.ExternalId))
                        .ToList();
                    return await PushReferencesChunk(relationships, token);
                }
                else
                {
                    throw;
                }
            }
        }

        #region deletes
        public async Task MarkDeleted(DeletedNodes deletes, CancellationToken token)
        {
            await Task.WhenAll(
                SetAssetsDeleted(deletes.Objects.Select(d => d.Id), token),
                DeleteRelationships(deletes.References.Select(d => d.Id), token)
            );
        }

        private async Task SetAssetsDeleted(IEnumerable<string> externalIds, CancellationToken token)
        {
            if (!cleanConfig.Assets) return;
            var updates = externalIds.Select(
                    extId =>
                        new AssetUpdateItem(extId)
                        {
                            Update = new AssetUpdate
                            {
                                Metadata = new UpdateDictionary<string>(
                                    new Dictionary<string, string>
                                    {
                                        { config.Extraction.Deletes.DeleteMarker, "true" }
                                    },
                                    Enumerable.Empty<string>()
                                )
                            }
                        }
                );
            var result = await destination.UpdateAssetsAsync(
                updates,
                RetryMode.OnError,
                SanitationMode.Clean,
                token
            );
            log.LogResult(result, RequestType.UpdateAssets, true);
            result.ThrowOnFatal();
        }

        private async Task DeleteRelationships(IEnumerable<string> externalIds, CancellationToken token)
        {
            if (!cleanConfig.Relationships || !config.Cognite!.DeleteRelationships) return;

            var tasks = externalIds
                .ChunkBy(1000)
                .Select(
                    chunk =>
                        destination.CogniteClient.Relationships.DeleteAsync(chunk, true, token)
                );
            await Task.WhenAll(tasks);
        }
        #endregion
    }
}

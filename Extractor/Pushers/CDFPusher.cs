/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Prometheus;
using Serilog;
using Cognite.Extractor.Utils;
using TimeRange = Cognite.Extractor.Common.TimeRange;
using System.Text.Json;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.OpcUa.Types;
using Cognite.OpcUa.HistoryStates;

namespace Cognite.OpcUa.Pushers
{
    /// <summary>
    /// Pusher against CDF
    /// </summary>
    public sealed class CDFPusher : IPusher
    {
        private readonly CognitePusherConfig config;
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        
        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }

        public List<UANode> PendingNodes { get; } = new List<UANode>();
        public List<UAReference> PendingReferences { get; } = new List<UAReference>();
        public UAExtractor Extractor { get; set; }
        public IPusherConfig BaseConfig { get; }

        private readonly HashSet<string> mismatchedTimeseries = new HashSet<string>();
        private readonly HashSet<string> missingTimeseries = new HashSet<string>();
        private readonly CogniteDestination destination;

        public CDFPusher(IServiceProvider clientProvider, CognitePusherConfig config)
        {
            this.config = config;
            BaseConfig = config;
            destination = clientProvider.GetRequiredService<CogniteDestination>();
        }

        private static readonly Counter dataPointsCounter = Metrics
            .CreateCounter("opcua_datapoints_pushed_cdf", "Number of datapoints pushed to CDF");
        private static readonly Counter dataPointPushes = Metrics
            .CreateCounter("opcua_datapoint_pushes_cdf", "Number of times datapoints have been pushed to CDF");
        private static readonly Counter dataPointPushFailures = Metrics
            .CreateCounter("opcua_datapoint_push_failures_cdf", "Number of completely failed pushes of datapoints to CDF");
        private static readonly Counter eventCounter = Metrics
            .CreateCounter("opcua_events_pushed_cdf", "Number of events pushed to CDF");
        private static readonly Counter eventPushCounter = Metrics
            .CreateCounter("opcua_event_pushes_cdf", "Number of times events have been pushed to CDF");
        private static readonly Counter eventPushFailures = Metrics
            .CreateCounter("opcua_event_push_failures_cdf", "Number of times events have been pushed to CDF");
        private static readonly Counter nodeEnsuringFailures = Metrics
            .CreateCounter("opcua_node_ensure_failures_cdf",
            "Number of completely failed requests to CDF when ensuring assets/timeseries exist");
        private static readonly Counter skippedEvents = Metrics
            .CreateCounter("opcua_skipped_events_cdf", "Number of events skipped by CDF pusher");

        private readonly ILogger log = Log.Logger.ForContext(typeof(CDFPusher));
        #region Interface

        /// <summary>
        /// Dequeues up to 100000 points from the BufferedDPQueue, then pushes them to CDF. On failure, writes to file if enabled.
        /// </summary>
        public async Task<bool?> PushDataPoints(IEnumerable<UADataPoint> points, CancellationToken token)
        {
            if (points == null) return null;
            var dataPointList = points
                .Where(dp => dp.Timestamp > DateTime.UnixEpoch)
                .GroupBy(dp => dp.Id)
                .Where(group => !mismatchedTimeseries.Contains(group.Key) && !missingTimeseries.Contains(group.Key))
                .ToDictionary(group => group.Key, group =>
                    group.Select(dp =>
                    {
                        if (dp.IsString) return dp;
                        if (!double.IsFinite(dp.DoubleValue.Value))
                        {
                            if (config.NonFiniteReplacement != null) return new UADataPoint(dp, config.NonFiniteReplacement.Value);
                            return null;
                        }
                        return dp;
                    }).Where(dp => dp != null));

            int count = dataPointList.Aggregate(0, (seed, points) => seed + points.Value.Count());

            if (count == 0) return null;

            var inserts = dataPointList.ToDictionary(kvp =>
                Identity.Create(kvp.Key),
                kvp => kvp.Value.Select(
                    dp => dp.IsString ? new Datapoint(dp.Timestamp, dp.StringValue) : new Datapoint(dp.Timestamp, dp.DoubleValue.Value))
                );
            if (config.Debug) return null;

            try
            {
                var error = await destination.InsertDataPointsIgnoreErrorsAsync(inserts, token);
                // No reason to return failure, even if these happen, nothing can be done about it. The log should reflect it,
                // and perhaps we should handle it better in the future.
                if (error.IdsNotFound.Any())
                {
                    log.Error("Failed to push datapoints to CDF, missing ids: {ids}", error.IdsNotFound);
                    foreach (var id in error.IdsNotFound)
                    {
                        missingTimeseries.Add(id.ExternalId);
                    }
                }
                if (error.IdsWithMismatchedData.Any())
                {
                    log.Error("Failed to push datapoints to CDF, mismatched timeseries: {ids}", error.IdsWithMismatchedData);
                    foreach (var id in error.IdsWithMismatchedData)
                    {
                        mismatchedTimeseries.Add(id.ExternalId);
                    }
                }

                int realCount = count;
                if (error.IdsNotFound.Any() || error.IdsWithMismatchedData.Any())
                {
                    foreach (var id in error.IdsNotFound.Concat(error.IdsWithMismatchedData))
                    {
                        realCount -= dataPointList[id.ExternalId].Count();
                    }
                }
                log.Debug("Successfully pushed {real} / {total} points to CDF", realCount, count);
                dataPointPushes.Inc();
                dataPointsCounter.Inc(realCount);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to push {count} points to CDF: {msg}", count, e.Message);
                dataPointPushFailures.Inc();
                // Return false indicating unexpected failure if we want to buffer.
                return false;
            }

            return true;
        }
        /// <summary>
        /// Dequeues up to 1000 events from the BufferedEventQueue, then pushes them to CDF.
        /// </summary>
        public async Task<bool?> PushEvents(IEnumerable<UAEvent> events, CancellationToken token)
        {
            if (events == null) return null;
            var eventList = new List<UAEvent>();
            int count = 0;
            foreach (var buffEvent in events)
            {
                if (buffEvent.Time < PusherUtils.CogniteMinTime || buffEvent.Time > PusherUtils.CogniteMaxTime)
                {
                    skippedEvents.Inc();
                    continue;
                }
                eventList.Add(buffEvent);
                count++;
            }

            if (count == 0 || config.Debug) return null;

            try
            {
                var result = await destination.EnsureEventsExistsAsync(eventList
                    .Select(evt => PusherUtils.EventToCDFEvent(evt, Extractor, config.DataSetId, nodeToAssetIds))
                    .Where(evt => evt != null), RetryMode.OnError, SanitationMode.Clean, token);

                var skipped = result.Errors.Aggregate(0, (seed, err) =>
                    seed + (err.Skipped?.Count() ?? 0));

                var fatalError = result.Errors.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
                if (fatalError != null)
                {
                    log.Error("Failed to push {NumFailedEvents} events to CDF: {msg}", count, fatalError.Exception.Message);
                    eventPushFailures.Inc();
                    return fatalError.Exception is ResponseException rex && (rex.Code == 400 || rex.Code == 409);
                }

                eventCounter.Inc(count - skipped);
                eventPushCounter.Inc();
                log.Debug("Successfully pushed {count} events to CDF", count - skipped);
                skippedEvents.Inc(skipped);
            }
            catch (Exception exc)
            {
                log.Error("Failed to push {NumFailedEvents} events to CDF: {msg}", count, exc.Message);
                eventPushFailures.Inc();
                return exc is ResponseException rex && (rex.Code == 400 || rex.Code == 409);
            }

            return true;
        }

        /// <summary>
        /// Empty queue, fetch info for each relevant node, test results against CDF, then synchronize any variables
        /// </summary>
        /// <param name="objects">List of objects to be synchronized</param>
        /// <param name="variables">List of variables to be synchronized</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        public async Task<bool> PushNodes(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> variables,
            UpdateConfig update,
            CancellationToken token)
        {
            if (variables == null) throw new ArgumentNullException(nameof(variables));
            if (objects == null) throw new ArgumentNullException(nameof(objects));
            if (update == null) throw new ArgumentNullException(nameof(update));

            if (!variables.Any() && !objects.Any())
            {
                log.Debug("Testing 0 nodes against CDF");
                return true;
            }

            log.Information("Testing {TotalNodesToTest} nodes against CDF", variables.Count() + objects.Count());
            if (config.Debug)
            {
                await Extractor.ReadProperties(objects.Concat(variables));
                foreach (var node in objects.Concat(variables))
                {
                    log.Verbose(node.ToDebugDescription());
                }
                return true;
            }

            try
            {
                await PushAssets(objects, update.Objects, token);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to ensure assets");
                nodeEnsuringFailures.Inc();
                return false;
            }

            try
            {
                await PushTimeseries(variables, update.Variables, token);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to ensure timeseries");
                nodeEnsuringFailures.Inc();
                return false;
            }
            log.Information("Finish pushing nodes to CDF");
            return true;
        }
        /// <summary>
        /// Reset the pusher, preparing it to be restarted
        /// </summary>
        public void Reset()
        {
            missingTimeseries.Clear();
            mismatchedTimeseries.Clear();
        }
        public async Task<bool> InitExtractedRanges(
            IEnumerable<VariableExtractionState> states,
            bool backfillEnabled,
            bool initMissing,
            CancellationToken token)
        {
            if (states == null) throw new ArgumentNullException(nameof(states));
            if (!states.Any() || config.Debug || !config.ReadExtractedRanges) return true;
            var ids = new List<string>();
            foreach (var state in states)
            {
                if (state.IsArray)
                {
                    for (int i = 0; i < state.ArrayDimensions[0]; i++)
                    {
                        ids.Add(Extractor.GetUniqueId(state.SourceId, i));
                    }
                }
                else
                {
                    ids.Add(state.Id);
                }
            }
            log.Information("Getting extracted ranges from CDF for {cnt} states", ids.Count);
            if (config.Debug) return true;

            Dictionary<string, TimeRange> ranges;
            try
            {
                var dict = await destination.GetExtractedRanges(ids.Select(Identity.Create).ToList(), token, backfillEnabled);
                ranges = dict.ToDictionary(kvp => kvp.Key.ExternalId, kvp => kvp.Value);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to get extracted ranges");
                return false;
            }

            foreach (var state in states)
            {
                if (ranges.TryGetValue(state.Id, out var range))
                {
                    state.InitExtractedRange(range.First, range.Last);
                }
                else if (initMissing)
                {
                    state.InitToEmpty();
                }
            }

            return true;
        }
        public async Task<bool?> TestConnection(FullConfig config, CancellationToken token)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (this.config.Debug) return true;
            try
            {
                await destination.TestCogniteConfig(token);
            }
            catch (Exception ex)
            {
                log.Error("Failed to get CDF login status, this is likely a problem with the network or configuration. Project {project} at {url}: {msg}", 
                    this.config.Project, this.config.Host, ex.Message);
                return false;
            }

            try
            {
                await destination.CogniteClient.TimeSeries.ListAsync(new TimeSeriesQuery { Limit = 1 }, token);
            }
            catch (ResponseException ex)
            {
                log.Error("Could not access CDF Time Series - most likely due " +
                          "to insufficient access rights on API key. Project {project} at {host}: {msg}",
                    this.config.Project, this.config.Host, ex.Message);
                return false;
            }

            if (!config.Events.Enabled) return true;

            try
            {
                await destination.CogniteClient.Events.ListAsync(new EventQuery { Limit = 1 }, token);
            }
            catch (ResponseException ex)
            {
                log.Error("Could not access CDF Events, though event emitters are specified - most likely due " +
                          "to insufficient acces rights on API key. Project {project} at {host}: {msg}",
                    this.config.Project, this.config.Host, ex.Message);
                return false;
            }

            return true;
        }
        #endregion
        private static string BuildColumnString(TypeUpdateConfig config, bool assets)
        {
            var fields = new List<string>();
            if (config.Context) fields.Add(assets ? "parentExternalId" : "assetExternalId");
            if (config.Description) fields.Add("description");
            if (config.Metadata) fields.Add("metadata");
            if (config.Name) fields.Add("name");
            return string.Join(',', fields);
        }

        #region assets

        private async Task UpdateRawAssets(IDictionary<string, UANode> assetMap, TypeUpdateConfig update, CancellationToken token)
        {
            string columns = BuildColumnString(update, true);
            await UpsertRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.AssetsTable, assetMap.Keys, async rows =>
            {
                var rowDict = rows.ToDictionary(row => row.Key);

                var toReadProperties = assetMap.Select(kvp => kvp.Value);
                await Extractor.ReadProperties(toReadProperties);

                var updates = assetMap
                    .Select(kvp => (kvp.Key, PusherUtils.CreateRawAssetUpdate(kvp.Value, Extractor,
                        rowDict.GetValueOrDefault(kvp.Key), update, config.MetadataMapping?.Assets)))
                    .Where(elem => elem.Item2 != null)
                    .ToDictionary(pair => pair.Key, pair => pair.Item2.Value);

                return updates;
            }, null, columns, token);
        }

        private async Task CreateRawAssets(IDictionary<string, UANode> assetMap, CancellationToken token)
        {
            await EnsureRawRows<AssetCreate>(config.RawMetadata.Database, config.RawMetadata.AssetsTable, assetMap.Keys, async ids =>
            {
                var assets = ids.Select(id => assetMap[id]);
                await Extractor.ReadProperties(assets);
                return assets.Select(node => PusherUtils.NodeToAsset(node, Extractor, null, config.MetadataMapping?.Assets))
                    .Where(asset => asset != null)
                    .ToDictionary(asset => asset.ExternalId);
            }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, token);
        }

        private async Task<IEnumerable<Asset>> CreateAssets(IDictionary<string, UANode> assetMap, CancellationToken token)
        {
            var assets = new List<Asset>();
            foreach (var chunk in Chunking.ChunkByHierarchy(assetMap.Values, config.CdfChunking.Assets, node => node.Id, node => node.ParentId))
            {
                var assetChunk = await destination.GetOrCreateAssetsAsync(chunk.Select(node => Extractor.GetUniqueId(node.Id)), async ids =>
                {
                    var assets = ids.Select(id => assetMap[id]);
                    await Extractor.ReadProperties(assets);
                    return assets
                        .Select(node => PusherUtils.NodeToAsset(node, Extractor, config.DataSetId, config.MetadataMapping?.Assets))
                        .Where(asset => asset != null);
                }, RetryMode.None, SanitationMode.Clean, token);

                var fatalError = assetChunk.Errors?.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
                if (fatalError != null) throw fatalError.Exception;

                if (assetChunk.Results == null) continue;

                foreach (var asset in assetChunk.Results)
                {
                    nodeToAssetIds[assetMap[asset.ExternalId].Id] = asset.Id;
                }
                assets.AddRange(assetChunk.Results);
            }
            return assets;
        }

        private async Task UpdateAssets(IDictionary<string, UANode> assetMap, IEnumerable<Asset> assets, TypeUpdateConfig update, CancellationToken token)
        {
            var updates = new List<AssetUpdateItem>();
            var existing = assets.ToDictionary(asset => asset.ExternalId);
            foreach (var kvp in assetMap)
            {
                if (existing.TryGetValue(kvp.Key, out var asset))
                {
                    var extras = Extractor.GetExtraMetadata(kvp.Value);
                    var assetUpdate = PusherUtils.GetAssetUpdate(asset, kvp.Value, Extractor, update, extras);

                    if (assetUpdate == null) continue;
                    if (assetUpdate.ParentExternalId != null || assetUpdate.Description != null
                        || assetUpdate.Name != null || assetUpdate.Metadata != null)
                    {
                        updates.Add(new AssetUpdateItem(asset.ExternalId) { Update = assetUpdate });
                    }
                }
            }
            if (updates.Any())
            {
                log.Information("Updating {cnt} assets in CDF", updates.Count);
                await destination.CogniteClient.Assets.UpdateAsync(updates, token);
            }
        }

        private async Task PushAssets(
            IEnumerable<UANode> objects,
            TypeUpdateConfig update,
            CancellationToken token)
        {
            if (config.SkipMetadata) return;

            var assetIds = new ConcurrentDictionary<string, UANode>(objects.ToDictionary(obj => Extractor.GetUniqueId(obj.Id)));
            var metaMap = config.MetadataMapping?.Assets;
            bool useRawAssets = config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.AssetsTable);

            if (useRawAssets)
            {
                if (update.AnyUpdate)
                {
                    await UpdateRawAssets(assetIds, update, token);
                }
                else
                {
                    await CreateRawAssets(assetIds, token);
                }
            }
            else
            {
                var assets = await CreateAssets(assetIds, token);
                
                if (update.AnyUpdate)
                {
                    await UpdateAssets(assetIds, assets, update, token);
                }
            }
        }

        #endregion

        #region timeseries
        private async Task UpdateRawTimeseries(
            IDictionary<string, UAVariable> tsMap,
            TypeUpdateConfig update,
            CancellationToken token)
        {
            string columns = BuildColumnString(update, false);
            await UpsertRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.TimeseriesTable, tsMap.Keys, async rows =>
            {
                var rowDict = rows.ToDictionary(row => row.Key);

                var toReadProperties = tsMap.Where(kvp => !rowDict.ContainsKey(kvp.Key)).Select(kvp => kvp.Value);
                await Extractor.ReadProperties(toReadProperties);

                var updates = tsMap
                    .Select(kvp => (kvp.Key, PusherUtils.CreateRawTsUpdate(kvp.Value, Extractor,
                        rowDict.GetValueOrDefault(kvp.Key), update, config.MetadataMapping?.Timeseries)))
                    .Where(elem => elem.Item2 != null)
                    .ToDictionary(pair => pair.Key, pair => pair.Item2.Value);

                return updates;
            }, null, columns, token);
        }

        private async Task CreateRawTimeseries(
            IDictionary<string, UAVariable> tsMap,
            CancellationToken token)
        {
            await EnsureRawRows<StatelessTimeSeriesCreate>(config.RawMetadata.Database, config.RawMetadata.TimeseriesTable, tsMap.Keys, async ids =>
            {
                var timeseries = ids.Select(id => tsMap[id]);
                await Extractor.ReadProperties(timeseries);
                return timeseries.Select(node => PusherUtils.VariableToStatelessTimeSeries(node, Extractor, null, config.MetadataMapping?.Timeseries))
                    .Where(ts => ts != null)
                    .ToDictionary(ts => ts.ExternalId);
            }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, token);
        }

        private async Task<IEnumerable<TimeSeries>> CreateTimeseries(
            IDictionary<string, UAVariable> tsMap,
            bool createMinimalTimeseries,
            CancellationToken token)
        {
            var timeseries = await destination.GetOrCreateTimeSeriesAsync(tsMap.Keys, async ids =>
            {
                var tss = ids.Select(id => tsMap[id]);
                if (!createMinimalTimeseries)
                {
                    await Extractor.ReadProperties(tss);
                }
                return tss.Select(ts => PusherUtils.VariableToTimeseries(ts,
                    Extractor,
                    config.DataSetId,
                    nodeToAssetIds,
                    config.MetadataMapping?.Timeseries,
                    createMinimalTimeseries))
                    .Where(ts => ts != null);
            }, RetryMode.None, SanitationMode.Clean, token);

            var fatalError = timeseries.Errors?.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
            if (fatalError != null) throw fatalError.Exception;

            if (timeseries.Results == null) return Array.Empty<TimeSeries>();

            var foundBadTimeseries = new List<string>();
            foreach (var ts in timeseries.Results)
            {
                var loc = tsMap[ts.ExternalId];
                if (nodeToAssetIds.TryGetValue(loc.ParentId, out var parentId))
                {
                    nodeToAssetIds[loc.Id] = parentId;
                }
                if (ts.IsString != loc.DataType.IsString)
                {
                    mismatchedTimeseries.Add(ts.ExternalId);
                    foundBadTimeseries.Add(ts.ExternalId);
                }
            }
            if (foundBadTimeseries.Any())
            {
                log.Debug("Found mismatched timeseries when ensuring: {tss}", string.Join(", ", foundBadTimeseries));
            }
            return timeseries.Results;
        }

        private async Task UpdateTimeseries(
            IDictionary<string, UAVariable> tsMap,
            IEnumerable<TimeSeries> timeseries,
            TypeUpdateConfig update,
            CancellationToken token)
        {
            var updates = new List<TimeSeriesUpdateItem>();
            var existing = timeseries.ToDictionary(asset => asset.ExternalId);
            foreach (var kvp in tsMap)
            {
                if (existing.TryGetValue(kvp.Key, out var ts))
                {
                    var tsUpdate = PusherUtils.GetTSUpdate(ts, kvp.Value, update, nodeToAssetIds, Extractor.GetExtraMetadata(kvp.Value));
                    if (tsUpdate == null) continue;
                    if (tsUpdate.AssetId != null || tsUpdate.Description != null
                        || tsUpdate.Name != null || tsUpdate.Metadata != null)
                    {
                        updates.Add(new TimeSeriesUpdateItem(ts.ExternalId) { Update = tsUpdate });
                    }
                }
            }
            if (updates.Any())
            {
                log.Information("Updating {cnt} timeseries in CDF", updates.Count);
                await destination.CogniteClient.TimeSeries.UpdateAsync(updates, token);
            }
        }

        private async Task PushTimeseries(
            IEnumerable<UAVariable> tsList,
            TypeUpdateConfig update,
            CancellationToken token)
        {
            var tsIds = new ConcurrentDictionary<string, UAVariable>(tsList.ToDictionary(ts => Extractor.GetUniqueId(ts.Id, ts.Index)));
            bool useRawTimeseries = config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.TimeseriesTable);

            bool simpleTimeseries = useRawTimeseries || config.SkipMetadata;

            var timeseries = await CreateTimeseries(tsIds, simpleTimeseries, token);

            if (config.SkipMetadata) return;

            if (useRawTimeseries)
            {
                if (update.AnyUpdate)
                {
                    await UpdateRawTimeseries(tsIds, update, token);
                }
                else
                {
                    await CreateRawTimeseries(tsIds, token);
                }
            }
            else if (update.AnyUpdate)
            {
                await UpdateTimeseries(tsIds, timeseries, update, token);
            }
        }
        #endregion

        #region raw-utils

        private async Task EnsureRawRows<T>(
            string dbName,
            string tableName,
            IEnumerable<string> keys,
            Func<IEnumerable<string>, Task<IDictionary<string, T>>> dtoBuilder,
            JsonSerializerOptions options,
            CancellationToken token)
        {
            string cursor = null;
            var existing = new HashSet<string>();
            do
            {
                try
                {
                    var result = await destination.CogniteClient.Raw.ListRowsAsync(dbName, tableName,
                        new RawRowQuery { Columns = new[] { "," }, Cursor = cursor, Limit = 10_000 }, token);
                    foreach (var item in result.Items)
                    {
                        existing.Add(item.Key);
                    }
                    cursor = result.NextCursor;
                }
                catch (ResponseException ex) when (ex.Code == 404) {
                    log.Warning("Table or database not found: {msg}", ex.Message);
                    break;
                }
            } while (cursor != null);

            var toCreate = keys.Except(existing);
            if (!toCreate.Any()) return;
            log.Information("Creating {cnt} raw rows in CDF", toCreate.Count());

            var createDtos = await dtoBuilder(toCreate);

            await destination.InsertRawRowsAsync(dbName, tableName, createDtos, options, token);
        }

        private async Task UpsertRawRows<T>(
            string dbName,
            string tableName,
            IEnumerable<string> toRetrieve,
            Func<IEnumerable<RawRow>, Task<IDictionary<string, T>>> dtoBuilder,
            JsonSerializerOptions options,
            string columns,
            CancellationToken token)
        {
            string cursor = null;
            var existing = new List<RawRow>();
            var keys = new HashSet<string>(toRetrieve);
            do
            {
                try
                {
                    var result = await destination.CogniteClient.Raw.ListRowsAsync(dbName, tableName,
                        new RawRowQuery { Columns = new[] { columns }, Cursor = cursor, Limit = 10_000 }, token);
                    foreach (var item in result.Items)
                    {
                        existing.Add(item);
                    }
                    cursor = result.NextCursor;
                }
                catch (ResponseException ex) when (ex.Code == 404)
                {
                    log.Warning("Table or database not found: {msg}", ex.Message);
                    break;
                }
            } while (cursor != null);

            var toCreate = await dtoBuilder(existing.Where(row => toRetrieve.Contains(row.Key)));
            if (!toCreate.Any()) return;
            log.Information("Creating or updating {cnt} raw rows in CDF", toCreate.Count);

            await destination.InsertRawRowsAsync(dbName, tableName, toCreate, options, token);
        }
        #endregion

        public async Task<bool> PushReferences(IEnumerable<UAReference> references, CancellationToken token)
        {
            var relationships = references
                .Select(reference => PusherUtils.ReferenceToRelationship(reference, config.DataSetId, Extractor))
                .DistinctBy(rel => rel.ExternalId);

            bool useRawRelationships = config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.RelationshipsTable);

            log.Information("Test {cnt} relationships against CDF", references.Count());
            try
            {
                if (useRawRelationships)
                {
                    await PushRawReferences(relationships, token);
                }   
                else
                {
                    await Task.WhenAll(relationships.ChunkBy(1000).Select(chunk => PushReferencesChunk(chunk, token)));
                }
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to ensure relationships");
                nodeEnsuringFailures.Inc();
                return false;
            }
            log.Information("Sucessfully pushed relationships to CDF");
            return true;
        }

        private async Task PushReferencesChunk(IEnumerable<CogniteSdk.Beta.RelationshipCreate> relationships, CancellationToken token)
        {
            try
            {
                await destination.CogniteClient.Beta.Relationships.CreateAsync(relationships, token);
            }
            catch (ResponseException ex)
            {
                if (ex.Duplicated.Any())
                {
                    var existing = new HashSet<string>();
                    foreach (var dict in ex.Duplicated)
                    {
                        if (dict.TryGetValue("externalId", out var value))
                        {
                            existing.Add((value as MultiValue.String).Value);
                        }
                    }
                    if (!existing.Any()) throw;

                    relationships = relationships.Where(rel => !existing.Contains(rel.ExternalId)).ToList();
                    await PushReferencesChunk(relationships, token);
                }
                else
                {
                    throw;
                }
            }
        }
        private async Task PushRawReferences(IEnumerable<CogniteSdk.Beta.RelationshipCreate> relationships, CancellationToken token)
        {
            await EnsureRawRows(
                config.RawMetadata.Database,
                config.RawMetadata.RelationshipsTable,
                relationships.Select(rel => rel.ExternalId),
                ids =>
                {
                    var idSet = ids.ToHashSet();
                    return Task.FromResult((IDictionary<string, CogniteSdk.Beta.RelationshipCreate>)
                        relationships.Where(rel => idSet.Contains(rel.ExternalId)).ToDictionary(rel => rel.ExternalId));
                },
                new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase },
                token);
        }

        public void Dispose() { }
    }
}

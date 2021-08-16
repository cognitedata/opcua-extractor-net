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

using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Prometheus;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TimeRange = Cognite.Extractor.Common.TimeRange;

namespace Cognite.OpcUa.Pushers
{
    /// <summary>
    /// Pusher against CDF
    /// </summary>
    public sealed class CDFPusher : IPusher
    {
        private readonly CognitePusherConfig config;
        private readonly ExtractionConfig extractionConfig;
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
            extractionConfig = clientProvider.GetRequiredService<FullConfig>().Extraction;
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
        private static readonly Gauge missingTimeseriesCnt = Metrics
            .CreateGauge("opcua_missing_timeseries", "Number of distinct timeseries that have been found to be missing in CDF");
        private static readonly Gauge mismatchedTimeseriesCnt = Metrics
            .CreateGauge("opcua_mismatched_timeseries", "Number of distinct timeseries that have been found to have different types in OPC-UA and in CDF");

        private readonly ILogger log = Log.Logger.ForContext(typeof(CDFPusher));
        #region Interface

        /// <summary>
        /// Attempts to push the given list of datapoints to CDF.
        /// </summary>'
        /// <returns>True if push succeeded, false if it failed, null if there were no points to push.</returns>
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
                            if (config.NonFiniteReplacement == null) return null;
                            return new UADataPoint(dp, config.NonFiniteReplacement.Value);
                        }
                        return dp;
                    }).Where(dp => dp != null).ToList());

            int count = dataPointList.Aggregate(0, (seed, points) => seed + points.Value.Count);

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
                    missingTimeseriesCnt.Set(missingTimeseries.Count);
                }
                if (error.IdsWithMismatchedData.Any())
                {
                    log.Error("Failed to push datapoints to CDF, mismatched timeseries: {ids}", error.IdsWithMismatchedData);
                    foreach (var id in error.IdsWithMismatchedData)
                    {
                        mismatchedTimeseries.Add(id.ExternalId);
                    }
                    mismatchedTimeseriesCnt.Set(mismatchedTimeseries.Count);
                }

                int realCount = count;
                if (error.IdsNotFound.Any() || error.IdsWithMismatchedData.Any())
                {
                    foreach (var id in error.IdsNotFound.Concat(error.IdsWithMismatchedData))
                    {
                        realCount -= dataPointList[id.ExternalId].Count;
                    }
                }
                log.Debug("Successfully pushed {real} / {total} points to CDF", realCount, count);
                dataPointPushes.Inc();
                dataPointsCounter.Inc(realCount);
            }
            catch (Exception e)
            {
                log.Error("Failed to push {count} points to CDF: {msg}", count, e.Message);
                dataPointPushFailures.Inc();
                // Return false indicating unexpected failure if we want to buffer.
                return false;
            }

            return true;
        }
        /// <summary>
        /// Attempts to push the given list of events to CDF.
        /// </summary>
        /// <returns>True if push succeeded, false if it failed, null if there were no events to push.</returns>
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
                    .Select(evt => evt.ToCDFEvent(Extractor, config.DataSetId, nodeToAssetIds))
                    .Where(evt => evt != null), RetryMode.OnError, SanitationMode.Clean, token);

                int skipped = 0;
                if (result.Errors != null)
                {
                    skipped = result.Errors.Aggregate(0, (seed, err) =>
                        seed + (err.Skipped?.Count() ?? 0));

                    var fatalError = result.Errors.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
                    if (fatalError != null)
                    {
                        log.Error("Failed to push {NumFailedEvents} events to CDF: {msg}", count, fatalError.Exception.Message);
                        eventPushFailures.Inc();
                        return fatalError.Exception is ResponseException rex && (rex.Code == 400 || rex.Code == 409);
                    }
                }

                eventCounter.Inc(count - skipped);
                eventPushCounter.Inc();
                log.Debug("Successfully pushed {count} events to CDF", count - skipped);
                skippedEvents.Inc(skipped);
            }
            catch (Exception exc)
            {
                log.Error(exc, "Failed to push {NumFailedEvents} events to CDF: {msg}", count, exc.Message);
                eventPushFailures.Inc();
                return exc is ResponseException rex && (rex.Code == 400 || rex.Code == 409);
            }

            return true;
        }

        /// <summary>
        /// Attempts to ensure that the given list of objects and variables are correctly reflected in CDF.
        /// </summary>
        /// <param name="objects">List of objects to be synchronized</param>
        /// <param name="variables">List of variables to be synchronized</param>
        /// <param name="update">Configuration of what fields, if any, should be updated.</param>
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
                    log.Verbose(node.ToString());
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
        /// <summary>
        /// Initialize extracted datapoint ranges on the given list of states.
        /// </summary>
        /// <param name="states">List of states representing timeseries in CDF.
        /// These must all exist.</param>
        /// <param name="backfillEnabled">True if backfill is enabled and start points should be read.</param>
        /// <returns>True if nothing failed unexpectedly.</returns>
        public async Task<bool> InitExtractedRanges(
            IEnumerable<VariableExtractionState> states,
            bool backfillEnabled,
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
                if (state.IsArray)
                {
                    for (int i = 0; i < state.ArrayDimensions[0]; i++)
                    {
                        if (ranges.TryGetValue(Extractor.GetUniqueId(state.SourceId, i), out var range))
                        {
                            if (range == TimeRange.Empty)
                            {
                                state.InitToEmpty();
                            }
                            else
                            {
                                state.InitExtractedRange(range.First, range.Last);
                            }
                        }
                    }
                }
                else
                {
                    if (ranges.TryGetValue(state.Id, out var range))
                    {
                        if (range == TimeRange.Empty)
                        {
                            state.InitToEmpty();
                        }
                        else
                        {
                            state.InitExtractedRange(range.First, range.Last);
                        }
                    }
                }
            }

            return true;
        }
        /// <summary>
        /// Test that the extractor is capable of pushing to CDF.
        /// Also fetches DataSet externalId.
        /// </summary>
        /// <param name="fullConfig">Configuration in use</param>
        /// <returns>True if pushing is possible, false if not.</returns>
        public async Task<bool?> TestConnection(FullConfig fullConfig, CancellationToken token)
        {
            if (fullConfig == null) throw new ArgumentNullException(nameof(fullConfig));
            if (config.Debug) return true;
            try
            {
                await destination.TestCogniteConfig(token);
            }
            catch (Exception ex)
            {
                log.Error("Failed to get CDF login status, this is likely a problem with the network or configuration. Project {project} at {url}: {msg}",
                    config.Project, config.Host, ex.Message);
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
                    config.Project, config.Host, ex.Message);
                return false;
            }

            if (fullConfig.Events.Enabled)
            {
                try
                {
                    await destination.CogniteClient.Events.ListAsync(new EventQuery { Limit = 1 }, token);
                }
                catch (ResponseException ex)
                {
                    log.Error("Could not access CDF Events, though event emitters are specified - most likely due " +
                              "to insufficient access rights on API key. Project {project} at {host}: {msg}",
                        config.Project, config.Host, ex.Message);
                    return false;
                }
            }

            if (!config.DataSetId.HasValue && !string.IsNullOrEmpty(config.DataSetExternalId))
            {
                try
                {
                    var dataSet = await destination.CogniteClient.DataSets.RetrieveAsync(new[] { config.DataSetExternalId }, false, token);
                    config.DataSetId = dataSet.First().Id;
                }
                catch (ResponseException ex)
                {
                    log.Error("Could not fetch data set by external id. It may not exist, or the user may lack" +
                        " sufficient access rights. Project {project} at {host}, id {id}: {msg}",
                        config.Project, config.Host, config.DataSetExternalId, ex.Message);
                    return false;
                }
            }

            return true;
        }
        /// <summary>
        /// Push list of references as relationships to CDF.
        /// </summary>
        /// <param name="references">List of references to push</param>
        /// <returns>True if nothing failed unexpectedly</returns>
        public async Task<bool> PushReferences(IEnumerable<UAReference> references, CancellationToken token)
        {
            if (references == null || !references.Any()) return true;

            var relationships = references
                .Select(reference => reference.ToRelationship(config.DataSetId, Extractor))
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
        #endregion

        #region assets
        /// <summary>
        /// Update list of nodes as assets in CDF Raw.
        /// </summary>
        /// <param name="assetMap">Id, node map for the assets that should be pushed.</param>
        private async Task UpdateRawAssets(IDictionary<string, UANode> assetMap, CancellationToken token)
        {
            await UpsertRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.AssetsTable, assetMap.Keys, async rows =>
            {
                var rowDict = rows.ToDictionary(row => row.Key);

                var toReadProperties = assetMap.Select(kvp => kvp.Value);
                await Extractor.ReadProperties(toReadProperties);

                var updates = assetMap
                    .Select(kvp => (kvp.Key, PusherUtils.CreateRawUpdate(Extractor.StringConverter,
                        kvp.Value, rowDict.GetValueOrDefault(kvp.Key), ConverterType.Node)))
                    .Where(elem => elem.Item2 != null)
                    .ToDictionary(pair => pair.Key, pair => pair.Item2.Value);

                return updates;
            }, null, token);
        }
        /// <summary>
        /// Create list of nodes as assets in CDF Raw.
        /// This does not create rows if they already exist.
        /// </summary>
        /// <param name="assetMap">Id, node map for the assets that should be pushed.</param>
        private async Task CreateRawAssets(IDictionary<string, UANode> assetMap, CancellationToken token)
        {
            await EnsureRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.AssetsTable, assetMap.Keys, async ids =>
            {
                var assets = ids.Select(id => (assetMap[id], id));
                await Extractor.ReadProperties(assets.Select(pair => pair.Item1));
                return assets.Select(pair => (pair.Item1.ToJson(Extractor.StringConverter, ConverterType.Node), pair.id))
                    .Where(pair => pair.Item1 != null)
                    .ToDictionary(pair => pair.Item2, pair => pair.Item1.RootElement);
            }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, token);
        }
        /// <summary>
        /// Create assets in CDF Clean.
        /// </summary>
        /// <param name="assetMap">Id, node map for the assets that should be pushed.</param>
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
                        .Select(node => node.ToCDFAsset(extractionConfig, Extractor,
                            Extractor.StringConverter, Extractor.DataTypeManager, config.DataSetId, config.MetadataMapping?.Assets))
                        .Where(asset => asset != null);
                }, RetryMode.None, SanitationMode.Clean, token);

                var fatalError = assetChunk.Errors?.FirstOrDefault(err => err.Type == ErrorType.FatalFailure
                    || err.Type == ErrorType.ItemMissing);
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
        /// <summary>
        /// Update assets in CDF Clean.
        /// </summary>
        /// <param name="assetMap">Id, node map for the assets that should be pushed.</param>
        /// <param name="assets">List of existing assets in CDF.</param>
        /// <param name="update">Configuration for which fields should be updated.</param>
        private async Task UpdateAssets(IDictionary<string, UANode> assetMap, IEnumerable<Asset> assets, TypeUpdateConfig update, CancellationToken token)
        {
            var updates = new List<AssetUpdateItem>();
            var existing = assets.ToDictionary(asset => asset.ExternalId);
            foreach (var kvp in assetMap)
            {
                if (existing.TryGetValue(kvp.Key, out var asset))
                {
                    var assetUpdate = PusherUtils.GetAssetUpdate(extractionConfig, asset, kvp.Value, Extractor, update);

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
        /// <summary>
        /// Master method for pushing assets to CDF raw or clean.
        /// </summary>
        /// <param name="objects">Assets to push</param>
        /// <param name="update">Configuration for which fields, if any, to update in CDF</param>
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
                    await UpdateRawAssets(assetIds, token);
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
        /// <summary>
        /// Update list of nodes as timeseries in CDF Raw.
        /// </summary>
        /// <param name="tsMap">Id, node map for the timeseries that should be pushed.</param>
        /// <param name="update">Config for what should be updated on each timeseries.</param>
        private async Task UpdateRawTimeseries(
            IDictionary<string, UAVariable> tsMap,
            CancellationToken token)
        {
            await UpsertRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.TimeseriesTable, tsMap.Keys, async rows =>
            {
                var rowDict = rows.ToDictionary(row => row.Key);

                var toReadProperties = tsMap.Where(kvp => !rowDict.ContainsKey(kvp.Key)).Select(kvp => kvp.Value);
                await Extractor.ReadProperties(toReadProperties);

                var updates = tsMap
                    .Select(kvp => (kvp.Key, PusherUtils.CreateRawUpdate(Extractor.StringConverter, kvp.Value,
                        rowDict.GetValueOrDefault(kvp.Key), ConverterType.Variable)))
                    .Where(elem => elem.Item2 != null)
                    .ToDictionary(pair => pair.Key, pair => pair.Item2.Value);

                return updates;
            }, null, token);
        }
        /// <summary>
        /// Create list of nodes as timeseries in CDF Raw.
        /// This does not create rows if they already exist.
        /// </summary>
        /// <param name="tsMap">Id, node map for the timeseries that should be pushed.</param>
        private async Task CreateRawTimeseries(
            IDictionary<string, UAVariable> tsMap,
            CancellationToken token)
        {
            await EnsureRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.TimeseriesTable, tsMap.Keys, async ids =>
            {
                var timeseries = ids.Select(id => (tsMap[id], id));
                await Extractor.ReadProperties(timeseries.Select(pair => pair.Item1));
                return timeseries.Select(pair => (pair.Item1.ToJson(Extractor.StringConverter, ConverterType.Variable), pair.id))
                    .Where(pair => pair.Item1 != null)
                    .ToDictionary(pair => pair.Item2, pair => pair.Item1.RootElement);
            }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, token);
        }
        /// <summary>
        /// Create timeseries in CDF Clean, optionally creates only minimal timeseries with no metadata or context.
        /// </summary>
        /// <param name="tsMap">Id, node map for the timeseries that should be pushed.</param>
        /// <param name="createMinimalTimeseries">True to create timeseries with no metadata.</param>
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
                return tss.Select(ts => ts.ToTimeseries(
                    extractionConfig,
                    Extractor,
                    Extractor.DataTypeManager,
                    Extractor.StringConverter,
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
        /// <summary>
        /// Update timeseries in CDF Clean.
        /// </summary>
        /// <param name="tsMap">Id, node map for the timeseries that should be pushed.</param>
        /// <param name="timeseries">List of existing timeseries in CDF.</param>
        /// <param name="update">Configuration for which fields should be updated.</param>
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
                    var tsUpdate = PusherUtils.GetTSUpdate(extractionConfig, Extractor.DataTypeManager, Extractor.StringConverter, ts, kvp.Value, update, nodeToAssetIds);
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

        /// <summary>
        /// Master method for pushing timeseries to CDF raw or clean.
        /// </summary>
        /// <param name="tsList">Timeseries to push</param>
        /// <param name="update">Configuration for which fields, if any, to update in CDF</param>
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
                    await UpdateRawTimeseries(tsIds, token);
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
        /// <summary>
        /// Ensure that raw rows given by <paramref name="keys"/> exist in the table given by
        /// <paramref name="dbName"/> and <paramref name="tableName"/>.
        /// Keys that do not exist are built into DTOs by <paramref name="dtoBuilder"/>.
        /// </summary>
        /// <typeparam name="T">Type of DTO to build</typeparam>
        /// <param name="dbName">Name of database in CDF Raw</param>
        /// <param name="tableName">Name of table in CDF Raw</param>
        /// <param name="keys">Keys of rows to ensure</param>
        /// <param name="dtoBuilder">Method to build DTOs for keys that were not found.</param>
        /// <param name="options"><see cref="JsonSerializerOptions"/> used for serialization.</param>
        private async Task EnsureRawRows<T>(
            string dbName,
            string tableName,
            IEnumerable<string> keys,
            Func<IEnumerable<string>, Task<IDictionary<string, T>>> dtoBuilder,
            JsonSerializerOptions options,
            CancellationToken token)
        {
            var rows = await GetRawRows(dbName, tableName, new[] { "," }, token);
            var existing = rows.Select(row => row.Key);

            var toCreate = keys.Except(existing);
            if (!toCreate.Any()) return;
            log.Information("Creating {cnt} raw rows in CDF", toCreate.Count());

            var createDtos = await dtoBuilder(toCreate);

            await destination.InsertRawRowsAsync(dbName, tableName, createDtos, options, token);
        }
        /// <summary>
        /// Insert or update raw rows given by <paramref name="toRetrieve"/> in table
        /// given by <paramref name="dbName"/> and <paramref name="tableName"/>.
        /// The dtoBuilder is called with all rows that already exist,
        /// so it must determine which rows should be updated and which should be created.
        /// </summary>
        /// <typeparam name="T">Type of DTO to build</typeparam>
        /// <param name="dbName">Name of database in CDF Raw</param>
        /// <param name="tableName">Name of table in CDF Raw</param>
        /// <param name="toRetrieve">Rows to retrieve</param>
        /// <param name="dtoBuilder">Method to build DTOs, called with existing rows.</param>
        /// <param name="options"><see cref="JsonSerializerOptions"/> used for serialization.</param>
        private async Task UpsertRawRows<T>(
            string dbName,
            string tableName,
            IEnumerable<string> toRetrieve,
            Func<IEnumerable<RawRow>, Task<IDictionary<string, T>>> dtoBuilder,
            JsonSerializerOptions options,
            CancellationToken token)
        {
            var existing = await GetRawRows(dbName, tableName, null, token);
            
            var keys = new HashSet<string>(toRetrieve);

            var toCreate = await dtoBuilder(existing.Where(row => keys.Contains(row.Key)));
            if (!toCreate.Any()) return;
            log.Information("Creating or updating {cnt} raw rows in CDF", toCreate.Count);

            await destination.InsertRawRowsAsync(dbName, tableName, toCreate, options, token);
        }

        public async Task<IEnumerable<RawRow>> GetRawRows(
            string dbName,
            string tableName,
            IEnumerable<string> columns,
            CancellationToken token)
        {
            string cursor = null;
            var rows = new List<RawRow>();
            do
            {
                try
                {
                    var result = await destination.CogniteClient.Raw.ListRowsAsync(dbName, tableName,
                        new RawRowQuery { Cursor = cursor, Limit = 10_000, Columns = columns }, token);
                    rows.AddRange(result.Items);
                    cursor = result.NextCursor;
                }
                catch (ResponseException ex) when (ex.Code == 404)
                {
                    log.Warning("Table or database not found: {msg}", ex.Message);
                    break;
                }
            } while (cursor != null);
            return rows;
        }
        #endregion

        #region references
        /// <summary>
        /// Create the given list of relationships in CDF, handles duplicates.
        /// </summary>
        /// <param name="relationships">Relationships to create</param>
        private async Task PushReferencesChunk(IEnumerable<RelationshipCreate> relationships, CancellationToken token)
        {
            if (!relationships.Any()) return;
            try
            {
                await destination.CogniteClient.Relationships.CreateAsync(relationships, token);
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
        /// <summary>
        /// Create the given list of relationships in CDF Raw, skips rows that already exist.
        /// </summary>
        /// <param name="relationships">Relationships to create.</param>
        private async Task PushRawReferences(IEnumerable<RelationshipCreate> relationships, CancellationToken token)
        {
            await EnsureRawRows(
                config.RawMetadata.Database,
                config.RawMetadata.RelationshipsTable,
                relationships.Select(rel => rel.ExternalId),
                ids =>
                {
                    var idSet = ids.ToHashSet();
                    return Task.FromResult((IDictionary<string, RelationshipCreate>)
                        relationships.Where(rel => idSet.Contains(rel.ExternalId)).ToDictionary(rel => rel.ExternalId));
                },
                new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase },
                token);
        }
        #endregion
        public void Dispose() { }
    }
}

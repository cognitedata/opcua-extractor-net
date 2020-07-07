/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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

namespace Cognite.OpcUa.Pushers
{
    /// <summary>
    /// Pusher against CDF
    /// </summary>
    public sealed class CDFPusher : IPusher
    {
        private readonly CognitePusherConfig config;
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        private readonly DateTime minDateTime = new DateTime(1971, 1, 1);
        
        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }


        public UAExtractor Extractor { get; set; }
        public IPusherConfig BaseConfig { get; }

        private readonly HashSet<string> mismatchedTimeseries = new HashSet<string>();
        private readonly HashSet<string> missingTimeseries = new HashSet<string>();
        private readonly IServiceProvider provider;

        public CDFPusher(IServiceProvider clientProvider, CognitePusherConfig config)
        {
            this.config = config;
            BaseConfig = config;
            provider = clientProvider;
        }

        private CogniteDestination GetDestination()
        {
            return provider.GetRequiredService<CogniteDestination>();
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
        public async Task<bool?> PushDataPoints(IEnumerable<BufferedDataPoint> points, CancellationToken token)
        {
            if (points == null) return null;
            var dataPointList = points
                .GroupBy(dp => dp.Id)
                .Where(group => !mismatchedTimeseries.Contains(group.Key) && !missingTimeseries.Contains(group.Key))
                .ToDictionary(group => group.Key, group =>
                    group.Select(dp =>
                    {
                        if (dp.IsString) return dp;
                        if (!double.IsFinite(dp.DoubleValue))
                        {
                            if (config.NonFiniteReplacement != null) return new BufferedDataPoint(dp, config.NonFiniteReplacement.Value);
                            return null;
                        }
                        return dp;
                    }).Where(dp => dp != null));

            int count = dataPointList.Aggregate(0, (seed, points) => seed + points.Value.Count());

            if (count == 0) return null;

            var destination = GetDestination();

            var inserts = dataPointList.ToDictionary(kvp =>
                Identity.Create(kvp.Key),
                kvp => kvp.Value.Select(
                    dp => dp.IsString ? new Datapoint(dp.Timestamp, dp.StringValue) : new Datapoint(dp.Timestamp, dp.DoubleValue))
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
        public async Task<bool?> PushEvents(IEnumerable<BufferedEvent> events, CancellationToken token)
        {
            if (events == null) return null;
            var eventList = new List<BufferedEvent>();
            int count = 0;
            foreach (var buffEvent in events)
            {
                if (buffEvent.Time < minDateTime)
                {
                    skippedEvents.Inc();
                    continue;
                }
                eventList.Add(buffEvent);
                count++;
            }
            if (count == 0) return null;

            if (config.Debug) return null;

            var destination = GetDestination();

            try
            {
                await destination.EnsureEventsExistsAsync(eventList
                    .Select(evt => PusherUtils.EventToCDFEvent(evt, Extractor, config.DataSetId, nodeToAssetIds))
                    .Where(evt => evt != null), true, token);
                eventCounter.Inc(count);
                eventPushCounter.Inc();
                log.Debug("Successfully pushed {count} events to CDF", count);
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
            IEnumerable<BufferedNode> objects,
            IEnumerable<BufferedVariable> variables,
            UpdateConfig update,
            CancellationToken token)
        {
            var tsList = new List<BufferedVariable>();

            if (variables == null) throw new ArgumentNullException(nameof(variables));
            if (objects == null) throw new ArgumentNullException(nameof(objects));
            if (update == null) throw new ArgumentNullException(nameof(update));


            if (!variables.Any() && !objects.Any())
            {
                log.Debug("Testing 0 nodes against CDF");
                return true;
            }

            foreach (var node in variables)
            { 
                if (Extractor.AllowTSMap(node))
                {
                    tsList.Add(node);
                }
            }

            log.Information("Testing {TotalNodesToTest} nodes against CDF", variables.Count() + objects.Count());
            if (config.Debug)
            {
                await Extractor.ReadProperties(objects.Concat(variables), token);
                foreach (var node in objects)
                {
                    log.Verbose(node.ToDebugDescription());
                }
                foreach (var node in variables)
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
                await PushTimeseries(tsList, update.Variables, token);
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
            IEnumerable<NodeExtractionState> states,
            bool backfillEnabled,
            bool initMissing,
            CancellationToken token)
        {
            if (states == null) throw new ArgumentNullException(nameof(states));
            if (!states.Any() || config.Debug || !config.ReadExtractedRanges) return true;
            var ids = new List<string>();
            foreach (var state in states)
            {
                if (state.ArrayDimensions != null && state.ArrayDimensions.Count > 0 && state.ArrayDimensions[0] > 0)
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

            var destination = GetDestination();

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
                if (ranges.ContainsKey(state.Id))
                {
                    state.InitExtractedRange(ranges[state.Id].First, ranges[state.Id].Last);
                }
                else if (initMissing)
                {
                    state.InitToEmpty();
                }
            }

            return true;
        }
        public async Task<bool?> TestConnection(FullConfig fullConfig, CancellationToken token)
        {
            if (fullConfig == null) throw new ArgumentNullException(nameof(fullConfig));
            if (config.Debug) return true;
            var destination = GetDestination();
            try
            {
                await destination.TestCogniteConfig(token);
            }
            catch (Exception ex)
            {
                log.Debug(ex, "Failed to get login status from CDF. Project {project} at {url}", config.Project, config.Host);
                log.Error("Failed to get CDF login status, this is likely a problem with the network. Project {project} at {url}", 
                    config.Project, config.Host);
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
                log.Debug(ex, "Could not access CDF Time Series");
                return false;
            }

            if (fullConfig.Events.EventIds == null || fullConfig.Events.EmitterIds == null ||
                !fullConfig.Events.EventIds.Any() || !fullConfig.Events.EmitterIds.Any()) return true;

            try
            {
                await destination.CogniteClient.Events.ListAsync(new EventQuery { Limit = 1 }, token);
            }
            catch (ResponseException ex)
            {
                log.Error("Could not access CDF Events, though event emitters are specified - most likely due " +
                          "to insufficient acces rights on API key. Project {project} at {host}: {msg}",
                    config.Project, config.Host, ex.Message);
                log.Debug(ex, "Could not access CDF Events");
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

        private async Task PushAssets(
            IEnumerable<BufferedNode> objects,
            TypeUpdateConfig update,
            CancellationToken token)
        {
            var assetIds = new ConcurrentDictionary<string, BufferedNode>(objects.ToDictionary(obj => Extractor.GetUniqueId(obj.Id)));
            var destination = GetDestination();
            bool useRawAssets = config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.AssetsTable);

            if (useRawAssets)
            {
                if (update.AnyUpdate)
                {
                    string columns = BuildColumnString(update, true);
                    await UpsertRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.AssetsTable, assetIds.Keys, async rows =>
                    {
                        var rowDict = rows.ToDictionary(row => row.Key);

                        var toReadProperties = assetIds.Select(kvp => kvp.Value);
                        await Extractor.ReadProperties(toReadProperties, token);

                        var updates = assetIds
                            .Select(kvp => (kvp.Key, PusherUtils.CreateRawAssetUpdate(kvp.Value, Extractor, rowDict.GetValueOrDefault(kvp.Key), update)))
                            .Where(elem => elem.Item2 != null)
                            .ToDictionary(pair => pair.Key, pair => pair.Item2.Value);

                        return updates;
                    }, null, columns, token);
                }
                else
                {
                    await EnsureRawRows<AssetCreate>(config.RawMetadata.Database, config.RawMetadata.AssetsTable, assetIds.Keys, async ids =>
                    {
                        var assets = ids.Select(id => assetIds[id]);
                        await Extractor.ReadProperties(assets, token);
                        return assets.Select(node => PusherUtils.NodeToAsset(node, Extractor, null)).Where(asset => asset != null)
                            .ToDictionary(asset => asset.ExternalId);
                    }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, token);
                }

            }
            else
            {
                var assets = new List<Asset>();
                foreach (var chunk in PusherUtils.ChunkByHierarchy(assetIds))
                {
                    var assetChunk = await destination.GetOrCreateAssetsAsync(chunk, async ids =>
                    {
                        var assets = ids.Select(id => assetIds[id]);
                        await Extractor.ReadProperties(assets, token);
                        return assets.Select(node => PusherUtils.NodeToAsset(node, Extractor, config.DataSetId)).Where(asset => asset != null);
                    }, token);
                    foreach (var asset in assetChunk)
                    {
                        nodeToAssetIds[assetIds[asset.ExternalId].Id] = asset.Id;
                    }
                    assets.AddRange(assetChunk);
                }
                
                if (update.AnyUpdate)
                {
                    var updates = new List<AssetUpdateItem>();
                    var existing = assets.ToDictionary(asset => asset.ExternalId);
                    foreach (var kvp in assetIds)
                    {
                        if (existing.TryGetValue(kvp.Key, out var asset))
                        {
                            var assetUpdate = new AssetUpdate();
                            if (update.Context && kvp.Value.ParentId != null && !kvp.Value.ParentId.IsNullNodeId)
                                assetUpdate.ParentExternalId = new UpdateNullable<string>(Extractor.GetUniqueId(kvp.Value.ParentId));

                            if (update.Description && !string.IsNullOrEmpty(kvp.Value.Description) && kvp.Value.Description != asset.Description)
                                assetUpdate.Description = new UpdateNullable<string>(kvp.Value.Description);

                            if (update.Name && !string.IsNullOrEmpty(kvp.Value.DisplayName) && kvp.Value.DisplayName != asset.Name)
                                assetUpdate.Name = new UpdateNullable<string>(kvp.Value.DisplayName);

                            if (update.Metadata && kvp.Value.Properties != null && kvp.Value.Properties.Any())
                            {
                                var newMetaData = PusherUtils.PropertiesToMetadata(kvp.Value.Properties)
                                    .Where(kvp => !string.IsNullOrEmpty(kvp.Value))
                                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                                if (asset.Metadata == null
                                    || newMetaData.Any(meta => !asset.Metadata.ContainsKey(meta.Key) || asset.Metadata[meta.Key] != meta.Value))
                                {
                                    assetUpdate.Metadata = new UpdateDictionary<string>(newMetaData, Array.Empty<string>());
                                }
                            }

                            if (assetUpdate.ParentExternalId != null || assetUpdate.Description != null
                                || assetUpdate.Name != null || assetUpdate.Metadata != null)
                            {
                                updates.Add(
                                    new AssetUpdateItem(asset.ExternalId)
                                    {
                                        Update = assetUpdate
                                    });
                            }
                        }
                    }
                    if (updates.Any())
                    {
                        log.Information("Updating {cnt} assets in CDF", updates.Count);
                        await destination.CogniteClient.Assets.UpdateAsync(updates, token);
                    }
                }
            }
        }

        private async Task PushTimeseries(
            IEnumerable<BufferedVariable> tsList,
            TypeUpdateConfig update,
            CancellationToken token)
        {
            var tsIds = new ConcurrentDictionary<string, BufferedVariable>(tsList.ToDictionary(ts => Extractor.GetUniqueId(ts.Id, ts.Index)));
            var destination = GetDestination();
            bool useRawTimeseries = config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.TimeseriesTable);

            var timeseries = await destination.GetOrCreateTimeSeriesAsync(tsIds.Keys, async ids =>
            {
                var tss = ids.Select(id => tsIds[id]);
                if (!useRawTimeseries)
                {
                    await Extractor.ReadProperties(tss, token);
                }
                return tss.Select(ts => PusherUtils.VariableToTimeseries(ts, Extractor, config.DataSetId, nodeToAssetIds, useRawTimeseries))
                    .Where(ts => ts != null);
            }, token);
            var foundBadTimeseries = new List<string>();
            foreach (var ts in timeseries)
            {
                var loc = tsIds[ts.ExternalId];
                if (nodeToAssetIds.ContainsKey(loc.ParentId))
                {
                    nodeToAssetIds[loc.Id] = nodeToAssetIds[loc.ParentId];
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

            if (update.AnyUpdate && !useRawTimeseries)
            {
                var updates = new List<TimeSeriesUpdateItem>();
                var existing = timeseries.ToDictionary(asset => asset.ExternalId);
                foreach (var kvp in tsIds)
                {
                    if (existing.TryGetValue(kvp.Key, out var ts))
                    {
                        var tsUpdate = new TimeSeriesUpdate();
                        if (update.Context)
                        {
                            if (kvp.Value.ParentId != null && !kvp.Value.ParentId.IsNullNodeId
                                && nodeToAssetIds.TryGetValue(kvp.Value.ParentId, out long assetId))
                            {
                                if (assetId != ts.AssetId && assetId > 0)
                                {
                                    tsUpdate.AssetId = new UpdateNullable<long?>(assetId);
                                }
                            }
                        }

                        if (update.Description && !string.IsNullOrEmpty(kvp.Value.Description) && kvp.Value.Description != ts.Description)
                            tsUpdate.Description = new UpdateNullable<string>(kvp.Value.Description);

                        if (update.Name && !string.IsNullOrEmpty(kvp.Value.DisplayName) && kvp.Value.DisplayName != ts.Name)
                            tsUpdate.Name = new UpdateNullable<string>(kvp.Value.DisplayName);

                        if (update.Metadata && kvp.Value.Properties != null && kvp.Value.Properties.Any())
                        {
                            var newMetaData = PusherUtils.PropertiesToMetadata(kvp.Value.Properties)
                                .Where(kvp => !string.IsNullOrEmpty(kvp.Value))
                                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                            if (ts.Metadata == null
                                || newMetaData.Any(meta => !ts.Metadata.ContainsKey(meta.Key) || ts.Metadata[meta.Key] != meta.Value))
                            {
                                tsUpdate.Metadata = new UpdateDictionary<string>(newMetaData, Array.Empty<string>());
                            }
                        }

                        if (tsUpdate.AssetId != null || tsUpdate.Description != null
                            || tsUpdate.Name != null || tsUpdate.Metadata != null)
                        {
                            updates.Add(
                                new TimeSeriesUpdateItem(ts.ExternalId)
                                {
                                    Update = tsUpdate
                                });
                        }
                    }
                }
                if (updates.Any())
                {
                    log.Information("Updating {cnt} timeseries in CDF", updates.Count);
                    await destination.CogniteClient.TimeSeries.UpdateAsync(updates, token);
                }
            }


            if (useRawTimeseries)
            {
                if (update.AnyUpdate)
                {
                    string columns = BuildColumnString(update, false);
                    await UpsertRawRows<JsonElement>(config.RawMetadata.Database,
                        config.RawMetadata.TimeseriesTable, tsIds.Keys, async rows =>
                    {
                        var rowDict = rows.ToDictionary(row => row.Key);

                        var toReadProperties = tsIds.Where(kvp => !rowDict.ContainsKey(kvp.Key)).Select(kvp => kvp.Value);
                        await Extractor.ReadProperties(toReadProperties, token);

                        var updates = tsIds
                            .Select(kvp => (kvp.Key, PusherUtils.CreateRawTsUpdate(kvp.Value, Extractor, rowDict.GetValueOrDefault(kvp.Key), update)))
                            .Where(elem => elem.Item2 != null)
                            .ToDictionary(pair => pair.Key, pair => pair.Item2.Value);

                        return updates;
                    }, null, columns, token);
                }
                else
                {
                    await EnsureRawRows<StatelessTimeSeriesCreate>(config.RawMetadata.Database, config.RawMetadata.TimeseriesTable, tsIds.Keys, async ids =>
                    {
                        var timeseries = ids.Select(id => tsIds[id]);
                        await Extractor.ReadProperties(timeseries, token);
                        return timeseries.Select(node => PusherUtils.VariableToStatelessTimeSeries(node, Extractor, null)).Where(ts => ts != null)
                            .ToDictionary(ts => ts.ExternalId);
                    }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, token);
                }

            }
        }

        private async Task EnsureRawRows<T>(
            string dbName,
            string tableName,
            IEnumerable<string> keys,
            Func<IEnumerable<string>, Task<IDictionary<string, T>>> dtoBuilder,
            JsonSerializerOptions options,
            CancellationToken token)
        {
            var destination = GetDestination();
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
            var destination = GetDestination();
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

        public void Dispose() { }
    }
}

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
using System.Globalization;
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

namespace Cognite.OpcUa
{
    /// <summary>
    /// Pusher against CDF
    /// </summary>
    public sealed class CDFPusher : IPusher
    {
        private readonly CognitePusherConfig config;
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        private readonly DateTime minDateTime = new DateTime(1971, 1, 1);
        
        public int Index { get; set; }
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
            numCdfPusher.Inc();
            provider = clientProvider;
        }

        private CogniteDestination GetDestination()
        {
            return provider.GetRequiredService<CogniteDestination>();
        }

        private static readonly Counter numCdfPusher = Metrics
            .CreateCounter("opcua_cdf_pusher_count", "Number of active CDF pushers");
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

            if (count == 0)
            {
                log.Verbose("Push 0 datapoints to CDF");
                return null;
            }

            var destination = GetDestination();

            var inserts = dataPointList.ToDictionary(kvp =>
                Identity.Create(kvp.Key),
                kvp => kvp.Value.Select(
                    dp => dp.IsString ? new Datapoint(dp.Timestamp, dp.StringValue) : new Datapoint(dp.Timestamp, dp.DoubleValue))
                );
            log.Debug("Push {cnt} datapoints to CDF", count);
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
                log.Error("Failed to push {count} points to CDF: {msg}", count, e.Message);
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
                if (buffEvent.Time < minDateTime || !nodeToAssetIds.ContainsKey(buffEvent.SourceNode) && !config.Debug)
                {
                    skippedEvents.Inc();
                    continue;
                }
                eventList.Add(buffEvent);
                count++;
            }
            if (count == 0)
            {
                log.Verbose("Push 0 events to CDF");
                return null;
            }
            log.Debug("Push {NumEventsToPush} events to CDF", count);
            if (config.Debug) return null;

            var destination = GetDestination();

            try
            {
                await destination.EnsureEventsExistsAsync(eventList.Select(EventToCDFEvent).Where(evt => evt != null), true, token);
                eventCounter.Inc(count);
                eventPushCounter.Inc();
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
        public async Task<bool> PushNodes(IEnumerable<BufferedNode> objects, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            var tsList = new List<BufferedVariable>();

            if (variables == null) throw new ArgumentNullException(nameof(variables));
            if (objects == null) throw new ArgumentNullException(nameof(objects));


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
                    log.Debug(node.ToDebugDescription());
                }
                foreach (var node in variables)
                {
                    log.Debug(node.ToDebugDescription());
                }
                return true;
            }

            var assetIds = new ConcurrentDictionary<string, BufferedNode>(objects.ToDictionary(obj => Extractor.GetUniqueId(obj.Id)));

            var destination = GetDestination();

            try
            {
                var assets = await destination.GetOrCreateAssetsAsync(assetIds.Keys, async ids =>
                {
                    var assets = ids.Select(id => assetIds[id]);
                    await Extractor.ReadProperties(assets, token);
                    return assets.Select(NodeToAsset).Where(asset => asset != null);
                }, token);
                foreach (var asset in assets)
                {
                    nodeToAssetIds[assetIds[asset.ExternalId].Id] = asset.Id;
                }
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to ensure assets");
                nodeEnsuringFailures.Inc();
                return false;
            }

            var tsIds = new ConcurrentDictionary<string, BufferedVariable>(tsList.ToDictionary(ts => Extractor.GetUniqueId(ts.Id, ts.Index)));

            try
            {
                var timeseries = await destination.GetOrCreateTimeSeriesAsync(tsIds.Keys, async ids =>
                {
                    var tss = ids.Select(id => tsIds[id]);
                    await Extractor.ReadProperties(tss, token);
                    return tss.Select(VariableToTimeseries).Where(ts => ts != null);
                }, token);
                var foundBadTimeseries = new List<string>();
                foreach (var ts in timeseries)
                {
                    var loc = tsIds[ts.ExternalId];
                    if (ts.IsString != loc.DataType.IsString)
                    {
                        mismatchedTimeseries.Add(ts.ExternalId);
                        foundBadTimeseries.Add(ts.ExternalId);
                    }
                }
                if (foundBadTimeseries.Any())
                {
                    log.Debug("Found mismatched timeseries when ensuring: {tss}", foundBadTimeseries);
                }
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
        public async Task<bool> InitExtractedRanges(IEnumerable<NodeExtractionState> states, bool backfillEnabled, CancellationToken token)
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

            var mappedIds = new HashSet<string>();

            foreach (var (id, tr) in ranges)
            {
                var state = Extractor.State.GetNodeState(id);
                state.InitExtractedRange(tr.First, tr.Last);
                mappedIds.Add(id);
            }

            if (states.Any(state => state.Initialized))
            {
                var notMapped = ids.Except(mappedIds);
                foreach (var id in notMapped)
                {
                    var state = Extractor.State.GetNodeState(id);
                    state.InitToEmpty();
                }
            }

            return true;
        }
        public async Task<bool?> TestConnection(FullConfig fullConfig, CancellationToken token)
        {
            if (fullConfig == null) throw new ArgumentNullException(nameof(fullConfig));
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
            }

            return true;
        }

        #endregion

        #region Pushing
        /// <summary>
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="variable">Variable to be converted</param>
        /// <returns>Complete timeseries write poco</returns>
        private TimeSeriesCreate VariableToTimeseries(BufferedVariable variable)
        {
            string externalId = Extractor.GetUniqueId(variable.Id, variable.Index);
            TimeSeriesCreate writePoco;
            try
            {
                writePoco = new TimeSeriesCreate
                {
                    Description = ExtractorUtils.Truncate(variable.Description, 1000),
                    ExternalId = externalId,
                    AssetId = nodeToAssetIds[variable.ParentId],
                    Name = ExtractorUtils.Truncate(variable.DisplayName, 255),
                    LegacyName = externalId,
                    IsString = variable.DataType.IsString,
                    IsStep = variable.DataType.IsStep,
                    DataSetId = config.DataSetId
                };
            }
            catch (Exception ex)
            {
                log.Warning("Failed to create timeseries object: {msg}", ex.Message);
                return null;
            }
            if (variable.Properties != null && variable.Properties.Any())
            {
                writePoco.Metadata = variable.Properties
                    .Where(prop => prop.Value != null)
                    .Take(16)
                    .ToDictionary(prop => ExtractorUtils.Truncate(prop.DisplayName, 32), prop => ExtractorUtils.Truncate(prop.Value.StringValue, 256));
            }
            return writePoco;
        }
        /// <summary>
        /// Converts BufferedNode into asset write poco.
        /// </summary>
        /// <param name="node">Node to be converted</param>
        /// <returns>Full asset write poco</returns>
        private AssetCreate NodeToAsset(BufferedNode node)
        {
            AssetCreate writePoco;
            try
            {
                writePoco = new AssetCreate
                {
                    Description = ExtractorUtils.Truncate(node.Description, 500),
                    ExternalId = Extractor.GetUniqueId(node.Id),
                    Name = string.IsNullOrEmpty(node.DisplayName)
                        ? ExtractorUtils.Truncate(Extractor.GetUniqueId(node.Id), 140)
                        : ExtractorUtils.Truncate(node.DisplayName, 140),
                    DataSetId = config.DataSetId
                };
            }
            catch (Exception ex)
            {
                log.Warning("Failed to create assets object: {msg}", ex.Message);
                return null;
            }

            if (node.ParentId != null && !node.ParentId.IsNullNodeId)
            {
                writePoco.ParentExternalId = Extractor.GetUniqueId(node.ParentId);
            }
            if (node.Properties != null && node.Properties.Any())
            {
                writePoco.Metadata = node.Properties
                    .Where(prop => prop.Value != null)
                    .Take(16)
                    .ToDictionary(prop => ExtractorUtils.Truncate(prop.DisplayName, 32), prop => ExtractorUtils.Truncate(prop.Value.StringValue, 256));
            }
            return writePoco;
        }
        /// <summary>
        /// Get the value of given object assumed to be a timestamp as the number of milliseconds since 1/1/1970
        /// </summary>
        /// <param name="value">Value of the object. Assumed to be a timestamp or numeric value</param>
        /// <returns>Milliseconds since epoch</returns>
        private static long GetTimestampValue(object value)
        {
            if (value is DateTime dt)
            {
                return new DateTimeOffset(dt).ToUnixTimeMilliseconds();
            }
            return Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }
        private static readonly HashSet<string> excludeMetaData = new HashSet<string> {
            "StartTime", "EndTime", "Type", "SubType"
        };
        /// <summary>
        /// Transform BufferedEvent into EventEntity to be sent to CDF.
        /// </summary>
        /// <param name="evt">Event to be transformed.</param>
        /// <returns>Final EventEntity object</returns>
        private EventCreate EventToCDFEvent(BufferedEvent evt)
        {
            EventCreate entity;
            try
            {
                entity = new EventCreate
                {
                    Description = ExtractorUtils.Truncate(evt.Message, 500),
                    StartTime = evt.MetaData.ContainsKey("StartTime")
                        ? GetTimestampValue(evt.MetaData["StartTime"])
                        : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                    EndTime = evt.MetaData.ContainsKey("EndTime")
                        ? GetTimestampValue(evt.MetaData["EndTime"])
                        : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                    AssetIds = new List<long> {nodeToAssetIds[evt.SourceNode]},
                    ExternalId = ExtractorUtils.Truncate(evt.EventId, 255),
                    Type = ExtractorUtils.Truncate(evt.MetaData.ContainsKey("Type")
                        ? Extractor.ConvertToString(evt.MetaData["Type"])
                        : Extractor.GetUniqueId(evt.EventType), 64),
                    DataSetId = config.DataSetId
                };
            }
            catch (Exception ex)
            {
                log.Warning("Failed to create event object: {msg}", ex.Message);
                return null;
            }

            var finalMetaData = new Dictionary<string, string>();
            int len = 1;
            finalMetaData["Emitter"] = Extractor.GetUniqueId(evt.EmittingNode);
            if (!evt.MetaData.ContainsKey("SourceNode"))
            {
                finalMetaData["SourceNode"] = Extractor.GetUniqueId(evt.SourceNode);
                len++;
            }
            if (evt.MetaData.ContainsKey("SubType"))
            {
                entity.Subtype = ExtractorUtils.Truncate(Extractor.ConvertToString(evt.MetaData["SubType"]), 64);
            }

            foreach (var dt in evt.MetaData)
            {
                if (!excludeMetaData.Contains(dt.Key))
                {
                    finalMetaData[ExtractorUtils.Truncate(dt.Key, 32)] =
                        ExtractorUtils.Truncate(Extractor.ConvertToString(dt.Value), 256);
                }

                if (len++ == 15) break;
            }

            if (finalMetaData.Any())
            {
                entity.Metadata = finalMetaData;
            }
            return entity;
        }
        #endregion

        public void Dispose() { }
    }
}

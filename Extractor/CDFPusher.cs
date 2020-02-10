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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Com.Cognite.V1.Timeseries.Proto;
using CogniteSdk;
using CogniteSdk.Assets;
using CogniteSdk.TimeSeries;
using CogniteSdk.DataPoints;
using CogniteSdk.Events;
using CogniteSdk.Login;
using Microsoft.Extensions.DependencyInjection;
using System.Net.Http;
using Opc.Ua;
using Prometheus.Client;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Pusher against CDF
    /// </summary>
    public sealed class CDFPusher : IPusher
    {
        private readonly CogniteClientConfig config;
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        private readonly DateTime minDateTime = new DateTime(1971, 1, 1);
        private readonly ConcurrentDictionary<string, TimeRange> ranges = new ConcurrentDictionary<string, TimeRange>();
        
        public int Index { get; set; }
        public Extractor Extractor { get; set; }
        public PusherConfig BaseConfig { get; }

        public ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; } = new ConcurrentQueue<BufferedDataPoint>();
        public ConcurrentQueue<BufferedEvent> BufferedEventQueue { get; } = new ConcurrentQueue<BufferedEvent>();

        private readonly HashSet<string> mismatchedTimeseries = new HashSet<string>();

        private readonly IHttpClientFactory factory;

        public CDFPusher(IServiceProvider clientProvider, CogniteClientConfig config)
        {
            this.config = config;
            BaseConfig = config;
            factory = clientProvider.GetRequiredService<IHttpClientFactory>();
            numCdfPusher.Inc();
        }

        private Client GetClient(string name = "Context")
        {
            return new Client.Builder()
                .SetHttpClient(factory.CreateClient(name))
                .AddHeader("api-key", config.ApiKey)
                .SetAppId("OPC-UA Extractor")
                .SetProject(config.Project)
                .SetServiceUrl(new Uri(config.Host, UriKind.Absolute))
                .Build();
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
            .CreateCounter("opcua_events_pushed", "Number of events pushed to CDF");
        private static readonly Counter eventPushCounter = Metrics
            .CreateCounter("opcua_event_pushes", "Number of times events have been pushed to CDF");
        private static readonly Counter eventPushFailures = Metrics
            .CreateCounter("opcua_event_push_failures", "Number of times events have been pushed to CDF");
        private static readonly Gauge trackedAssets = Metrics
            .CreateGauge("opcua_tracked_assets", "Number of objects on the opcua server mapped to assets in CDF");
        private static readonly Gauge trackedTimeseres = Metrics
            .CreateGauge("opcua_tracked_timeseries", "Number of variables on the opcua server mapped to timeseries in CDF");
        private static readonly Counter nodeEnsuringFailures = Metrics
            .CreateCounter("opcua_node_ensure_failures",
            "Number of completely failed requests to CDF when ensuring assets/timeseries exist");
        private static readonly Counter duplicatedEvents = Metrics
            .CreateCounter("opcua_duplicated_events", "Number of events that failed to push to CDF due to already existing in CDF");
        private static readonly Counter skippedDatapoints = Metrics
            .CreateCounter("opcua_skipped_datapoints_cdf", "Number of datapoints skipped by CDF pusher");
        private static readonly Counter skippedEvents = Metrics
            .CreateCounter("opcua_skipped_events_cdf", "Number of events skipped by CDF pusher");

        private static readonly ILogger log = Log.Logger.ForContext(typeof(CDFPusher));
        #region Interface

        /// <summary>
        /// Dequeues up to 100000 points from the BufferedDPQueue, then pushes them to CDF. On failure, writes to file if enabled.
        /// </summary>

        public async Task<IEnumerable<BufferedDataPoint>> PushDataPoints(CancellationToken token)
        {
            int count = 0;
            var dataPointList = new Dictionary<string, List<BufferedDataPoint>>();
            while (BufferedDPQueue.Any())
            {
                while (BufferedDPQueue.TryDequeue(out BufferedDataPoint buffer))
                {
                    if (buffer.Timestamp < minDateTime || mismatchedTimeseries.Contains(buffer.Id))
                    {
                        skippedDatapoints.Inc();
                        continue;
                    }
                    // We do not subscribe to changes in history, so an update to a point within the known range is due to
                    // something being out of synch.
                    if (ranges.ContainsKey(buffer.Id)
                        && buffer.Timestamp < ranges[buffer.Id].End 
                        && buffer.Timestamp > ranges[buffer.Id].Start) continue;

                    if (!buffer.IsString && (!double.IsFinite(buffer.DoubleValue) || buffer.DoubleValue >= 1E100 || buffer.DoubleValue <= -1E100))
                    {
                        if (config.NonFiniteReplacement != null)
                        {
                            buffer = new BufferedDataPoint(buffer, config.NonFiniteReplacement.Value);
                        }
                        else
                        {
                            skippedDatapoints.Inc();
                            continue;
                        }
                    }

                    if (buffer.IsString && buffer.StringValue == null)
                    {
                        buffer = new BufferedDataPoint(buffer, "");
                    }

                    count++;
                    if (!dataPointList.ContainsKey(buffer.Id))
                    {
                        dataPointList[buffer.Id] = new List<BufferedDataPoint>();
                    }
                    dataPointList[buffer.Id].Add(buffer);
                }

                if (count == 0)
                {
                    log.Verbose("Push 0 datapoints to CDF");
                    return null;
                }
                log.Debug("Push {NumDatapointsToPush} datapoints to CDF", count);
            }
            var dpChunks = ExtractorUtils.ChunkDictOfLists(dataPointList, 100000, 10000).ToArray();
            var pushTasks = dpChunks.Select(chunk => PushDataPointsChunk(chunk, token)).ToList();
            var results = await Task.WhenAll(pushTasks);


            if (results.All(res => res))
            {
                foreach (var group in dataPointList)
                {
                    var last = group.Value.Max(dp => dp.Timestamp);
                    var first = group.Value.Min(dp => dp.Timestamp);
                    if (!ranges.ContainsKey(group.Key))
                    {
                        ranges[group.Key] = new TimeRange(first, last);
                    }
                    else
                    {
                        if (last < ranges[group.Key].End)
                        {
                            ranges[group.Key].End = last;
                        }

                        if (first > ranges[group.Key].Start)
                        {
                            ranges[group.Key].Start = first;
                        }
                    }
                }
                return Array.Empty<BufferedDataPoint>();
            }
            int index = 0;
            var failedPoints = new List<BufferedDataPoint>();
            foreach (var result in results)
            {
                if (!result)
                {
                    foreach (var points in dpChunks[index].Values)
                    {
                        failedPoints.AddRange(points);
                    }
                }

                index++;
            }
            return failedPoints;
        }
        private async Task<bool> PushDataPointsChunk(IDictionary<string, IEnumerable<BufferedDataPoint>> dataPointList, CancellationToken token) {
            if (config.Debug) return true;
            int count = 0;
            var inserts = dataPointList.Select(kvp =>
            {
                string externalId = kvp.Key;
                var values = kvp.Value;
                var item = new DataPointInsertionItem
                {
                    ExternalId = externalId
                };
                if (values.First().IsString)
                {
                    item.StringDatapoints = new StringDatapoints();
                    item.StringDatapoints.Datapoints.AddRange(values.Select(ipoint =>
                        new StringDatapoint
                        {
                            Timestamp = new DateTimeOffset(ipoint.Timestamp).ToUnixTimeMilliseconds(),
                            Value = ipoint.StringValue
                        }));
                }
                else
                {
                    item.NumericDatapoints = new NumericDatapoints();
                    item.NumericDatapoints.Datapoints.AddRange(values.Select(ipoint =>
                        new NumericDatapoint
                        {
                            Timestamp = new DateTimeOffset(ipoint.Timestamp).ToUnixTimeMilliseconds(),
                            Value = ipoint.DoubleValue
                        }));
                }

                count += values.Count();
                return item;
            });

            var req = new DataPointInsertionRequest();
            req.Items.AddRange(inserts);
            if (!req.Items.Any()) return true;

            var client = GetClient("Data");
            try
            {
                await client.DataPoints.CreateAsync(req, token);
            }
            catch (Exception e)
            {
                log.Warning(e, "Failed to push {count} points to CDF", count);
                dataPointPushFailures.Inc();
                // Return false indicating unexpected failure if we want to buffer.
                return !(e is ResponseException ex) || ex.Code == 400 || ex.Code == 409;
            }

            dataPointPushes.Inc();
            dataPointsCounter.Inc(count);
            return true;
        }
        /// <summary>
        /// Dequeues up to 1000 events from the BufferedEventQueue, then pushes them to CDF.
        /// </summary>
        public async Task<IEnumerable<BufferedEvent>> PushEvents(CancellationToken token)
        {
            var eventList = new List<BufferedEvent>();
            int count = 0;
            while (BufferedEventQueue.TryDequeue(out BufferedEvent buffEvent) && count++ < 1000)
            {
                if (buffEvent.Time < minDateTime || !nodeToAssetIds.ContainsKey(buffEvent.SourceNode) && !config.Debug)
                {
                    skippedEvents.Inc();
                    continue;
                }
                eventList.Add(buffEvent);
            }
            if (count == 0)
            {
                log.Verbose("Push 0 events to CDF");
                return null;
            }
            log.Debug("Push {NumEventsToPush} events to CDF", count);
            var chunks = ExtractorUtils.ChunkBy(eventList, 1000).ToArray();
            if (config.Debug) return null;
            bool[] results = await Task.WhenAll(chunks.Select(chunk => PushEventsChunk(chunk, token)));
            if (results.All(result => result)) return Array.Empty<BufferedEvent>();
            int index = 0;
            var failedEvents = new List<BufferedEvent>();
            foreach (var result in results)
            {
                if (!result)
                {
                    failedEvents.AddRange(chunks[index]);
                }

                index++;
            }

            return failedEvents;
        }

        private async Task<bool> PushEventsChunk(IEnumerable<BufferedEvent> events, CancellationToken token)
        {
            var client = GetClient("Data");
            IEnumerable<EventWriteDto> eventEntities = events.Select(EventToCDFEvent).DistinctBy(evt => evt.ExternalId).ToList();
            var count = events.Count();
            try
            {
                await client.Events.CreateAsync(eventEntities, token);
            }
            catch (ResponseException ex)
            {
                if (ex.Duplicated.Any())
                {
                    var duplicates = ex.Duplicated.Where(dict => dict.ContainsKey("externalId")).Select(dict => dict["externalId"].ToString())
                        .ToList();
                    log.Warning("{numduplicates} duplicated event ids, retrying", duplicates.Count);
                    duplicatedEvents.Inc(duplicates.Count);
                    eventEntities = eventEntities.Where(evt => !duplicates.Contains(evt.ExternalId));
                    try
                    {
                        await client.Events.CreateAsync(eventEntities, token);
                    }
                    catch (Exception exc)
                    {
                        log.Error(exc, "Failed to push {NumFailedEvents} events to CDF", eventEntities.Count());
                        eventPushFailures.Inc();
                        return !(exc is ResponseException rex) || rex.Code == 400 || rex.Code == 409;
                    }
                }
                else
                {
                    log.Error(ex, "Failed to push {NumFailedEvents} events to CDF", count);
                    eventPushFailures.Inc();
                    return ex.Code == 400 || ex.Code == 409;
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to push {NumFailedEvents} events to CDF", count);
                eventPushFailures.Inc();
                return false;
            }
            eventCounter.Inc(count);
            eventPushCounter.Inc();
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

            try
            {
                await Task.WhenAll(ExtractorUtils.ChunkBy(objects, config.AssetChunk).Select(items => EnsureAssets(items, token)).ToList());
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to ensure assets");
                nodeEnsuringFailures.Inc(); 
                return false;
            }

            trackedAssets.Inc(objects.Count());
            // At this point the assets should all be synchronized and mapped
            // Now: Try get latest TS data, if this fails, then create missing and retry with the remainder. Similar to assets.
            // This also sets the LastTimestamp property of each BufferedVariable
            // Synchronize TS with CDF, also get timestamps. Has to be done in three steps:
            // Get by externalId, create missing, get latest timestamps. All three can be done by externalId.
            // Eventually the API will probably support linking TS to assets by using externalId, for now we still need the
            // node to assets map.
            // We only need timestamps for historizing timeseries, and it is much more expensive to get latest compared to just
            // fetching the timeseries itself
            try
            {
                await Task.WhenAll(ExtractorUtils.ChunkBy(tsList, config.TimeSeriesChunk).Select(items => EnsureTimeseries(items, token)).ToList());
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to ensure timeseries");
                nodeEnsuringFailures.Inc();
                return false;
            }
            trackedTimeseres.Inc(tsList.Count);
            log.Information("Finish pushing nodes to CDF");
            return true;
        }
        /// <summary>
        /// Reset the pusher, preparing it to be restarted
        /// </summary>
        public void Reset()
        {
            nodeToAssetIds.Clear();
            trackedAssets.Set(0);
            trackedTimeseres.Set(0);
            BufferedDPQueue.Clear();
            BufferedEventQueue.Clear();
            ranges.Clear();
        }
        private async Task<IEnumerable<(string, DateTime)>> GetEarliestTimestampChunk(IEnumerable<string> ids, CancellationToken token)
        {
            var client = GetClient();
            var dps = await client.DataPoints.ListAsync(new DataPointsQuery
            {
                Items = ids.Select(id =>
                    new DataPointsQueryItem
                    {
                        ExternalId = id
                    }),
                Start = "0",
                Limit = 1
            }, token);

            var res = new List<(string, DateTime)>();
            foreach (var dp in dps.Items)
            {
                if (dp.NumericDatapoints?.Datapoints?.Any() ?? false)
                {
                    var ts = DateTimeOffset.FromUnixTimeMilliseconds(dp.NumericDatapoints.Datapoints.First().Timestamp).DateTime;
                    res.Add((dp.ExternalId, ts));
                }
                else if (dp.StringDatapoints?.Datapoints?.Any() ?? false)
                {
                    var ts = DateTimeOffset.FromUnixTimeMilliseconds(dp.StringDatapoints.Datapoints.First().Timestamp).DateTime;
                    res.Add((dp.ExternalId, ts));
                }
                else
                {
                    res.Add((dp.ExternalId, DateTime.MinValue));
                }
            }

            return res;
        }
        private async Task<IEnumerable<(string, DateTime)>> GetEarliestTimestamp(IEnumerable<string> ids, CancellationToken token)
        {
            var tasks = ExtractorUtils.ChunkBy(ids, config.EarliestChunk).Select(chunk => GetEarliestTimestampChunk(chunk, token)).ToList();
            await Task.WhenAll(tasks);
            return tasks.SelectMany(task => task.Result);
        }

        private async Task<IEnumerable<(string, DateTime)>> GetLatestTimestampChunk(IEnumerable<string> ids, CancellationToken token)
        {
            var client = GetClient();
            IEnumerable<DataPointsReadDto<DataPointDto>> dps;
            try
            {
                dps = await client.DataPoints.LatestAsync(new DataPointsLatestQueryDto
                {
                    Items = ids.Select(IdentityWithBefore.Create)
                }, token);
            }
            catch (ResponseException ex)
            {
                log.Information("Ex: {msg}", ex.Message);
                foreach (var missing in ex.Missing)
                {
                    foreach (var kvp in missing)
                    {
                        log.Information("{k}: {v}", kvp.Key, (kvp.Value as MultiValue.Long).Value);
                    }
                }
                throw;
            }

            var res = new List<(string, DateTime)>();
            foreach (var dp in dps)
            {
                if (dp.DataPoints.Any())
                {
                    var ts = DateTimeOffset.FromUnixTimeMilliseconds(dp.DataPoints.First().Timestamp).DateTime;
                    res.Add((dp.ExternalId, ts));
                }
                else
                {
                    res.Add((dp.ExternalId, DateTime.MinValue));
                }
            }

            return res;
        }

        private async Task<IEnumerable<(string, DateTime)>> GetLatestTimestamp(IEnumerable<string> ids,
            CancellationToken token)
        {
            var tasks = ExtractorUtils.ChunkBy(ids, config.LatestChunk).Select(chunk => GetLatestTimestampChunk(chunk, token)).ToList();
            await Task.WhenAll(tasks);
            return tasks.SelectMany(task => task.Result);
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
                        ids.Add(Extractor.GetUniqueId(state.Id, i));
                    }
                }
                else
                {
                    ids.Add(Extractor.GetUniqueId(state.Id));
                }
            }
            var tasks = new List<Task>();
            var latestTask = GetLatestTimestamp(ids, token);
            tasks.Add(latestTask);
            Task<IEnumerable<(string, DateTime)>> earliestTask = null;
            if (backfillEnabled)
            {
                earliestTask = GetEarliestTimestamp(ids, token);
                tasks.Add(earliestTask);
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to get extracted ranges");
                return false;
            }

            foreach (var dp in latestTask.Result)
            {
                if (backfillEnabled && dp.Item2 == DateTime.MinValue)
                {
                    // No value found, so the timeseries is empty. If backfill is enabled this means that we start the range now, otherwise it means
                    // that we start at time zero.
                    ranges[dp.Item1] = new TimeRange(DateTime.UtcNow, DateTime.UtcNow);
                    continue;
                }
                ranges[dp.Item1] = new TimeRange(dp.Item2, dp.Item2);
            }

            if (backfillEnabled)
            {
                foreach (var dp in earliestTask.Result)
                {
                    if (dp.Item2 == DateTime.MinValue) continue;
                    ranges[dp.Item1].Start = dp.Item2;
                }
            }

            foreach (var id in ids)
            {
                var state = Extractor.GetNodeState(id);
                state.InitExtractedRange(ranges[id].Start, ranges[id].End);
            }

            return true;
        }

        public async Task<bool> TestConnection(CancellationToken token)
        {
            // Use data client because it gives up after a little while
            var client = GetClient("Data");
            LoginStatusReadDto loginStatus;
            try
            {
                loginStatus = await client.Login.StatusAsync(new CancellationToken());
            }
            catch (Exception ex)
            {
                log.Debug(ex, "Failed to get login status from CDF. Project {project} at {url}", config.Project, config.Host);
                log.Error("Failed to get CDF login status, this is likely a problem with the network. Project {project} at {url}", 
                    config.Project, config.Host);
                return false;
            }
            if (!loginStatus.LoggedIn)
            {
                log.Error("API key is invalid. Project {project} at {url}", config.Project, config.Host);
                return false;
            }
            if (!loginStatus.Project.Equals(config.Project, StringComparison.InvariantCulture))
            {
                log.Error("API key is not associated with project {project} at {url}", config.Project, config.Host);
                return false;
            }
            try
            {
                await client.TimeSeries.ListAsync(new TimeSeriesQueryDto { Limit = 1 });
            }
            catch (ResponseException ex)
            {
                log.Error("Could not access CDF Time Series - most likely due to insufficient access rights on API key. Project {project} at {host}: {msg}",
                    config.Project, config.Host, ex.Message);
                log.Debug(ex, "Could not access CDF Time Series - most likely due to insufficient access rights on API key. {project} at {host}",
                    config.Project, config.Host);
                return false;
            }

            return true;
        }

        #endregion

        #region Pushing
        /// <summary>
        /// Test if given list of assets exists, then create any that do not, checking for properties.
        /// </summary>
        /// <param name="assetList">List of assets to be tested</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        private async Task EnsureAssets(IEnumerable<BufferedNode> assetList, CancellationToken token)
        {
            if (!assetList.Any()) return;
            var assetIds = assetList.ToDictionary(node => Extractor.GetUniqueId(node.Id));
            ISet<string> missingAssetIds = new HashSet<string>();

            log.Information("Test {NumAssetsToTest} assets", assetList.Count());
            var client = GetClient();
            var assetIdentities = assetIds.Keys.Select(Identity.Create);
            try
            {
                var readResults = await client.Assets.RetrieveAsync(assetIdentities, token);
                log.Information("Found {NumRetrievedAssets} assets", readResults.Count());
                foreach (var resultItem in readResults)
                {
                    nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
                }
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing != null && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out MultiValue raw))
                        {
                            if (!(raw is MultiValue.String value)) continue;
                            missingAssetIds.Add(value.Value);
                        }
                    }

                    log.Information("Found {NumMissingAssets} missing assets", ex.Missing.Count());
                }
                else throw;
            }

            if (!missingAssetIds.Any()) return;
            log.Information("Create {NumAssetsToCreate} new assets", missingAssetIds.Count);

            var getMetaData = missingAssetIds.Select(id => assetIds[id]);
            await Extractor.ReadProperties(getMetaData, token);
            
            var createAssets = missingAssetIds.Select(id => NodeToAsset(assetIds[id]));

            var writeResults = await client.Assets.CreateAsync(createAssets, token);
            foreach (var resultItem in writeResults)
            {
                nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
            }
            var idsToMap = assetIds.Keys
                .Where(id => !missingAssetIds.Contains(id))
                .Select(Identity.Create);

            if (!idsToMap.Any()) return;

            log.Information("Get remaining {NumFinalIdsToRetrieve} assetids", idsToMap.Count());
            var remainingResults = await client.Assets.RetrieveAsync(idsToMap, token);
            foreach (var resultItem in remainingResults)
            { 
                nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
            }
        }
        /// <summary>
        /// Test if given list of timeseries exists, then create any that do not, checking for properties.
        /// </summary>
        /// <param name="tsList">List of timeseries to be tested</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        private async Task EnsureTimeseries(IEnumerable<BufferedVariable> tsList, CancellationToken token)
        {
            if (!tsList.Any()) return;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                string externalId = Extractor.GetUniqueId(node.Id, node.Index);
                tsIds.Add(externalId, node);
                if (node.Index == -1)
                {
                    if (nodeToAssetIds.ContainsKey(node.ParentId))
                    {
                        nodeToAssetIds[node.Id] = nodeToAssetIds[node.ParentId];
                    }
                    else
                    {
                        log.Warning("Parentless timeseries: {id}", node.Id);
                    }
                }
            }
            log.Information("Test {NumTimeseriesToTest} timeseries", tsIds.Count);
            var missingTSIds = new HashSet<string>();
            var client = GetClient();
            try
            {
                var readResults = await client.TimeSeries.RetrieveAsync(tsIds.Keys.Select(Identity.Create), token);
                log.Information("Found {NumRetrievedTimeseries} timeseries", readResults.Count());
                foreach (var res in readResults)
                {
                    var state = Extractor.GetNodeState(res.ExternalId);
                    if (state.DataType.IsString != res.IsString)
                    {
                        mismatchedTimeseries.Add(res.ExternalId);
                    }
                }
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out MultiValue raw))
                        {
                            if (!(raw is MultiValue.String value)) continue;
                            missingTSIds.Add(value.Value);
                        }
                    }
                    log.Information("Found {NumMissingTimeseries} missing timeseries", ex.Missing.Count());
                }
                else throw;
            }

            if (!missingTSIds.Any()) return;

            log.Information("Create {NumTimeseriesToCreate} new timeseries", missingTSIds.Count);

            var getMetaData = missingTSIds.Select(id => tsIds[id]);

            await Extractor.ReadProperties(getMetaData, token);

            var createTimeseries = getMetaData.Select(VariableToTimeseries);
            await client.TimeSeries.CreateAsync(createTimeseries, token);

            var remaining = tsIds.Keys.Except(missingTSIds);
            if (!remaining.Any()) return;
            var remainingResults = await client.TimeSeries.RetrieveAsync(remaining.Select(Identity.Create), token);

            foreach (var res in remainingResults)
            {
                var state = Extractor.GetNodeState(res.ExternalId);
                if (state.DataType.IsString != res.IsString)
                {
                    log.Warning("Mismatched timeseries: {id}. "
                                + (state.DataType.IsString ? "Expected double but got string" : "Expected string but got double"), 
                        res.ExternalId);
                    mismatchedTimeseries.Add(res.ExternalId);
                }
            }
        }
        /// <summary>
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="variable">Variable to be converted</param>
        /// <returns>Complete timeseries write poco</returns>
        private TimeSeriesWriteDto VariableToTimeseries(BufferedVariable variable)
        {
            string externalId = Extractor.GetUniqueId(variable.Id, variable.Index);
            var writePoco = new TimeSeriesWriteDto
            {
                Description = ExtractorUtils.Truncate(variable.Description, 1000),
                ExternalId = externalId,
                AssetId = nodeToAssetIds[variable.ParentId],
                Name = ExtractorUtils.Truncate(variable.DisplayName, 255),
                LegacyName = externalId,
                IsString = variable.DataType.IsString,
                IsStep = variable.DataType.IsStep
            };
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
        private AssetWriteDto NodeToAsset(BufferedNode node)
        {
            var writePoco = new AssetWriteDto
            {
                Description = ExtractorUtils.Truncate(node.Description, 500),
                ExternalId = Extractor.GetUniqueId(node.Id),
                Name = string.IsNullOrEmpty(node.DisplayName)
                    ? ExtractorUtils.Truncate(Extractor.GetUniqueId(node.Id), 140) : ExtractorUtils.Truncate(node.DisplayName, 140)
            };
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
            else
            {
                return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            }
        }
        private static readonly HashSet<string> excludeMetaData = new HashSet<string> {
            "StartTime", "EndTime", "Type", "SubType"
        };
        /// <summary>
        /// Transform BufferedEvent into EventEntity to be sent to CDF.
        /// </summary>
        /// <param name="evt">Event to be transformed.</param>
        /// <returns>Final EventEntity object</returns>
        private EventWriteDto EventToCDFEvent(BufferedEvent evt)
        {
            var entity = new EventWriteDto
            {
                Description = ExtractorUtils.Truncate(evt.Message, 500),
                StartTime = evt.MetaData.ContainsKey("StartTime")
                    ? GetTimestampValue(evt.MetaData["StartTime"])
                    : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                EndTime = evt.MetaData.ContainsKey("EndTime")
                    ? GetTimestampValue(evt.MetaData["EndTime"])
                    : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                AssetIds = new List<long> { nodeToAssetIds[evt.SourceNode] },
                ExternalId = ExtractorUtils.Truncate(evt.EventId, 255),
                Type = ExtractorUtils.Truncate(evt.MetaData.ContainsKey("Type")
                    ? Extractor.ConvertToString(evt.MetaData["Type"])
                    : Extractor.GetUniqueId(evt.EventType), 64)
            };
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

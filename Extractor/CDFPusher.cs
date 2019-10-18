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
using Com.Cognite.V1.Timeseries.Proto;
using CogniteSdk;
using CogniteSdk.Assets;
using CogniteSdk.TimeSeries;
using CogniteSdk.DataPoints;
using CogniteSdk.Events;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Prometheus.Client;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Pusher against CDF
    /// </summary>
    public class CDFPusher : IPusher
    {
        private readonly IServiceProvider clientProvider;
        private readonly CogniteClientConfig config;
        private readonly IDictionary<string, bool> nodeIsHistorizing = new Dictionary<string, bool>();
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        public Extractor Extractor { private get; set; }
        public UAClient UAClient { private get; set; }
        public PusherConfig BaseConfig { get; }

        public ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; } = new ConcurrentQueue<BufferedDataPoint>();
        public ConcurrentQueue<BufferedEvent> BufferedEventQueue { get; } = new ConcurrentQueue<BufferedEvent>();

        public CDFPusher(IServiceProvider clientProvider, CogniteClientConfig config)
        {
            this.config = config;
            BaseConfig = config;
            this.clientProvider = clientProvider;
        }

        private Client GetClient(string name = "Context")
        {
            Client client = name == "Context"
                ? (Client)clientProvider.GetRequiredService<ContextCDFClient>()
                : clientProvider.GetRequiredService<DataCDFClient>();
            return client
                .AddHeader("api-key", config.ApiKey)
                .SetAppId("OPC-UA Extractor")
                .SetProject(config.Project)
                .SetServiceUrl(config.Host);
        }

        private static readonly Counter dataPointsCounter = Metrics
            .CreateCounter("opcua_datapoints_pushed", "Number of datapoints pushed to CDF");
        private static readonly Counter dataPointPushes = Metrics
            .CreateCounter("opcua_datapoint_pushes", "Number of times datapoints have been pushed to CDF");
        private static readonly Counter dataPointPushFailures = Metrics
            .CreateCounter("opcua_datapoint_push_failures", "Number of completely failed pushes of datapoints to CDF");
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

        #region Interface

        /// <summary>
        /// Dequeues up to 100000 points from the BufferedDPQueue, then pushes them to CDF. On failure, writes to file if enabled.
        /// </summary>
        private IEnumerable<IDictionary<string, IEnumerable<BufferedDataPoint>>> ChunkDatapointDict(
            IDictionary<string, List<BufferedDataPoint>> points)
        {
            var ret = new List<Dictionary<string, IEnumerable<BufferedDataPoint>>>();
            var current = new Dictionary<string, IEnumerable<BufferedDataPoint>>();
            ret.Add(current);
            int count = 0;
            int tscount = 0;
            foreach ((string key, var value) in points)
            {
                int pcount = value.Count;
                if (count + pcount <= 100000 && tscount < 10000)
                {
                    current[key] = value;
                    count += pcount;
                    tscount++;
                }
                else
                {
                    var point = (IEnumerable<BufferedDataPoint>) value;
                    var toAdd = point.Take(Math.Min(100000 - count, pcount));
                    current[key] = toAdd;
                    count = 0;
                    tscount = 0;
                    current = new Dictionary<string, IEnumerable<BufferedDataPoint>>();
                    ret.Add(current);
                    if (pcount > 100000 - count)
                    {
                        point = point.Skip(100000 - count);
                        var dictionaries = Utils.ChunkBy(point, 100000)
                            .Select(chunk => new Dictionary<string, IEnumerable<BufferedDataPoint>> { { key, chunk } }).ToList();
                        current = dictionaries.Last();
                        count = current.First().Value.Count();
                        ret.AddRange(dictionaries.Take(dictionaries.Count - 1));
                    }
                }
            }
            return ret;
        }
        public async Task PushDataPoints(CancellationToken token)
        {
            int count = 0;
            var dataPointList = new Dictionary<string, List<BufferedDataPoint>>();
            while (BufferedDPQueue.Any())
            {
                while (BufferedDPQueue.TryDequeue(out BufferedDataPoint buffer))
                {
                    if (buffer.Timestamp <= DateTime.MinValue) continue;
                    count++;
                    if (!dataPointList.ContainsKey(buffer.Id))
                    {
                        dataPointList[buffer.Id] = new List<BufferedDataPoint>();
                    }
                    dataPointList[buffer.Id].Add(buffer);
                }

                if (count == 0)
                {
                    Log.Debug("Push 0 datapoints to CDF");
                    return;
                }
                Log.Information("Push {NumDatapointsToPush} datapoints to CDF", count);
            }

            var pushTasks = ChunkDatapointDict(dataPointList).Select(chunk => PushDataPointsChunk(chunk, token))
                .ToList();
            await Task.WhenAll(pushTasks);
        }
        private async Task PushDataPointsChunk(IDictionary<string, IEnumerable<BufferedDataPoint>> dataPointList, CancellationToken token) {
            if (config.Debug) return;
            int count = 0;
            var finalDataPoints = dataPointList.Select(kvp =>
            {
                var item = new DataPointInsertionItem
                {
                    ExternalId = kvp.Key
                };
                if (kvp.Value.First().IsString)
                {
                    item.StringDatapoints = new StringDatapoints();
                    item.StringDatapoints.Datapoints.AddRange(kvp.Value.Select(point =>
                        {
                            count++;
                            return new StringDatapoint
                            {
                                Timestamp = new DateTimeOffset(point.Timestamp).ToUnixTimeMilliseconds(),
                                Value = point.StringValue
                            };
                        }
                    ));
                }
                else
                {
                    item.NumericDatapoints = new NumericDatapoints();
                    item.NumericDatapoints.Datapoints.AddRange(kvp.Value.Select(point =>
                        {
                            count++;
                            return new NumericDatapoint
                            {
                                Timestamp = new DateTimeOffset(point.Timestamp).ToUnixTimeMilliseconds(),
                                Value = point.DoubleValue
                            };
                        }
                    ));
                }

                return item;
            });
            var req = new DataPointInsertionRequest();
            req.Items.AddRange(finalDataPoints);
            var client = GetClient("Data");
            bool buffer = false;
            bool failed = false;
            try
            {
                await client.DataPoints.InsertAsync(req, token);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to insert {NumFailedDatapoints} datapoints into CDF", count);
				dataPointPushFailures.Inc();
                if (!(e is ResponseException ex) || ex.Code != 400 && ex.Code != 409)
                {
                    buffer = true;
                } 
                failed = true;
            }
            if (config.BufferOnFailure && !string.IsNullOrEmpty(config.BufferFile) && buffer)
            {
                try
                {
                    Utils.WriteBufferToFile(dataPointList.Values.SelectMany(val => val).ToList(), config, token, nodeIsHistorizing);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to write buffer to file");
                }
            }
            if (failed) return;
            if (config.BufferOnFailure && !Utils.BufferFileEmpty && !string.IsNullOrEmpty(config.BufferFile))
            {
                Utils.ReadBufferFromFile(BufferedDPQueue, config, token, nodeIsHistorizing);
            }
            dataPointPushes.Inc();
            dataPointsCounter.Inc(count);
        }
        /// <summary>
        /// Dequeues up to 1000 events from the BufferedEventQueue, then pushes them to CDF.
        /// </summary>
        public async Task PushEvents(CancellationToken token)
        {
            var eventList = new List<BufferedEvent>();
            int count = 0;
            while (BufferedEventQueue.TryDequeue(out BufferedEvent buffEvent) && count++ < 1000)
            {
                if (nodeToAssetIds.ContainsKey(buffEvent.SourceNode) || config.Debug)
                {
                    eventList.Add(buffEvent);
                }
                else
                {
                    Log.Warning("Event with unknown sourceNode: {nodeId}", buffEvent.SourceNode);
                }
            }
            if (count == 0)
            {
                Log.Debug("Push 0 events to CDF");
                return;
            }
            Log.Information("Push {NumEventsToPush} events to CDF", count);
            if (config.Debug) return;
            IEnumerable<EventEntity> events = eventList.Select(EventToCDFEvent).ToList();
            var client = GetClient("Data");
            try
            {
                await client.Events.CreateAsync(events, token);
            }
            catch (ResponseException ex)
            {
                if (ex.Duplicated.Any())
                {
                    var duplicates = ex.Duplicated.Where(dict => dict.ContainsKey("externalId")).Select(dict => dict["externalId"].ToString());
                    Log.Warning("{numduplicates} duplicated event ids, retrying", duplicates.Count());

                    events = events.Where(evt => !duplicates.Contains(evt.ExternalId));
                    try
                    {
                        await client.Events.CreateAsync(events, token);
                    }
                    catch (Exception exc)
                    {
                        Log.Error(exc, "Failed to push {NumFailedEvents} events to CDF", count);
                        eventPushFailures.Inc();
                        return;
                    }
                }
                else
                {
                    Log.Error(ex, "Failed to push {NumFailedEvents} events to CDF", count);
                    eventPushFailures.Inc();
                    return;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to push {NumFailedEvents} events to CDF", count);
                eventPushFailures.Inc();
                return;
            }
            eventCounter.Inc(count);
            eventPushCounter.Inc();
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

            if (!variables.Any() && !objects.Any())
            {
                Log.Debug("Testing 0 nodes against CDF");
                return true;
            }
            foreach (var node in variables)
            {
                if (Extractor.AllowTSMap(node))
                {
                    tsList.Add(node);
                }
            }
            Log.Information("Testing {TotalNodesToTest} nodes against CDF", variables.Count() + objects.Count());
            if (config.Debug)
            {

                await Extractor.ReadProperties(objects.Concat(variables), token);
                foreach (var node in objects)
                {
                    Log.Debug(node.ToDebugDescription());
                }
                foreach (var node in variables)
                {
                    Log.Debug(node.ToDebugDescription());
                }
                return true;
            }
            try
            {
                foreach (var task in Utils.ChunkBy(objects, config.AssetChunk).Select(items => EnsureAssets(items, token)))
                {
                    if (!await task) return false;
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
                foreach (var task in Utils.ChunkBy(tsList, config.TimeSeriesChunk).Select(items => EnsureTimeseries(items, token)))
                {
                    if (!await task) return false;
                }
                trackedTimeseres.Inc(tsList.Count);
            }
            catch (Exception e)
            {
                if (e is TaskCanceledException) throw;
                Log.Error(e, "Failed to push to CDF");
                return false;
            }
            // This can be done in this thread, as the history read stuff is done in separate threads, so there should only be a single
            // createSubscription service called here
            Log.Information("Finish pushing nodes to CDF");
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
        }
        /// <summary>
        /// Fetch the latest timestamp from the destination system for each NodeExtractionState provided
        /// </summary>
        /// <param name="states">Historizing NodeExtractionStates to get timestamps for</param>
        /// <returns>True if no task failed unexpectedly</returns>
        public async Task<bool> InitLatestTimestamps(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            if (config.Debug) return true;
            var stateMap = states.ToDictionary(state => UAClient.GetUniqueId(state.Id,
                state.ArrayDimensions != null && state.ArrayDimensions[0] > 0 ? 0 : -1), state => state);
            var client = GetClient();
            foreach (var idChunk in Utils.ChunkBy(stateMap.Keys, config.LatestChunk))
            {
                Log.Information("Get latest timestamp from CDF for {num} nodes", idChunk.Count());
                var points = idChunk.Select<string, (Identity, string)>(id => (Identity.ExternalId(id), null));
                try
                {
                    var dps = await client.DataPoints.GetLatestAsync(points, token);
                    foreach (var dp in dps)
                    {
                        if (dp.NumericDataPoints.Any())
                        {
                            stateMap[dp.ExternalId].InitTimestamp(DateTimeOffset.FromUnixTimeMilliseconds(dp.NumericDataPoints.First().TimeStamp).DateTime);
                        }
                        else if (dp.StringDataPoints.Any())
                        {
                            stateMap[dp.ExternalId].InitTimestamp(DateTimeOffset.FromUnixTimeMilliseconds(dp.StringDataPoints.First().TimeStamp).DateTime);
                        }
                        else
                        {
                            stateMap[dp.ExternalId].InitTimestamp(Utils.Epoch);
                        }
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to get latest timestamp");
                    if (e is ResponseException ex)
                    {
                        foreach (var id in ex.Missing)
                        {
                            Log.Information("Missing: {id}", id["externalId"].ToString());
                        }
                    }
                    return false;
                }
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
        private async Task<bool> EnsureAssets(IEnumerable<BufferedNode> assetList, CancellationToken token)
        {
            var assetIds = assetList.ToDictionary(node => UAClient.GetUniqueId(node.Id));
            // TODO: When v1 gets support for ExternalId on assets when associating timeseries, we can drop a lot of this.
            // Specifically anything related to NodeToAssetIds
            ISet<string> missingAssetIds = new HashSet<string>();

            Log.Information("Test {NumAssetsToTest} assets", assetList.Count());
            var client = GetClient();
            var assetIdentities = assetIds.Keys.Select(Identity.ExternalId);
            try
            {
                var readResults = await client.Assets.GetByIdsAsync(assetIdentities, token);
                Log.Information("Found {NumRetrievedAssets} assets", readResults.Count());
                foreach (var resultItem in readResults)
                {
                    nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
                }
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out ErrorValue value))
                        {
                            missingAssetIds.Add(value.ToString());
                        }
                    }
                    Log.Information("Found {NumMissingAssets} missing assets", ex.Missing.Count());
                }
                else
                {
                    nodeEnsuringFailures.Inc();
                    Log.Error(ex, "Failed to get assets");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to get assets");
                nodeEnsuringFailures.Inc();
                return false;
            }
            if (missingAssetIds.Any())
            {
                Log.Information("Create {NumAssetsToCreate} new assets", missingAssetIds.Count);

                var getMetaData = missingAssetIds.Select(id => assetIds[id]);
                try
                {
                    await Extractor.ReadProperties(getMetaData, token);
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to get node properties");
                    nodeEnsuringFailures.Inc();
                    return false;
                }
                var createAssets = missingAssetIds.Select(id => NodeToAsset(assetIds[id]));
                try
                {
                    var writeResults = await client.Assets.CreateAsync(createAssets, token);
                    foreach (var resultItem in writeResults)
                    {
                        nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to create assets");
                    nodeEnsuringFailures.Inc();
                    return false;
                }
                var idsToMap = assetIds.Keys
                    .Where(id => !missingAssetIds.Contains(id))
                    .Select(Identity.ExternalId);

                if (idsToMap.Any())
                {
                    Log.Information("Get remaining {NumFinalIdsToRetrieve} assetids", idsToMap.Count());
                    try
                    {
                        var readResults = await client.Assets.GetByIdsAsync(idsToMap, token);
                        foreach (var resultItem in readResults)
                        {
                            nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
                        }
                    }
                    catch (Exception e)
                    {
                        Log.Error(e, "Failed to get asset ids");
                        nodeEnsuringFailures.Inc();
                        return false;
                    }
                }
            }
            return true;
        }
        /// <summary>
        /// Test if given list of timeseries exists, then create any that do not, checking for properties.
        /// </summary>
        /// <param name="tsList">List of timeseries to be tested</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        private async Task<bool> EnsureTimeseries(IEnumerable<BufferedVariable> tsList, CancellationToken token)
        {
            if (!tsList.Any()) return true;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                string externalId = UAClient.GetUniqueId(node.Id, node.Index);
                tsIds.Add(externalId, node);
                nodeIsHistorizing[externalId] = false;
                if (node.Index == -1)
                {
                    if (nodeToAssetIds.ContainsKey(node.ParentId))
                    {
                        nodeToAssetIds[node.Id] = nodeToAssetIds[node.ParentId];
                    }
                    else
                    {
                        Log.Warning("Parentless timeseries: {id}", node.Id);
                    }
                }
            }

            Log.Information("Test {NumTimeseriesToTest} timeseries", tsIds.Count);
            var missingTSIds = new HashSet<string>();
            var client = GetClient();
            try
            {
                var readResults = await client.TimeSeries.GetByIdsAsync(tsIds.Keys.Select(Identity.ExternalId), token);
                Log.Information("Found {NumRetrievedTimeseries} timeseries", readResults.Count());
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out ErrorValue value))
                        {
                            missingTSIds.Add(value.ToString());
                        }
                    }
                    Log.Information("Found {NumMissingTimeseries} missing timeseries", ex.Missing.Count());
                }
                else
                {
                    nodeEnsuringFailures.Inc();
                    Log.Error(ex, "Failed to get timeseries");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to get timeseries");
                nodeEnsuringFailures.Inc();
                return false;
            }
            if (missingTSIds.Any())
            {
                Log.Information("Create {NumTimeseriesToCreate} new timeseries", missingTSIds.Count);

                var getMetaData = missingTSIds.Select(id => tsIds[id]);
                await Extractor.ReadProperties(getMetaData, token);
                var createTimeseries = getMetaData.Select(VariableToTimeseries);
                try
                {
                    await client.TimeSeries.CreateAsync(createTimeseries, token);
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to create TS");
                    nodeEnsuringFailures.Inc();
                    return false;
                }
            }
            return true;
        }
        /// <summary>
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="variable">Variable to be converted</param>
        /// <returns>Complete timeseries write poco</returns>
        private TimeSeriesEntity VariableToTimeseries(BufferedVariable variable)
        {
            string externalId = UAClient.GetUniqueId(variable.Id, variable.Index);
            var writePoco = new TimeSeriesEntity
            {
                Description = Utils.Truncate(variable.Description, 1000),
                ExternalId = externalId,
                AssetId = nodeToAssetIds[variable.ParentId],
                Name = Utils.Truncate(variable.DisplayName, 255),
                LegacyName = externalId,
                IsString = variable.DataType.IsString,
                IsStep = variable.DataType.IsStep
            };
            if (variable.Properties != null && variable.Properties.Any())
            {
                writePoco.MetaData = variable.Properties
                    .Where(prop => prop.Value != null)
                    .Take(16)
                    .ToDictionary(prop => Utils.Truncate(prop.DisplayName, 32), prop => Utils.Truncate(prop.Value.StringValue, 256));
            }
            return writePoco;
        }
        /// <summary>
        /// Converts BufferedNode into asset write poco.
        /// </summary>
        /// <param name="node">Node to be converted</param>
        /// <returns>Full asset write poco</returns>
        private AssetEntity NodeToAsset(BufferedNode node)
        {
            var writePoco = new AssetEntity
            {
                Description = Utils.Truncate(node.Description, 500),
                ExternalId = UAClient.GetUniqueId(node.Id),
                Name = string.IsNullOrEmpty(node.DisplayName) ? Utils.Truncate(UAClient.GetUniqueId(node.Id), 140) : Utils.Truncate(node.DisplayName, 140)
            };
            if (node.ParentId != null && !node.ParentId.IsNullNodeId)
            {
                writePoco.ParentExternalId = UAClient.GetUniqueId(node.ParentId);
            }
            if (node.Properties != null && node.Properties.Any())
            {
                writePoco.MetaData = node.Properties
                    .Where(prop => prop.Value != null)
                    .Take(16)
                    .ToDictionary(prop => Utils.Truncate(prop.DisplayName, 32), prop => Utils.Truncate(prop.Value.StringValue, 256));
            }
            return writePoco;
        }
        /// <summary>
        /// Get the value of given object assumed to be a timestamp as the number of milliseconds since 1/1/1970
        /// </summary>
        /// <param name="value">Value of the object. Assumed to be a timestamp or numeric value</param>
        /// <returns>Milliseconds since epoch</returns>
        private long GetTimestampValue(object value)
        {
            if (value is DateTime dt)
            {
                return new DateTimeOffset(dt).ToUnixTimeMilliseconds();
            }
            else
            {
                return Convert.ToInt64(value);
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
        private EventEntity EventToCDFEvent(BufferedEvent evt)
        {
            var entity = new EventEntity
            {
                Description = Utils.Truncate(evt.Message, 500),
                StartTime = evt.MetaData.ContainsKey("StartTime") ? GetTimestampValue(evt.MetaData["StartTime"]) : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                EndTime = evt.MetaData.ContainsKey("EndTime") ? GetTimestampValue(evt.MetaData["EndTime"]) : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                AssetIds = new List<long> { nodeToAssetIds[evt.SourceNode] },
                ExternalId = Utils.Truncate(evt.EventId, 255),
                Type = Utils.Truncate(evt.MetaData.ContainsKey("Type") ? UAClient.ConvertToString(evt.MetaData["Type"]) : UAClient.GetUniqueId(evt.EventType), 64)
            };
            if (!evt.MetaData.ContainsKey("SourceNode"))
            {
                evt.MetaData["SourceNode"] = UAClient.GetUniqueId(evt.SourceNode);
            }
            if (evt.MetaData.ContainsKey("SubType"))
            {
                entity.SubType = Utils.Truncate(UAClient.ConvertToString(evt.MetaData["SubType"]), 64);
            }
            var metaData = evt.MetaData
                .Where(kvp => !excludeMetaData.Contains(kvp.Key))
                .Take(16)
                .ToDictionary(kvp => Utils.Truncate(kvp.Key, 32), kvp => Utils.Truncate(UAClient.ConvertToString(kvp.Value), 256));
            if (metaData.Any())
            {
                entity.MetaData = metaData;
            }
            return entity;
        }
        #endregion
    }
}

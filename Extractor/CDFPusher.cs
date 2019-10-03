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
        public PusherConfig BaseConfig { get; private set; }

        public ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; } = new ConcurrentQueue<BufferedDataPoint>();

        public CDFPusher(IServiceProvider clientProvider, CogniteClientConfig config)
        {
            this.config = config;
            BaseConfig = config;
            this.clientProvider = clientProvider;
        }

        private Client GetClient()
        {
            return clientProvider.GetRequiredService<Client>()
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
        private static readonly Gauge trackedAssets = Metrics
            .CreateGauge("opcua_tracked_assets", "Number of objects on the opcua server mapped to assets in CDF");
        private static readonly Gauge trackedTimeseres = Metrics
            .CreateGauge("opcua_tracked_timeseries", "Number of variables on the opcua server mapped to timeseries in CDF");
        private static readonly Counter nodeEnsuringFailures = Metrics
            .CreateCounter("opcua_node_ensure_failures",
            "Number of completely failed requests to CDF when ensuring assets/timeseries exist");

        #region Interface
        /// <summary>
        /// Dequeues up to 100000 points from the queue, then pushes them to CDF. On failure, writes to file if enabled.
        /// </summary>
        /// <param name="dataPointQueue">Queue to be emptied</param>
        public async Task PushDataPoints(CancellationToken token)
        {
            var dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            while (BufferedDPQueue.TryDequeue(out BufferedDataPoint buffer) && count++ < 100000)
            {
                if (buffer.timestamp > DateTime.MinValue)
                {
                    dataPointList.Add(buffer);
                }
            }
            if (count == 0)
            {
                Log.Debug("Push 0 datapoints to CDF");
                return;
            }
            Log.Information("Push {NumDatapointsToPush} datapoints to CDF", count);

            if (config.Debug) return;
            var finalDataPoints = dataPointList.GroupBy(dp => dp.Id, (id, points) =>
            {
                var item = new DataPointInsertionItem
                {
                    ExternalId = id
                };
                if (points.First().isString)
                {
                    item.StringDatapoints = new StringDatapoints();
                    item.StringDatapoints.Datapoints.AddRange(points.Select(point =>
                        new StringDatapoint
                        {
                            Timestamp = new DateTimeOffset(point.timestamp).ToUnixTimeMilliseconds(),
                            Value = point.stringValue
                        }
                    ));
                }
                else
                {
                    item.NumericDatapoints = new NumericDatapoints();
                    item.NumericDatapoints.Datapoints.AddRange(points.Select(point =>
                        new NumericDatapoint
                        {
                            Timestamp = new DateTimeOffset(point.timestamp).ToUnixTimeMilliseconds(),
                            Value = point.doubleValue
                        }
                    ));
                }
                return item;
            });
            var req = new DataPointInsertionRequest();
            req.Items.AddRange(finalDataPoints);
            var client = GetClient();
            try
            {
                await client.DataPoints.InsertAsync(req, token);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to insert {NumFailedDatapoints} into CDF", count);
				dataPointPushFailures.Inc();
                if (config.BufferOnFailure && !string.IsNullOrEmpty(config.BufferFile))
                {
                    try
                    {
                        Utils.WriteBufferToFile(dataPointList, config, token, nodeIsHistorizing);
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Failed to write buffer to file");
                    }
                }
                return;
            }
            if (config.BufferOnFailure && !Utils.BufferFileEmpty && !string.IsNullOrEmpty(config.BufferFile))
            {
                Utils.ReadBufferFromFile(BufferedDPQueue, config, token, nodeIsHistorizing);
            }
            dataPointsCounter.Inc(count);
            dataPointPushes.Inc();
        }
        /// <summary>
        /// Empty queue, fetch info for each relevant node, test results against CDF, then synchronize any variables
        /// </summary>
        /// <param name="nodeQueue">Queue to be emptied</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        public async Task<bool> PushNodes(IEnumerable<BufferedNode> nodes, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            var histTsList = new List<BufferedVariable>();
            var tsList = new List<BufferedVariable>();

            if (variables.Count() == 0 && nodes.Count() == 0)
            {
                Log.Debug("Testing 0 nodes against CDF");
                return true;
            }
            foreach (var node in variables)
            {
                if (Extractor.AllowTSMap(node))
                {
                    if (node.Historizing)
                    {
                        histTsList.Add(node);
                    }
                    else
                    {
                        tsList.Add(node);
                    }
                }
            }
            Log.Information("Testing {TotalNodesToTest} nodes against CDF", variables.Count() + nodes.Count());
            if (config.Debug)
            {

                await Extractor.ReadProperties(nodes.Concat(variables), token);
                foreach (var node in nodes)
                {
                    Log.Debug(node.ToDebugDescription());
                }
                foreach (var node in variables)
                {
                    Log.Debug(node.ToDebugDescription());
                }
                return true;
            }
            var client = GetClient();
            try
            {
                foreach (var task in Utils.ChunkBy(nodes, config.AssetChunk).Select(items => EnsureAssets(items, token)))
                {
                    if (!await task) return false;
                }
                trackedAssets.Inc(nodes.Count());
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

                foreach (var task in Utils.ChunkBy(histTsList, config.LatestChunk).Select(items => EnsureHistorizingTimeseries(items, token)))
                {
                    if (!await task) return false;
                }
                trackedTimeseres.Inc(histTsList.Count);
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
        /// Try to get latest timestamp from given list of timeseries, then create any not found and try again
        /// </summary>
        /// <param name="tsList">List of timeseries to be tested</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        private async Task<bool> EnsureHistorizingTimeseries(IEnumerable<BufferedVariable> tsList, CancellationToken token)
        {
            if (!tsList.Any()) return true;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                string externalId = UAClient.GetUniqueId(node.Id, node.Index);
                tsIds.Add(externalId, node);
                nodeIsHistorizing[externalId] = true;
            }

            Log.Information("Test {NumHistorizingTimeseriesToTest} historizing timeseries", tsIds.Count);
            var missingTSIds = new HashSet<string>();

            var pairedTsIds = tsIds.Keys.Select<string, (Identity, string)>(key => (Identity.ExternalId(key), null));
            var client = GetClient();
            try
            {
                var readResults = await client.DataPoints.GetLatestAsync(pairedTsIds, token);
                Log.Information("Found {NumRetrievedHistorizingTimeseries} historizing timeseries", readResults.Count());
                foreach (var resultItem in readResults)
                {
                    if (resultItem.NumericDataPoints.Any())
                    {
                        tsIds[resultItem.ExternalId].LatestTimestamp =
                            DateTimeOffset.FromUnixTimeMilliseconds(resultItem.NumericDataPoints.First().TimeStamp).DateTime;
                    }
                }
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out ErrorValue value) && value != null)
                        {
                            missingTSIds.Add(value.ToString());
                        }
                    }
                    Log.Information("Found {NumMissingHistorizedTimeseries} missing historizing timeseries", ex.Missing.Count());
                }
                else
                {
                    nodeEnsuringFailures.Inc();
                    Log.Error(ex, "Failed to get historizing timeseries");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to get historizing timeseries");
                nodeEnsuringFailures.Inc();
                return false;
            }
            if (missingTSIds.Any())
            {
                Log.Information("Create {NumHistorizingTimeseriesToCreate} new historizing timeseries", missingTSIds.Count);

                var getMetaData = missingTSIds.Select(id => tsIds[id]);
                await Extractor.ReadProperties(getMetaData, token);
                var createTimeseries = getMetaData.Select(VariableToTimeseries);
                try
                {
                    await client.TimeSeries.CreateAsync(createTimeseries, token);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to create historizing timeseries");
                    nodeEnsuringFailures.Inc();
                    return false;
                }

                var idsToMap = tsIds.Keys
                    .Where(key => !missingTSIds.Contains(key) && tsIds[key].Index < 1)
                    .Select<string, (Identity, string)>(key => (Identity.ExternalId(key), null));

                if (idsToMap.Any())
                {
                    Log.Information("Get remaining {NumFinalHistorizingTimeseriesData} historizing timeseries ids", idsToMap.Count());
                    try
                    {
                        var readResults = await client.DataPoints.GetLatestAsync(idsToMap, token);
                        foreach (var resultItem in readResults)
                        {
                            if (resultItem.NumericDataPoints.Any())
                            {
                                tsIds[resultItem.ExternalId].LatestTimestamp =
                                    DateTimeOffset.FromUnixTimeMilliseconds(resultItem.NumericDataPoints.First().TimeStamp).DateTime;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Failed to get historizing timeseries ids");
                        nodeEnsuringFailures.Inc();
                        return false;
                    }
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
                Description = variable.Description,
                ExternalId = externalId,
                AssetId = nodeToAssetIds[variable.ParentId],
                Name = variable.DisplayName,
                LegacyName = externalId,
                IsString = variable.DataType.isString,
                IsStep = variable.DataType.isStep
            };
            if (variable.properties != null && variable.properties.Any())
            {
                writePoco.MetaData = variable.properties
                    .Where(prop => prop.Value != null)
                    .ToDictionary(prop => prop.DisplayName, prop => prop.Value.stringValue);
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
                Description = node.Description,
                ExternalId = UAClient.GetUniqueId(node.Id),
                Name = node.DisplayName
            };
            if (node.ParentId != null && !node.ParentId.IsNullNodeId)
            {
                writePoco.ParentExternalId = UAClient.GetUniqueId(node.ParentId);
            }
            if (node.properties != null && node.properties.Any())
            {
                writePoco.MetaData = node.properties
                    .Where(prop => prop.Value != null)
                    .ToDictionary(prop => prop.DisplayName, prop => prop.Value.stringValue);
            }
            return writePoco;
        }
        #endregion
    }
}

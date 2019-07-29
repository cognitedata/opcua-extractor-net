using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Com.Cognite.V1.Timeseries.Proto;
using Fusion;
using Fusion.Api;
using Fusion.Assets;
using Fusion.Timeseries;
using Opc.Ua;
using Prometheus.Client;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Pusher against CDF
    /// </summary>
    public class CDFPusher : IPusher
    {
        private readonly IHttpClientFactory clientFactory;
        private readonly CogniteClientConfig config;
        private readonly BulkSizes bulkConfig;
        private readonly IDictionary<string, bool> nodeIsHistorizing = new Dictionary<string, bool>();
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        public Extractor Extractor { private get; set; }
        public UAClient UAClient { private get; set; }
        private NodeId _rootId;
        public NodeId RootNode { get { return _rootId; } set { nodeToAssetIds.Add(value.ToString(), rootAsset); _rootId = value; } }

        private readonly long rootAsset = -1;

        public CDFPusher(IHttpClientFactory clientFactory, FullConfig config)
        {
            this.config = config.CogniteConfig;
            this.clientFactory = clientFactory;
            bulkConfig = config.BulkSizes;
            rootAsset = config.CogniteConfig.RootAssetId;
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
        public async Task PushDataPoints(ConcurrentQueue<BufferedDataPoint> dataPointQueue)
        {
            var dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            while (dataPointQueue.TryDequeue(out BufferedDataPoint buffer) && count++ < 100000)
            {
                if (buffer.timestamp > 0L)
                {
                    dataPointList.Add(buffer);
                }
            }

            if (count == 0) return;
            if (config.Debug) return;
            var finalDataPoints = dataPointList.GroupBy(dp => dp.Id, (id, points) =>
            {
                var item = new DataPointInsertionItem
                {
                    ExternalId = id,
                    NumericDatapoints = new NumericDatapoints()
                };
                item.NumericDatapoints.Datapoints.AddRange(points.Select(point =>
                    new NumericDatapoint
                    {
                        Timestamp = point.timestamp,
                        Value = point.doubleValue
                    }
                ));
                return item;
            });
            var req = new DataPointInsertionRequest();
            req.Items.AddRange(finalDataPoints);
            Logger.LogInfo($"Push {count} datapoints to CDF");
			using (HttpClient httpClient = clientFactory.CreateClient())
            {
                httpClient.Timeout = TimeSpan.FromSeconds(30);
                Client client = Client.Create(httpClient)
                    .AddHeader("api-key", config.ApiKey)
                    .SetProject(config.Project);
                try
                {
                    await Utils.RetryAsync(async () =>
                        await client.InsertDataAsync(req), "Failed to insert into CDF");
                }
                catch (ResponseException)
                {
                    Logger.LogError($"Failed to insert {count} datapoints into CDF");
					dataPointPushFailures.Inc();
                    if (config.BufferOnFailure && !string.IsNullOrEmpty(config.BufferFile))
                    {
                        try
                        {
                            Utils.WriteBufferToFile(dataPointList, config, nodeIsHistorizing);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogError("Failed to write buffer to file");
                            Logger.LogException(ex);
                        }
                    }
                    return;
                }
                if (config.BufferOnFailure && !Utils.BufferFileEmpty && !string.IsNullOrEmpty(config.BufferFile))
                {
                    Utils.ReadBufferFromFile(dataPointQueue, config, nodeIsHistorizing);
                }
                dataPointsCounter.Inc(count);
                dataPointPushes.Inc();
            }
        }
        /// <summary>
        /// Empty queue, fetch info for each relevant node, test results against CDF, then synchronize any variables
        /// </summary>
        /// <param name="nodeQueue">Queue to be emptied</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        public async Task<bool> PushNodes(ConcurrentQueue<BufferedNode> nodeQueue)
        {
            var nodeMap = new Dictionary<string, BufferedNode>();
            var assetList = new List<BufferedNode>();
            var varList = new List<BufferedVariable>();
            var histTsList = new List<BufferedVariable>();
            var tsList = new List<BufferedVariable>();

            while (nodeQueue.TryDequeue(out BufferedNode buffer))
            {
                if (buffer.IsVariable && buffer is BufferedVariable buffVar)
                {
                    if (buffVar.IsProperty)
                    {
                        nodeMap.TryGetValue(UAClient.GetUniqueId(buffVar.ParentId), out BufferedNode parent);
                        if (parent == null) continue;
                        if (parent.properties == null)
                        {
                            parent.properties = new List<BufferedVariable>();
                        }
                        parent.properties.Add(buffVar);
                    }
                    else
                    {
                        varList.Add(buffVar);
                    }
                }
                else
                {
                    assetList.Add(buffer);
                }
                nodeMap.Add(UAClient.GetUniqueId(buffer.Id), buffer);
            }
            if (varList.Count == 0 && assetList.Count == 0) return true;
            Logger.LogInfo($"Getting data for {varList.Count} variables and {assetList.Count} objects");
            try
            {
                UAClient.ReadNodeData(assetList.Concat(varList));
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to read node data");
                Logger.LogException(e);
            }
            foreach (var node in varList)
            {
                if (Extractor.AllowTSMap(node))
                {
                    if (node.Historizing)
                    {
                        histTsList.Add(node);
                        lock (Extractor.NotInSyncLock)
                        {
                            Extractor.NotInSync.Add(UAClient.GetUniqueId(node.Id));
                        }
                    }
                    else
                    {
                        tsList.Add(node);
                    }
                }
            }
            Logger.LogInfo($"Testing {varList.Count + assetList.Count} nodes against CDF");
            if (config.Debug)
            {
                Extractor.SynchronizeNodes(tsList.Concat(histTsList));
                return true;
            }
            using (HttpClient httpClient = clientFactory.CreateClient())
            {
                Client client = Client.Create(httpClient)
                    .AddHeader("api-key", config.ApiKey)
                    .SetProject(config.Project);
                try
                {
                    foreach (var assets in Utils.ChunkBy(assetList, bulkConfig.CDFAssets))
                    {
                        if (!await EnsureAssets(assets, client)) return false;
                    }
                    trackedAssets.Inc(assetList.Count);
                    // At this point the assets should all be synchronized and mapped
                    // Now: Try get latest TS data, if this fails, then create missing and retry with the remainder. Similar to assets.
                    // This also sets the LastTimestamp property of each BufferedVariable
                    // Synchronize TS with CDF, also get timestamps. Has to be done in three steps:
                    // Get by externalId, create missing, get latest timestamps. All three can be done by externalId.
                    // Eventually the API will probably support linking TS to assets by using externalId, for now we still need the
                    // node to assets map.
                    // We only need timestamps for historizing timeseries, and it is much more expensive to get latest compared to just
                    // fetching the timeseries itself
                    foreach (var timeseries in Utils.ChunkBy(tsList, bulkConfig.CDFTimeseries))
                    {
                        if (!await EnsureTimeseries(timeseries, client)) return false;
                    }
                    trackedTimeseres.Inc(tsList.Count);

                    foreach (var timeseries in Utils.ChunkBy(histTsList, bulkConfig.CDFTimeseries))
                    {
                        if (!await EnsureHistorizingTimeseries(timeseries, client)) return false;
                    }
                    trackedTimeseres.Inc(histTsList.Count);
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to push to CDF");
                    Logger.LogException(e);
                    return false;
                }
            }
            // This can be done in this thread, as the history read stuff is done in separate threads, so there should only be a single
            // createSubscription service called here
            try
            {
                Extractor.SynchronizeNodes(tsList.Concat(histTsList));
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to synchronize nodes");
                Logger.LogException(e);
                return false;
            }
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
        /// <param name="client">Cognite client to be used</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        private async Task<bool> EnsureAssets(IEnumerable<BufferedNode> assetList, Client client)
        {
            var assetIds = assetList.ToDictionary(node => UAClient.GetUniqueId(node.Id));
            // TODO: When v1 gets support for ExternalId on assets when associating timeseries, we can drop a lot of this.
            // Specifically anything related to NodeToAssetIds
            ISet<string> missingAssetIds = new HashSet<string>();

            Logger.LogInfo($"Test {assetList.Count()} assets");

            var assetIdentities = assetIds.Keys.Select(Identity.ExternalId);
            try
            {
                var readResults = await Utils.RetryAsync(() => client.GetAssetsByIdsAsync(assetIdentities), "Failed to get assets", true);
                if (readResults != null)
                {
                    Logger.LogInfo($"Found {readResults.Count()} assets");
                    foreach (var resultItem in readResults)
                    {
                        nodeToAssetIds.TryAdd(UAClient.GetUniqueId(assetIds[resultItem.ExternalId].Id), resultItem.Id);
                    }
                }
                else
                {
                    Logger.LogError("Failed to get assets");
                    nodeEnsuringFailures.Inc();
                    return false;
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
                    Logger.LogInfo($"Found {ex.Missing.Count()} missing assets");
                }
                else
                {
                    nodeEnsuringFailures.Inc();
                    Logger.LogError("Failed to fetch asset ids");
                    Logger.LogException(ex);
                    return false;
                }
            }
            if (missingAssetIds.Any())
            {
                Logger.LogInfo($"Create {missingAssetIds.Count} new assets");

                var getMetaData = missingAssetIds.Select(id => assetIds[id]);
                try
                {
                    UAClient.GetNodeProperties(getMetaData);
                }
                catch (Exception e)
                {
                    Logger.LogException(e);
                }
                var createAssets = missingAssetIds.Select(id => NodeToAsset(assetIds[id]));
                var writeResults = await Utils.RetryAsync(() => client.CreateAssetsAsync(createAssets), "Failed to create assets");
                if (writeResults != null)
                {
                    foreach (var resultItem in writeResults)
                    {
                        nodeToAssetIds.TryAdd(UAClient.GetUniqueId(assetIds[resultItem.ExternalId].Id), resultItem.Id);
                    }
                }
                else
                {
                    Logger.LogError("Failed to create assets");
                    nodeEnsuringFailures.Inc();
                    return false;
                }
                var idsToMap = assetIds.Keys
                    .Where(id => !missingAssetIds.Contains(id))
                    .Select(Identity.ExternalId);

                if (idsToMap.Any())
                {
                    Logger.LogInfo($"Get remaining {idsToMap.Count()} assetids");
                    var readResults = await Utils.RetryAsync(() => client.GetAssetsByIdsAsync(idsToMap), "Failed to get asset ids");
                    if (readResults != null)
                    {
                        foreach (var resultItem in readResults)
                        {
                            nodeToAssetIds.TryAdd(UAClient.GetUniqueId(assetIds[resultItem.ExternalId].Id), resultItem.Id);
                        }
                    }
                    else
                    {
                        Logger.LogError("Failed to get asset ids");
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
        /// <param name="client">Cognite client to be used</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        private async Task<bool> EnsureTimeseries(IEnumerable<BufferedVariable> tsList, Client client)
        {
            if (!tsList.Any()) return true;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                string externalId = UAClient.GetUniqueId(node.Id);
                tsIds.Add(externalId, node);
                nodeIsHistorizing[externalId] = false;
            }

            Logger.LogInfo($"Test {tsIds.Keys.Count} timeseries");
            var missingTSIds = new HashSet<string>();
            try
            {
                var readResults = await Utils.RetryAsync(() =>
                    client.GetTimeseriesByIdsAsync(tsIds.Keys.Select(Identity.ExternalId)), "Failed to get timeseries", true);
                if (readResults != null)
                {
                    Logger.LogInfo($"Found {readResults.Count()} timeseries");
                }
                else
                {
                    Logger.LogError("Failed to get timeseries");
                    nodeEnsuringFailures.Inc();
                    return false;
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
                            missingTSIds.Add(value.ToString());
                        }
                    }
                    Logger.LogInfo($"Found {ex.Missing.Count()} missing timeseries");
                }
                else
                {
                    nodeEnsuringFailures.Inc();
                    Logger.LogError("Failed to fetch timeseries data");
                    Logger.LogException(ex);
                    return false;
                }
            }
            if (missingTSIds.Any())
            {
                Logger.LogInfo($"Create {missingTSIds.Count} new timeseries");

                var getMetaData = missingTSIds.Select(id => tsIds[id]);
                UAClient.GetNodeProperties(getMetaData);
                var createTimeseries = getMetaData.Select(VariableToTimeseries);
                if (await Utils.RetryAsync(() => client.CreateTimeseriesAsync(createTimeseries), "Failed to create TS") == null)
                {
                    Logger.LogError("Failed to create TS");
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
        /// <param name="client">Cognite client to be used</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        private async Task<bool> EnsureHistorizingTimeseries(IEnumerable<BufferedVariable> tsList, Client client)
        {
            if (!tsList.Any()) return true;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                string externalId = UAClient.GetUniqueId(node.Id);
                tsIds.Add(externalId, node);
                nodeIsHistorizing[externalId] = true;
            }

            Logger.LogInfo($"Test {tsIds.Keys.Count} historizing timeseries");
            var missingTSIds = new HashSet<string>();

            var pairedTsIds = tsIds.Keys.Select<string, (Identity, string)>(key => (Identity.ExternalId(key), null));
            try
            {
                var readResults = await Utils.RetryAsync(() =>
                    client.GetLatestDataPointAsync(pairedTsIds), "Failed to get historizing timeseries", true);
                if (readResults != null)
                {
                    Logger.LogInfo($"Found {readResults.Count()} historizing timeseries");
                    foreach (var resultItem in readResults)
                    {
                        if (resultItem.NumericDataPoints.Any())
                        {
                            tsIds[resultItem.ExternalId].LatestTimestamp =
                                DateTimeOffset.FromUnixTimeMilliseconds(resultItem.NumericDataPoints.First().TimeStamp).DateTime;
                        }
                    }
                }
                else
                {
                    Logger.LogError("Failed to get historizing timeseries");
                    nodeEnsuringFailures.Inc();
                    return false;
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
                    Logger.LogInfo($"Found {ex.Missing.Count()} missing historizing timeseries");
                }
                else
                {
                    nodeEnsuringFailures.Inc();
                    Logger.LogError("Failed to fetch historizing timeseries data");
                    Logger.LogException(ex);
                    return false;
                }
            }
            if (missingTSIds.Any())
            {
                Logger.LogInfo($"Create {missingTSIds.Count} new historizing timeseries");

                var getMetaData = missingTSIds.Select(id => tsIds[id]);
                UAClient.GetNodeProperties(getMetaData);
                var createTimeseries = getMetaData.Select(VariableToTimeseries);

                if (await Utils.RetryAsync(() => client.CreateTimeseriesAsync(createTimeseries), "Failed to create historizing TS") == null)
                {
                    Logger.LogError("Failed to create historizing TS");
                    nodeEnsuringFailures.Inc();
                    return false;
                }

                var idsToMap = tsIds.Keys
                    .Where(key => !missingTSIds.Contains(key))
                    .Select<string, (Identity, string)>(key => (Identity.ExternalId(key), null));

                if (idsToMap.Any())
                {
                    Logger.LogInfo($"Get remaining {idsToMap.Count()} historizing timeseries ids");
                    var readResults = await Utils.RetryAsync(() => client.GetLatestDataPointAsync(idsToMap),
                        "Failed to get historizing timeseries ids");
                    if (readResults != null)
                    {
                        foreach (var resultItem in readResults)
                        {
                            if (resultItem.NumericDataPoints.Any())
                            {
                                tsIds[resultItem.ExternalId].LatestTimestamp =
                                    DateTimeOffset.FromUnixTimeMilliseconds(resultItem.NumericDataPoints.First().TimeStamp).DateTime;
                            }
                        }
                    }
                    else
                    {
                        Logger.LogError("Failed to get historizing timeseries ids");
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
        private TimeseriesWritePoco VariableToTimeseries(BufferedVariable variable)
        {
            string externalId = UAClient.GetUniqueId(variable.Id);
            var writePoco = new TimeseriesWritePoco
            {
                Description = variable.Description,
                ExternalId = externalId,
                AssetId = nodeToAssetIds[UAClient.GetUniqueId(variable.ParentId)],
                Name = variable.DisplayName,
                LegacyName = externalId
            };
            if (variable.properties != null && variable.properties.Any())
            {
                writePoco.MetaData = variable.properties
                    .Where(prop => prop.Value != null)
                    .ToDictionary(prop => prop.DisplayName, prop => prop.Value.stringValue);
            }
            writePoco.IsStep |= variable.DataType == DataTypes.Boolean;
            return writePoco;
        }
        /// <summary>
        /// Converts BufferedNode into asset write poco.
        /// </summary>
        /// <param name="node">Node to be converted</param>
        /// <returns>Full asset write poco</returns>
        private AssetWritePoco NodeToAsset(BufferedNode node)
        {
            var writePoco = new AssetWritePoco
            {
                Description = node.Description,
                ExternalId = UAClient.GetUniqueId(node.Id),
                Name = node.DisplayName
            };
            if (node.ParentId == RootNode)
            {
                writePoco.ParentId = rootAsset;
            }
            else
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

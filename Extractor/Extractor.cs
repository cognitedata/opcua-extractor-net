using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Sdk;
using Cognite.Sdk.Api;
using Cognite.Sdk.Assets;
using Cognite.Sdk.Timeseries;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus.Client;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Main extractor class, tying together the <see cref="UAClient"/> and CDF client.
    /// </summary>
    public class Extractor
    {
        private readonly UAClient UAClient;
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        private readonly IDictionary<string, bool> nodeIsHistorizing = new Dictionary<string, bool>();
        private readonly object notInSyncLock = new object();
        private readonly ISet<NodeId> notInSync = new HashSet<NodeId>();
        private bool buffersEmpty;
        private readonly NodeId rootNode;
        private readonly long rootAsset = -1;
        private readonly CogniteClientConfig config;
        private readonly IHttpClientFactory clientFactory;
        private readonly ConcurrentQueue<BufferedDataPoint> bufferedDPQueue = new ConcurrentQueue<BufferedDataPoint>();
        private readonly ConcurrentQueue<BufferedNode> bufferedNodeQueue = new ConcurrentQueue<BufferedNode>();
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1);
        private static readonly int retryCount = 3;
        private readonly bool debug;
        private bool pushingDatapoints;
        private bool runningPush = true;
        private bool bufferFileEmpty;
        public bool Started { get; private set; }

        private static readonly Counter dataPointsCounter = Metrics
            .CreateCounter("opcua_datapoints_pushed", "Number of datapoints pushed to CDF");
        private static readonly Counter dataPointPushes = Metrics
            .CreateCounter("opcua_datapoint_pushes", "Number of times datapoints have been pushed to CDF");
        private static readonly Counter dataPointPushFailures = Metrics
            .CreateCounter("opcua_datapoint_push_failures", "Number of completely failed pushes of datapoints to CDF");
        private static readonly Gauge startTime = Metrics
            .CreateGauge("opcua_start_time", "Start time for the extractor");
        private static readonly Gauge trackedAssets = Metrics
            .CreateGauge("opcua_tracked_assets", "Number of objects on the opcua server mapped to assets in CDF");
        private static readonly Gauge trackedTimeseres = Metrics
            .CreateGauge("opcua_tracked_timeseries", "Number of variables on the opcua server mapped to timeseries in CDF");

        /// <summary>
        /// Primary constructor, creates and starts the UAClient and starts the dataPushTimer.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="clientFactory"></param>
        public Extractor(FullConfig config, IHttpClientFactory clientFactory)
        {
            this.clientFactory = clientFactory;
            UAClient = new UAClient(config, this);
            this.config = config.CogniteConfig;
            debug = config.CogniteConfig.Debug;
            runningPush = debug;

            UAClient.Run().Wait();
            if (!UAClient.Started)
            {
                return;
            }
            Started = true;
            startTime.Set(DateTime.Now.Subtract(Epoch).TotalMilliseconds);
            rootNode = UAClient.ToNodeId(config.CogniteConfig.RootNodeId, config.CogniteConfig.RootNodeNamespace);
            if (rootNode.IsNullNodeId)
            {
                rootNode = ObjectIds.ObjectsFolder;
            }
            rootAsset = config.CogniteConfig.RootAssetId;
            nodeToAssetIds.Add(rootNode, rootAsset);

            Task.Run(async () =>
            {
                while (runningPush)
                {
                    pushingDatapoints = true;
                    await PushDataPointsToCDF();
                    pushingDatapoints = false;
                    Thread.Sleep(config.CogniteConfig.DataPushDelay);
                }
            });
        }
        #region Interface
        /// <summary>
        /// Restarts the extractor, to some extent, clears known asset ids, allows data to be pushed to CDF, and begins mapping the opcua
        /// directory again
        /// </summary>
        public void RestartExtractor()
        {
            // In theory, a disconnect might be a server restart, which can cause namespaces to change.
            // This invalidates our stored mapping, so we need to redo everything, remap structure, read history,
            // synchronize history
            UAClient.WaitForOperations().Wait();
            buffersEmpty = false;
            nodeToAssetIds.Clear();
            MapUAToCDF();
        }
        /// <summary>
        /// Closes the extractor, mainly just shutting down the opcua client.
        /// </summary>
        public void Close()
        {
            WaitForFinalPush().Wait();
            if (!UAClient.Started) return;
            try
            {
                UAClient.Close();
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to cleanly shut down UAClient");
                Logger.LogException(e);
            }
            UAClient.WaitForOperations().Wait();
        }
        /// <summary>
        /// Starts the extractor, calling BrowseDirectory on the root node, then pushes all nodes to CDF once finished.
        /// </summary>
        public void MapUAToCDF()
        {
            trackedAssets.Set(0);
            trackedTimeseres.Set(0);
			Logger.LogInfo("Begin mapping directory");
            try
            {
                UAClient.BrowseDirectoryAsync(rootNode, HandleNode).Wait();
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to map directory");
                Logger.LogException(e);
                return;
            }
            Logger.LogInfo("End mapping directory");
            try
            {
                PushNodesToCDF().Wait();
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to push nodes to CDF");
                Logger.LogException(e);
            }
        }
        /// <summary>
        /// Disables pushing to CDF, then waits until the final push has been completed
        /// </summary>
        /// <returns></returns>
        public async Task WaitForFinalPush()
        {
            runningPush = false;
            while (pushingDatapoints) await Task.Delay(100);
        }
        #endregion

        #region Handlers
        /// <summary>
        /// Callback for the browse operation, creates <see cref="BufferedNode"/>s and enqueues them.
        /// </summary>
        /// <remarks>
        /// A FIFO queue ensures that parents will always be created before their children
        /// </remarks>
        /// <param name="node">Description of the node to be handled</param>
        /// <param name="parentId">Id of the parent node</param>
        private void HandleNode(ReferenceDescription node, NodeId parentId)
        {
            if (node.NodeClass == NodeClass.Object)
            {
                var bufferedNode = new BufferedNode(UAClient.ToNodeId(node.NodeId), node.DisplayName.Text, parentId);
                Logger.LogData(bufferedNode);
                bufferedNodeQueue.Enqueue(bufferedNode);
            }
            else if (node.NodeClass == NodeClass.Variable)
            {
                var bufferedNode = new BufferedVariable(UAClient.ToNodeId(node.NodeId), node.DisplayName.Text, parentId);
                if (node.TypeDefinition == VariableTypeIds.PropertyType)
                {
                    bufferedNode.IsProperty = true;
                }
                Logger.LogData(bufferedNode);
                bufferedNodeQueue.Enqueue(bufferedNode);
            }
            else
            {
                throw new Exception("Invalid node type");
            }
        }
        /// <summary>
        /// Handles notifications on subscribed items, pushes all new datapoints to the queue.
        /// </summary>
        /// <param name="item">Modified item</param>
        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (!buffersEmpty && notInSync.Contains(item.ResolvedNodeId)) return;

            foreach (var datapoint in item.DequeueValues())
            {
                var buffDp = new BufferedDataPoint(
                    (long)datapoint.SourceTimestamp.Subtract(Epoch).TotalMilliseconds,
                    UAClient.GetUniqueId(item.ResolvedNodeId),
                    UAClient.ConvertToDouble(datapoint)
                );
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Logger.LogWarning("Bad datapoint: " + buffDp.nodeId);
                    return;
                }
                Logger.LogData(buffDp);
                // Logger.LogData(new BufferedDataPoint(buffDp.ToStorableBytes().Skip(sizeof(ushort)).ToArray()));

                if (debug) return;
                bufferedDPQueue.Enqueue(buffDp);
            }
        }
        /// <summary>
        /// Callback for HistoryRead operations. Simply pushes all datapoints to the queue.
        /// </summary>
        /// <param name="data">Collection of data to be handled</param>
        /// <param name="final">True if this is the final call for this node, and the lock may be removed</param>
        /// <param name="nodeid">Id of the node in question</param>
        private void HistoryDataHandler(HistoryReadResultCollection data, bool final, NodeId nodeid)
        {
            if (final)
            {
                lock (notInSyncLock)
                {
                    notInSync.Remove(nodeid);
                    buffersEmpty |= notInSync.Count == 0;
                }
            }
            if (data == null) return;

            if (!(ExtensionObject.ToEncodeable(data[0].HistoryData) is HistoryData hdata) || hdata.DataValues == null) return;
            Logger.LogInfo("Fetch " + hdata.DataValues.Count + " datapoints for nodeid " + nodeid);
            foreach (var datapoint in hdata.DataValues)
            {
                var buffDp = new BufferedDataPoint(
                    (long)datapoint.SourceTimestamp.Subtract(Epoch).TotalMilliseconds,
                    UAClient.GetUniqueId(nodeid),
                    UAClient.ConvertToDouble(datapoint)
                );
                Logger.LogData(buffDp);
                bufferedDPQueue.Enqueue(buffDp);
            }
        }
        #endregion

        #region Push Data
        /// <summary>
        /// Write a list of datapoints to buffer file. Only writes non-historizing datapoints.
        /// </summary>
        /// <param name="dataPoints">List of points to be buffered</param>
        private async Task WriteBufferToFile(IEnumerable<BufferedDataPoint> dataPoints)
        {
            using (FileStream fs = new FileStream(config.BufferFile, FileMode.Append, FileAccess.Write))
            {
                int count = 0;
                foreach (var dp in dataPoints)
                {
                    if (nodeIsHistorizing[dp.nodeId]) continue;
                    count++;
                    bufferFileEmpty = false;
                    byte[] bytes = dp.ToStorableBytes();
                    await fs.WriteAsync(bytes, 0, bytes.Length);
                }
                if (count > 0)
                {
                    Logger.LogInfo("Write " + count + " datapoints to file");
                }

            }
        }
        /// <summary>
        /// Reads buffer from file into the datapoint queue
        /// </summary>
        private async Task ReadBufferFromFile()
        {
            using (FileStream fs = new FileStream(config.BufferFile, FileMode.OpenOrCreate, FileAccess.Read))
            {
                byte[] sizeBytes = new byte[sizeof(ushort)];
                while (true)
                {
                    int read = await fs.ReadAsync(sizeBytes, 0, sizeBytes.Length);
                    if (read < sizeBytes.Length) break;
                    ushort size = BitConverter.ToUInt16(sizeBytes, 0);
                    byte[] dataBytes = new byte[size];
                    int dRead = await fs.ReadAsync(dataBytes, 0, size);
                    if (dRead < size) break;
                    var buffDp = new BufferedDataPoint(dataBytes);
                    if (buffDp.nodeId == null || !nodeIsHistorizing.ContainsKey(buffDp.nodeId))
                    {
                        Logger.LogWarning("Bad datapoint in file");
                        continue;
                    }
                    bufferedDPQueue.Enqueue(buffDp);
                }
            }
            File.Create(config.BufferFile).Close();
            bufferFileEmpty = true;
        }
        /// <summary>
        /// Dequeues up to 100000 points from the queue, then pushes them to CDF.
        /// </summary>
        private async Task PushDataPointsToCDF()
        {
            if (debug) return;
            var dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            while (bufferedDPQueue.TryDequeue(out BufferedDataPoint buffer) && count++ < 100000)
            {
                if (buffer.timestamp > 0L)
                {
                    dataPointList.Add(buffer);
                }
            }
            if (count == 0) return;
            var organizedDatapoints = new Dictionary<NodeId, Tuple<IList<DataPointPoco>, Identity>>();
            foreach (BufferedDataPoint dataPoint in dataPointList)
            {
                if (!organizedDatapoints.TryGetValue(dataPoint.nodeId, out var dataPoints))
                {
                    dataPoints = new Tuple<IList<DataPointPoco>, Identity>
                    (
                        new List<DataPointPoco>(),
                        Identity.ExternalId(dataPoint.nodeId)
                    );
                    organizedDatapoints.Add(dataPoint.nodeId, dataPoints);
                }
                dataPoints.Item1.Add(new DataPointPoco
                {
                    TimeStamp = dataPoint.timestamp,
                    Value = Numeric.Float(dataPoint.doubleValue)
                });
            }

            var finalDataPoints = new List<DataPointsWritePoco>();
            foreach (var dataPointTuple in organizedDatapoints.Values)
            {
                finalDataPoints.Add(new DataPointsWritePoco
                {
                    Identity = dataPointTuple.Item2,
                    DataPoints = dataPointTuple.Item1
                });
            }

            
            Logger.LogInfo("Push " + count + " datapoints to CDF");

            using (HttpClient httpClient = clientFactory.CreateClient())
            {
                Client client = Client.Create(httpClient)
                    .AddHeader("api-key", config.ApiKey)
                    .SetProject(config.Project);
                if (!await RetryAsync(async () => await client.InsertDataAsync(finalDataPoints), "Failed to insert into CDF"))
                {
                    Logger.LogError("Failed to insert " + count + " datapoints into CDF");
                    dataPointPushFailures.Inc();
                    if (config.BufferOnFailure && !string.IsNullOrEmpty(config.BufferFile))
                    {
                        try
                        {
                            await WriteBufferToFile(dataPointList);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogError("Failed to write buffer to file");
                            Logger.LogException(ex);
                        }
                    }
                }
                else
                {
                    if (config.BufferOnFailure && !bufferFileEmpty && !string.IsNullOrEmpty(config.BufferFile))
                    {
                        await ReadBufferFromFile();
                    }
                    dataPointsCounter.Inc(count);
                    dataPointPushes.Inc();
                }
            }
        }
        /// <summary>
        /// Test if given list of assets exists, then create any that do not, checking for properties.
        /// </summary>
        /// <param name="assetList">List of assets to be tested</param>
        /// <param name="client">Cognite client to be used</param>
        private async Task EnsureAssets(IEnumerable<BufferedNode> assetList, Client client)
        {
            IDictionary<string, BufferedNode> assetIds = new Dictionary<string, BufferedNode>();
            foreach (BufferedNode node in assetList)
            {
                assetIds.Add(UAClient.GetUniqueId(node.Id), node);
            }
            // TODO: When v1 gets support for ExternalId on assets when associating timeseries, we can drop a lot of this.
            // Specifically anything related to NodeToAssetIds
            ISet<string> missingAssetIds = new HashSet<string>();
            IList<Identity> assetIdentities = new List<Identity>(assetIds.Keys.Count);

            Logger.LogInfo("Test " + assetList.Count() + " assets");

            foreach (var id in assetIds.Keys)
            {
                assetIdentities.Add(Identity.ExternalId(id));
            }
            try
            {
                var readResults = await RetryAsync(() => client.GetAssetsByIdsAsync(assetIdentities), "Failed to get assets", true);
                if (readResults != null)
                {
                    Logger.LogInfo("Found " + readResults.Count() + " assets");
                    foreach (var resultItem in readResults)
                    {
                        nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
                    }
                }
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400)
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out ErrorValue value))
                        {
                            missingAssetIds.Add(value.ToString());
                        }
                    }
                    Logger.LogInfo("Found " + ex.Missing.Count() + " missing assets");
                }
                else
                {
                    Logger.LogError("Failed to fetch asset ids");
                    Logger.LogException(ex);
                }
            }
            if (missingAssetIds.Any())
            {
                Logger.LogInfo("Create " + missingAssetIds.Count + " new assets");
                IList<BufferedNode> getMetaData = new List<BufferedNode>();
                foreach (string externalid in missingAssetIds)
                {
                    getMetaData.Add(assetIds[externalid]);
                }
                try
                {
                    UAClient.GetNodeProperties(getMetaData);
                }
                catch (Exception e)
                {
                    Logger.LogException(e);
                }
                var createAssets = new List<AssetWritePoco>();
                foreach (string externalId in missingAssetIds)
                {
                    createAssets.Add(assetIds[externalId].ToAsset(externalId, rootNode, rootAsset, UAClient));
                }
                var writeResults = await RetryAsync(() => client.CreateAssetsAsync(createAssets), "Failed to create assets");
                if (writeResults != null)
                {
                    foreach (var resultItem in writeResults)
                    {
                        nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
                    }
                }
                IList<Identity> idsToMap = new List<Identity>();
                foreach (string id in assetIds.Keys)
                {
                    if (!missingAssetIds.Contains(id))
                    {
                        idsToMap.Add(Identity.ExternalId(id));
                    }
                }
                if (idsToMap.Any())
                {
                    Logger.LogInfo("Get remaining " + idsToMap.Count + " assetids");
                    var readResults = await RetryAsync(() => client.GetAssetsByIdsAsync(idsToMap), "Failed to get asset ids");
                    if (readResults != null)
                    {
                        foreach (var resultItem in readResults)
                        {
                            nodeToAssetIds.TryAdd(assetIds[resultItem.ExternalId].Id, resultItem.Id);
                        }
                    }
                }
            }
        }
        /// <summary>
        /// Test if given list of timeseries exists, then create any that do not, checking for properties.
        /// </summary>
        /// <param name="tsList">List of timeseries to be tested</param>
        /// <param name="client">Cognite client to be used</param>
        private async Task EnsureTimeseries(IEnumerable<BufferedVariable> tsList, Client client)
        {
            if (!tsList.Any()) return;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                string externalId = UAClient.GetUniqueId(node.Id);
                tsIds.Add(externalId, node);
                nodeIsHistorizing.TryAdd(externalId, false);
            }

            Logger.LogInfo("Test " + tsIds.Keys.Count + " timeseries");
            var missingTSIds = new HashSet<string>();

            try
            {
                var readResults = await RetryAsync(() =>
                    client.GetTimeseriesByIdsAsync(tsIds.Keys), "Failed to get timeseries", true);
                if (readResults != null)
                {
                    Logger.LogInfo("Found " + readResults.Count() + " timeseries");
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
                    Logger.LogInfo("Found " + ex.Missing.Count() + " missing timeseries");
                }
                else
                {
                    Logger.LogError("Failed to fetch timeseries data");
                    Logger.LogException(ex);
                }
            }
            if (missingTSIds.Any())
            {
                Logger.LogInfo("Create " + missingTSIds.Count + " new timeseries");
                var createTimeseries = new List<TimeseriesWritePoco>();
                var getMetaData = new List<BufferedNode>();
                foreach (string externalid in missingTSIds)
                {
                    getMetaData.Add(tsIds[externalid]);
                }
                UAClient.GetNodeProperties(getMetaData);
                foreach (string externalId in missingTSIds)
                {
                    createTimeseries.Add(tsIds[externalId].ToTimeseries(externalId, nodeToAssetIds));
                }
                await RetryAsync(() => client.CreateTimeseriesAsync(createTimeseries), "Failed to create TS");
            }
        }
        /// <summary>
        /// Try to get latest timestamp from given list of timeseries, then create any not found and try again
        /// </summary>
        /// <param name="tsList">List of timeseries to be tested</param>
        /// <param name="client">Cognite client to be used</param>
        private async Task EnsureHistorizingTimeseries(IEnumerable<BufferedVariable> tsList, Client client)
        {
            if (!tsList.Any()) return;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                string externalId = UAClient.GetUniqueId(node.Id);
                tsIds.Add(externalId, node);
                nodeIsHistorizing.TryAdd(externalId, true);
            }

            Logger.LogInfo("Test " + tsIds.Keys.Count + " historizing timeseries");
            var missingTSIds = new HashSet<string>();
            var pairedTsIds = new List<(Identity, string)>();

            foreach (var key in tsIds.Keys)
            {
                pairedTsIds.Add((Identity.ExternalId(key), null));
            }

            try
            {
                var readResults = await RetryAsync(() =>
                    client.GetTimeseriesLatestDataAsync(pairedTsIds), "Failed to get historizing timeseries", true);
                if (readResults != null)
                {
                    Logger.LogInfo("Found " + readResults.Count() + " historizing timeseries");
                    foreach (var resultItem in readResults)
                    {
                        if (resultItem.DataPoints.Any())
                        {
                            tsIds[resultItem.ExternalId.Value].LatestTimestamp =
                                Epoch.AddMilliseconds(resultItem.DataPoints.First().TimeStamp);
                        }
                    }
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
                    Logger.LogInfo("Found " + ex.Missing.Count() + " missing historizing timeseries");
                }
                else
                {
                    Logger.LogError("Failed to fetch historizing timeseries data");
                    Logger.LogException(ex);
                }
            }
            if (missingTSIds.Any())
            {
                Logger.LogInfo("Create " + missingTSIds.Count + " new historizing timeseries");
                var createTimeseries = new List<TimeseriesWritePoco>();
                var getMetaData = new List<BufferedNode>();
                foreach (string externalid in missingTSIds)
                {
                    getMetaData.Add(tsIds[externalid]);
                }
                UAClient.GetNodeProperties(getMetaData);
                foreach (string externalId in missingTSIds)
                {
                    createTimeseries.Add(tsIds[externalId].ToTimeseries(externalId, nodeToAssetIds));
                }
                await RetryAsync(() => client.CreateTimeseriesAsync(createTimeseries), "Failed to create historizing TS");
                var idsToMap = new HashSet<(Identity, string)>();
                foreach (var key in tsIds.Keys)
                {
                    if (!missingTSIds.Contains(key))
                    {
                        idsToMap.Add((Identity.ExternalId(key), null));
                    }
                }
                if (idsToMap.Any())
                {
                    Logger.LogInfo("Get remaining " + idsToMap.Count + " historizing timeseries ids");
                    var readResults = await RetryAsync(() => client.GetTimeseriesLatestDataAsync(idsToMap),
                        "Failed to get historizing timeseries ids");
                    if (readResults != null)
                    {
                        foreach (var resultItem in readResults)
                        {
                            if (resultItem.DataPoints.Any())
                            {
                                tsIds[resultItem.ExternalId.Value].LatestTimestamp =
                                    Epoch.AddMilliseconds(resultItem.DataPoints.First().TimeStamp);
                            }
                        }
                    }
                }
            }
        }
        /// <summary>
        /// Empty queue, fetch info for each relevant node, test results against CDF, then synchronize any variables
        /// </summary>
        private async Task PushNodesToCDF()
        {
            var nodeMap = new Dictionary<NodeId, BufferedNode>();
            var assetList = new List<BufferedNode>();
            var varList = new List<BufferedVariable>();
            var histTsList = new List<BufferedVariable>();
            var tsList = new List<BufferedVariable>();

            int count = 0;
            while (bufferedNodeQueue.TryDequeue(out BufferedNode buffer))
            {
                if (buffer.IsVariable)
                {
                    var buffVar = (BufferedVariable)buffer;

                    if (buffVar.IsProperty)
                    {
                        nodeMap.TryGetValue(buffVar.ParentId, out BufferedNode parent);
                        if (parent == null) continue;
                        if (parent.properties == null)
                        {
                            parent.properties = new List<BufferedVariable>();
                        }
                        parent.properties.Add(buffVar);
                    }
                    else
                    {
                        count++;
                        varList.Add(buffVar);
                    }
                }
                else
                {
                    count++;
                    assetList.Add(buffer);
                }
                nodeMap.Add(buffer.Id, buffer);
            }
            if (count == 0) return;
            Logger.LogInfo("Getting data for " + varList.Count() + " variables and " + assetList.Count() + " objects");
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
                if (node.IsProperty) continue;
                if (node.DataType >= DataTypes.Boolean && node.DataType <= DataTypes.Double && node.ValueRank == -1)
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
            Logger.LogInfo("Testing " + count + " nodes against CDF");
            if (!debug)
            {
                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                    try
                    {
                        {
                            int per = 1000;
                            int remaining = assetList.Count;
                            IEnumerable<BufferedNode> tempAssetList = assetList;
                            while (remaining > 0)
                            {
                                await EnsureAssets(tempAssetList.Take(Math.Min(remaining, per)), client);
                                tempAssetList = tempAssetList.Skip(Math.Min(remaining, per));
                                remaining -= per;
                            }
                            trackedAssets.Inc(assetList.Count);

                        }
                        // At this point the assets should all be synchronized and mapped
                        // Now: Try get latest TS data, if this fails, then create missing and retry with the remainder. Similar to assets.
                        // This also sets the LastTimestamp property of each BufferedVariable
                        // Synchronize TS with CDF, also get timestamps. Has to be done in three steps:
                        // Get by externalId, create missing, get latest timestamps. All three can be done by externalId.
                        // Eventually the API will probably support linking TS to assets by using externalId, for now we still need the
                        // node to assets map.
                        // We only need timestamps for historizing timeseries, and it is much more expensive to get latest compared to just
                        // fetching the timeseries itself
                        {
                            int per = 1000;
                            int remaining = tsList.Count;
                            IEnumerable<BufferedVariable> tempTsList = tsList;
                            while (remaining > 0)
                            {
                                await EnsureTimeseries(tempTsList.Take(Math.Min(remaining, per)), client);
                                tempTsList = tempTsList.Skip(Math.Min(remaining, per));
                                remaining -= per;
                            }
                            trackedTimeseres.Inc(tsList.Count);
                        }
                        {
                            int per = 100;
                            int remaining = histTsList.Count;
                            IEnumerable<BufferedVariable> tempHistTsList = histTsList;
                            while (remaining > 0)
                            {
                                await EnsureHistorizingTimeseries(tempHistTsList.Take(Math.Min(remaining, per)), client);
                                tempHistTsList = tempHistTsList.Skip(Math.Min(remaining, per));
                                remaining -= per;
                            }
                            trackedTimeseres.Inc(histTsList.Count);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogError("Failed to push to CDF");
                        Logger.LogException(e);
                    }
                }
            }
            // This can be done in this thread, as the history read stuff is done in separate threads, so there should only be a single
            // createSubscription service called here
            Logger.LogInfo("Begin synchronize nodes");
            UAClient.SynchronizeNodes(tsList.Concat(histTsList), HistoryDataHandler, SubscriptionHandler);
            Logger.LogInfo("End synchronize nodes");
        }
        #endregion

        #region Util
        /// <summary>
        /// Retry the given asynchronous action a fixed number of times, logging each failure, and delaying with exponential backoff.
        /// </summary>
        /// <typeparam name="T">Expected return type</typeparam>
        /// <param name="action">Asynchronous action to be performed</param>
        /// <param name="failureMessage">Message to log on failure, in addition to attempt number</param>
        /// <param name="expectResponseException">If true, expect a <see cref="ResponseException"/> and throw it immediately if found</param>
        /// <returns></returns>
        private async Task<T> RetryAsync<T>(Func<Task<T>> action, string failureMessage, bool expectResponseException = false)
        {
            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    return await action();
                }
                catch (Exception e)
                {
                    if (e.GetType() == typeof(ResponseException))
                    {
                        var re = (ResponseException)e;
                        if (i == retryCount - 1 || expectResponseException)
                        {
                            throw re;
                        }
                    }
                    Logger.LogWarning(failureMessage + ", " + e.Message + ": attempt " + (i + 1) + "/" + retryCount);
                    Logger.LogException(e);
                }
                Thread.Sleep(500 * (2 << i));
            }
            return default(T);
        }
        #endregion
    }
}
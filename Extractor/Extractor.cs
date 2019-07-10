using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Cognite.Sdk;
using Cognite.Sdk.Api;
using Cognite.Sdk.Assets;
using Cognite.Sdk.Timeseries;
using Opc.Ua;
using Opc.Ua.Client;

namespace Cognite.OpcUa
{
    public class Extractor
    {
        readonly UAClient UAClient;
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        readonly object notInSyncLock = new object();
        readonly ISet<NodeId> notInSync = new HashSet<NodeId>();
        bool buffersEmpty;
        public bool Blocking { get; set; }
        readonly NodeId rootNode;
        readonly long rootAsset = -1;
        readonly CogniteClientConfig config;
        private readonly IHttpClientFactory clientFactory;
        private readonly ConcurrentQueue<BufferedDataPoint> bufferedDPQueue = new ConcurrentQueue<BufferedDataPoint>();
        private readonly ConcurrentQueue<BufferedNode> bufferedNodeQueue = new ConcurrentQueue<BufferedNode>();

        private readonly System.Timers.Timer dataPushTimer;
        public static readonly DateTime epoch = new DateTime(1970, 1, 1);
        private static readonly int retryCount = 4;
        private readonly bool debug;
        public Extractor(FullConfig config, IHttpClientFactory clientFactory)
        {
            this.clientFactory = clientFactory;
            UAClient = new UAClient(config, this);
            this.config = config.CogniteConfig;
            debug = config.CogniteConfig.Debug;
            UAClient.Run().Wait();
            if (!UAClient.Started)
            {
                Logger.LogError("Failed to start UAClient");
                return;
            }

            rootNode = UAClient.ToNodeId(config.CogniteConfig.RootNodeId, config.CogniteConfig.RootNodeNamespace);
            if (rootNode.IsNullNodeId)
            {
                rootNode = ObjectIds.ObjectsFolder;
            }
            rootAsset = config.CogniteConfig.RootAssetId;
            nodeToAssetIds.Add(rootNode, rootAsset);

            dataPushTimer = new System.Timers.Timer
            {
                Interval = config.CogniteConfig.DataPushDelay,
                AutoReset = true
            };
            dataPushTimer.Elapsed += PushDataPointsToCDF;
            if (!debug)
            {
                dataPushTimer.Start();
            }
        }
        #region Interface

        public void RestartExtractor()
        {
            // In theory, a disconnect might be a server restart, which can cause namespaces to change.
            // This invalidates our stored mapping, so we need to redo everything, remap structure, read history,
            // synchronize history
            Blocking = false;
            buffersEmpty = false;
            nodeToAssetIds.Clear();
            MapUAToCDF();
        }
        public void Close()
        {
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
        }
        public void MapUAToCDF()
        {
			Logger.LogInfo("Begin mapping directory");
            UAClient.BrowseDirectoryAsync(rootNode, HandleNode).Wait();
            Logger.LogInfo("End mapping directory");
            while (bufferedNodeQueue.Any())
            {
                PushNodesToCDF().Wait();
            }
        }
        #endregion

        #region Handlers

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
        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (Blocking || !buffersEmpty && notInSync.Contains(item.ResolvedNodeId)) return;

            foreach (var datapoint in item.DequeueValues())
            {
                var buffDp = new BufferedDataPoint(
                    (long)datapoint.SourceTimestamp.Subtract(epoch).TotalMilliseconds,
                    item.ResolvedNodeId,
                    UAClient.ConvertToDouble(datapoint)
                );
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Logger.LogWarning("Bad datapoint: " + buffDp.nodeId);
                    return;
                }
                Logger.LogData(buffDp);

                if (debug) return;
                bufferedDPQueue.Enqueue(buffDp);
            }
        }
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
                    (long)datapoint.SourceTimestamp.Subtract(epoch).TotalMilliseconds,
                    nodeid,
                    UAClient.ConvertToDouble(datapoint)
                );
                Logger.LogData(buffDp);
                bufferedDPQueue.Enqueue(buffDp);
            }
        }
        #endregion

        #region Push Data

        private async void PushDataPointsToCDF(object sender, ElapsedEventArgs e)
        {
            dataPushTimer.Stop();

            var dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            while (bufferedDPQueue.TryDequeue(out BufferedDataPoint buffer) && count++ < 100000)
            {
                if (buffer.timestamp > 0L)
                {
                    dataPointList.Add(buffer);
                }
            }

            var organizedDatapoints = new Dictionary<NodeId, Tuple<IList<DataPointPoco>, Identity>>();
            foreach (BufferedDataPoint dataPoint in dataPointList)
            {
                if (!organizedDatapoints.TryGetValue(dataPoint.nodeId, out var dataPoints))
                {
                    dataPoints = new Tuple<IList<DataPointPoco>, Identity>
                    (
                        new List<DataPointPoco>(),
                        Identity.ExternalId(UAClient.GetUniqueId(dataPoint.nodeId))
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

            if (count == 0)
            {
                dataPushTimer.Start();
                return;
            }
            Logger.LogVerbose("pushdata", "Push " + count + " datapoints to CDF");

            using (HttpClient httpClient = clientFactory.CreateClient())
            {
                Client client = Client.Create(httpClient)
                    .AddHeader("api-key", config.ApiKey)
                    .SetProject(config.Project);
                if (!await RetryAsync(async () => await client.InsertDataAsync(finalDataPoints), "Failed to insert into CDF"))
                {
                    Logger.LogError("Failed to insert " + count + " datapoints into CDF");
                }
            }
            dataPushTimer.Start();
        }
        private async Task EnsureAssets(List<BufferedNode> assetList, Client client)
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
        private async Task EnsureTimeseries(List<BufferedVariable> tsList, Client client)
        {
            if (!tsList.Any()) return;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                tsIds.Add(UAClient.GetUniqueId(node.Id), node);
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
        private async Task EnsureHistorizingTimeseries(List<BufferedVariable> tsList, Client client)
        {
            if (!tsList.Any()) return;
            var tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                tsIds.Add(UAClient.GetUniqueId(node.Id), node);
            }

            var tsKeys = new HashSet<string>(tsIds.Keys);
            while (tsKeys.Any())
            {
                int toTest = Math.Min(100, tsKeys.Count);
                Logger.LogInfo("Test " + toTest + " historizing timeseries");
                var missingTSIds = new HashSet<string>();
                var pairedTsIds = new List<(Identity, string)>();

                foreach (var key in tsKeys.Take(toTest))
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
                                    epoch.AddMilliseconds(resultItem.DataPoints.First().TimeStamp);
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
                    foreach (var key in tsKeys.Take(toTest))
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
                                        epoch.AddMilliseconds(resultItem.DataPoints.First().TimeStamp);
                                }
                            }
                        }
                    }
                }
                tsKeys = tsKeys.Skip(toTest).ToHashSet();
            }
        }
        private async Task PushNodesToCDF()
        {
            var nodeMap = new Dictionary<NodeId, BufferedNode>();
            var assetList = new List<BufferedNode>();
            var varList = new List<BufferedVariable>();
            var histTsList = new List<BufferedVariable>();
            var tsList = new List<BufferedVariable>();

            int count = 0;
            while (bufferedNodeQueue.TryDequeue(out BufferedNode buffer) && count < 1000)
            {
                if (buffer.IsVariable)
                {
                    var buffVar = (BufferedVariable)buffer;

                    if (buffVar.IsProperty)
                    {
                        var parent = nodeMap[buffVar.ParentId];
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
                        await EnsureAssets(assetList, client);
                        // At this point the assets should all be synchronized and mapped
                        // Now: Try get latest TS data, if this fails, then create missing and retry with the remainder. Similar to assets.
                        // This also sets the LastTimestamp property of each BufferedVariable
                        // Synchronize TS with CDF, also get timestamps. Has to be done in three steps:
                        // Get by externalId, create missing, get latest timestamps. All three can be done by externalId.
                        // Eventually the API will probably support linking TS to assets by using externalId, for now we still need the
                        // node to assets map.
                        await EnsureTimeseries(tsList, client);
                        await EnsureHistorizingTimeseries(histTsList, client);
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
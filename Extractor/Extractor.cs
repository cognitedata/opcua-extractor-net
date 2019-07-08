using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using System.Linq;
using Opc.Ua;
using Opc.Ua.Client;
using Cognite.Sdk.Api;
using Cognite.Sdk;
using Cognite.Sdk.Assets;
using Cognite.Sdk.Timeseries;
using System.Collections.Concurrent;
using System.Timers;
using System.Threading;

namespace Cognite.OpcUa
{
    class Extractor
    {
        readonly UAClient UAClient;
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();
        readonly object notInSyncLock = new object();
        readonly ISet<long> notInSync = new HashSet<long>();
        bool buffersEmpty;
        bool blocking;
        readonly NodeId rootNode;
        readonly long rootAsset = -1;
        readonly CogniteClientConfig config;
        private readonly IHttpClientFactory clientFactory;
        private readonly ConcurrentQueue<BufferedDataPoint> bufferedDPQueue = new ConcurrentQueue<BufferedDataPoint>();
        private readonly ConcurrentQueue<BufferedNode> bufferedNodeQueue = new ConcurrentQueue<BufferedNode>();

        private readonly System.Timers.Timer dataPushTimer;
        private readonly System.Timers.Timer nodePushTimer;
        public readonly DateTime epoch = new DateTime(1970, 1, 1);
        private static readonly int retryCount = 2;
        private static readonly bool debug = false;
        public Extractor(FullConfig config, IHttpClientFactory clientFactory)
        {
            this.clientFactory = clientFactory;
            UAClient = new UAClient(config.uaconfig, config.nsmaps, this);
            this.config = config.cogniteConfig;
            UAClient.Run().Wait();

            rootNode = UAClient.ToNodeId(config.cogniteConfig.RootNodeId, config.cogniteConfig.RootNodeNamespace);
            if (rootNode.IsNullNodeId)
            {
                rootNode = ObjectIds.ObjectsFolder;
            }
            rootAsset = config.cogniteConfig.RootAssetId;
            nodeToAssetIds.Add(rootNode, rootAsset);

            // UAClient.AddChangeListener(rootNode, StructureChangeHandler);

            dataPushTimer = new System.Timers.Timer
            {
                Interval = config.cogniteConfig.DataPushDelay,
                AutoReset = true
            };
            dataPushTimer.Elapsed += PushDataPointsToCDF;
            if (!debug)
            {
                dataPushTimer.Start();
            }
            nodePushTimer = new System.Timers.Timer
            {
                Interval = config.cogniteConfig.NodePushDelay,
                AutoReset = true
            };
            nodePushTimer.Elapsed += PushNodesToCDF;
            nodePushTimer.Start();
        }
        public void SetBlocking()
        {
            blocking = true;
        }
        public void RestartExtractor()
        {
            // In theory, a disconnect might be a server restart, which can cause namespaces to change.
            // This invalidates our stored mapping, so we need to redo everything, remap structure, read history,
            // synchronize history
            // UAClient.ClearSubscriptions();
            blocking = false;
            buffersEmpty = false;
            nodeToAssetIds.Clear();
            MapUAToCDF();
        }
        public void Close()
        {
            UAClient.Close();
        }
        public void AddSingleDataPoint(BufferedDataPoint dataPoint)
        {
			Logger.LogData(dataPoint);
            bufferedDPQueue.Enqueue(dataPoint);
        }
        public void MapUAToCDF()
        {
			Logger.LogInfo("Begin mapping directory");
            UAClient.BrowseDirectoryAsync(rootNode, HandleNode).Wait();
        }
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
                Thread.Sleep(500);
            }
            return default(T);
        }
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
            if (blocking || !debug && !buffersEmpty && notInSync.Contains(nodeToAssetIds[item.ResolvedNodeId])) return;

            foreach (var datapoint in item.DequeueValues())
            {
                long tsId = !debug ? nodeToAssetIds[item.ResolvedNodeId] : 0;
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
            if (final && !debug)
            {
                lock(notInSyncLock)
                {
                    notInSync.Remove(nodeToAssetIds[nodeid]);
                    buffersEmpty |= notInSync.Count == 0;
                }
            }
            if (data == null) return;

            HistoryData hdata = ExtensionObject.ToEncodeable(data[0].HistoryData) as HistoryData;
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
        private async void PushDataPointsToCDF(object sender, ElapsedEventArgs e)
        {
            dataPushTimer.Stop();

            List<BufferedDataPoint> dataPointList = new List<BufferedDataPoint>();

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
                        Identity.Id(nodeToAssetIds[dataPoint.nodeId])
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
            // Specifically anything related to NodeToTimeseriesIds
            ISet<string> missingAssetIds = new HashSet<string>();
            IList<Identity> assetIdentities = new List<Identity>(assetIds.Keys.Count);
            foreach (var id in assetIds.Keys)
            {
                assetIdentities.Add(Identity.ExternalId(id));
            }
            try
            {
                var readResults = await RetryAsync(() => client.GetAssetsByIdsAsync(assetIdentities), "Failed to get assets", true);
                if (readResults != null)
                {
                    foreach (var resultItem in readResults)
                    {
                        nodeToAssetIds.Add(assetIds[resultItem.ExternalId].Id, resultItem.Id);
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
                var createAssets = new List<AssetWritePoco>();
                foreach (string externalId in missingAssetIds)
                {
                    BufferedNode node = assetIds[externalId];
                    var writePoco = new AssetWritePoco
                    {
                        Description = node.Description,
                        ExternalId = externalId,
                        Name = node.DisplayName
                    };
                    if (node.ParentId == rootNode)
                    {
                        writePoco.ParentId = rootAsset;
                    }
                    else
                    {
                        writePoco.ParentExternalId = UAClient.GetUniqueId(node.ParentId);
                    }
                    createAssets.Add(writePoco);
                }
                var writeResults = await RetryAsync(() => client.CreateAssetsAsync(createAssets), "Failed to create assets");
                if (writeResults != null)
                {
                    foreach (var resultItem in writeResults)
                    {
                        nodeToAssetIds.Add(assetIds[resultItem.ExternalId].Id, resultItem.Id);
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
                    var readResults = await RetryAsync(() => client.GetAssetsByIdsAsync(idsToMap), "Failed to get asset ids");
                    if (readResults != null)
                    {
                        foreach (var resultItem in readResults)
                        {
                            nodeToAssetIds.Add(assetIds[resultItem.ExternalId].Id, resultItem.Id);
                        }
                    }
                }
            }
        }
        private async Task EnsureTimeseries(List<BufferedVariable> tsList, Client client)
        {
            IDictionary<string, BufferedVariable> tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                tsIds.Add(UAClient.GetUniqueId(node.Id), node);
            }

            ISet<string> missingTSIds = new HashSet<string>();
            IList<(Identity, string)> pairedTsIds = new List<(Identity, string)>();
            foreach (string id in tsIds.Keys)
            {
                pairedTsIds.Add((Identity.ExternalId(id), null));
            }

            try
            {
                var readResults = await RetryAsync(() =>
                    client.GetTimeseriesLatestDataAsync(pairedTsIds), "Failed to get timeseries", true);
                if (readResults != null)
                {
                    foreach (var resultItem in readResults)
                    {
                        nodeToAssetIds.Add(tsIds[resultItem.ExternalId.Value].Id, resultItem.Id);
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
                if (ex.Code == 400)
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out ErrorValue value))
                        {
                            missingTSIds.Add(value.ToString());
                        }
                    }
                }
                else
                {
                    Logger.LogError("Failed to fetch timeseries data");
                    Logger.LogException(ex);
                }
            }
            if (missingTSIds.Any())
            {
                var createTimeseries = new List<TimeseriesWritePoco>();
                foreach (string externalId in missingTSIds)
                {
                    BufferedVariable node = tsIds[externalId];
                    var writePoco = new TimeseriesWritePoco
                    {
                        Description = node.Description,
                        ExternalId = externalId,
                        AssetId = nodeToAssetIds[node.ParentId],
                        Name = node.DisplayName,
                        LegacyName = externalId
                    };
                    createTimeseries.Add(writePoco);
                }
                var writeResults = await RetryAsync(() => client.CreateTimeseriesAsync(createTimeseries), "Failed to create TS");
                if (writeResults != null)
                {
                    foreach (var resultItem in writeResults)
                    {
                        nodeToAssetIds.Add(tsIds[resultItem.ExternalId].Id, resultItem.Id);
                    }
                }
                IList<(Identity, string)> idsToMap = new List<(Identity, string)>();
                foreach (string id in tsIds.Keys)
                {
                    if (!missingTSIds.Contains(id))
                    {
                        idsToMap.Add((Identity.ExternalId(id), id));
                    }
                }
                if (idsToMap.Any())
                {
                    var readResults = await RetryAsync(() => client.GetTimeseriesLatestDataAsync(pairedTsIds),
                        "Failed to get timeseries ids");
                    if (readResults != null)
                    {
                        foreach (var resultItem in readResults)
                        {
                            nodeToAssetIds.Add(tsIds[resultItem.ExternalId.Value].Id, resultItem.Id);
                            if (resultItem.DataPoints.Any())
                            {
                                tsIds[resultItem.ExternalId.Value].LatestTimestamp =
                                    epoch.AddMilliseconds(resultItem.DataPoints.First().TimeStamp);
                            }
                        }
                    }
                }
            }
        }
        private async void PushNodesToCDF(object sender, ElapsedEventArgs e)
        {
            nodePushTimer.Stop();
            List<BufferedNode> assetList = new List<BufferedNode>();
            List<BufferedVariable> tsList = new List<BufferedVariable>();

            int count = 0;
            while (bufferedNodeQueue.TryDequeue(out BufferedNode buffer) && count++ < 1000)
            {
                if (buffer.IsVariable)
                {
                    tsList.Add((BufferedVariable)buffer);
                }
                else
                {
                    assetList.Add(buffer);
                }
            }
            if (count == 0)
            {
                nodePushTimer.Start();
                return;
            }
            UAClient.ReadNodeData(assetList.Concat(tsList));
            Logger.LogInfo("Testing " + count + " nodes against CDF");
            if (!debug)
            {
                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);

                    await EnsureAssets(assetList, client);
                    // At this point the assets should all be synchronized and mapped
                    // Now: Try get latest TS data, if this fails, then create missing and retry with the remainder. Similar to assets.
                    // This also sets the LastTimestamp property of each BufferedVariable
                    // Synchronize TS with CDF, also get timestamps. Has to be done in three steps:
                    // Get by externalId, create missing, get latest timestamps. All three can be done by externalId.
                    // Eventually the API will probably support linking TS to assets by using externalId, for now we still need the
                    // node to assets map.
                    await EnsureTimeseries(tsList, client);
                }
            }
            // This can be done in this thread, as the history read stuff is done in separate threads, so there should only be a single
            // createSubscription service called here
            UAClient.SynchronizeNodes(tsList, HistoryDataHandler, SubscriptionHandler);
            nodePushTimer.Start();
        }
    }
    public class BufferedNode
    {
        public readonly NodeId Id;
        public readonly string DisplayName;
        public readonly bool IsVariable;
        public readonly NodeId ParentId;
        public string Description { get; set; }
        public BufferedNode(NodeId Id, string DisplayName, NodeId ParentId) : this(Id, DisplayName, false, ParentId) {}
        protected BufferedNode(NodeId Id, string DisplayName, bool IsVariable, NodeId ParentId)
        {
            this.Id = Id;
            this.DisplayName = DisplayName;
            this.IsVariable = IsVariable;
            this.ParentId = ParentId;
        }
    }
    public class BufferedVariable : BufferedNode
    {
        public uint DataType { get; set; }
        public bool Historizing { get; set; }
        public int ValueRank { get; set; }
        public DateTime LatestTimestamp { get; set; } = new DateTime(1970, 1, 1);
        public BufferedVariable(NodeId Id, string DisplayName, NodeId ParentId) : base(Id, DisplayName, true, ParentId) {}
    }
    public class BufferedDataPoint
    {
        public readonly long timestamp;
        public readonly NodeId nodeId;
        public readonly double doubleValue;
        public readonly string stringValue;
        public readonly bool isString;
        public BufferedDataPoint(long timestamp, NodeId nodeId, double value)
        {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
            doubleValue = value;
            isString = false;
        }
        public BufferedDataPoint(long timestamp, NodeId nodeId, string value)
        {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
            stringValue = value;
            isString = true;
        }
    }
}
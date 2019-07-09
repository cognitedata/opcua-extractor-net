﻿using System;
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
        public static readonly DateTime epoch = new DateTime(1970, 1, 1);
        private static readonly int retryCount = 2;
        private readonly bool debug;
        public Extractor(FullConfig config, IHttpClientFactory clientFactory)
        {
            this.clientFactory = clientFactory;
            UAClient = new UAClient(config.Uaconfig, config.Nsmaps, this);
            this.config = config.CogniteConfig;
            debug = config.CogniteConfig.Debug;
            UAClient.Run().Wait();

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
            Logger.LogInfo("End mapping directory");
            while (bufferedNodeQueue.Any())
            {
                PushNodesToCDF().Wait();
            }
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
                Thread.Sleep(500 * (2 << i));
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
                    if (node.properties != null && node.properties.Any())
                    {
                        writePoco.MetaData = new Dictionary<string, string>();
                        foreach (var property in node.properties)
                        {
                            if (property.Value != null)
                            {
                                writePoco.MetaData.Add(property.DisplayName, property.Value.stringValue);
                            }
                        }
                    }
                    createAssets.Add(writePoco);
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
            IDictionary<string, BufferedVariable> tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                tsIds.Add(UAClient.GetUniqueId(node.Id), node);
            }

            Logger.LogInfo("Test " + tsIds.Keys.Count + " timeseries");
            ISet<string> missingTSIds = new HashSet<string>();

            try
            {
                var readResults = await RetryAsync(() =>
                    client.GetTimeseriesByIdsAsync(tsIds.Keys), "Failed to get timeseries", true);
                if (readResults != null)
                {
                    Logger.LogInfo("Found " + readResults.Count() + " timeseries");
                    foreach (var resultItem in readResults)
                    {
                        nodeToAssetIds.TryAdd(tsIds[resultItem.ExternalId].Id, resultItem.Id);
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
                IList<BufferedNode> getMetaData = new List<BufferedNode>();
                foreach (string externalid in missingTSIds)
                {
                    getMetaData.Add(tsIds[externalid]);
                }
                UAClient.GetNodeProperties(getMetaData);
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
                    if (node.properties != null && node.properties.Any())
                    {
                        writePoco.MetaData = new Dictionary<string, string>();
                        foreach (var property in node.properties)
                        {
                            if (property.Value != null)
                            {
                                writePoco.MetaData.Add(property.DisplayName, property.Value.stringValue);
                            }
                        }
                    }
                    createTimeseries.Add(writePoco);
                }
                var writeResults = await RetryAsync(() => client.CreateTimeseriesAsync(createTimeseries), "Failed to create TS");
                if (writeResults != null)
                {
                    foreach (var resultItem in writeResults)
                    {
                        nodeToAssetIds.TryAdd(tsIds[resultItem.ExternalId].Id, resultItem.Id);
                    }
                }
                ISet<string> idsToMap = new HashSet<string>();
                foreach (var key in tsIds.Keys)
                {
                    if (!missingTSIds.Contains(key) && !nodeToAssetIds.ContainsKey(tsIds[key].Id))
                    {
                        idsToMap.Add(key);
                    }
                }
                if (idsToMap.Any())
                {
                    Logger.LogInfo("Get remaining " + idsToMap.Count + " timeseries ids");
                    var readResults = await RetryAsync(() => client.GetTimeseriesByIdsAsync(idsToMap),
                        "Failed to get timeseries ids");
                    if (readResults != null)
                    {
                        foreach (var resultItem in readResults)
                        {
                            nodeToAssetIds.TryAdd(tsIds[resultItem.ExternalId].Id, resultItem.Id);
                        }
                    }
                }
            }
        }
        private async Task EnsureHistorizingTimeseries(List<BufferedVariable> tsList, Client client)
        {
            if (!tsList.Any()) return;
            IDictionary<string, BufferedVariable> tsIds = new Dictionary<string, BufferedVariable>();
            foreach (BufferedVariable node in tsList)
            {
                tsIds.Add(UAClient.GetUniqueId(node.Id), node);
            }

            ISet<string> tsKeys = new HashSet<string>(tsIds.Keys);
            while (tsKeys.Any())
            {
                int toTest = Math.Min(100, tsKeys.Count);
                Logger.LogInfo("Test " + toTest + " historizing timeseries");
                ISet<string> missingTSIds = new HashSet<string>();
                IList<(Identity, string)> pairedTsIds = new List<(Identity, string)>();

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
                            nodeToAssetIds.TryAdd(tsIds[resultItem.ExternalId.Value].Id, resultItem.Id);
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
                    IList<BufferedNode> getMetaData = new List<BufferedNode>();
                    foreach (string externalid in missingTSIds)
                    {
                        getMetaData.Add(tsIds[externalid]);
                    }
                    UAClient.GetNodeProperties(getMetaData);
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
                        if (node.properties != null && node.properties.Any())
                        {
                            writePoco.MetaData = new Dictionary<string, string>();
                            foreach (var property in node.properties)
                            {
                                if (property.Value != null)
                                {
                                    writePoco.MetaData.Add(property.DisplayName, property.Value.stringValue);
                                }
                            }
                        }
                        createTimeseries.Add(writePoco);
                    }
                    var writeResults = await RetryAsync(() => client.CreateTimeseriesAsync(createTimeseries), "Failed to create historizing TS");
                    if (writeResults != null)
                    {
                        foreach (var resultItem in writeResults)
                        {
                            nodeToAssetIds.TryAdd(tsIds[resultItem.ExternalId].Id, resultItem.Id);
                        }
                    }
                    ISet<(Identity, string)> idsToMap = new HashSet<(Identity, string)>();
                    foreach (var key in tsKeys.Take(toTest))
                    {
                        if (!missingTSIds.Contains(key) && !nodeToAssetIds.ContainsKey(tsIds[key].Id))
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
                                nodeToAssetIds.TryAdd(tsIds[resultItem.ExternalId.Value].Id, resultItem.Id);
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
            Dictionary<NodeId, BufferedNode> nodeMap = new Dictionary<NodeId, BufferedNode>();
            List<BufferedNode> assetList = new List<BufferedNode>();
            List<BufferedVariable> varList = new List<BufferedVariable>();
            List<BufferedVariable> histTsList = new List<BufferedVariable>();
            List<BufferedVariable> tsList = new List<BufferedVariable>();

            int count = 0;
            while (bufferedNodeQueue.TryDequeue(out BufferedNode buffer) && count++ < 1000)
            {
                if (buffer.IsVariable)
                {
                    var buffVar = (BufferedVariable)buffer;
                    if (buffVar.IsProperty)
                    {
                        count--;
                        var parent = nodeMap[buffVar.ParentId];
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
                nodeMap.Add(buffer.Id, buffer);
            }
            if (count == 0) return;
            Logger.LogInfo("Getting data for " + (varList.Count() + assetList.Count()) + " nodes");
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
                if (node.Historizing)
                {
                    histTsList.Add(node);
                }
                else
                {
                    tsList.Add(node);
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
                        throw e;
                    }
                }
            }
            // This can be done in this thread, as the history read stuff is done in separate threads, so there should only be a single
            // createSubscription service called here
            Logger.LogInfo("Begin synchronize nodes");
            UAClient.SynchronizeNodes(tsList.Concat(histTsList), HistoryDataHandler, SubscriptionHandler);
            Logger.LogInfo("End synchronize nodes");
        }
    }
    public class BufferedNode
    {
        public readonly NodeId Id;
        public readonly string DisplayName;
        public readonly bool IsVariable;
        public readonly NodeId ParentId;
        public string Description { get; set; }
        public IList<BufferedVariable> properties;
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
        public bool IsProperty { get; set; }
        public DateTime LatestTimestamp { get; set; } = new DateTime(1970, 1, 1);
        public BufferedDataPoint Value { get; set; }
        public BufferedVariable(NodeId Id, string DisplayName, NodeId ParentId) : base(Id, DisplayName, true, ParentId) {}
        public void SetDataPoint(DataValue value)
        {
            if (value == null || value.Value == null) return;
            if (DataType < DataTypes.SByte || DataType > DataTypes.Double || IsProperty)
            {
                Value = new BufferedDataPoint(
                    (long)value.SourceTimestamp.Subtract(Extractor.epoch).TotalMilliseconds,
                    Id,
                    UAClient.ConvertToString(value));
            }
            else
            {
                Value = new BufferedDataPoint(
                    (long)value.SourceTimestamp.Subtract(Extractor.epoch).TotalMilliseconds,
                    Id,
                    UAClient.ConvertToDouble(value));
            }
        }
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
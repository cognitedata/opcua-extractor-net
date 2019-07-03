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

namespace Cognite.OpcUa
{
    class Extractor
    {
        readonly UAClient UAClient;
        readonly IDictionary<NodeId, long> NodeToTimeseriesId = new Dictionary<NodeId, long>();
        readonly object notInSyncLock = new object();
        readonly ISet<long> notInSync = new HashSet<long>();
        bool buffersEmpty;
        bool blocking;
        readonly NodeId rootNode;
        readonly long rootAsset = -1;
        readonly CogniteClientConfig config;
        private readonly IHttpClientFactory clientFactory;
        private readonly ConcurrentQueue<BufferedDataPoint> bufferQueue = new ConcurrentQueue<BufferedDataPoint>();
        private readonly Timer pushTimer;
        private readonly DateTime epoch = new DateTime(1970, 1, 1);
        private static readonly int retryCount = 2;
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

            pushTimer = new Timer
            {
                Interval = config.cogniteConfig.PushDelay,
                AutoReset = true
            };
            pushTimer.Elapsed += PushDataPointsToCDF;
            pushTimer.Start();

            MapUAToCDF();

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
            UAClient.ClearSubscriptions();
            blocking = false;
            buffersEmpty = false;
            NodeToTimeseriesId.Clear();
            MapUAToCDF();
        }
        public void Close()
        {
            UAClient.Close();
        }
        private void MapUAToCDF()
        {
            Console.WriteLine("Begin mapping");
            UAClient.BrowseDirectory(rootNode, HandleNode, rootAsset).Wait();
        }
        private async Task<T> retryAsync<T>(Func<Task<T>> action, string failureMessage)
        {
            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    return await action();
                }
                catch (Exception e)
                {
                    Console.WriteLine(failureMessage + ", " + e.Message + ": attempt " + (i + 1) + "/" + retryCount);
                }
            }
            return default(T);
        }
        private async Task<long> HandleNode(ReferenceDescription node, long parentId)
        {
            string externalId = UAClient.GetUniqueId(node.NodeId);
            if (node.NodeClass == NodeClass.Object)
            {
                Console.WriteLine("{0}, {1}, {2}", node.BrowseName,
                    node.DisplayName, node.NodeClass);

                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                    GetAssets.Assets assets = await retryAsync(async () =>
                        await client.GetAssetsAsync(new List<GetAssets.Option>
                        {
                            GetAssets.Option.ExternalIdPrefix(externalId)
                        }),
                        "Failed to get asset"
                    );

                    if (assets != null && assets.Items.Any())
                    {
                        Console.WriteLine("Asset found: {0}", assets.Items.First().Name);
                        return assets.Items.First().Id;
                    }
                    Console.WriteLine("Asset not found: {0}", node.BrowseName);
                    AssetCreateDto asset = node.DisplayName.Text.Create()
                        .SetExternalId(externalId)
                        .SetParentId(parentId);

                    IEnumerable<AssetReadDto> result = await retryAsync(async () =>
                        await client.CreateAssetsAsync(new List<AssetCreateDto>
                        {
                            asset
                        }),
                        "Failed to create asset"
                    );

                    if (result != null && result.Any())
                    {
                        return result.First().Id;
                    }
                    return 0;
                }
            }
            if (node.NodeClass == NodeClass.Variable)
            {
                DateTime startTime = DateTime.MinValue;
                NodeId nodeId = UAClient.ToNodeId(node.NodeId);
                uint dataType = UAClient.GetDatatype(nodeId);
                if (dataType < DataTypes.SByte || dataType > DataTypes.Double) return parentId;

                Console.WriteLine("{0}, {1}, {2}, {3}", node.BrowseName,
                    node.DisplayName, node.NodeClass, UAClient.GetDescription(UAClient.ToNodeId(node.NodeId)));
                long timeSeriesId;
                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                    QueryDataLatest query = QueryDataLatest.Create();

                    IEnumerable<PointResponseDataPoints> result;
                    try
                    {
                        result = await client.GetTimeseriesLatestDataAsync(new List<QueryDataLatest>
                        {
                            QueryDataLatest.Create().ExternalId(externalId)
                        });
                    }
                    catch (ResponseException)
                    {
                        // Fails if TS is not found.
                        // TODO, differentiate between "not found" and other error
                        result = null;
                    }
                    if (result != null && result.Any())
                    {
                        Console.WriteLine("TS found: {0} ,{1}", result.First().Id, result.First().ExternalId);
                        timeSeriesId = result.First().Id;
                        if (result.First().DataPoints.Any())
                        {
                            startTime = epoch.AddMilliseconds(result.First().DataPoints.First().TimeStamp);
                        }
                    }
                    else
                    {
                        Console.WriteLine("TS not found: {0}, {1}", dataType, node.DisplayName.Text);
                        LocalizedText description = UAClient.GetDescription(nodeId);
                        TimeseriesResponse cresult = await retryAsync(async () =>
                            await client.CreateTimeseriesAsync(new List<TimeseriesCreateDto>
                            {
                                Timeseries.Create()
                                    .SetName(node.DisplayName.Text)
                                    .SetExternalId(externalId)
                                    .SetDescription(description.Text)
                                    .SetAssetId(parentId)
                            }),
                            "Failed to create TS"
                        );

                        if (cresult != null && cresult.Items.Any())
                        {
                            timeSeriesId = cresult.Items.First().Id;
                        }
                        else
                        {
                            throw new Exception("Failed to create timeseries: " + node.DisplayName);
                        }
                    }
                }
                buffersEmpty = false;
                lock (NodeToTimeseriesId)
                {
                    NodeToTimeseriesId.Add(nodeId, timeSeriesId);
                }
                lock (notInSyncLock)
                {
                    notInSync.Add(timeSeriesId);
                }
                if (startTime == DateTime.MinValue)
                {
                    startTime = new DateTime(1970, 1, 1); // TODO, maybe fix this if possible?
                }
                try
                {
                    Parallel.Invoke(() => UAClient.SynchronizeDataNode(
                        UAClient.ToNodeId(node.NodeId),
                        startTime,
                        HistoryDataHandler,
                        SubscriptionHandler
                    ));
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to synchronize node: " + e.StackTrace);
                }
                return parentId; // I'm not 100% sure if variables can have children, if they can, it is probably best to collapse
                // that structure in CDF.
            } // An else case here is impossible according to specifications, if the resultset is empty, then this is never called
            throw new Exception("Invalid node type");
        }

        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (blocking || !buffersEmpty && notInSync.Contains(NodeToTimeseriesId[item.ResolvedNodeId])) return;

            foreach (var datapoint in item.DequeueValues())
            {
                long tsId = NodeToTimeseriesId[item.ResolvedNodeId];
                Console.WriteLine("{0}: {1}, {2}, {3}", item.DisplayName, datapoint.Value,
                    datapoint.SourceTimestamp, datapoint.StatusCode);
                bufferQueue.Enqueue(new BufferedDataPoint(
                    (long)datapoint.SourceTimestamp.Subtract(epoch).TotalMilliseconds,
                    item.ResolvedNodeId,
                    (double)datapoint.Value
                ));
            }
        }
        private void HistoryDataHandler(HistoryReadResultCollection data, bool final, NodeId nodeid)
        {
            if (final)
            {
                lock(notInSyncLock)
                {
                    notInSync.Remove(NodeToTimeseriesId[nodeid]);
                    buffersEmpty |= notInSync.Count == 0;
                }
            }
            if (data == null) return;

            HistoryData hdata = ExtensionObject.ToEncodeable(data[0].HistoryData) as HistoryData;
            Console.WriteLine("Fetch {0} datapoints for nodeid {1}", hdata.DataValues.Count, nodeid);
            foreach (var datapoint in hdata.DataValues)
            {
                bufferQueue.Enqueue(new BufferedDataPoint(
                    (long)datapoint.SourceTimestamp.Subtract(epoch).TotalMilliseconds,
                    nodeid,
                    (double)datapoint.Value
                ));
            }
        }
        private async void PushDataPointsToCDF(object sender, ElapsedEventArgs e)
        {
            pushTimer.Stop();

            List<BufferedDataPoint> dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            while (bufferQueue.TryDequeue(out BufferedDataPoint buffer) && count++ < 100000)
            {
                dataPointList.Add(buffer);
            }

            IDictionary<NodeId, Tuple<IList<DataPoint>, Identity>> organizedDatapoints =
                new Dictionary<NodeId, Tuple<IList<DataPoint>, Identity>>();
            foreach (BufferedDataPoint dataPoint in dataPointList)
            {
                if (!organizedDatapoints.TryGetValue(dataPoint.nodeId, out var dataPoints))
                {
                    dataPoints = new Tuple<IList<DataPoint>, Identity>
                    (
                        new List<DataPoint>(),
                        Identity.Id(NodeToTimeseriesId[dataPoint.nodeId])
                    );
                    organizedDatapoints.Add(dataPoint.nodeId, dataPoints);
                }
                dataPoints.Item1.Add(dataPoint.isString
                    ? (DataPoint)DataPoint.String(dataPoint.timestamp, dataPoint.stringValue)
                    : DataPoint.Float(dataPoint.timestamp, dataPoint.doubleValue));
            }

            List <DataPoints> finalDataPoints = new List<DataPoints>();
            foreach (var dataPointTuple in organizedDatapoints.Values)
            {
                finalDataPoints.Add(new DataPoints
                {
                    Identity = dataPointTuple.Item2,
                    DataPoints = dataPointTuple.Item1
                });
            }

            Console.WriteLine("Push {0} datapoints to CDF", count);
            if (count == 0)
            {
                pushTimer.Start();
                return;
            }
            using (HttpClient httpClient = clientFactory.CreateClient())
            {
                Client client = Client.Create(httpClient)
                    .AddHeader("api-key", config.ApiKey)
                    .SetProject(config.Project);
                try
                {
                    await retryAsync(async () => await client.InsertDataAsync(finalDataPoints), "Failed to insert into CDF");
                }
                catch (ResponseException)
                {
                    Console.WriteLine("Failed to insert into CDF");
                    // Write to file, then later on success, read and wipe.
                }
            }
            pushTimer.Start();
        }
    }
}
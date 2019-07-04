﻿using System;
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
        private readonly IDictionary<NodeId, long> NodeToTimeseriesId = new Dictionary<NodeId, long>();
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
        public readonly DateTime epoch = new DateTime(1970, 1, 1);
        private static readonly int retryCount = 2;
        private static bool debug = true;
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
            if (!debug)
            {
                pushTimer.Start();
            }
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
            // UAClient.ClearSubscriptions();
            blocking = false;
            buffersEmpty = false;
            NodeToTimeseriesId.Clear();
            MapUAToCDF();
        }
        public void Close()
        {
            UAClient.Close();
        }
        public void AddSingleDataPoint(BufferedDataPoint dataPoint)
        {
            Console.WriteLine("{0}, {1}, {2}", dataPoint.timestamp, dataPoint.doubleValue, dataPoint.nodeId);
            bufferQueue.Enqueue(dataPoint);
        }
        private void MapUAToCDF()
        {
            Console.WriteLine("Begin mapping");
            UAClient.BrowseDirectory(rootNode, HandleNode, rootAsset).Wait();
        }
        private async Task<T> RetryAsync<T>(Func<Task<T>> action, string failureMessage)
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
                    if (e.GetType() == typeof(ResponseException))
                    {
                        var re = (ResponseException)e;
                        Console.WriteLine(re.Data0);
                    }
                    else if (e.GetType() == typeof(DecodeException))
                    {
                        var re = (DecodeException)e;
                        Console.WriteLine(re.Data0);
                    }
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
                if (debug) return parentId + 1;
                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    httpClient.Timeout = TimeSpan.FromMilliseconds(30000);
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                    var assets = await RetryAsync(async () =>
                        await client.GetAssetsAsync(new List<GetAssets.Option>
                        {
                            GetAssets.Option.ExternalIdPrefix(externalId)
                        }),
                        "Failed to get asset"
                    );
                    if (assets == null) return 0;

                    if (assets != null && assets.Items.Any())
                    {
                        Console.WriteLine("Asset found: {0}", assets.Items.First().Name);
                        return assets.Items.First().Id;
                    }
                    Console.WriteLine("Asset not found: {0}", node.BrowseName);
                    var result = await RetryAsync(async () =>
                        await client.CreateAssetsAsync(new List<Asset>
                        {
                            new Asset
                            {
                                ExternalId = externalId,
                                ParentId = parentId
                            }
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

                Console.WriteLine("{0}, {1}, {2}", node.BrowseName,
                    node.DisplayName, node.NodeClass);
                if (!debug)
                {
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
                            Console.WriteLine("TS found: {0}", result.First().Id);
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
                            TimeseriesResponse cresult = await RetryAsync(async () =>
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
                        startTime = epoch;
                    }
                }
                try
                {
                    await Task.Run(() => UAClient.SynchronizeDataNode(
                        UAClient.ToNodeId(node.NodeId),
                        startTime,
                        HistoryDataHandler,
                        SubscriptionHandler
                    )).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to synchronize node: " + e.Message);
                    Console.WriteLine(e.StackTrace);
                }
                return 0;
            }
            throw new Exception("Invalid node type");
        }

        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (blocking || !debug && !buffersEmpty && notInSync.Contains(NodeToTimeseriesId[item.ResolvedNodeId])) return;

            foreach (var datapoint in item.DequeueValues())
            {
                long tsId = !debug ? NodeToTimeseriesId[item.ResolvedNodeId] : 0;
                Console.WriteLine("{0}: {1}, {2}, {3}", item.DisplayName, datapoint.Value,
                    datapoint.SourceTimestamp, datapoint.StatusCode);
                if (debug) return;
                bufferQueue.Enqueue(new BufferedDataPoint(
                    (long)datapoint.SourceTimestamp.Subtract(epoch).TotalMilliseconds,
                    item.ResolvedNodeId,
                    UAClient.ConvertToDouble(datapoint)
                ));
            }
        }
        private void HistoryDataHandler(HistoryReadResultCollection data, bool final, NodeId nodeid)
        {
            if (final && !debug)
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
                    UAClient.ConvertToDouble(datapoint)
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
                if (buffer.timestamp > 0L)
                {
                    dataPointList.Add(buffer);
                }
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
                if (!await RetryAsync(async () => await client.InsertDataAsync(finalDataPoints), "Failed to insert into CDF"))
                {
                    Console.WriteLine("Failed to insert into CDF, points: ");
                    foreach (BufferedDataPoint datap in dataPointList)
                    {
                        Console.WriteLine("{0}, {1}, {2}, {3}", datap.timestamp, datap.nodeId, datap.doubleValue, NodeToTimeseriesId[datap.nodeId]);
                    }
                }
            }
            pushTimer.Start();
        }
    }
}
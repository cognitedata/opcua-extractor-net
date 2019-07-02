#define TEST_UA

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

namespace Cognite.OpcUa
{
    class Extractor
    {
#if TEST_UA
        long dummyTsSequence = 0;
        object tsSequenceLock = new object();
#endif
        readonly UAClient UAClient;
        readonly IDictionary<NodeId, long> NodeToTimeseriesId = new Dictionary<NodeId, long>();
        object notInSyncLock = new object();
        readonly ISet<long> notInSync = new HashSet<long>();
        bool buffersEmpty;
        bool blocking;
        readonly NodeId rootNode;
        readonly long rootAsset = -1;
        readonly CogniteClientConfig config;
        private readonly IHttpClientFactory clientFactory;
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
        public void MapUAToCDF()
        {
            Console.WriteLine("Begin mapping");
            UAClient.BrowseDirectory(rootNode, HandleNode, rootAsset).Wait();
        }
        private async Task<long> HandleNode(ReferenceDescription node, long parentId)
        {
            string externalId = UAClient.GetUniqueId(node.NodeId);
            if (node.NodeClass == NodeClass.Object)
            {
#if TEST_UA
                Console.WriteLine(new String(' ', (int)(parentId * 4 + 1)) + "{0}, {1}, {2}", node.BrowseName,
                    node.DisplayName, node.NodeClass);
                return parentId + 1;
#else
                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                    GetAssets.Assets assets = await client.GetAssetsAsync(new List<GetAssets.Option>
                    {
                        GetAssets.Option.ExternalIdPrefix(externalId)
                    });
                    if (assets.Items.Any())
                    {
                        Console.WriteLine("Asset found: {0}", assets.Items.First().Name);
                        return assets.Items.First().Id;
                    }
                    Console.WriteLine("Asset not found: {0}", node.BrowseName);
                    AssetCreateDto asset = node.DisplayName.Text.Create()
                        .SetExternalId(externalId)
                        .SetParentId(parentId);
                    var result = await client.CreateAssetsAsync(new List<AssetCreateDto>
                    {
                        asset
                    });
                    if (result.Any())
                    {
                        return result.First().Id;
                    }
                    throw new Exception("Failed to create asset: " + node.DisplayName);
                }
#endif
            }
            if (node.NodeClass == NodeClass.Variable)
            {
                DateTime startTime = DateTime.MinValue;
                NodeId nodeId = UAClient.ToNodeId(node.NodeId);
                // Get datetime from CDF using generated externalId.
                // If need be, synchronize new timeseries with CDF
#if TEST_UA
                Console.WriteLine(new String(' ', (int)(parentId * 4 + 1)) + "{0}, {1}, {2}, {3}", node.BrowseName,
                    node.DisplayName, node.NodeClass, UAClient.GetDescription(UAClient.ToNodeId(node.NodeId)));
                lock (tsSequenceLock)
                {
                    NodeToTimeseriesId.Add(nodeId, ++dummyTsSequence);
                    lock(notInSyncLock)
                    {
                        notInSync.Add(dummyTsSequence);
                    }
                }
#else
                long timeSeriesId;
                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                    QueryDataLatest query = QueryDataLatest.Create();
                    IEnumerable<PointResponseDataPoints> result = await client.GetTimeseriesLatestDataAsync(new List<QueryDataLatest>
                    {
                        QueryDataLatest.Create().ExternalId(externalId)
                    });
                    if (result.Any())
                    {
                        timeSeriesId = result.First().Id;
                        if (result.First().DataPoints.Any())
                        {
                            startTime = new DateTime(result.First().DataPoints.First().TimeStamp);
                        }
                    }
                    else
                    {
                        LocalizedText description = UAClient.GetDescription(nodeId);
                        TimeseriesResponse cresult = await client.CreateTimeseriesAsync(new List<TimeseriesCreateDto>
                        {
                            Timeseries.Create()
                                .SetName(node.DisplayName.Text)
                                .SetExternalId(externalId)
                                .SetDescription(description.Text)
                        });
                        if (cresult.Items.Any())
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
#endif
                Parallel.Invoke(() => UAClient.SynchronizeDataNode(
                    UAClient.ToNodeId(node.NodeId),
                    startTime,
                    HistoryDataHandler,
                    SubscriptionHandler
                ));
                return parentId; // I'm not 100% sure if variables can have children, if they can, it is probably best to collapse
                // that structure in CDF.
            } // An else case here is impossible according to specifications, if the resultset is empty, then this is never called
            throw new Exception("Invalid node type");
        }

        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (blocking || !buffersEmpty && notInSync.Contains(NodeToTimeseriesId[item.ResolvedNodeId])) return;
#if TEST_UA
            foreach (var j in item.DequeueValues())
            {
                Console.WriteLine("{0}: {1}, {2}, {3}", item.DisplayName, j.Value, j.SourceTimestamp, j.StatusCode);
            }            
#else
            using (HttpClient httpClient = clientFactory.CreateClient())
            {
                Client client = Client.Create(httpClient)
                    .AddHeader("api-key", config.ApiKey)
                    .SetProject(config.Project);
                long tsId = NodeToTimeseriesId[item.ResolvedNodeId];
                List<DataPoint> dataPoints = new List<DataPoint>();
                foreach (var datapoint in item.DequeueValues())
                {
                    
                    dataPoints.Add(DataPoint.Float(datapoint.SourceTimestamp.Ticks, (double)datapoint.Value));
                }
                
                client.InsertDataAsync(new List<DataPoints>
                {
                    new DataPoints()
                    {
                        Identity = Identity.Id(tsId),
                        DataPoints = dataPoints
                    }
                });
            }
#endif
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
#if TEST_UA
            if (data == null) return;
            foreach (HistoryReadResult res in data)
            {
                HistoryData hdata = ExtensionObject.ToEncodeable(res.HistoryData) as HistoryData;
                Console.WriteLine("Found {0} results", hdata.DataValues.Count);
                foreach (var item in hdata.DataValues)
                {
                    Console.WriteLine("{0}: {1}", item.SourceTimestamp, item.Value);
                }
            }
#else
            using (HttpClient httpClient = clientFactory.CreateClient())
            {
                Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                long tsId = NodeToTimeseriesId[nodeid];
                List<DataPoint> dataPoints = new List<DataPoint>();
                HistoryData hdata = ExtensionObject.ToEncodeable(data[0].HistoryData) as HistoryData;
                foreach (var datapoint in hdata.DataValues)
                {
                    dataPoints.Add(DataPoint.Float(datapoint.SourceTimestamp.Ticks, (double)datapoint.Value));
                }
                client.InsertDataAsync(new List<DataPoints>
                {
                    new DataPoints()
                    {
                        Identity = Identity.Id(tsId),
                        DataPoints = dataPoints
                    }
                });
            }
#endif
        }
    }
}
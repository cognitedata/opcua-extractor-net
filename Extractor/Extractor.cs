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
                Console.WriteLine("{0}, {1}, {2}", node.BrowseName,
                    node.DisplayName, node.NodeClass);
                // return parentId + 1;
                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                    GetAssets.Assets assets;
                    try
                    {
                        assets = await client.GetAssetsAsync(new List<GetAssets.Option>
                        {
                            GetAssets.Option.ExternalIdPrefix(externalId)
                        });
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to get asset" + e.StackTrace);
                        throw e;
                    }
                    if (assets.Items.Any())
                    {
                        Console.WriteLine("Asset found: {0}", assets.Items.First().Name);
                        return assets.Items.First().Id;
                    }
                    Console.WriteLine("Asset not found: {0}", node.BrowseName);
                    AssetCreateDto asset = node.DisplayName.Text.Create()
                        .SetExternalId(externalId)
                        .SetParentId(parentId);
                    try
                    {
                        var result = await client.CreateAssetsAsync(new List<AssetCreateDto>
                    {
                        asset
                    });
                        if (result.Any())
                        {
                            return result.First().Id;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to create asset: " + e.StackTrace);
                        throw e;
                    }
                    throw new Exception("Failed to create asset: " + node.DisplayName);
                }
            }
            if (node.NodeClass == NodeClass.Variable)
            {
                DateTime startTime = DateTime.MinValue;
                NodeId nodeId = UAClient.ToNodeId(node.NodeId);
                // Get datetime from CDF using generated externalId.
                // If need be, synchronize new timeseries with CDF
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
                        result = new List<PointResponseDataPoints>();
                    }
                    if (result.Any())
                    {
                        Console.WriteLine("TS found: {0} ,{1}", result.First().Id, result.First().ExternalId);
                        timeSeriesId = result.First().Id;
                        if (result.First().DataPoints.Any())
                        {
                            startTime = new DateTime(result.First().DataPoints.First().TimeStamp);
                        }
                    }
                    else
                    {
                        uint dataType = UAClient.GetDatatype(nodeId);
                        Console.WriteLine("TS not found: {0}, {1}", dataType, node.DisplayName.Text);

                        if (dataType < DataTypes.SByte || dataType > DataTypes.Double) return parentId;
                        LocalizedText description = UAClient.GetDescription(nodeId);
                        TimeseriesResponse cresult = await client.CreateTimeseriesAsync(new List<TimeseriesCreateDto>
                        {
                            Timeseries.Create()
                                .SetName(node.DisplayName.Text)
                                .SetExternalId(externalId)
                                .SetDescription(description.Text)
                                .SetAssetId(parentId)
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
                try
                {
                    await UAClient.SynchronizeDataNode(
                        UAClient.ToNodeId(node.NodeId),
                        startTime,
                        HistoryDataHandler,
                        SubscriptionHandler
                    ).ConfigureAwait(false);
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
            Task.Run(async () =>
            {
                using (HttpClient httpClient = clientFactory.CreateClient())
                {
                    Client client = Client.Create(httpClient)
                        .AddHeader("api-key", config.ApiKey)
                        .SetProject(config.Project);
                    long tsId = NodeToTimeseriesId[item.ResolvedNodeId];
                    List<DataPoint> dataPoints = new List<DataPoint>();
                    foreach (var datapoint in item.DequeueValues())
                    {
                        Console.WriteLine("{0}: {1}, {2}, {3}: {4}", item.DisplayName, datapoint.Value,
                            datapoint.SourceTimestamp, datapoint.StatusCode, tsId);
                        dataPoints.Add(DataPoint.Float(
                            (long)datapoint.SourceTimestamp.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds,
                            (double)datapoint.Value));
                    }

                    await client.InsertDataAsync(new List<DataPoints>
                    {
                        new DataPoints()
                        {
                            Identity = Identity.Id(tsId),
                            DataPoints = dataPoints
                        }
                    });
                }
            });
        }
        private async Task HistoryDataHandler(HistoryReadResultCollection data, bool final, NodeId nodeid)
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
                    Console.WriteLine("{0}: {1}", datapoint.SourceTimestamp, datapoint.Value);
                    dataPoints.Add(DataPoint.Float(
                        (long)datapoint.SourceTimestamp.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds,
                        (double)datapoint.Value));
                }
                Console.WriteLine("Begin insert data {0}", tsId);
                await client.InsertDataAsync(new List<DataPoints>
                {
                    new DataPoints
                    {
                        Identity = Identity.Id(tsId),
                        DataPoints = dataPoints
                    }
                });
            }
        }
    }
}
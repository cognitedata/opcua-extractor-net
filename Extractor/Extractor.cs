using System;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
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
        public UAClient UAClient { get; set; } = null;
        IDictionary<NodeId, long> NodeToTimeseriesId;
        ISet<long> notInSync = new HashSet<long>();
        bool buffersEmpty;
        NodeId rootNode;
        long rootAsset = -1;
        CogniteClientConfig config;
        private readonly IHttpClientFactory clientFactory;
        public Extractor(FullConfig config, IHttpClientFactory clientFactory)
        {
            this.clientFactory = clientFactory;
            UAClient = new UAClient(config.uaconfig, config.nsmaps, this);
            this.config = config.cogniteConfig;
            UAClient.Run().Wait();

            rootNode = UAClient.ToNodeId(config.cogniteConfig.RootNodeId, config.cogniteConfig.RootNodeNamespace);
            rootAsset = config.cogniteConfig.RootAssetId;
            // Asynchronously starts the session
            UAClient.DebugBrowseDirectory(rootNode);

        }
        public void RestartExtractor()
        {
            UAClient.ClearSubscriptions();
            NodeToTimeseriesId.Clear();
            MapUAToCDF();
        }
        public void MapUAToCDF()
        {
            UAClient.BrowseDirectory(rootNode, HandleNode, rootAsset).Wait();
        }
        private async Task<long> HandleNode(ReferenceDescription node, long parentId)
        {
            string externalId = UAClient.GetUniqueId(node.NodeId);
            if (node.NodeClass == NodeClass.Object)
            {
                // Get object from CDF, then return id.
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
                    var asset = Asset.Create(node.DisplayName.Text)
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
            }
            if (node.NodeClass == NodeClass.Variable)
            {
                DateTime startTime = DateTime.MinValue;
                NodeId nodeId = UAClient.ToNodeId(node.NodeId);
                // Get datetime from CDF using generated externalId.
                // If need be, synchronize new timeseries with CDF
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
                lock (notInSync)
                {
                    notInSync.Add(timeSeriesId);
                }
                if (startTime == DateTime.MinValue)
                {
                    startTime = new DateTime(1970, 1, 1); // TODO, maybe fix this if possible?
                }
                
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
            if (!buffersEmpty && notInSync.Contains(NodeToTimeseriesId[item.ResolvedNodeId])) return;
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
        }
        private void HistoryDataHandler(HistoryReadResultCollection data, bool final, NodeId nodeid)
        {
            if (final)
            {
                lock(notInSync)
                {
                    notInSync.Remove(NodeToTimeseriesId[nodeid]);
                }
            }
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
        }
    }
}
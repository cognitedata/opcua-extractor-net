using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;

namespace opcua_extractor_net
{
    class Extractor
    {
        public UAClient Client { get; set; } = null;
        IDictionary<NodeId, int> NodeToTimeseriesId;
        ISet<int> notInSync = new HashSet<int>();
        bool buffersEmpty;
        NodeId rootNode;
        int rootAsset = -1;
        public Extractor(FullConfig config)
        {
            // this.Client = new UAClient(fullConfig.uaconfig, fullConfig.nsmaps, this);
            // Asynchronously starts the session
            // Client.Run().Wait();

        }
        public void RestartExtractor()
        {
            Client.ClearSubscriptions();
            NodeToTimeseriesId.Clear();
            if (rootAsset < 0 || rootNode == null)
            {
                throw new Exception("May not restart unconfigured Extractor");
            }
            MapUAToCDF(rootNode, rootAsset);
        }
        public void MapUAToCDF(NodeId rootNode, int rootAsset)
        {
            this.rootAsset = rootAsset;
            this.rootNode = rootNode;
            Client.BrowseDirectory(rootNode, HandleNode, rootAsset);
        }
        private int HandleNode(ReferenceDescription node, int parentId)
        {
            string externalId = Client.GetUniqueId(node.NodeId);
            if (node.NodeClass == NodeClass.Object)
            {
                int assetId = 123;
                // Get object from CDF, then return id.
                return assetId;
            }
            if (node.NodeClass == NodeClass.Variable)
            {
                DateTime startTime = DateTime.MinValue;
                // Get datetime from CDF using generated externalId.
                // If need be, synchronize new timeseries with CDF
                int timeSeriesId = 0;
                buffersEmpty = false;
                lock (notInSync)
                {
                    notInSync.Add(timeSeriesId);
                }
                if (startTime == DateTime.MinValue)
                {
                    startTime = new DateTime(1970, 1, 1);
                }
                
                Parallel.Invoke(() => Client.SynchronizeDataNode(
                    Client.ToNodeId(node.NodeId),
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
            // Post to CDF
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
            // Post to CDF
        }
    }
}
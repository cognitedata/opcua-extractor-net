using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;

namespace opcua_extractor_net
{
    class Extractor
    {
        UAClient Client;
        IDictionary<NodeId, int> NodeToTimeseriesId;
        public Extractor(UAClient Client)
        {
            this.Client = Client;
            // Asynchronously starts the session
            Client.Run().Wait();
        }
        public void RestartExtractor()
        {
            Client.ClearSubscriptions();
            NodeToTimeseriesId.Clear();
        }
        public void MapUAToCDF(NodeId rootNode, int rootAsset)
        {
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
                if (startTime == DateTime.MinValue)
                {
                    startTime = new DateTime(1970, 1, 1);
                }
                // I think this is the only thing we really need to consider using paralellism for.
                // Presumably, both SubscriptionHandler and HistoryDataHandler can be made to only read from member variables
                Parallel.Invoke(() => Client.SynchronizeDataNode(
                    Client.ToNodeId(node.NodeId),
                    startTime,
                    HistoryDataHandler,
                    SubscriptionHandler
                ));
                return parentId; // I'm not 100% sure if variables can have children, if they can, it is probably best to collapse
                // that structure in CDF.
            } // An else case here is impossible according to specifications, if the resultset is empty, then this is never called
            return 0;
        }

        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            // Post to CDF
        }
        private void HistoryDataHandler(HistoryReadResultCollection data)
        {
            // Post to CDF
        }
    }
}

using System.Collections.Generic;
using Opc.Ua;
using Opc.Ua.Server;

namespace Server
{
    public interface IServerRequestCallbacks
    {
        public void OnBrowse(OperationContext context, BrowseDescriptionCollection nodesToBrowse) { }

        public void OnBrowseNext(OperationContext context, ByteStringCollection continuationPoints) { }

        public void OnHistoryRead(OperationContext context, ExtensionObject historyReadDetails, HistoryReadValueIdCollection nodesToRead) { }

        public void OnRead(OperationContext context, ReadValueIdCollection nodesToRead) { }

        public void OnCreateMonitoredItems(OperationContext context, uint subscriptionId, IList<MonitoredItemCreateRequest> itemsToCreate) { }

        public void OnCreateSubscription(OperationContext context, double requestedPublishingInterval, uint requestedLifetimeCount, uint requestedMaxKeepAliveCount, uint maxNotificationsPerPublish, bool publishingEnabled, byte priority) { }
    }

    public class EmptyServerCallbacks : IServerRequestCallbacks { }

    public class AggregateCallbacks : IServerRequestCallbacks
    {
        private IEnumerable<IServerRequestCallbacks> children;

        public AggregateCallbacks(params IServerRequestCallbacks[] children)
        {
            this.children = children;
        }

        public void OnBrowse(OperationContext context, BrowseDescriptionCollection nodesToBrowse)
        {
            foreach (var child in children) child.OnBrowse(context, nodesToBrowse);
        }

        public void OnBrowseNext(OperationContext context, ByteStringCollection continuationPoints)
        {
            foreach (var child in children) child.OnBrowseNext(context, continuationPoints);
        }

        public void OnHistoryRead(OperationContext context, ExtensionObject historyReadDetails, HistoryReadValueIdCollection nodesToRead)
        {
            foreach (var child in children) child.OnHistoryRead(context, historyReadDetails, nodesToRead);
        }

        public void OnRead(OperationContext context, ReadValueIdCollection nodesToRead)
        {
            foreach (var child in children) child.OnRead(context, nodesToRead);
        }

        public void OnCreateMonitoredItems(OperationContext context, uint subscriptionId, IList<MonitoredItemCreateRequest> itemsToCreate)
        {
            foreach (var child in children) child.OnCreateMonitoredItems(context, subscriptionId, itemsToCreate);
        }

        public void OnCreateSubscription(OperationContext context, double requestedPublishingInterval, uint requestedLifetimeCount, uint requestedMaxKeepAliveCount, uint maxNotificationsPerPublish, bool publishingEnabled, byte priority)
        {
            foreach (var child in children) child.OnCreateSubscription(context, requestedPublishingInterval, requestedLifetimeCount, requestedMaxKeepAliveCount, maxNotificationsPerPublish, publishingEnabled, priority);
        }
    }
}
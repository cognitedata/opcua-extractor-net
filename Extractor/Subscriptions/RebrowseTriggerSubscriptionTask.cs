using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Subscriptions
{
    internal class RebrowseTriggerSubscriptionTask : BaseCreateSubscriptionTask<(NodeId, string)>
    {
        private MonitoredItemNotificationEventHandler handler;

        public RebrowseTriggerSubscriptionTask(
            MonitoredItemNotificationEventHandler handler,
            Dictionary<NodeId, (NodeId, string)> ids,
            IClientCallbacks callbacks) : base(SubscriptionName.RebrowseTriggers, ids, callbacks)
        {
            this.handler = handler;
        }

        public override string TaskName => "Create rebrowse trigger subscription";

        public override Task<bool> ShouldRun(ILogger logger, SessionManager _, CancellationToken _1)
        {
            return Task.FromResult(true);
        }

        protected override MonitoredItem CreateMonitoredItem((NodeId, string) item, FullConfig config)
        {
            var newItem = new MonitoredItem
            {
                StartNodeId = item.Item1,
                SamplingInterval = 1000,
                DisplayName = "Publication " + item.Item2,
                QueueSize = 1,
                DiscardOldest = true,
                AttributeId = Attributes.Value,
                NodeClass = NodeClass.Variable,
                CacheQueueSize = 1
            };
            newItem.Notification += handler;
            return newItem;
        }
    }
}

using Cognite.OpcUa.Config;
using Opc.Ua.Client;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Subscriptions
{
    internal class NodeMetricsSubscriptionTask : BaseCreateSubscriptionTask<NodeMetricState>
    {
        private readonly MonitoredItemNotificationEventHandler handler;
        public NodeMetricsSubscriptionTask(MonitoredItemNotificationEventHandler handler, Dictionary<NodeId, NodeMetricState> states, IClientCallbacks callbacks)
            : base(SubscriptionName.NodeMetrics, states, callbacks)
        {
            this.handler = handler;
        }


        public override string TaskName => "Create node metrics subscription";

        public override Task<bool> ShouldRun(ILogger logger, SessionManager sessionManager, CancellationToken token)
        {
            return Task.FromResult(true);
        }

        protected override MonitoredItem CreateMonitoredItem(NodeMetricState item, FullConfig config)
        {
            var newItem = new MonitoredItem
            {
                StartNodeId = item.SourceId,
                SamplingInterval = config.Subscriptions.SamplingInterval,
                DisplayName = "Value " + item.Id,
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

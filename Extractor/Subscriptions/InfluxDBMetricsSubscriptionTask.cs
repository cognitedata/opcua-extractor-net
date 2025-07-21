using Cognite.OpcUa.Config;
using Opc.Ua.Client;
using Opc.Ua;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Extensions.Logging;
using Cognite.OpcUa.Connect;

namespace Cognite.OpcUa.Subscriptions
{
    internal class InfluxDBMetricsSubscriptionTask : BaseCreateSubscriptionTask<InfluxDBMetricState>
    {
        private readonly MonitoredItemNotificationEventHandler handler;
        public InfluxDBMetricsSubscriptionTask(MonitoredItemNotificationEventHandler handler, Dictionary<NodeId, InfluxDBMetricState> states, IClientCallbacks callbacks)
            : base(SubscriptionName.InfluxDBMetrics, states, callbacks)
        {
            this.handler = handler;
        }

        public override string TaskName => "Create InfluxDB metrics subscription";

        public override Task<bool> ShouldRun(ILogger logger, SessionManager sessionManager, CancellationToken token)
        {
            return Task.FromResult(true);
        }

        protected override MonitoredItem CreateMonitoredItem(InfluxDBMetricState item, FullConfig config)
        {
            var newItem = new MonitoredItem
            {
                StartNodeId = item.SourceId,
                SamplingInterval = config.Subscriptions.SamplingInterval,
                DisplayName = "InfluxDB Value " + item.Id,
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
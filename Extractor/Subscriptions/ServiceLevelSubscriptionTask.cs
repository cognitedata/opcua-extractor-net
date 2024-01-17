using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Subscriptions
{
    internal class ServiceLevelSubscriptionTask : BaseCreateSubscriptionTask<NodeId>
    {
        private MonitoredItemNotificationEventHandler handler;
        public ServiceLevelSubscriptionTask(MonitoredItemNotificationEventHandler handler, IClientCallbacks callbacks)
            : base(SubscriptionName.ServiceLevel, new Dictionary<NodeId, NodeId> { { VariableIds.Server_ServiceLevel, VariableIds.Server_ServiceLevel } }, callbacks)
        {
            this.handler = handler;
        }


        public override string TaskName => "Create service level subscription";

        public override Task<bool> ShouldRun(ILogger logger, SessionManager sessionManager, CancellationToken token)
        {
            return Task.FromResult(true);
        }

        protected override MonitoredItem CreateMonitoredItem(NodeId item, FullConfig config)
        {
            var newItem = new MonitoredItem
            {
                StartNodeId = item,
                SamplingInterval = 1000,
                DisplayName = "Value " + item.ToString(),
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

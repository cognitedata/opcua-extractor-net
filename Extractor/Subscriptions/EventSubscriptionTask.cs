using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Subscriptions
{
    public class EventSubscriptionTask : BaseCreateSubscriptionTask<EventExtractionState>
    {
        private readonly EventFilter filter;
        private readonly MonitoredItemNotificationEventHandler handler;
        public EventSubscriptionTask(
            MonitoredItemNotificationEventHandler handler,
            IEnumerable<EventExtractionState> states,
            EventFilter filter,
            IClientCallbacks callbacks)
            : base(SubscriptionName.Events, states.ToDictionary(s => s.SourceId), callbacks)
        {
            this.filter = filter;
            this.handler = handler;
        }

        public override string TaskName => "Create event subscriptions";

        public override Task<bool> ShouldRun(ILogger logger, SessionManager sessionManager, CancellationToken token)
        {
            return Task.FromResult(true);
        }

        protected override MonitoredItem CreateMonitoredItem(EventExtractionState item, FullConfig config)
        {
            var subConfig = config.Subscriptions.GetMatchingConfig(item);
            var newItem = new MonitoredItem
            {
                StartNodeId = item.SourceId,
                AttributeId = Attributes.EventNotifier,
                DisplayName = "Events: " + item.Id,
                SamplingInterval = subConfig.SamplingInterval,
                QueueSize = (uint)Math.Max(0, subConfig.QueueLength),
                NodeClass = NodeClass.Object,
                CacheQueueSize = Math.Max(0, subConfig.QueueLength),
                Filter = filter
            };
            newItem.Notification += handler;
            return newItem;
        }
    }
}

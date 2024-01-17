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
    /// <summary>
    /// Task for creating subscriptions to monitored items managed by UAHistoryExtractionState
    /// </summary>
    public class DataPointSubscriptionTask : BaseCreateSubscriptionTask<VariableExtractionState>
    {
        private readonly MonitoredItemNotificationEventHandler handler;
        public DataPointSubscriptionTask(MonitoredItemNotificationEventHandler handler, IEnumerable<VariableExtractionState> states, IClientCallbacks callbacks)
            : base(SubscriptionName.DataPoints, states.ToDictionary(s => s.SourceId), callbacks)
        {
            this.handler = handler;
        }

        public override string TaskName => "Create data point subscriptions";

        public override Task<bool> ShouldRun(ILogger logger, SessionManager sessionManager, CancellationToken token)
        {
            return Task.FromResult(true);
        }

        protected override MonitoredItem CreateMonitoredItem(VariableExtractionState item, FullConfig config)
        {
            var subConfig = config.Subscriptions.GetMatchingConfig(item);
            var newItem = new MonitoredItem
            {
                StartNodeId = item.SourceId,
                DisplayName = "Value: " + item.DisplayName,
                SamplingInterval = subConfig.SamplingInterval,
                QueueSize = (uint)Math.Max(0, subConfig.QueueLength),
                AttributeId = Attributes.Value,
                NodeClass = NodeClass.Variable,
                CacheQueueSize = Math.Max(0, subConfig.QueueLength),
                Filter = subConfig.DataChangeFilter?.Filter
            };
            newItem.Notification += handler;
            return newItem;
        }
    }
}

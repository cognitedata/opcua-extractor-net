using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Subscriptions
{
    public class AuditSubscriptionTask : BaseCreateSubscriptionTask<string>
    {
        private readonly MonitoredItemNotificationEventHandler handler;
        public AuditSubscriptionTask(MonitoredItemNotificationEventHandler handler, IClientCallbacks callbacks)
            : base(SubscriptionName.Audit, new Dictionary<NodeId, string>
            {
                { ObjectIds.Server, "Audit: Server" }
            }, callbacks)
        {
            this.handler = handler;
        }

        protected override MonitoredItem CreateMonitoredItem(string item, FullConfig config)
        {
            var newItem = new MonitoredItem
            {
                StartNodeId = ObjectIds.Server,
                Filter = auditFilter,
                AttributeId = Attributes.EventNotifier,
                SamplingInterval = config.Subscriptions.SamplingInterval,
                QueueSize = (uint)Math.Max(0, config.Subscriptions.QueueLength),
                NodeClass = NodeClass.Object,
                DisplayName = item
            };
            newItem.Notification += handler;
            return newItem;
        }

        public override Task<bool> ShouldRun(ILogger logger, SessionManager sessionManager, CancellationToken token)
        {
            return Task.FromResult(true);
        }

        private static readonly EventFilter auditFilter = BuildAuditFilter();

        public override string TaskName => "Create audit event subscription";

        private static EventFilter BuildAuditFilter()
        {
            var whereClause = new ContentFilter();
            var eventTypeOperand = new SimpleAttributeOperand
            {
                TypeDefinitionId = ObjectTypeIds.BaseEventType,
                AttributeId = Attributes.Value
            };
            eventTypeOperand.BrowsePath.Add(BrowseNames.EventType);
            var op1 = new LiteralOperand
            {
                Value = ObjectTypeIds.AuditAddNodesEventType
            };
            var op2 = new LiteralOperand
            {
                Value = ObjectTypeIds.AuditAddReferencesEventType
            };
            var elem1 = whereClause.Push(FilterOperator.Equals, eventTypeOperand, op1);
            var elem2 = whereClause.Push(FilterOperator.Equals, eventTypeOperand, op2);
            whereClause.Push(FilterOperator.Or, elem1, elem2);
            var selectClauses = new SimpleAttributeOperandCollection();
            foreach (string path in new[]
            {
                BrowseNames.EventType,
                BrowseNames.NodesToAdd,
                BrowseNames.ReferencesToAdd,
                BrowseNames.EventId
            })
            {
                var op = new SimpleAttributeOperand
                {
                    AttributeId = Attributes.Value,
                    TypeDefinitionId = ObjectTypeIds.BaseEventType
                };
                op.BrowsePath.Add(path);
                selectClauses.Add(op);
            }
            return new EventFilter
            {
                WhereClause = whereClause,
                SelectClauses = selectClauses
            };
        }
    }
}

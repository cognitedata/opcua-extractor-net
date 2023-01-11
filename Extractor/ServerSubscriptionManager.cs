using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Cognite.OpcUa.History;
using Microsoft.Extensions.Logging;

using Opc.Ua;
using Opc.Ua.Client;

namespace Cognite.OpcUa
{
    public class ServerSubscriptionManager
    {
        private readonly ILogger<ServerSubscriptionManager> _logger;
        private readonly UAClient _uaClient;

        private readonly List<NodeId> nodeIds = new List<NodeId> {
            Variables.OPCUANamespaceMetadata_NamespacePublicationDate,
            VariableIds.NamespaceMetadataType_NamespacePublicationDate,
            "ns=1;s=Input4",
            VariableIds.NamespacesType_NamespaceIdentifier_Placeholder_NamespacePublicationDate,
        };

        private readonly uint[] attributes = new[]
        {
            Attributes.NodeId,
            Attributes.DisplayName,
            Attributes.DataType,
            Attributes.NodeClass,
            Attributes.Description
        };

        public ServerSubscriptionManager(ILogger<ServerSubscriptionManager> logger, UAClient uaClient)
        {
            _logger = logger;
            _uaClient = uaClient;
        }

        public async Task CreateCustomServerSubscriptions(CancellationToken token)
        {
            var readValueIds = new ReadValueIdCollection(
                nodeIds.SelectMany(
                    node => attributes.Select(att => new ReadValueId { AttributeId = att, NodeId = node })
                )
            );
            var results = await _uaClient.ReadAttributes(readValueIds, nodeIds.Count, token, "server subscriptions");
            IDictionary<NodeId, UAHistoryExtractionState> nodes = new Dictionary<NodeId, UAHistoryExtractionState>();

            for (var id = 0; id < nodeIds.Count; id++)
            {
                var nc = (NodeClass)results[id * attributes.Count() + 3].Value;
                if (nc != NodeClass.Variable) continue;
                var rawDt = results[id * attributes.Count() + 2].GetValue(NodeId.Null);

                nodes[nodeIds[id]] = new ServerItemSubscriptionState(_uaClient, nodeIds[id]);
            }

            await _uaClient.AddSubscriptions(nodes.Values, "NodeMetrics", SubscriptionHandler,
                state => new MonitoredItem
                {
                    StartNodeId = state.SourceId,
                    SamplingInterval = 100,
                    DisplayName = "Value " + state.Id,
                    QueueSize = 1,
                    DiscardOldest = true,
                    AttributeId = Attributes.Value,
                    NodeClass = NodeClass.Variable,
                    CacheQueueSize = 1
                }, token, "metric");
        }

        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs _)
        {
            _logger.LogInformation("Something happened");
        }
    }

    public class ServerItemSubscriptionState : UAHistoryExtractionState
    {
        public ServerItemSubscriptionState(IUAClientAccess client, NodeId id) : base(client, id, false, false)
        {
        }
    }
}
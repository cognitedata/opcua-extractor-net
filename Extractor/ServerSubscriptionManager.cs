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
        private readonly ServerNamespacesToRebrowseConfig? _config;

        private readonly uint[] attributes = new[]
        {
            Attributes.NodeId,
            Attributes.DisplayName,
            Attributes.DataType,
            Attributes.NodeClass,
            Attributes.Description
        };

        public ServerSubscriptionManager(
            ILogger<ServerSubscriptionManager> logger,
            UAClient uaClient,
            ServerNamespacesToRebrowseConfig? config
        )
        {
            _logger = logger;
            _uaClient = uaClient;
            _config = config;
        }

        public async Task CreateCustomServerSubscriptions(UAExtractor extractor, CancellationToken token)
        {
            var serverNode = await _uaClient.GetServerNode(token);

            List<NodeId> nodeIds = new List<NodeId>();
            var targets = _config?.NamespaceNames.Select(name => name.ToNodeId(_uaClient));

            await _uaClient.Browser.BrowseDirectory(
                new[] { serverNode.Id },
                (refDef, id) =>
                {
                    var nodeId = (NodeId) refDef.NodeId;
                    if (targets.Contains(nodeId))
                    {
                        nodeIds.Add(nodeId);
                        _logger.LogInformation(
                            "Subscription to a rebrowse on node {node} with {id} is now set",
                            refDef.DisplayName.ToString(), nodeId
                        );
                    }
                },
                token,
                maxDepth: -1,
                doFilter: false
            );

            var readValueIds = new ReadValueIdCollection(
                nodeIds.SelectMany(
                    node => attributes.Select(att => new ReadValueId { AttributeId = att, NodeId = node })
                )
            );
            var results = await _uaClient.ReadAttributes(readValueIds, nodeIds.Count, token, "server subscriptions");
            var nodes = new Dictionary<NodeId, UAHistoryExtractionState>();

            for (var id = 0; id < nodeIds.Count; id++)
            {
                var nc = (NodeClass)results[id * attributes.Count() + 3].Value;
                if (nc != NodeClass.Variable) continue;
                var rawDt = results[id * attributes.Count() + 2].GetValue(NodeId.Null);

                nodes[nodeIds[id]] = new ServerItemSubscriptionState(_uaClient, nodeIds[id]);
            }

            await _uaClient.AddSubscriptions(nodes.Values, "NodeMetrics",
               async (MonitoredItem item, MonitoredItemNotificationEventArgs _) => await extractor.Rebrowse(),
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
    }

    public class ServerItemSubscriptionState : UAHistoryExtractionState
    {
        public ServerItemSubscriptionState(IUAClientAccess client, NodeId id) : base(client, id, false, false)
        {
        }
    }
}
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
        private readonly RebrowseTriggersConfig _config;
        private readonly UAExtractor _extractor;

        public ServerSubscriptionManager(
            ILogger<ServerSubscriptionManager> logger,
            UAClient uaClient,
            RebrowseTriggersConfig config,
            UAExtractor extractor
        )
        {
            _logger = logger;
            _uaClient = uaClient;
            _config = config;
            _extractor = extractor;
        }

        public async Task EnableCustomServerSubscriptions(CancellationToken token)
        {
            var targetNodes = _config.Targets?.GetValues;
            List<NodeId> nodeIds = new List<NodeId>();
            var filteredNamespaces = _config.Namespaces;
            var filteredNamespacesCount = filteredNamespaces?.Count();
            var shouldFilterNamespaces = filteredNamespacesCount > 0;

            var serverNamespaces = ObjectIds.Server_Namespaces;

            var grouping = new Dictionary<NodeId, List<ReferenceDescription>>();
            // displayName: nodeId
            var namespaceNameToId = new Dictionary<string, string>();

            await _uaClient.Browser.BrowseDirectory(
                new[] { serverNamespaces },
                (refDef, parent) =>
                {
                    var nodeId = (NodeId)refDef.NodeId;

                    if (parent == serverNamespaces && !grouping.ContainsKey(nodeId))
                    {
                        grouping.Add(nodeId, new List<ReferenceDescription>());
                        namespaceNameToId.Add(refDef.DisplayName.ToString(), nodeId.ToString());
                    }
                    else if (
                        grouping.ContainsKey(parent)
                        // Ensures that the type of node being added is a variable node class
                        && refDef.NodeClass == NodeClass.Variable
                        // Filters targets nodes
                        && targetNodes.Contains(refDef.DisplayName.ToString())
                    )
                    {
                        grouping.GetValueOrDefault(parent).Add(refDef);
                    }
                },
                token,
                maxDepth: 1,
                doFilter: false,
                ignoreVisited: false
            );

            // To be used in filtering namespaces
            var availableNamespaces = namespaceNameToId.Keys;

            // Filters by namespaces
            var processedNamespaces = (
                shouldFilterNamespaces
                    ? filteredNamespaces.Intersect(availableNamespaces)
                    : availableNamespaces
            ).ToList();

            if (shouldFilterNamespaces && processedNamespaces.Count() < filteredNamespacesCount)
            {
                _logger.LogInformation(
                    "Some namespaces could not be processed as they do not exist on the server: {namespaces}",
                    filteredNamespaces.Except(processedNamespaces)
                );
            }

            foreach (var @namespace in processedNamespaces)
            {
                var nodeId = namespaceNameToId.GetValueOrDefault(@namespace);
                var references = grouping.GetValueOrDefault(nodeId);

                nodeIds.AddRange(
                    references.Where(@ref => targetNodes.Contains(@ref.DisplayName.ToString()))
                        .Select(@ref => (NodeId)@ref.NodeId)
                );
            };

            if (nodeIds.Count() > 0) _logger.LogInformation("The following nodes will be subscribed to a rebrowse: {nodes}", nodeIds);

            var nodes = nodeIds.Select(node => new ServerItemSubscriptionState(_uaClient, node)).ToList();

            await Subscribe(nodes, token);
        }

        private async Task Subscribe(List<ServerItemSubscriptionState> nodes, CancellationToken token)
        {
            var sub = await _uaClient.AddSubscriptions(
                nodes, "TriggerRebrowse",
                (MonitoredItem item, MonitoredItemNotificationEventArgs _) =>
                {
                    // Check value.
                    _logger.LogInformation("{value}", item.LastMessage.PublishTime);
                    _extractor.Looper.QueueRebrowse();
                },
                state => new MonitoredItem
                {
                    StartNodeId = state.SourceId,
                    SamplingInterval = 1000,
                    DisplayName = "Value " + state.Id,
                    QueueSize = 1,
                    DiscardOldest = true,
                    AttributeId = Attributes.Value,
                    NodeClass = NodeClass.Variable,
                    CacheQueueSize = 1
                }, token, "namespaces "
            );
        }
    }

    public class ServerItemSubscriptionState : UAHistoryExtractionState
    {
        public ServerItemSubscriptionState(IUAClientAccess client, NodeId id) : base(client, id, false, false)
        {
        }
    }
}
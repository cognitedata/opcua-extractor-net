using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Subscriptions;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public class RebrowseTriggerManager
    {
        private readonly ILogger<RebrowseTriggerManager> logger;
        private readonly UAClient _uaClient;
        private readonly RebrowseTriggersConfig _config;
        private readonly UAExtractor _extractor;

        public RebrowseTriggerManager(
            ILogger<RebrowseTriggerManager> logger,
            UAClient uaClient,
            RebrowseTriggersConfig config,
            UAExtractor extractor
        )
        {
            this.logger = logger;
            _uaClient = uaClient;
            _config = config;
            _extractor = extractor;
        }

        public async Task EnableCustomServerSubscriptions(CancellationToken token)
        {
            var targetNodes = _config.Targets.GetTargets;
            var filteredNamespaces = _config.Namespaces;
            var filteredNamespacesCount = filteredNamespaces?.Count();
            var shouldFilterNamespaces = filteredNamespacesCount > 0;

            var serverNamespaces = ObjectIds.Server_Namespaces;

            var grouping = new Dictionary<NodeId, List<ReferenceDescription>>();
            // displayName: NodeId
            var namespaceNameToId = new Dictionary<string, NodeId>();

            await _uaClient.Browser.BrowseDirectory(
                new[] { serverNamespaces },
                (refDef, parent, visited) =>
                {
                    if (visited) return;
                    var nodeId = (NodeId)refDef.NodeId;

                    if (parent == serverNamespaces && !grouping.ContainsKey(nodeId))
                    {
                        grouping.Add(nodeId, new List<ReferenceDescription>());
                        if (!namespaceNameToId.TryAdd(refDef.DisplayName.ToString(), nodeId))
                        {
                            logger.LogWarning("Duplicate namespace {Namespace} found on the server, this is a bug in the server. Rebrowse triggers may not work properly.",
                                refDef.DisplayName);
                        }
                    }
                    else if (
                        grouping.TryGetValue(parent, out var group)
                        // Ensures that the type of node being added is a variable node class
                        && refDef.NodeClass == NodeClass.Variable
                        // Filters targets nodes
                        && targetNodes.Contains(refDef.DisplayName.ToString())
                    )
                    {
                        group.Add(refDef);
                    }
                },
                token,
                maxDepth: 1,
                doFilter: false
            );

            // To be used in filtering namespaces
            var availableNamespaces = namespaceNameToId.Keys;

            // Filters by namespaces
            var processedNamespaces = (
                shouldFilterNamespaces
                    ? filteredNamespaces.Intersect(availableNamespaces)
                    : availableNamespaces
            ).ToList();

            if (shouldFilterNamespaces && processedNamespaces.Count < filteredNamespacesCount)
            {
                logger.LogInformation(
                    "Some namespaces were not found for rebrowse subscription as they do not exist on the server: {Namespaces}",
                    filteredNamespaces.Except(processedNamespaces)
                );
            }

            var nodes = new Dictionary<NodeId, (NodeId, string)>();

            foreach (var @namespace in processedNamespaces)
            {
                var nodeId = namespaceNameToId.GetValueOrDefault(@namespace);
                var references = grouping.GetValueOrDefault(nodeId);

                foreach (var reference in references)
                {
                    if (!targetNodes.Contains(reference.DisplayName.Text)) continue;
                    var id = _uaClient.ToNodeId(reference.NodeId);
                    nodes.TryAdd(id, (id, reference.DisplayName.Text));
                }
            };

            if (nodes.Any()) CreateSubscriptions(nodes);
        }

        private void OnRebrowseTriggerNotification(MonitoredItem item, MonitoredItemNotificationEventArgs _)
        {
            try
            {
                var values = item.DequeueValues();
                var value = values.Count > 0
                    ? values[0].GetValue(UAExtractor.StartTime)
                    : UAExtractor.StartTime;

                if (UAExtractor.StartTime < value)
                {
                    logger.LogInformation("Triggering a rebrowse due to a change in the value of {NodeId} to {Value}", item.ResolvedNodeId, value);
                    _extractor.Looper.QueueRebrowse();
                }
                else
                {
                    logger.LogDebug("Received a rebrowse trigger notification with time {Time}, which is not greater than extractor start time {StartTime}",
                        value,
                        UAExtractor.StartTime);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unexpected error handling rebrowse trigger");
            }
        }

        private void CreateSubscriptions(Dictionary<NodeId, (NodeId, string)> nodes)
        {
            if (_uaClient.SubscriptionManager == null) throw new InvalidOperationException("Client not initialized");

            _uaClient.SubscriptionManager.EnqueueTask(new RebrowseTriggerSubscriptionTask(
                OnRebrowseTriggerNotification, nodes));
        }
    }
}
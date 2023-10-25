using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Subscriptions;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;

namespace Cognite.OpcUa
{
    public class NamespacePublicationDateStorableState : BaseStorableState
    {
        public long LastTimestamp { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    public class NamespacePublicationDateState : IExtractionState
    {
        public bool Existing { get; set; }
        public long LastTimestamp { get; set; }
        public DateTime? LastTimeModified { get; set; }
        public string Id { get; set; }

        public NamespacePublicationDateState(string id)
        {
            Id = id;
        }
    }

    public class RebrowseTriggerManager
    {
        private readonly ILogger<RebrowseTriggerManager> logger;
        private readonly UAClient _uaClient;
        private readonly RebrowseTriggersConfig _config;
        private readonly UAExtractor _extractor;
        private readonly string _npdStore;
        private IDictionary<string, NamespacePublicationDateState> _extractionStates =
            new Dictionary<string, NamespacePublicationDateState>();

        public RebrowseTriggerManager(
            ILogger<RebrowseTriggerManager> logger,
            UAClient uaClient,
            RebrowseTriggersConfig config,
            string namespacePublicationDateStore,
            UAExtractor extractor
        )
        {
            this.logger = logger;
            _uaClient = uaClient;
            _config = config;
            _extractor = extractor;
            _npdStore = namespacePublicationDateStore;
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
                    if (visited)
                        return;
                    var nodeId = (NodeId)refDef.NodeId;

                    if (parent == serverNamespaces && !grouping.ContainsKey(nodeId))
                    {
                        grouping.Add(nodeId, new List<ReferenceDescription>());
                        if (!namespaceNameToId.TryAdd(refDef.DisplayName.ToString(), nodeId))
                        {
                            logger.LogWarning(
                                "Duplicate namespace {Namespace} found on the server, this is a bug in the server. Rebrowse triggers may not work properly.",
                                refDef.DisplayName
                            );
                        }
                    }
                    else if (
                        grouping.TryGetValue(parent, out var group)
                        // Ensures that the type of node being added is a variable node class
                        && refDef.NodeClass == NodeClass.Variable
                        // Filter's targets nodes
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
                    if (!targetNodes.Contains(reference.DisplayName.Text))
                        continue;
                    var id = _uaClient.ToNodeId(reference.NodeId);
                    nodes.TryAdd(id, (id, reference.DisplayName.Text));
                    _extractionStates.TryAdd(
                        id.ToString(),
                        new NamespacePublicationDateState(id.ToString())
                    );
                }
            }

            if (_extractor.StateStorage != null)
            {
                await _extractor.StateStorage.RestoreExtractionState<
                    NamespacePublicationDateStorableState,
                    NamespacePublicationDateState
                >(
                    _extractionStates,
                    _npdStore,
                    (value, item) =>
                    {
                        value.LastTimestamp = item.LastTimestamp;
                    },
                    token
                );
            }

            if (nodes.Any())
                CreateSubscriptions(nodes, token);
        }

        private MonitoredItemNotificationEventHandler OnRebrowseTriggerNotification(
            CancellationToken token
        )
        {
            // Action<MonitoredItem, MonitoredItemNotificationEventArgs>
            return (MonitoredItem item, MonitoredItemNotificationEventArgs _) =>
            {
                try
                {
                    // item.DisplayName
                    var values = item.DequeueValues();
                    var valueTime = (
                        (DateTimeOffset)values[0].ServerTimestamp
                    ).ToUnixTimeMilliseconds();
                    var id = item.ResolvedNodeId.ToString();
                    var shouldRebrowse = EvaluateTimestampFor(id, valueTime);
                    if (shouldRebrowse)
                    {
                        logger.LogInformation(
                            "Triggering a rebrowse due to a change in the value of {NodeId} to {Value}",
                            item.ResolvedNodeId,
                            valueTime
                        );
                        _extractor.Looper.QueueRebrowse();
                        Task.Run(async () => await UpsertStoredTimestampFor(id, valueTime, token));
                    }
                    else
                    {
                        logger.LogDebug(
                            "Received a rebrowse trigger notification with time {Time}, which is not greater than last rebrowse time {lastTime}",
                            valueTime,
                            UAExtractor.StartTime
                        );
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Unexpected error handling rebrowse trigger");
                }
            };
        }

        private bool EvaluateTimestampFor(string id, long valueTime) =>
            (_extractionStates.TryGetValue(id, out var lastState))
            && lastState.LastTimestamp < valueTime;

        private async Task UpsertStoredTimestampFor(
            string id,
            long valueTime,
            CancellationToken token
        )
        {
            if (_extractor.StateStorage == null)
                return;

            if (_extractionStates.TryGetValue(id, out var lastState))
            {
                logger.LogInformation($"Updating state for: {id}");
                lastState.LastTimestamp = valueTime;
            }
            else
            {
                logger.LogInformation($"No state found for: {id}");
                var npds = new NamespacePublicationDateState(id);
                npds.LastTimestamp = valueTime;
                _extractionStates[id] = npds;
            }
            _extractionStates[id].LastTimeModified = DateTime.UtcNow;

            await _extractor.StateStorage.StoreExtractionState<
                NamespacePublicationDateStorableState,
                NamespacePublicationDateState
            >(
                _extractionStates.Values.ToList(),
                _npdStore,
                (state) =>
                    new NamespacePublicationDateStorableState
                    {
                        Id = state.Id,
                        LastTimestamp = valueTime,
                    },
                token
            );
        }

        private void CreateSubscriptions(
            Dictionary<NodeId, (NodeId, string)> nodes,
            CancellationToken token
        )
        {
            if (_uaClient.SubscriptionManager == null)
                throw new InvalidOperationException("Client not initialized");

            _uaClient.SubscriptionManager.EnqueueTask(
                new RebrowseTriggerSubscriptionTask(OnRebrowseTriggerNotification(token), nodes)
            );
        }

        private void OnServerReconnect() { }
    }
}

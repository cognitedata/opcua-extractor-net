using Cognite.Extractor.StateStorage;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.NodeSources;
using LiteDB;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    internal class NodeExistsState : BaseExtractionState
    {
        public string? Namespace { get; }
        public string? RawId { get; }

        public NodeExistsState(string id, string? ns, string? rawId, DateTime time) : base(id)
        {
            LastTimeModified = time;
            Namespace = ns;
            RawId = rawId;
        }

        public NodeExistsState(string id, NodeId nodeId, SessionContext context, DateTime time) : base(id)
        {
            LastTimeModified = time;
            Namespace = context.NamespaceTable.GetString(nodeId.NamespaceIndex);
            var buffer = new StringBuilder();
            NodeId.Format(buffer, nodeId.Identifier, nodeId.IdType, 0);
            RawId = buffer.ToString();
        }
    }

    public class KnownNodesState : BaseStorableState
    {
        public string? Namespace { get; set; }
        [BsonField(Name = "rawId")]
        public string? RawId { get; set; }
        public NodeId GetNodeId(SessionContext context, ILogger log)
        {
            var id = context.ToNodeId(RawId, Namespace);
            if (id.IsNullNodeId) log.LogWarning("Failed to delete node defined by {Ns};{RawId}, unable to construct a valid node ID", Namespace, RawId);
            return id;
        }
    }

    public class DeletedNodes
    {
        public IEnumerable<KnownNodesState> Objects { get; }
        public IEnumerable<KnownNodesState> Variables { get; }
        public IEnumerable<KnownNodesState> References { get; }
        public DeletedNodes(IEnumerable<KnownNodesState> objects, IEnumerable<KnownNodesState> variables, IEnumerable<KnownNodesState> references)
        {
            Objects = objects;
            Variables = variables;
            References = references;
        }

        public DeletedNodes()
        {
            Objects = Enumerable.Empty<KnownNodesState>();
            Variables = Enumerable.Empty<KnownNodesState>();
            References = Enumerable.Empty<KnownNodesState>();
        }

        public DeletedNodes Merge(DeletedNodes other)
        {
            return new DeletedNodes(
                Objects.Concat(other.Objects).Distinct().ToList(),
                Variables.Concat(other.Variables).Distinct().ToList(),
                References.Concat(other.References).Distinct().ToList());
        }
    }

    public class DeletesManager
    {
        private readonly IExtractionStateStore stateStore;
        private readonly IUAClientAccess client;
        private readonly FullConfig config;
        private readonly ILogger logger;
        public DeletesManager(IExtractionStateStore stateStore, IUAClientAccess client, ILogger<DeletesManager> logger, FullConfig config)
        {
            this.stateStore = stateStore;
            this.client = client;
            this.config = config;
            this.logger = logger;
        }

        private async Task<Dictionary<string, KnownNodesState>> GetExistingStates(string tableName, CancellationToken token)
        {
            try
            {
                return (await stateStore.GetAllExtractionStates<KnownNodesState>(tableName, token)).ToDictionary(s => s.Id);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to get states from state store, assuming it is empty");
                return new Dictionary<string, KnownNodesState>();
            }
        }

        private async Task<IEnumerable<KnownNodesState>> GetDeletedItems(string? tableName, Dictionary<string, NodeExistsState> states, CancellationToken token)
        {
            if (tableName == null) return Enumerable.Empty<KnownNodesState>();

            var oldStates = await GetExistingStates(tableName, token);

            var time = DateTime.UtcNow;
            var deletedStates = oldStates.Where(s => !states.ContainsKey(s.Key)).Select(kvp => kvp.Value).ToList();
            if (deletedStates.Count != 0)
            {
                logger.LogInformation("Found {Del} stored nodes in {Tab} that no longer exist and will be marked as deleted", deletedStates.Count, tableName);
                if (!config.DryRun) await stateStore.DeleteExtractionState(deletedStates.Select(d =>
                    new NodeExistsState(d.Id, d.Namespace, d.RawId, time)), tableName, token);
            }

            var newStates = states.Values.Where(s => !oldStates.ContainsKey(s.Id)).ToList();
            if (newStates.Any())
            {
                logger.LogInformation("Found {New} new nodes in {Tab}, adding to state store...", newStates.Count, tableName);
                if (!config.DryRun) await stateStore.StoreExtractionState(newStates, tableName,
                    s => new KnownNodesState { Id = s.Id, Namespace = s.Namespace, RawId = s.RawId }, token);
            }

            return deletedStates;
        }

        public async Task<DeletedNodes> GetDiffAndStoreIds(NodeSourceResult result, SessionContext context, CancellationToken token)
        {
            if (!result.CanBeUsedForDeletes) return new DeletedNodes();

            var time = DateTime.UtcNow.AddSeconds(-1);
            var newVariables = result.DestinationVariables.Select(v => (v.Id, v.GetUniqueId(context)!)).ToDictionary(
                i => i.Item2,
                i => new NodeExistsState(i.Item2, i.Item1, context, time));
            var newObjects = result.DestinationObjects.Select(o => (o.Id, o.GetUniqueId(context)!)).ToDictionary(
                i => i.Item2,
                i => new NodeExistsState(i.Item2, i.Item1, context, time));
            var newReferences = result.DestinationReferences.Select(r => client.GetRelationshipId(r)!).ToDictionary(
                i => i,
                i => new NodeExistsState(i, null, null, time));

            var res = await Task.WhenAll(
                GetDeletedItems(config.StateStorage?.KnownObjectsStore, newObjects, token),
                GetDeletedItems(config.StateStorage?.KnownVariablesStore, newVariables, token),
                GetDeletedItems(config.StateStorage?.KnownReferencesStore, newReferences, token)
            );

            return new DeletedNodes(res[0], res[1], res[2]);
        }
    }
}

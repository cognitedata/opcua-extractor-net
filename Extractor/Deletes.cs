using Cognite.Extractor.StateStorage;
using Cognite.OpcUa.NodeSources;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    internal class NodeExistsState : BaseExtractionState
    {
        public NodeExistsState(string id) : base(id)
        {
        }
    }

    public class DeletedNodes
    {
        public IEnumerable<string> Objects { get; }
        public IEnumerable<string> Variables { get; }
        public IEnumerable<string> References { get; }
        public DeletedNodes(IEnumerable<string> objects, IEnumerable<string> variables, IEnumerable<string> references)
        {
            Objects = objects;
            Variables = variables;
            References = references;
        }

        public DeletedNodes()
        {
            Objects = Enumerable.Empty<string>();
            Variables = Enumerable.Empty<string>();
            References = Enumerable.Empty<string>();
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

        private async Task<Dictionary<string, BaseStorableState>> GetExistingStates(string tableName, CancellationToken token)
        {
            try
            {
                return (await stateStore.GetAllExtractionStates<BaseStorableState>(tableName, token)).ToDictionary(s => s.Id);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to get states from state store, assuming it is empty");
                return new Dictionary<string, BaseStorableState>();
            }
        }

        private async Task<IEnumerable<string>> GetDeletedItems(string? tableName, Dictionary<string, NodeExistsState> states, CancellationToken token)
        {
            // If there are no states we don't check for deletes. This means we can't ever delete the _last_ of a type, but that is likely to be
            // an issue or a bug either way.
            if (tableName == null || !states.Any()) return Enumerable.Empty<string>();

            var oldStates = await GetExistingStates(tableName, token);

            var deletedStates = oldStates.Where(s => !states.ContainsKey(s.Key)).Select(kvp => kvp.Value).ToList();
            if (deletedStates.Any())
            {
                logger.LogInformation("Found {Del} stored nodes in {Tab} that no longer exist and will be marked as deleted", deletedStates.Count, tableName);
                await stateStore.DeleteExtractionState(deletedStates.Select(d => new NodeExistsState(d.Id)), tableName, token);
            }

            var newStates = states.Values.Where(s => !oldStates.ContainsKey(s.Id)).ToList();
            if (newStates.Any())
            {
                logger.LogInformation("Found {New} new nodes in {Tab}, adding to state store...", newStates.Count, tableName);
                await stateStore.StoreExtractionState(newStates, tableName, s => new BaseStorableState { Id = s.Id }, token);
            }

            return deletedStates.Select(d => d.Id);
        }

        public async Task<DeletedNodes> GetDiffAndStoreIds(NodeSourceResult result, CancellationToken token)
        {
            if (!result.CanBeUsedForDeletes) return new DeletedNodes();

            var newVariables = result.DestinationVariables.Select(v => client.GetUniqueId(v.Id, v.Index)!).ToDictionary(
                i => i, i => new NodeExistsState(i));
            var newObjects = result.DestinationObjects.Select(o => client.GetUniqueId(o.Id)!).ToDictionary(i => i, i => new NodeExistsState(i));
            var newReferences = result.DestinationReferences.Select(r => client.GetRelationshipId(r)!).ToDictionary(i => i, i => new NodeExistsState(i));

            var res = await Task.WhenAll(
                GetDeletedItems(config.StateStorage?.KnownObjectsStore, newObjects, token),
                GetDeletedItems(config.StateStorage?.KnownVariablesStore, newVariables, token),
                GetDeletedItems(config.StateStorage?.KnownReferencesStore, newReferences, token)
            );

            return new DeletedNodes(res[0], res[1], res[2]);
        }
    }
}

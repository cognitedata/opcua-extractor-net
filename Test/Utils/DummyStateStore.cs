using Cognite.Extractor.StateStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Test.Utils
{
    internal sealed class DummyStateStore : IExtractionStateStore
    {
        public int NumDeleteState { get; private set; }
        public int NumRestoreState { get; private set; }
        public int NumStoreState { get; private set; }

        public Task DeleteExtractionState(IEnumerable<IExtractionState> extractionStates, string tableName, CancellationToken token)
        {
            NumDeleteState++;
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }

        public Task<IEnumerable<T>> GetAllExtractionStates<T>(string tableName, CancellationToken token) where T : BaseStorableState
        {
            return Task.FromResult(Enumerable.Empty<T>());
        }

        public Task RestoreExtractionState<T, K>(IDictionary<string, K> extractionStates, string tableName, Action<K, T> restoreStorableState, CancellationToken token)
            where T : BaseStorableState
            where K : IExtractionState
        {
            NumRestoreState++;
            return Task.CompletedTask;
        }

        public Task RestoreExtractionState<K>(IDictionary<string, K> extractionStates, string tableName, bool initializeMissing, CancellationToken token) where K : BaseExtractionState
        {
            NumRestoreState++;
            return Task.CompletedTask;
        }

        public Task StoreExtractionState<T, K>(IEnumerable<K> extractionStates, string tableName, Func<K, T> buildStorableState, CancellationToken token)
            where T : BaseStorableState
            where K : IExtractionState
        {
            NumStoreState++;
            return Task.CompletedTask;
        }

        public Task StoreExtractionState<K>(IEnumerable<K> extractionStates, string tableName, CancellationToken token) where K : BaseExtractionState
        {
            NumStoreState++;
            return Task.CompletedTask;
        }
    }
}

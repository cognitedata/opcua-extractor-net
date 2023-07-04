using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Cognite.OpcUa.Nodes;
using CogniteSdk;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface IRawWriter
    {
        static JsonSerializerOptions options =>
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        Task<IEnumerable<RawRow<Dictionary<string, JsonElement>>>> GetRawRows(
            string dbName,
            string tableName,
            IEnumerable<string>? columns
        );

        Task PushNodes<T>(
            UAExtractor extractor,
            string database,
            string table,
            ConcurrentDictionary<string, T> rows,
            bool shouldUpdate,
            BrowseReport report
        )
            where T : BaseUANode;

        Task PushReferences(
            string database,
            string table,
            IEnumerable<RelationshipCreate> relationships,
            BrowseReport report
        );
    }
}

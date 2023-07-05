using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Cognite.OpcUa.Types;
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
            IEnumerable<string>? columns,
            CancellationToken token
        );

        Task<Result>  PushNodes<T>(
            UAExtractor extractor,
            string database,
            string table,
            IDictionary<string, T> rows,
            ConverterType converter,
            bool shouldUpdate,
            CancellationToken token
        )
            where T : BaseUANode;

        Task<Result> PushReferences(
            string database,
            string table,
            IEnumerable<RelationshipCreate> relationships,
            CancellationToken token
        );
    }
}

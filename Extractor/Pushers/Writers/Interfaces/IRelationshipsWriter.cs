using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using CogniteSdk;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface IRelationshipsWriter
    {
        Task<Result> PushReferences(IEnumerable<RelationshipCreate> relationships, CancellationToken token);
    }
}

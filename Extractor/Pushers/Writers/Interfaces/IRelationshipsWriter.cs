using System.Collections.Generic;
using System.Threading.Tasks;
using CogniteSdk;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface IRelationshipsWriter
    {
        Task PushReferences(
            IEnumerable<RelationshipCreate> relationships,
            BrowseReport report
        );
    }
}

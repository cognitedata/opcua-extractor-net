using System.Collections.Generic;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Types;

namespace Cognite.OpcUa.Pushers.Destinations.Interfaces
{
    public interface IRelationshipsWriter
    {
        FullConfig config { get; }

        Task PushReferences(IEnumerable<UAReference> references, BrowseReport report);
    }
}

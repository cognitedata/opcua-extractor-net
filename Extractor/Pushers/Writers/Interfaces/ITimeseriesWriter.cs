using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface ITimeseriesWriter
    {
        Task PushVariables(
            UAExtractor extractor,
            ConcurrentDictionary<string, UAVariable> timeseriesMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            HashSet<string> mismatchedTimeseries,
            TypeUpdateConfig update,
            BrowseReport report
        )
;
    }
}

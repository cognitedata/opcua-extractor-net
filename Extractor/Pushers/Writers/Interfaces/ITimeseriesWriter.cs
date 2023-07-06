using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface ITimeseriesWriter
    {
        Task<Result> PushVariables(
            UAExtractor extractor,
            IDictionary<string, UAVariable> timeseriesMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            HashSet<string> mismatchedTimeseries,
            TypeUpdateConfig update,
            CancellationToken token
        );
    }
}

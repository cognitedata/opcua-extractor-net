using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface IAssetsWriter
    {
        Task<Result> PushNodes(
            UAExtractor extractor,
            ConcurrentDictionary<string, BaseUANode> assetMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            TypeUpdateConfig config,
            CancellationToken token
        );
    }
}

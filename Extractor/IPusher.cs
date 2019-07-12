using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public interface IPusher
    {
        UniqueId RootNode { get; set; }
        Extractor Extractor { get; set; }
        Task PushDataPoints(ConcurrentQueue<BufferedDataPoint> dataPointQueue);
        Task PushNodes(ConcurrentQueue<BufferedNode> nodeQueue);
        void Reset();
    }
}

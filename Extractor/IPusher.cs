using System.Collections.Concurrent;
using System.Threading.Tasks;
using Opc.Ua;

namespace Cognite.OpcUa
{
    public interface IPusher
    {
        /// <summary>
        /// UniqueId representation of OPC-UA like root node. Mapping is expected to start from here.
        /// </summary>
        NodeId RootNode { get; set; }
        /// <summary>
        /// Parent extractor
        /// </summary>
        Extractor Extractor { set; }
        /// <summary>
        /// The UAClient to use as source
        /// </summary>
        UAClient UAClient { set; }
        /// <summary>
        /// Push data points, emptying the queue
        /// </summary>
        /// <param name="dataPointQueue">Data points to be pushed</param>
        Task PushDataPoints(ConcurrentQueue<BufferedDataPoint> dataPointQueue);
        /// <summary>
        /// Push nodes, emptying the queue
        /// </summary>
        /// <param name="nodeQueue">Nodes to be pushed</param>
        Task<bool> PushNodes(ConcurrentQueue<BufferedNode> nodeQueue);
        /// <summary>
        /// Reset relevant persistent information in the pusher, preparing it to be restarted
        /// </summary>
        void Reset();
    }
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus.Client;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Main extractor class, tying together the <see cref="UAClient"/> and CDF client.
    /// </summary>
    public class Extractor
    {
        private readonly UAClient UAClient;
        private bool buffersEmpty;
		private FullConfig config;
        public NodeId RootNode { get; private set; }
        private readonly ConcurrentQueue<BufferedDataPoint> bufferedDPQueue = new ConcurrentQueue<BufferedDataPoint>();
        private readonly ConcurrentQueue<BufferedNode> bufferedNodeQueue = new ConcurrentQueue<BufferedNode>();
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1);
        private readonly bool debug;
        private bool pushingDatapoints;
        private bool runningPush = true;
        private readonly IPusher pusher;

        public bool Started { get; private set; }


        private static readonly Gauge startTime = Metrics
            .CreateGauge("opcua_start_time", "Start time for the extractor");

        /// <summary>
        /// Primary constructor, creates and starts the UAClient and starts the dataPushTimer.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="pusher"></param>
        public Extractor(FullConfig config, IPusher pusher, UAClient UAClient)
        {
            this.pusher = pusher;
            this.UAClient = UAClient;
			this.config = config;
            UAClient.Extractor = this;

            debug = config.CogniteConfig.Debug;
            runningPush = !debug;
            pusher.Extractor = this;
            pusher.UAClient = UAClient;
        }
        #region Interface

        public bool Start()
        {
            Logger.LogInfo("Start UAClient");
            try
            {
                UAClient.Run().Wait();
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to start UAClient");
                Logger.LogException(e);
                return false;
            }
            if (!UAClient.Started)
            {
                return false;
            }
            Started = true;
            startTime.Set(DateTime.Now.Subtract(Epoch).TotalMilliseconds);
            RootNode = UAClient.ToNodeId(config.CogniteConfig.RootNodeId, config.CogniteConfig.RootNodeNamespace);
            if (RootNode.IsNullNodeId)
            {
                RootNode = ObjectIds.ObjectsFolder;
            }
            pusher.RootNode = RootNode;
            Task.Run(async () =>
            {
                while (runningPush)
                {
					try
					{
						pushingDatapoints = true;
						await pusher.PushDataPoints(bufferedDPQueue);
						pushingDatapoints = false;
						Thread.Sleep(config.CogniteConfig.DataPushDelay);
					}
                    catch (Exception e)
					{
						Logger.LogError("Failed to push datapoints");
						Logger.LogException(e);
					}
                }
            });
            return true;
        }
        /// <summary>
        /// Restarts the extractor, to some extent, clears known asset ids, allows data to be pushed to CDF, and begins mapping the opcua
        /// directory again
        /// </summary>
        public void RestartExtractor()
        {
            // In theory, a disconnect might be a server restart, which can cause namespaces to change.
            // This invalidates our stored mapping, so we need to redo everything, remap structure, read history,
            // synchronize history
            UAClient.WaitForOperations().Wait();
            buffersEmpty = false;
            MapUAToCDF();
        }
        /// <summary>
        /// Closes the extractor, mainly just shutting down the opcua client.
        /// </summary>
        public void Close()
        {
            WaitForFinalPush().Wait();
            if (!UAClient.Started) return;
            try
            {
                UAClient.Close();
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to cleanly shut down UAClient");
                Logger.LogException(e);
            }
            UAClient.WaitForOperations().Wait();
        }
        /// <summary>
        /// Starts the extractor, calling BrowseDirectory on the root node, then pushes all nodes to CDF once finished.
        /// </summary>
        public void MapUAToCDF()
        {
            pusher.Reset();
			Logger.LogInfo("Begin mapping directory");
            try
            {
                UAClient.BrowseDirectoryAsync(RootNode, HandleNode).Wait();
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to map directory");
                Logger.LogException(e);
                return;
            }
            Logger.LogInfo("End mapping directory");
            try
            {
                pusher.PushNodes(bufferedNodeQueue).Wait();
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to push nodes to CDF");
                Logger.LogException(e);
            }
        }
        /// <summary>
        /// Disables pushing to CDF, then waits until the final push has been completed
        /// </summary>
        /// <returns></returns>
        public async Task WaitForFinalPush()
        {
            runningPush = false;
            while (pushingDatapoints) await Task.Delay(100);
        }
        public void SynchronizeNodes(IEnumerable<BufferedVariable> variables)
        {
            UAClient.SynchronizeNodes(variables, HistoryDataHandler, SubscriptionHandler);
        }
        public bool AllowTSMap(BufferedVariable node)
        {
            return UAClient.IsNumericType(node.DataType) && node.ValueRank == ValueRanks.Scalar;
        }
        #endregion

        #region Handlers
        /// <summary>
        /// Callback for the browse operation, creates <see cref="BufferedNode"/>s and enqueues them.
        /// </summary>
        /// <remarks>
        /// A FIFO queue ensures that parents will always be created before their children
        /// </remarks>
        /// <param name="node">Description of the node to be handled</param>
        /// <param name="parentId">Id of the parent node</param>
        private void HandleNode(ReferenceDescription node, NodeId parentId)
        {
            if (node.NodeClass == NodeClass.Object)
            {
                var bufferedNode = new BufferedNode(UAClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                Logger.LogData(bufferedNode);
                bufferedNodeQueue.Enqueue(bufferedNode);
            }
            else if (node.NodeClass == NodeClass.Variable)
            {
                var bufferedNode = new BufferedVariable(UAClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                if (node.TypeDefinition == VariableTypeIds.PropertyType)
                {
                    bufferedNode.IsProperty = true;
                }
                Logger.LogData(bufferedNode);
                bufferedNodeQueue.Enqueue(bufferedNode);
            }
            else
            {
                throw new Exception("Invalid node type");
            }
        }
        /// <summary>
        /// Handles notifications on subscribed items, pushes all new datapoints to the queue.
        /// </summary>
        /// <param name="item">Modified item</param>
        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            string uniqueId = UAClient.GetUniqueId(item.ResolvedNodeId);
            if (!buffersEmpty && pusher.NotInSync.Contains(uniqueId)) return;

            foreach (var datapoint in item.DequeueValues())
            {
                var buffDp = new BufferedDataPoint(
                    (long)datapoint.SourceTimestamp.Subtract(Epoch).TotalMilliseconds,
                    uniqueId,
                    UAClient.ConvertToDouble(datapoint.Value)
                );
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Logger.LogWarning("Bad datapoint: " + buffDp.Id);
                    return;
                }
                Logger.LogData(buffDp);
                // Logger.LogData(new BufferedDataPoint(buffDp.ToStorableBytes().Skip(sizeof(ushort)).ToArray()));

                if (debug) return;
                bufferedDPQueue.Enqueue(buffDp);
            }
        }
        /// <summary>
        /// Callback for HistoryRead operations. Simply pushes all datapoints to the queue.
        /// </summary>
        /// <param name="data">Collection of data to be handled</param>
        /// <param name="final">True if this is the final call for this node, and the lock may be removed</param>
        /// <param name="nodeid">Id of the node in question</param>
        private void HistoryDataHandler(HistoryReadResultCollection data, bool final, NodeId nodeid)
        {
            string uniqueId = UAClient.GetUniqueId(nodeid);
            if (final)
            {
                lock (pusher.NotInSyncLock)
                {
                    pusher.NotInSync.Remove(uniqueId);
                    buffersEmpty = pusher.NotInSync.Count == 0;
                }
            }
            if (data == null) return;

            if (!(ExtensionObject.ToEncodeable(data[0].HistoryData) is HistoryData hdata) || hdata.DataValues == null) return;
            Logger.LogInfo("Fetch " + hdata.DataValues.Count + " datapoints for nodeid " + nodeid);
            foreach (var datapoint in hdata.DataValues)
            {
                var buffDp = new BufferedDataPoint(
                    (long)datapoint.SourceTimestamp.Subtract(Epoch).TotalMilliseconds,
                    uniqueId,
                    UAClient.ConvertToDouble(datapoint.Value)
                );
                Logger.LogData(buffDp);
                bufferedDPQueue.Enqueue(buffDp);
            }
        }
        #endregion
    }
}
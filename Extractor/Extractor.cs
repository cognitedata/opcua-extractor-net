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
		private readonly FullConfig config;
        public NodeId RootNode { get; private set; }
        private readonly ConcurrentQueue<BufferedDataPoint> bufferedDPQueue = new ConcurrentQueue<BufferedDataPoint>();
        private readonly ConcurrentQueue<BufferedNode> bufferedNodeQueue = new ConcurrentQueue<BufferedNode>();
        private readonly bool debug;
        private bool pushingDatapoints;
        private bool runningPush = true;
        private readonly IPusher pusher;

        public bool Started { get; private set; }


        private static readonly Gauge startTime = Metrics
            .CreateGauge("opcua_start_time", "Start time for the extractor");

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pusher">Pusher to be used</param>
        /// <param name="UAClient">UAClient to use</param>
        public Extractor(FullConfig config, IPusher pusher, UAClient UAClient)
        {
            this.pusher = pusher;
            this.UAClient = UAClient;
			this.config = config;
            UAClient.Extractor = this;

            debug = config.CogniteConfig.Debug;
            pusher.Extractor = this;
            pusher.UAClient = UAClient;
        }
        #region Interface

        /// <summary>
        /// Start the extractor, starting the data-push and the UAClient
        /// </summary>
        /// <returns>True on success</returns>
        public bool Start()
        {
            if (UAClient.Started) return true;
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
            startTime.Set(new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds());
            RootNode = config.CogniteConfig.RootNode.ToNodeId(UAClient);
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
        /// Restarts the extractor, to some extent, clears known asset ids,
        /// allows data to be pushed to CDF, and begins mapping the opcua
        /// directory again
        /// </summary>
        public void RestartExtractor()
        {
            UAClient.WaitForOperations().Wait();
            buffersEmpty = false;
            MapUAToCDF().Wait();
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
            UAClient.WaitForOperations().Wait(10000);
            Logger.LogInfo("Extractor closed");
        }
        /// <summary>
        /// Starts the extractor, calling BrowseDirectory on the root node, then pushes all nodes to CDF once finished.
        /// </summary>
        public async Task MapUAToCDF()
        {
            pusher.Reset();
			Logger.LogInfo("Begin mapping directory");
            try
            {
                await UAClient.BrowseDirectoryAsync(RootNode, HandleNode);
            }
            catch (Exception e)
            {
                throw e;
            }
            Logger.LogInfo("End mapping directory");
            if (!await pusher.PushNodes(bufferedNodeQueue))
            {
                throw new Exception("Pushing nodes to CDF failed");
            }
        }
        /// <summary>
        /// Disables pushing to CDF, then waits until the final push has been completed
        /// </summary>
        public async Task WaitForFinalPush()
        {
            runningPush = false;
            while (pushingDatapoints) await Task.Delay(100);
        }
        /// <summary>
        /// Starts synchronization of nodes with opcua using normal callbacks
        /// </summary>
        /// <param name="variables">Variables to be synchronized</param>
        public void SynchronizeNodes(IEnumerable<BufferedVariable> variables)
        {
            UAClient.SynchronizeNodes(variables, HistoryDataHandler, SubscriptionHandler);
        }
        /// <summary>
        /// Is the variable allowed to be mapped to a timeseries?
        /// </summary>
        /// <param name="node">Variable to be tested</param>
        /// <returns>True if variable may be mapped to a timeseries</returns>
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
                    new DateTimeOffset(datapoint.SourceTimestamp).ToUnixTimeMilliseconds(),
                    uniqueId,
                    UAClient.ConvertToDouble(datapoint.Value)
                );
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Logger.LogWarning("Bad datapoint: " + buffDp.Id);
                    return;
                }
                Logger.LogData(buffDp);

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
        private void HistoryDataHandler(HistoryData data, bool final, NodeId nodeid)
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
            if (data == null || data.DataValues == null) return;

            if (data.DataValues == null) return;
            foreach (var datapoint in data.DataValues)
            {
                var buffDp = new BufferedDataPoint(
                    new DateTimeOffset(datapoint.SourceTimestamp).ToUnixTimeMilliseconds(),
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
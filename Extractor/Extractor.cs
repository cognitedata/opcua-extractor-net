using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
        private readonly IPusher pusher;
        /// <summary>
        /// The set of uniqueIds discovered, but not yet synced with the pusher
        /// </summary>
        public ISet<string> NotInSync { get; } = new HashSet<string>();
        public object NotInSyncLock { get; } = new object();

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

            pusher.Extractor = this;
            pusher.UAClient = UAClient;
        }
        #region Interface

        /// <summary>
        /// Start the extractor, starting the data-push and the UAClient
        /// </summary>
        /// <returns>True on success</returns>
        public async Task RunExtractor(CancellationToken token, bool quitAfterMap = false)
        {
            if (!UAClient.Started)
            {
                Logger.LogInfo("Start UAClient");
                try
                {
                    UAClient.Run(token).Wait();
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to start UAClient");
                    Logger.LogException(e);
                    throw;
                }
                if (!UAClient.Started)
                {
                    throw new Exception("UAClient failed to start");
                }
            }
            Started = true;
            startTime.Set(new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds());
            RootNode = config.CogniteConfig.RootNode.ToNodeId(UAClient);
            if (RootNode.IsNullNodeId)
            {
                RootNode = ObjectIds.ObjectsFolder;
            }
            pusher.RootNode = RootNode;

            IEnumerable<Task> tasks = new List<Task>
            {
                Task.Run(async () =>
                {
                    while (!token.IsCancellationRequested && !UAClient.Failed)
                    {
                        try
                        {
                            await pusher.PushDataPoints(bufferedDPQueue, token);
                            await Task.Delay(config.CogniteConfig.DataPushDelay, token);
                        }
                        catch (TaskCanceledException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            Logger.LogError("Failed to push datapoints");
                            Logger.LogException(e);
                        }
                    }
                }),
                MapUAToCDF(token)
            };

            Task failedTask = null;
            while (tasks.Any() && tasks.All(task => !task.IsFaulted))
            {
                try
                {
                    await Task.WhenAny(tasks);
                }
                catch (Exception)
                {
                    Logger.LogError("Task failed unexpectedly");
                }
                if (quitAfterMap) return;
                failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
                if (failedTask != null) break;
                tasks = tasks.Where(task => !task.IsCompleted);
            }
            if (!token.IsCancellationRequested)
            {
                if (failedTask != null)
                {
                    Logger.LogException(failedTask.Exception);
                    throw failedTask.Exception;
                }
                throw new Exception("Processes quit without failing");
            }
            throw new TaskCanceledException();
        }
        /// <summary>
        /// Restarts the extractor, to some extent, clears known asset ids,
        /// allows data to be pushed to CDF, and begins mapping the opcua
        /// directory again
        /// </summary>
        public void RestartExtractor(CancellationToken token)
        {
            UAClient.WaitForOperations().Wait();
            buffersEmpty = false;
            MapUAToCDF(token).Wait();
        }
        /// <summary>
        /// Closes the extractor, mainly just shutting down the opcua client.
        /// </summary>
        public void Close()
        {
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
        private async Task MapUAToCDF(CancellationToken token)
        {
            pusher.Reset();
			Logger.LogInfo("Begin mapping directory");
            try
            {
                await UAClient.BrowseDirectoryAsync(RootNode, HandleNode, token);
            }
            catch (Exception)
            {
                throw;
            }
            Logger.LogInfo("End mapping directory");
            if (!await pusher.PushNodes(bufferedNodeQueue, token))
            {
                throw new Exception("Pushing nodes to CDF failed");
            }
        }
        /// <summary>
        /// Starts synchronization of nodes with opcua using normal callbacks
        /// </summary>
        /// <param name="variables">Variables to be synchronized</param>
        public void SynchronizeNodes(IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            UAClient.SynchronizeNodes(variables, HistoryDataHandler, SubscriptionHandler, token);
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
            if (!buffersEmpty && NotInSync.Contains(uniqueId)) return;

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
                lock (NotInSyncLock)
                {
                    NotInSync.Remove(uniqueId);
                    buffersEmpty = NotInSync.Count == 0;
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

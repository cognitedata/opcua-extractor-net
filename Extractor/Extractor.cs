/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
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
        private readonly IEnumerable<IPusher> pushers;
        /// <summary>
        /// The set of uniqueIds discovered, but not yet synced with the pusher
        /// </summary>
        public ISet<string> NotInSync { get; } = new HashSet<string>();
        public object NotInSyncLock { get; } = new object();

        public bool Started { get; private set; }


        private static readonly Gauge startTime = Metrics
            .CreateGauge("opcua_start_time", "Start time for the extractor");

        public Extractor(FullConfig config, IEnumerable<IPusher> pushers, UAClient UAClient)
        {
            this.pushers = pushers;
            this.UAClient = UAClient;
            this.config = config;
            UAClient.Extractor = this;
            Logger.LogInfo($"Building extractor with {pushers.Count()} pushers");
            foreach (var pusher in pushers)
            {
                pusher.Extractor = this;
                pusher.UAClient = UAClient;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pusher">Pusher to be used</param>
        /// <param name="UAClient">UAClient to use</param>
        public Extractor(FullConfig config, IPusher pusher, UAClient UAClient) : this(config, new List<IPusher> { pusher }, UAClient)
        {
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
            RootNode = config.UAConfig.RootNode.ToNodeId(UAClient);
            if (config.UAConfig.NameOverrides != null)
            {
                foreach (var kvp in config.UAConfig.NameOverrides)
                {
                    UAClient.AddNodeOverride(kvp.Value.ToNodeId(UAClient), kvp.Key);
                }
            }

            IEnumerable<Task> tasks = pushers.Select(pusher =>
            {
                return Task.Run(async () =>
                {
                    while (!token.IsCancellationRequested && !UAClient.Failed)
                    {
                        try
                        {
                            await pusher.PushDataPoints(token);
                            await Task.Delay(config.Pushers.First().DataPushDelay, token);
                        }
                        catch (TaskCanceledException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            Logger.LogError($"Failed to push datapoints on pusher of type {pusher.GetType().Name}");
                            Logger.LogException(e);
                        }
                    }
                });
            }).Append(MapUAToCDF(token));

            Task failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
            while (tasks.Any() && failedTask == null)
            {
                try
                {
                    await Task.WhenAny(tasks);
                }
                catch (Exception)
                {
                    Logger.LogError("Task failed unexpectedly");
                }
                failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
                if (quitAfterMap) return;
                if (failedTask != null) break;
                tasks = tasks.Where(task => !task.IsCompleted && !task.IsFaulted && !task.IsCanceled);
            }
            if (!token.IsCancellationRequested)
            {
                if (failedTask != null)
                {
                    Logger.LogException(failedTask.Exception);
                    ExceptionDispatchInfo.Capture(failedTask.Exception).Throw();
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
            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }
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
            var pushes = pushers.Select(pusher => pusher.PushNodes(token));
            var result = await Task.WhenAll(pushes);
            if (!result.All(res => res))
            {
                throw new Exception("Pushing nodes failed");
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
                foreach (var pusher in pushers)
                {
                    pusher.BufferedNodeQueue.Enqueue(bufferedNode);
                }
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
                foreach (var pusher in pushers)
                {
                    pusher.BufferedNodeQueue.Enqueue(bufferedNode);
                }
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
                    datapoint.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToDouble(datapoint.Value)
                );
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Logger.LogWarning("Bad datapoint: " + buffDp.Id);
                    return;
                }
                Logger.LogData(buffDp);
                foreach (var pusher in pushers)
                {
                    pusher.BufferedDPQueue.Enqueue(buffDp);
                }
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
                    datapoint.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToDouble(datapoint.Value)
                );
                Logger.LogData(buffDp);
                foreach (var pusher in pushers)
                {
                    pusher.BufferedDPQueue.Enqueue(buffDp);
                }
            }
        }
        #endregion
    }
}

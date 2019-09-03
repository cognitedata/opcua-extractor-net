﻿/* Cognite Extractor for OPC-UA
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus.Client;
using Serilog;

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
        private readonly ConcurrentQueue<BufferedNode> commonQueue = new ConcurrentQueue<BufferedNode>();
        public ConcurrentDictionary<NodeId, BufferedVariable> ActiveVariables { get; } = new ConcurrentDictionary<NodeId, BufferedVariable>();  

        // Concurrent reading of properties
        private readonly HashSet<NodeId> pendingProperties = new HashSet<NodeId>();
        private readonly object propertySetLock = new object();
        private readonly List<Task> propertyReadTasks = new List<Task>();

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
            Log.Information("Building extractor with {NumPushers} pushers", pushers.Count());
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
                Log.Information("Start UAClient");
                try
                {
                    UAClient.Run(token).Wait();
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to start UAClient");
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
                    Log.Information("Start push loop");
                    while (!token.IsCancellationRequested && !UAClient.Failed)
                    {
                        try
                        {
                            await pusher.PushDataPoints(token);
                            await Task.Delay(pusher.BaseConfig.DataPushDelay, token);
                        }
                        catch (TaskCanceledException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            Log.Error(e, "Failed to push datapoints on pusher of type {FailedPusherName}", pusher.GetType().Name);
                        }
                    }
                });
            }).Append(MapUAToCDF(token)).ToList();

            Task failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
            while (tasks.Any() && failedTask == null)
            {
                try
                {
                    await Task.WhenAny(tasks);
                }
                catch (Exception)
                {
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
                Log.Error(e, "Failed to cleanly shut down UAClient");
            }
            UAClient.WaitForOperations().Wait(10000);
            Log.Information("Extractor closed");
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
            Log.Debug("Begin mapping directory");
            try
            {
                await UAClient.BrowseDirectoryAsync(RootNode, HandleNode, token);
            }
            catch (Exception)
            {
                throw;
            }
            Log.Debug("End mapping directory");
            var varList = new List<BufferedVariable>();
            var nodeList = new List<BufferedNode>();
            var nodeMap = new Dictionary<string, BufferedNode>();
            var tsList = new List<BufferedVariable>();
            var fullTsList = new List<BufferedVariable>();

            while (commonQueue.TryDequeue(out BufferedNode buffer))
            {
                if (buffer.IsVariable && buffer is BufferedVariable buffVar)
                {
                    if (buffVar.IsProperty)
                    {
                        nodeMap.TryGetValue(UAClient.GetUniqueId(buffVar.ParentId), out BufferedNode parent);
                        if (parent == null) continue;
                        if (parent.properties == null)
                        {
                            parent.properties = new List<BufferedVariable>();
                        }
                        parent.properties.Add(buffVar);
                    }
                    else
                    {
                        varList.Add(buffVar);
                    }
                }
                else
                {
                    nodeList.Add(buffer);
                }
                nodeMap.Add(UAClient.GetUniqueId(buffer.Id), buffer);
            }
            Log.Information("Getting data for {NumVariables} variables and {NumObjects} objects", varList.Count, nodeList.Count);
            try
            {
                UAClient.ReadNodeData(nodeList.Concat(varList), token);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to read node data");
            }
            foreach (var node in varList)
            {
                if (AllowTSMap(node))
                {
                    tsList.Add(node);
                    ActiveVariables[node.Id] = node;
                    if (node.ArrayDimensions != null && node.ArrayDimensions.Length > 0 && node.ArrayDimensions[0] > 0)
                    {
                        for (int i = 0; i < node.ArrayDimensions[0]; i++)
                        {
                            fullTsList.Add(new BufferedVariable(node, i));
                        }
                        nodeList.Add(node);
                    }
                    else
                    {
                        fullTsList.Add(node);
                    }
                    if (node.Historizing)
                    {
                        lock (NotInSyncLock)
                        {
                            NotInSync.Add(UAClient.GetUniqueId(node.Id));
                        }
                    }
                }
            }

            var pushes = pushers.Select(pusher => pusher.PushNodes(nodeList, fullTsList, token)).ToList();
            var result = await Task.WhenAll(pushes);
            if (!result.All(res => res))
            {
                throw new Exception("Pushing nodes failed");
            }
            if (pushers.Any() && pushers.First().GetType().Name == "TestPusher") return;
            Log.Information("Synchronize {NumNodesToSynch} nodes", tsList.Count);
            try
            {
                await SynchronizeNodes(tsList, token);
            }
            catch (Exception e)
            {
                if (e is TaskCanceledException) throw;
                Log.Error("Failed to synchronize nodes");
                throw;
            }
        }
        public async Task ReadProperties(IEnumerable<BufferedNode> nodes, CancellationToken token)
        {
            Task newTask = null;
            lock (propertySetLock)
            {
                nodes = nodes.Where(node => !pendingProperties.Contains(node.Id) && !node.PropertiesRead);
                if (nodes.Any())
                {
                    newTask = Task.Run(() => UAClient.GetNodeProperties(nodes, token));
                    propertyReadTasks.Add(newTask);
                }
            }
            await Task.WhenAll(propertyReadTasks);
            lock (propertySetLock)
            {
                if (!pendingProperties.Any()) return;
                foreach (var node in nodes)
                {
                    node.PropertiesRead = true;
                    pendingProperties.Remove(node.Id);
                }
                if (newTask != null)
                {
                    propertyReadTasks.Remove(newTask);
                }
            }
        }
        /// <summary>
        /// Starts synchronization of nodes with opcua using normal callbacks
        /// </summary>
        /// <param name="variables">Variables to be synchronized</param>
        public async Task SynchronizeNodes(IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            await UAClient.SynchronizeNodes(variables, HistoryDataHandler, SubscriptionHandler, token);
        }
        /// <summary>
        /// Is the variable allowed to be mapped to a timeseries?
        /// </summary>
        /// <param name="node">Variable to be tested</param>
        /// <returns>True if variable may be mapped to a timeseries</returns>
        public bool AllowTSMap(BufferedVariable node)
        {
            return (UAClient.IsNumericType(node.DataType) || config.UAConfig.AllowStringVariables)
                && (node.ValueRank == ValueRanks.Scalar
                    || config.UAConfig.MaxArraySize > 0 && node.ArrayDimensions != null && node.ArrayDimensions.Length == 1
                    && node.ArrayDimensions[0] > 0 && node.ArrayDimensions[0] <= config.UAConfig.MaxArraySize);
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
                Log.Debug(bufferedNode.ToDebugDescription());
                commonQueue.Enqueue(bufferedNode);
            }
            else if (node.NodeClass == NodeClass.Variable)
            {
                var bufferedNode = new BufferedVariable(UAClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                if (node.TypeDefinition == VariableTypeIds.PropertyType)
                {
                    bufferedNode.IsProperty = true;
                }
                Log.Debug(bufferedNode.ToDebugDescription());
                commonQueue.Enqueue(bufferedNode);
            }
        }
        private IEnumerable<BufferedDataPoint> ToDataPoint(DataValue value, BufferedVariable variable, string uniqueId)
        {
            if (variable.ArrayDimensions != null && variable.ArrayDimensions.Length > 0 && variable.ArrayDimensions[0] > 0)
            {
                var ret = new List<BufferedDataPoint>();
                if (!(value.Value is Array))
                {
                    Log.Warning("Bad datapoint on variable {BadPointName}, {BadPointValue}", uniqueId, value.Value.ToString());
                    return new BufferedDataPoint[0];
                }
                var values = (Array)value.Value;
                for (int i = 0; i < Math.Min(variable.ArrayDimensions[0], values.Length); i++)
                {
                    ret.Add(UAClient.IsNumericType(variable.DataType)
                        ? new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            UAClient.ConvertToDouble(values.GetValue(i)))
                        : new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            UAClient.ConvertToString(values.GetValue(i)))
                        );
                }
                return ret;
            }
            return new BufferedDataPoint[1] { UAClient.IsNumericType(variable.DataType)
                ? new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToDouble(value.Value))
                : new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToString(value.Value)) };
        }
        /// <summary>
        /// Handles notifications on subscribed items, pushes all new datapoints to the queue.
        /// </summary>
        /// <param name="item">Modified item</param>
        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            string uniqueId = UAClient.GetUniqueId(item.ResolvedNodeId);
            if (!buffersEmpty && NotInSync.Contains(uniqueId)) return;
            var variable = ActiveVariables[item.ResolvedNodeId];

            foreach (var datapoint in item.DequeueValues())
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Log.Warning("Bad datapoint: {BadDatapointExternalId}", uniqueId);
                    continue;
                }
                var buffDps = ToDataPoint(datapoint, variable, uniqueId);
                foreach (var buffDp in buffDps)
                {
                    Log.Debug(buffDp.ToDebugDescription());
                }
                foreach (var pusher in pushers)
                {
                    foreach (var buffDp in buffDps)
                    {
                        pusher.BufferedDPQueue.Enqueue(buffDp);
                    }
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
            var variable = ActiveVariables[nodeid];

            if (data.DataValues == null) return;
            foreach (var datapoint in data.DataValues)
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Log.Warning("Bad datapoint: {BadDatapointExternalId}", uniqueId);
                    continue;
                }
                var buffDps = ToDataPoint(datapoint, variable, uniqueId);
                foreach (var buffDp in buffDps)
                {
                    Log.Debug(buffDp.ToDebugDescription());
                }
                foreach (var pusher in pushers)
                {
                    foreach (var buffDp in buffDps)
                    {
                        pusher.BufferedDPQueue.Enqueue(buffDp);
                    }
                }
            }
        }
        #endregion
    }
}

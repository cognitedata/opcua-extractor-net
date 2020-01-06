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
using System.IO;
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
    /// Main extractor class, tying together the <see cref="uaClient"/> and CDF client.
    /// </summary>
    public class Extractor
    {
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        public readonly FailureBuffer FailureBuffer;
        private readonly HistoryReader historyReader;
        public NodeId RootNode { get; private set; }
        private readonly IEnumerable<IPusher> pushers;
        private readonly ConcurrentQueue<BufferedNode> commonQueue = new ConcurrentQueue<BufferedNode>();

        // Concurrent reading of properties
        private readonly HashSet<NodeId> pendingProperties = new HashSet<NodeId>();
        private readonly object propertySetLock = new object();
        private readonly List<Task> propertyReadTasks = new List<Task>();

        public ConcurrentDictionary<NodeId, NodeExtractionState> NodeStates { get; } =
            new ConcurrentDictionary<NodeId, NodeExtractionState>();

        private readonly ConcurrentDictionary<string, NodeExtractionState> nodeStatesByExtId = new ConcurrentDictionary<string, NodeExtractionState>();

        public ConcurrentDictionary<NodeId, EventExtractionState> EmitterStates { get; } =
            new ConcurrentDictionary<NodeId, EventExtractionState>();

        private readonly ConcurrentDictionary<string, EventExtractionState> emitterStatesByExtId =
            new ConcurrentDictionary<string, EventExtractionState>();

        private readonly ConcurrentQueue<NodeId> extraNodesToBrowse = new ConcurrentQueue<NodeId>();
        private bool restart;
        private readonly AutoResetEvent triggerUpdateOperations = new AutoResetEvent(false);

        private readonly object managedNodesLock = new object();
        private HashSet<NodeId> managedNodes = new HashSet<NodeId>();

        private HashSet<NodeId> ignoreDataTypes;

        public ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> ActiveEvents { get; }
            = new ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>>();

        private bool pushEvents;
        private bool pushData;

        public bool Started { get; private set; }
        public bool Pushing { get; private set; }

        private bool nextPushFlag;


        private static readonly Gauge startTime = Metrics
            .CreateGauge("opcua_start_time", "Start time for the extractor");

        public static readonly Counter BadDataPoints = Metrics
            .CreateCounter("opcua_bad_datapoints", "Datapoints skipped due to bad status");

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pushers">List of pushers to be used</param>
        /// <param name="UAClient">UAClient to be used</param>
        public Extractor(FullConfig config, IEnumerable<IPusher> pushers, UAClient uaClient)
        {
            this.pushers = pushers;
            this.uaClient = uaClient;
            FailureBuffer = new FailureBuffer(config.FailureBuffer, this);
            this.config = config;
            this.uaClient.Extractor = this;
            historyReader = new HistoryReader(uaClient, this, pushers, config.History);
            Log.Information("Building extractor with {NumPushers} pushers", pushers.Count());
            foreach (var pusher in pushers)
            {
                pusher.Extractor = this;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pusher">Pusher to be used</param>
        /// <param name="UAClient">UAClient to use</param>
        public Extractor(FullConfig config, IPusher pusher, UAClient uaClient) : this(config, new List<IPusher> { pusher }, uaClient)
        {
        }
        #region Interface

        /// <summary>
        /// Run the extractor, this starts the main MapUAToCDF task, then maintains the data/event push loop.
        /// </summary>
        /// <param name="quitAfterMap">If true, terminate the extractor after first map iteration</param>
        public async Task RunExtractor(CancellationToken token, bool quitAfterMap = false)
        {
            if (!uaClient.Started)
            {
                Log.Information("Start UAClient");
                try
                {
                    await uaClient.Run(token);
                }
                catch (Exception ex)
                {
                    Utils.LogException(ex, "Unexpected error starting UAClient",
                        "Handled service result exception on starting UAClient");
                    throw;
                }

                if (!uaClient.Started)
                {
                    throw new Exception("UAClient failed to start");
                }
            }
            if (config.FailureBuffer.Enabled && config.FailureBuffer.FilePath != null)
            {
                Directory.CreateDirectory(config.FailureBuffer.FilePath);
            }

            ConfigureExtractor(token);

            Started = true;
            startTime.Set(new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds());

            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }

            Log.Debug("Begin mapping directory");
            await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, token);
            Log.Debug("End mapping directory");

            IEnumerable<Task> synchTasks;
            try
            {
                synchTasks = await MapUAToDestinations(token);
            }
            catch (Exception ex)
            {
                Utils.LogException(ex, "Unexpected error in MapUAToDestinations", 
                    "Handled service result exception in MapUAToDestinations");
                throw;
            }

            Pushing = true;

            IEnumerable<Task> tasks = pushers
                .Select(pusher => PusherLoop(pusher, token))
                .Concat(synchTasks).Append(Task.Run(() => ExtraTaskLoop(token), token)).ToList();
            if (config.Extraction.AutoRebrowsePeriod > 0)
            {
                tasks = tasks.Append(Task.Run(() => RebrowseLoop(token))).ToList();
            }

            triggerUpdateOperations.Reset();
            var failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
            if (failedTask != null)
            {
                Utils.LogException(failedTask.Exception, "Unexpected error in main task list", "Handled error in main task list");
            }
            while (tasks.Any() && failedTask == null)
            {

                try
                {
                    var terminated = await Task.WhenAny(tasks);
                    if (terminated.IsFaulted)
                    {
                        Utils.LogException(terminated.Exception, 
                            "Unexpected error in main task list", 
                            "Handled error in main task list");
                    }
                }
                catch (Exception ex)
                {
                    Utils.LogException(ex, "Unexpected error in main task list", "Handled error in main task list");
                }
                failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);

                if (quitAfterMap) return;
                if (failedTask != null) break;
                tasks = tasks
                    .Where(task => !task.IsCompleted && !task.IsFaulted && !task.IsCanceled)
                    .ToList();
            }

            if (token.IsCancellationRequested) throw new TaskCanceledException();
            if (failedTask != null)
            {
                ExceptionDispatchInfo.Capture(failedTask.Exception).Throw();
            }
            throw new Exception("Processes quit without failing");
        }
        /// <summary>
        /// Restarts the extractor, to some extent, clears known asset ids,
        /// allows data to be pushed to CDF, and begins mapping the opcua
        /// directory again
        /// </summary>
        public void RestartExtractor(CancellationToken token)
        {
            restart = true;
            triggerUpdateOperations.Set();
        }
        /// <summary>
        /// Closes the extractor, mainly just shutting down the opcua client.
        /// </summary>
        public void Close()
        {
            if (!uaClient.Started) return;
            try
            {
                uaClient.Close();
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to cleanly shut down UAClient");
            }
            uaClient.WaitForOperations().Wait(10000);
            Log.Information("Extractor closed");
        }

        public string GetUniqueId(NodeId id, int index = -1)
        {
            return uaClient.GetUniqueId(id, index);
        }

        public string ConvertToString(object value)
        {
            return uaClient.ConvertToString(value);
        }
        /// <summary>
        /// Read properties for the given list of BufferedNode. This in intelligent, and keeps track of which properties are in the process of being read,
        /// to prevent multiple pushers from starting PropertyRead operations at the same time. If this is called on a given node twice in short time, the second call
        /// waits on the first.
        /// </summary>
        /// <param name="nodes">Nodes to get properties for</param>
        public async Task ReadProperties(IEnumerable<BufferedNode> nodes, CancellationToken token)
        {
            Task newTask = null;
            List<Task> tasksToWaitFor;
            lock (propertySetLock)
            {
                nodes = nodes.Where(node => !pendingProperties.Contains(node.Id) && !node.PropertiesRead).ToList();
                if (nodes.Any())
                {
                    newTask = Task.Run(() => uaClient.GetNodeProperties(nodes, token));
                    propertyReadTasks.Add(newTask);
                }

                foreach (var node in nodes)
                {
                    pendingProperties.Add(node.Id);
                }

                tasksToWaitFor = propertyReadTasks.ToList();
            }

            await Task.WhenAll(tasksToWaitFor);
            lock (propertySetLock)
            {
                if (newTask != null)
                {
                    propertyReadTasks.Remove(newTask);
                }
                if (!pendingProperties.Any()) return;
                foreach (var node in nodes)
                {
                    node.PropertiesRead = true;
                    pendingProperties.Remove(node.Id);
                }
            }
        }
        /// <summary>
        /// Is the variable allowed to be mapped to a timeseries?
        /// </summary>
        /// <param name="node">Variable to be tested</param>
        /// <returns>True if variable may be mapped to a timeseries</returns>
        public bool AllowTSMap(BufferedVariable node)
        {
            return (!node.DataType.IsString || config.Extraction.AllowStringVariables)
                && (node.ValueRank == ValueRanks.Scalar
                    || config.Extraction.MaxArraySize > 0 && node.ArrayDimensions != null && node.ArrayDimensions.Length == 1
                    && node.ArrayDimensions[0] > 0 && node.ArrayDimensions[0] <= config.Extraction.MaxArraySize)
                && !ignoreDataTypes.Contains(node.DataType.raw);
        }

        public NodeExtractionState GetNodeState(string externalId)
        {
            return nodeStatesByExtId.GetValueOrDefault(externalId);
        }

        public NodeExtractionState GetNodeState(NodeId id)
        {
            return NodeStates.GetValueOrDefault(id);
        }

        public EventExtractionState GetEmitterState(string externalId)
        {
            return emitterStatesByExtId.GetValueOrDefault(externalId);
        }

        public EventExtractionState GetEmitterState(NodeId id)
        {
            return EmitterStates.GetValueOrDefault(id);
        }
        /// <summary>
        /// Wait for the next push of data to CDF
        /// </summary>
        /// <param name="timeout">Timeout in 1/10th of a second</param>
        public async Task WaitForNextPush(int timeout = 100)
        {
            int time = 0;
            while (!nextPushFlag && time++ < timeout) await Task.Delay(100);
            if (time >= timeout && !nextPushFlag)
            {
                throw new Exception("Waiting for push timed out");
            }
        }
        #endregion
        #region Loops

        private async Task PusherLoop(IPusher pusher, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    if (pushData)
                    {
                        var failed = await pusher.PushDataPoints(token);
                        // If failed is null, there were no points, so we can't conclude success or failure
                        if (failed != null && config.FailureBuffer.Enabled)
                        {
                            if (failed.Any())
                            {
                                await FailureBuffer.WriteDatapoints(failed, pusher.Index,
                                    pusher.BaseConfig.NonFiniteReplacement, token);
                            }
                            else if (FailureBuffer.Any)
                            {
                                var recovered = await FailureBuffer.ReadDatapoints(pusher.Index, token);
                                foreach (var dp in recovered)
                                {
                                    pusher.BufferedDPQueue.Enqueue(dp);
                                }
                            }
                        }
                    }
                    if (pushEvents)
                    {
                        var failed = await pusher.PushEvents(token);
                        if (failed != null && config.FailureBuffer.Enabled)
                        {
                            if (failed.Any())
                            {
                                await FailureBuffer.WriteEvents(failed, pusher.Index, token);
                            }
                            else if (FailureBuffer.AnyEvents)
                            {
                                var recovered = await FailureBuffer.ReadEvents(pusher.Index, token);
                                foreach (var evt in recovered)
                                {
                                    pusher.BufferedEventQueue.Enqueue(evt);
                                }
                            }
                        }
                    }

                    nextPushFlag = true;
                    await Task.Delay(pusher.BaseConfig.DataPushDelay, token);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to push on pusher of type {FailedPusherName}", pusher.GetType().Name);
                }
            }
        }
        private async Task RebrowseLoop(CancellationToken token)
        {
            var delay = TimeSpan.FromMinutes(config.Extraction.AutoRebrowsePeriod);
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(delay, token);
                }
                catch (TaskCanceledException)
                {
                    return;
                }

                await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, token);
                var historyTasks = await MapUAToDestinations(token);
                await Task.WhenAll(historyTasks);
            }
        }
        /// <summary>
        /// Waits for triggerUpdateOperations to fire, then executes all the tasks in the queue.
        /// </summary>
        private async Task ExtraTaskLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                WaitHandle.WaitAny(new[] { triggerUpdateOperations, token.WaitHandle });
                if (token.IsCancellationRequested) break;
                var tasks = new List<Task>();
                if (restart)
                {
                    extraNodesToBrowse.Clear();
                    tasks.Add(Task.Run(async () =>
                    {
                        Started = false;
                        await uaClient.WaitForOperations();
                        ConfigureExtractor(token);
                        uaClient.ResetVisitedNodes();
                        await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, token);
                        var synchTasks = await MapUAToDestinations(token);
                        await Task.WhenAll(synchTasks);
                        Started = true;
                        Log.Information("Successfully restarted extractor");
                    }));
                    restart = false;
                } 
                else if (extraNodesToBrowse.Any())
                {
                    var nodesToBrowse = new List<NodeId>();
                    while (extraNodesToBrowse.TryDequeue(out NodeId id))
                    {
                        nodesToBrowse.Add(id);
                    }

                    tasks.Add(Task.Run(async () =>
                    {
                        await uaClient.BrowseNodeHierarchy(nodesToBrowse.Distinct(), HandleNode, token);
                        var historyTasks = await MapUAToDestinations(token);
                        await Task.WhenAll(historyTasks);
                    }));
                }
                await Task.WhenAll(tasks);
            }
        }
        #endregion
        #region Mapping
        /// <summary>
        /// Set up extractor once UAClient is started
        /// </summary>
        private void ConfigureExtractor(CancellationToken token)
        {
            RootNode = config.Extraction.RootNode.ToNodeId(uaClient, ObjectIds.ObjectsFolder);

            ignoreDataTypes = config.Extraction.IgnoreDataTypes != null
                ? config.Extraction.IgnoreDataTypes.Select(proto => proto.ToNodeId(uaClient)).ToHashSet()
                : new HashSet<NodeId>();

            if (config.Extraction.NodeMap != null)
            {
                foreach (var kvp in config.Extraction.NodeMap)
                {
                    uaClient.AddNodeOverride(kvp.Value.ToNodeId(uaClient), kvp.Key);
                }
            }

            foreach (var state in NodeStates.Values)
            {
                state.ClearIsStreaming();
            }

            if (config.Events.EmitterIds != null && config.Events.EventIds != null && config.Events.EmitterIds.Any() && config.Events.EventIds.Any())
            {
                pushEvents = true;
                var emitters = config.Events.EmitterIds.Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)).ToList();
                foreach (var id in emitters)
                {
                    EmitterStates[id] = new EventExtractionState(id);
                    emitterStatesByExtId[uaClient.GetUniqueId(id)] = EmitterStates[id];
                }
                if (config.Events.HistorizingEmitterIds != null && config.Events.HistorizingEmitterIds.Any())
                {
                    var histEmitters = config.Events.HistorizingEmitterIds.Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)).ToList();
                    foreach (var id in histEmitters)
                    {
                        if (!EmitterStates.ContainsKey(id)) throw new Exception("Historical emitter not in emitter list");
                        EmitterStates[id].Historizing = true;
                    }
                }
                var eventFields = uaClient.GetEventFields(config.Events.EventIds.Select(proto => proto.ToNodeId(uaClient, ObjectTypeIds.BaseEventType)).ToList(), token);
                foreach (var field in eventFields)
                {
                    ActiveEvents[field.Key] = field.Value;
                }
            }
        }
        /// <summary>
        /// Read nodes from commonQueue and sort them into lists of context objects, destination timeseries and source variables
        /// </summary>
        /// <param name="objects">List of destination context objects</param>
        /// <param name="timeseries">List of destination timeseries</param>
        /// <param name="variables">List of source variables</param>
        private void GetNodesFromQueue(CancellationToken token, out List<BufferedNode> objects, out List<BufferedVariable> timeseries, out List<BufferedVariable> variables)
        {
            objects = new List<BufferedNode>();
            timeseries = new List<BufferedVariable>();
            variables = new List<BufferedVariable>();
            var rawVariables = new List<BufferedVariable>();
            var nodeMap = new Dictionary<string, BufferedNode>();

            while (commonQueue.TryDequeue(out BufferedNode buffer))
            {
                if (buffer.IsVariable && buffer is BufferedVariable buffVar)
                {
                    if (buffVar.IsProperty)
                    {
                        nodeMap.TryGetValue(uaClient.GetUniqueId(buffVar.ParentId), out BufferedNode parent);
                        if (parent == null) continue;
                        if (parent.Properties == null)
                        {
                            parent.Properties = new List<BufferedVariable>();
                        }
                        parent.Properties.Add(buffVar);
                    }
                    else
                    {
                        rawVariables.Add(buffVar);
                    }
                }
                else
                {
                    objects.Add(buffer);
                }
                nodeMap.Add(uaClient.GetUniqueId(buffer.Id), buffer);
            }
            Log.Information("Getting data for {NumVariables} variables and {NumObjects} objects", 
                rawVariables.Count, objects.Count);
            uaClient.ReadNodeData(objects.Concat(rawVariables), token);
            foreach (var node in objects.Concat(rawVariables))
            {
                Log.Debug(node.ToDebugDescription());
            }

            foreach (var node in rawVariables)
            {
                if (AllowTSMap(node))
                {
                    variables.Add(node);
                    NodeStates[node.Id] = new NodeExtractionState(node);
                    if (node.ArrayDimensions != null && node.ArrayDimensions.Length > 0 && node.ArrayDimensions[0] > 0)
                    {
                        for (int i = 0; i < node.ArrayDimensions[0]; i++)
                        {
                            timeseries.Add(new BufferedVariable(node, i));
                            nodeStatesByExtId[uaClient.GetUniqueId(node.Id, i)] = NodeStates[node.Id];
                        }
                        objects.Add(node);
                    }
                    else
                    {
                        nodeStatesByExtId[uaClient.GetUniqueId(node.Id)] = NodeStates[node.Id];
                        timeseries.Add(node);
                    }
                }
            }
        }
        /// <summary>
        /// Push given lists of nodes to pusher destinations, and fetches latest timestamp for relevant nodes.
        /// </summary>
        /// <param name="objects">Objects to synchronize with destinations</param>
        /// <param name="timeseries">Variables to synchronize with destinations</param>
        private async Task PushNodes(IEnumerable<BufferedNode> objects, IEnumerable<BufferedVariable> timeseries, CancellationToken token)
        {
            var pushes = pushers.Select(pusher => pusher.PushNodes(objects, timeseries, token)).ToList();
            var pushResult = await Task.WhenAll(pushes);
            if (!pushResult.All(res => res)) throw new Exception("Pushing nodes failed");

            var statesToSync = timeseries
                .Select(ts => ts.Id)
                .Distinct()
                .Select(id => NodeStates[id])
                .Where(state => state.Historizing);

            var getRangePushes = pushers.Select(pusher => pusher.InitExtractedRanges(statesToSync, config.History.Backfill, token));

            if (EmitterStates.Any(kvp => kvp.Value.Historizing))
            {
                var getEventRanges = pushers.Select(pusher =>
                    pusher.InitExtractedEventRanges(EmitterStates.Values.Where(state => state.Historizing),
                        timeseries.Concat(objects).Select(ts => ts.Id).Distinct(), config.History.Backfill, token));
                getRangePushes = getRangePushes.Concat(getEventRanges);
            }


            var getRangeResult = await Task.WhenAll(getRangePushes);
            if (!getRangeResult.All(res => res)) throw new Exception("Getting latest timestamp failed");
            // If any nodes are still at default values, meaning no pusher can initialize them, initialize to default values.
            foreach (var state in statesToSync)
            {
                if (state.ExtractedRange.Start == DateTime.MinValue && state.ExtractedRange.End == DateTime.MaxValue)
                {
                    if (config.History.Backfill)
                    {
                        state.ExtractedRange.Start = DateTime.UtcNow;
                        state.ExtractedRange.End = DateTime.UtcNow;
                    }
                    else
                    {
                        state.ExtractedRange.End = DateTime.MinValue;
                    }
                }
            }

            foreach (var state in EmitterStates.Values.Where(state => state.Historizing))
            {
                if (state.ExtractedRange.Start == DateTime.MinValue && state.ExtractedRange.End == DateTime.MaxValue)
                {
                    if (config.History.Backfill)
                    {
                        state.ExtractedRange.Start = DateTime.UtcNow;
                        state.ExtractedRange.End = DateTime.UtcNow;
                    }
                    else
                    {
                        state.ExtractedRange.End = DateTime.MinValue;
                    }
                }
            }

        }

        private async Task SynchronizeEvents(IEnumerable<NodeId> nodes, CancellationToken token)
        {
            await Task.Run(() => uaClient.SubscribeToEvents(EmitterStates.Keys, nodes, EventSubscriptionHandler, token));
            await historyReader.FrontfillEvents(EmitterStates.Values.Where(state => state.Historizing), nodes, token);
            if (config.History.Backfill)
            {
                await historyReader.BackfillEvents(EmitterStates.Values.Where(state => state.Historizing), nodes, token);
            }
        }

        private async Task SynchronizeNodes(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            await Task.Run(() => uaClient.SubscribeToNodes(states, DataSubscriptionHandler, token));
            await historyReader.FrontfillData(states.Where(state => state.Historizing), token);
            if (config.History.Backfill)
            {
                await historyReader.BackfillData(states.Where(state => state.Historizing), token);
            }
        }
        /// <summary>
        /// Start synchronization of given list of variables with the server.
        /// </summary>
        /// <param name="variables">Variables to synchronize</param>
        /// <param name="objects">Recently added objects, used for event subscriptions</param>
        /// <returns>Two tasks, one for data and one for events</returns>
        private IEnumerable<Task> Synchronize(IEnumerable<BufferedVariable> variables, IEnumerable<BufferedNode> objects, CancellationToken token)
        {
            var states = variables.Select(ts => ts.Id).Distinct().Select(id => NodeStates[id]);

            Log.Information("Synchronize {NumNodesToSynch} nodes", variables.Count());
            var tasks = new List<Task>();
            // Create tasks to subscribe to nodes, then start history read. We might lose data if history read finished before subscriptions were created.
            if (states.Any())
            {
                tasks.Add(SynchronizeNodes(states, token));
            }
            if (EmitterStates.Any())
            {
                tasks.Add(SynchronizeEvents(objects.Concat(variables).Select(node => node.Id).Distinct().ToList(), token));
            }

            if (config.Extraction.EnableAuditDiscovery)
            {
                uaClient.SubscribeToAuditEvents(AuditEventSubscriptionHandler);
            }
            return tasks;
        }
        /// <summary>
        /// Empties the node queue, pushing nodes to each destination, and starting subscriptions and history.
        /// </summary>
        private async Task<IEnumerable<Task>> MapUAToDestinations(CancellationToken token)
        {
            GetNodesFromQueue(token, out var objects, out var timeseries, out var variables);

            if (!objects.Any() && !timeseries.Any() && !variables.Any())
            {
                Log.Information("Mapping resulted in no new nodes");
                return Array.Empty<Task>();
            }

            pushData = NodeStates.Any();

            await PushNodes(objects, timeseries, token);

            lock (managedNodesLock)
            {
                managedNodes = managedNodes.Concat(variables.Concat(objects).Select(node => node.Id)).ToHashSet();
            }

            return Synchronize(variables, objects, token);
        }
        #endregion

        #region Handlers
        /// <summary>
        /// Callback for the browse operation, creates <see cref="BufferedNode"/>s and enqueues them.
        /// </summary>
        /// <param name="node">Description of the node to be handled</param>
        /// <param name="parentId">Id of the parent node</param>
        private void HandleNode(ReferenceDescription node, NodeId parentId)
        {
            Log.Verbose("HandleNode {parent} {node}", parentId, node);

            if (node.NodeClass == NodeClass.Object)
            {
                var bufferedNode = new BufferedNode(uaClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                Log.Verbose("HandleNode Object {name}", bufferedNode.DisplayName);
                commonQueue.Enqueue(bufferedNode);
            }
            else if (node.NodeClass == NodeClass.Variable)
            {
                var bufferedNode = new BufferedVariable(uaClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                if (node.TypeDefinition == VariableTypeIds.PropertyType)
                {
                    bufferedNode.IsProperty = true;
                }
                Log.Verbose("HandleNode Variable {name}", bufferedNode.DisplayName);
                commonQueue.Enqueue(bufferedNode);
            }
        }
        /// <summary>
        /// Transform a given DataValue into a datapoint or a list of datapoints if the variable in question has array type.
        /// </summary>
        /// <param name="value">DataValue to be transformed</param>
        /// <param name="variable">NodeExtractionState for variable the datavalue belongs to</param>
        /// <param name="uniqueId"></param>
        /// <returns>UniqueId to be used, for efficiency</returns>
        public IEnumerable<BufferedDataPoint> ToDataPoint(DataValue value, NodeExtractionState variable, string uniqueId)
        {
            if (variable.ArrayDimensions != null && variable.ArrayDimensions.Length > 0 && variable.ArrayDimensions[0] > 0)
            {
                var ret = new List<BufferedDataPoint>();
                if (!(value.Value is Array))
                {
                    BadDataPoints.Inc();
                    Log.Debug("Bad array datapoint: {BadPointName} {BadPointValue}", uniqueId, value.Value.ToString());
                    return Enumerable.Empty<BufferedDataPoint>();
                }
                var values = (Array)value.Value;
                for (int i = 0; i < Math.Min(variable.ArrayDimensions[0], values.Length); i++)
                {
                    var dp = variable.DataType.IsString
                        ? new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            uaClient.ConvertToString(values.GetValue(i)))
                        : new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            UAClient.ConvertToDouble(values.GetValue(i)));
                    ret.Add(dp);
                }
                return ret;
            }
            var sdp = variable.DataType.IsString
                ? new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    uaClient.ConvertToString(value.Value))
                : new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToDouble(value.Value));
            return new[] { sdp };
        }
        /// <summary>
        /// Handles notifications on subscribed items, pushes all new datapoints to the queue.
        /// </summary>
        /// <param name="item">Modified item</param>
        private void DataSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            string uniqueId = uaClient.GetUniqueId(item.ResolvedNodeId);
            var node = NodeStates[item.ResolvedNodeId];
            
            foreach (var datapoint in item.DequeueValues())
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    BadDataPoints.Inc();
                    Log.Debug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId, datapoint.SourceTimestamp);
                    continue;
                }
                var buffDps = ToDataPoint(datapoint, node, uniqueId);
                node.UpdateFromStream(buffDps);
                if (!node.IsStreaming) return;
                foreach (var buffDp in buffDps)
                {
                    Log.Verbose("Subscription DataPoint {dp}", buffDp.ToDebugDescription());
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
        /// Construct event from filter and collection of event fields
        /// </summary>
        /// <param name="filter">Filter that resulted in this event</param>
        /// <param name="eventFields">Fields for a single event</param>
        /// <returns></returns>
        public BufferedEvent ConstructEvent(EventFilter filter, VariantCollection eventFields, NodeId emitter)
        {
            int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                       && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                Log.Warning("Triggered event has no type, ignoring.");
                return null;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            // Many servers don't handle filtering on history data.
            if (eventType == null || !ActiveEvents.ContainsKey(eventType))
            {
                Log.Verbose("Invalid event type: {eventType}", eventType);
                return null;
            }
            var targetEventFields = ActiveEvents[eventType];

            var extractedProperties = new Dictionary<string, object>();

            for (int i = 0; i < filter.SelectClauses.Count; i++)
            {
                var clause = filter.SelectClauses[i];
                if (!targetEventFields.Any(field =>
                    field.Item1 == clause.TypeDefinitionId
                    && field.Item2 == clause.BrowsePath[0]
                    && clause.BrowsePath.Count == 1)) continue;

                string name = clause.BrowsePath[0].Name;
                if (config.Events.ExcludeProperties.Contains(name) || config.Events.BaseExcludeProperties.Contains(name)) continue;
                if (config.Events.DestinationNameMap.ContainsKey(name) && name != "EventId" && name != "SourceNode" && name != "EventType")
                {
                    name = config.Events.DestinationNameMap[name];
                }
                if (!extractedProperties.ContainsKey(name) || extractedProperties[name] == null)
                {
                    extractedProperties[name] = eventFields[i].Value;
                }
            }
            try
            {
                var sourceNode = extractedProperties["SourceNode"];
                if (!managedNodes.Contains(sourceNode))
                {
                    Log.Verbose("Invalid source node for event of type: {type}", eventType);
                    return null;
                }
                var buffEvent = new BufferedEvent
                {
                    Message = uaClient.ConvertToString(extractedProperties.GetValueOrDefault("Message")),
                    EventId = config.Extraction.IdPrefix + Convert.ToBase64String((byte[])extractedProperties["EventId"]),
                    SourceNode = (NodeId)extractedProperties["SourceNode"],
                    Time = (DateTime)extractedProperties.GetValueOrDefault("Time"),
                    EventType = (NodeId)extractedProperties["EventType"],
                    MetaData = extractedProperties
                        .Where(kvp => kvp.Key != "Message" && kvp.Key != "EventId" && kvp.Key != "SourceNode"
                                      && kvp.Key != "Time" && kvp.Key != "EventType")
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                    EmittingNode = emitter,
                    ReceivedTime = DateTime.UtcNow,
                };
                return buffEvent;
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to construct bufferedEvent from raw fields");
                return null;
            }
        }
        /// <summary>
        /// Handle subscription callback for events
        /// </summary>
        private void EventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (!(eventArgs.NotificationValue is EventFieldList triggeredEvent))
            {
                Log.Warning("No event in event subscription notification: {}", item.StartNodeId);
                return;
            }
            var eventFields = triggeredEvent.EventFields;
            if (!(item.Filter is EventFilter filter))
            {
                Log.Warning("Triggered event without filter");
                return;
            }
            var buffEvent = ConstructEvent(filter, eventFields, item.ResolvedNodeId);
            if (buffEvent == null) return;
            var eventState = EmitterStates[item.ResolvedNodeId];
            eventState.UpdateFromStream(buffEvent);

            // Either backfill/frontfill is done, or we are not outside of each respective bound
            if (!((eventState.IsStreaming || buffEvent.Time < eventState.ExtractedRange.End)
                  && (eventState.BackfillDone || buffEvent.Time > eventState.ExtractedRange.Start))) return;

            Log.Verbose(buffEvent.ToDebugDescription());
            foreach (var pusher in pushers)
            {
                pusher.BufferedEventQueue.Enqueue(buffEvent);
            }
        }
        /// <summary>
        /// Handle subscription callback for audit events (AddReferences/AddNodes). Triggers partial re-browse when necessary
        /// </summary>
        private void AuditEventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (!(eventArgs.NotificationValue is EventFieldList triggeredEvent))
            {
                Log.Warning("No event in event subscription notification: {}", item.StartNodeId);
                return;
            }

            var eventFields = triggeredEvent.EventFields;
            if (!(item.Filter is EventFilter filter))
            {
                Log.Warning("Triggered event without filter");
                return;
            }
            int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                       && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                Log.Warning("Triggered event has no type, ignoring");
                return;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            if (eventType == null || eventType != ObjectTypeIds.AuditAddNodesEventType && eventType != ObjectTypeIds.AuditAddReferencesEventType)
            {
                Log.Warning("Non-audit event triggered on audit event listener");
                return;
            }

            if (eventType == ObjectTypeIds.AuditAddNodesEventType)
            {
                // This is a neat way to get the contents of the event, which may be fairly complicated (variant of arrays of extensionobjects)
                var e = new AuditAddNodesEventState(null);
                e.Update(uaClient.GetSystemContext(), filter.SelectClauses, triggeredEvent);
                if (e.NodesToAdd?.Value == null)
                {
                    Log.Warning("Missing NodesToAdd object on AddNodes event");
                    return;
                }

                var addedNodes = e.NodesToAdd.Value;

                var relevantIds = addedNodes.Where(added => added != null &&
                    (added.NodeClass == NodeClass.Variable || added.NodeClass == NodeClass.Object)
                    && (added.TypeDefinition != VariableTypeIds.PropertyType)
                    && (managedNodes.Contains(uaClient.ToNodeId(added.ParentNodeId))))
                    .Select(added => uaClient.ToNodeId(added.ParentNodeId))
                    .Distinct();
                if (!relevantIds.Any()) return;
                Log.Information("Trigger rebrowse on {numnodes} node ids due to addNodes event", relevantIds.Count());

                foreach (var id in relevantIds)
                {
                    extraNodesToBrowse.Enqueue(id);
                }
                triggerUpdateOperations.Set();
                return;
            }

            var ev = new AuditAddReferencesEventState(null);
            ev.Update(uaClient.GetSystemContext(), filter.SelectClauses, triggeredEvent);

            if (ev.ReferencesToAdd?.Value == null)
            {
                Log.Warning("Missing ReferencesToAdd object on AddReferences event");
                return;
            }

            var addedReferences = ev.ReferencesToAdd.Value;

            var relevantRefIds = addedReferences.Where(added =>
                (added.IsForward && managedNodes.Contains(uaClient.ToNodeId(added.SourceNodeId))))
                .Select(added => uaClient.ToNodeId(added.SourceNodeId))
                .Distinct();

            if (!relevantRefIds.Any()) return;

            Log.Information("Trigger rebrowse on {numnodes} node ids due to addReference event", relevantRefIds.Count());

            foreach (var id in relevantRefIds)
            {
                extraNodesToBrowse.Enqueue(id);
            }
            triggerUpdateOperations.Set();
        }
        #endregion
    }
}

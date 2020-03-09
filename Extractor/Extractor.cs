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
    /// Main extractor class, tying together the <see cref="uaClient"/> and CDF client.
    /// </summary>
    public class Extractor : IDisposable
    {
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        public FailureBuffer FailureBuffer { get; }
        public StateStorage StateStorage { get; }
        public State State { get; }
        public Streamer Streamer { get; }

        private readonly HistoryReader historyReader;
        public NodeId RootNode { get; private set; }
        private readonly IEnumerable<IPusher> pushers;
        private readonly ConcurrentQueue<BufferedNode> commonQueue = new ConcurrentQueue<BufferedNode>();

        private readonly ConcurrentDictionary<(NodeId, int), BufferedNode> activeNodes =
            new ConcurrentDictionary<(NodeId, int), BufferedNode>();

        // Concurrent reading of properties
        private readonly HashSet<NodeId> pendingProperties = new HashSet<NodeId>();
        private readonly object propertySetLock = new object();
        private readonly List<Task> propertyReadTasks = new List<Task>();

        private readonly ConcurrentQueue<NodeId> extraNodesToBrowse = new ConcurrentQueue<NodeId>();
        private bool restart;
        private bool quit;
        private readonly AutoResetEvent triggerUpdateOperations = new AutoResetEvent(false);
        private readonly ManualResetEvent triggerHistoryRestart = new ManualResetEvent(false);

        private HashSet<NodeId> ignoreDataTypes;

        public bool Started { get; private set; }
        public bool Pushing { get; private set; }

        private bool nextPushFlag;


        private static readonly Gauge startTime = Metrics
            .CreateGauge("opcua_start_time", "Start time for the extractor");

        public static readonly Counter BadDataPoints = Metrics
            .CreateCounter("opcua_bad_datapoints", "Datapoints skipped due to bad status");

        public static readonly Gauge Starting = Metrics
            .CreateGauge("opcua_extractor_starting", "1 if the extractor is in the startup phase");

        private static readonly Gauge trackedAssets = Metrics
            .CreateGauge("opcua_tracked_assets", "Number of objects on the opcua server mapped to assets");

        private static readonly Gauge trackedTimeseres = Metrics
            .CreateGauge("opcua_tracked_timeseries", "Number of variables on the opcua server mapped to timeseries");

        private static readonly ILogger log = Log.Logger.ForContext(typeof(Extractor));

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pushers">List of pushers to be used</param>
        /// <param name="UAClient">UAClient to be used</param>
        public Extractor(FullConfig config, IEnumerable<IPusher> pushers, UAClient uaClient)
        {
            this.pushers = pushers ?? throw new ArgumentNullException(nameof(pushers));
            this.uaClient = uaClient ?? throw new ArgumentNullException(nameof(uaClient));
            this.config = config ?? throw new ArgumentNullException(nameof(config));

            State = new State(this);
            Streamer = new Streamer(this, config);

            if (!string.IsNullOrWhiteSpace(config.StateStorage.Location))
            {
                StateStorage = new StateStorage(this, config);
            }

            if (config.FailureBuffer.Enabled)
            {
                FailureBuffer = new FailureBuffer(config, this);
            }
            this.uaClient.Extractor = this;
            historyReader = new HistoryReader(uaClient, this, config.History);
            log.Information("Building extractor with {NumPushers} pushers", pushers.Count());
            if (config.Extraction.IdPrefix == "events.")
            {
                throw new ConfigurationException("Avoid using \"events.\" as IdPrefix, as it is used internally");
            }
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
            Starting.Set(1);
            if (!uaClient.Started)
            {
                log.Information("Start UAClient");
                try
                {
                    await uaClient.Run(token);
                }
                catch (Exception ex)
                {
                    ExtractorUtils.LogException(ex, "Unexpected error starting UAClient",
                        "Handled service result exception on starting UAClient");
                    throw;
                }

                if (!uaClient.Started)
                {
                    throw new ExtractorFailureException("UAClient failed to start");
                }
            }

            ConfigureExtractor(token);

            Started = true;
            startTime.Set(new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds());

            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }

            log.Debug("Begin mapping directory");
            await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, token);
            log.Debug("End mapping directory");

            IEnumerable<Task> synchTasks;
            try
            {
                synchTasks = await MapUAToDestinations(token);
            }
            catch (Exception ex)
            {
                ExtractorUtils.LogException(ex, "Unexpected error in MapUAToDestinations", 
                    "Handled service result exception in MapUAToDestinations");
                throw;
            }

            if (config.FailureBuffer.Enabled)
            {
                await FailureBuffer.InitializeBufferStates(State.NodeStates, State.AllActiveIds, token);
            }

            Pushing = true;

            var tasks = new List<Task>
                {
                    Task.Run(async () => await PushersLoop(token), token),
                    Task.Run(async () => await ExtraTaskLoop(token), token)
                }.Concat(synchTasks);

            if (config.Extraction.AutoRebrowsePeriod > 0)
            {
                tasks = tasks.Append(Task.Run(() => RebrowseLoop(token)));
            }

            if (StateStorage != null && config.StateStorage.Interval > 0)
            {
                tasks = tasks.Append(Task.Run(() => StoreStateLoop(token)));
            }

            tasks = tasks.Append(Task.Run(() => WaitHandle.WaitAny(
                new[] {triggerHistoryRestart, token.WaitHandle})));

            tasks = tasks.ToList();

            triggerUpdateOperations.Reset();
            var failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
            if (failedTask != null)
            {
                ExtractorUtils.LogException(failedTask.Exception, "Unexpected error in main task list", "Handled error in main task list");
            }

            while (tasks.Any() && failedTask == null)
            {

                try
                {
                    var terminated = await Task.WhenAny(tasks);
                    if (terminated.IsFaulted)
                    {
                        ExtractorUtils.LogException(terminated.Exception, 
                            "Unexpected error in main task list", 
                            "Handled error in main task list");
                    }
                }
                catch (Exception ex)
                {
                    ExtractorUtils.LogException(ex, "Unexpected error in main task list", "Handled error in main task list");
                }
                failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);

                if (quitAfterMap) return;
                if (failedTask != null) break;
                tasks = tasks
                    .Where(task => !task.IsCompleted && !task.IsFaulted && !task.IsCanceled)
                    .ToList();

                if (triggerHistoryRestart.WaitOne(0))
                {
                    triggerHistoryRestart.Reset();
                    tasks = tasks.Append(RestartHistory(token)).Append(Task.Run(() => WaitHandle.WaitAny(
                        new[] { triggerHistoryRestart, token.WaitHandle }))).ToList();
                }
            }

            if (token.IsCancellationRequested) throw new TaskCanceledException();
            if (failedTask != null)
            {
                if (failedTask.Exception != null)
                {
                    ExceptionDispatchInfo.Capture(failedTask.Exception).Throw();
                }

                throw new ExtractorFailureException("Task failed without exception");
            }
            throw new ExtractorFailureException("Processes quit without failing");
        }
        /// <summary>
        /// Restarts the extractor, to some extent, clears known asset ids,
        /// allows data to be pushed to CDF, and begins mapping the opcua
        /// directory again
        /// </summary>
        public void RestartExtractor()
        {
            historyReader.Terminate(CancellationToken.None, 30).Wait();
            foreach (var state in State.NodeStates) {
                state.ResetStreamingState();
            }

            foreach (var state in State.EmitterStates)
            {
                state.ResetStreamingState();
            }

            WaitForNextPush().Wait();
            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }
            Starting.Set(1);
            restart = true;
            triggerUpdateOperations.Set();
        }

        public void QuitExtractorInternally()
        {
            quit = true;
            triggerUpdateOperations.Set();
        }

        public Task<bool> TerminateHistory(int timeout, CancellationToken token)
        {
            return historyReader.Terminate(token, timeout);
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
            catch (ServiceResultException e)
            {
                log.Error(e, "Failed to cleanly shut down UAClient");
            }
            uaClient.WaitForOperations().Wait(10000);
            log.Information("Extractor closed");
        }

        public string GetUniqueId(NodeId id, int index = -1)
        {
            if (index == -1)
            {
                return State.GetUniqueId(id) ?? uaClient.GetUniqueId(id, -1);
            }

            return uaClient.GetUniqueId(id, index);
        }

        public string ConvertToString(object value)
        {
            return uaClient.ConvertToString(value);
        }
        /// <summary>
        /// Read properties for the given list of BufferedNode. This is intelligent, and keeps track of which properties are in the process of being read,
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
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (node.DataType.IsString && !config.Extraction.AllowStringVariables) return false;
            if (ignoreDataTypes.Contains(node.DataType.Raw)) return false;
            if (node.ValueRank == ValueRanks.Scalar) return true;

            if (node.ArrayDimensions != null && node.ArrayDimensions.Count == 1)
            {
                int length = node.ArrayDimensions.First();
                return config.Extraction.MaxArraySize < 0 || length > 0 && length <= config.Extraction.MaxArraySize;
            }

            return false;
        }
        /// <summary>
        /// Wait for the next push of data to CDF
        /// </summary>
        /// <param name="timeout">Timeout in 1/10th of a second</param>
        public async Task WaitForNextPush(int timeout = 100)
        {
            nextPushFlag = false;
            int time = 0;
            while (!nextPushFlag && time++ < timeout) await Task.Delay(100);
            if (time >= timeout && !nextPushFlag)
            {
                throw new TimeoutException("Waiting for push timed out");
            }
            log.Debug("Waited {s} milliseconds for push", time * 100);
        }
        #endregion
        #region Loops

        private async Task RestartHistory(CancellationToken token)
        {
            await Task.WhenAll(Task.Run(async () =>
            {
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.Historizing),
                    State.AllActiveIds.ToList(), token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillEvents(
                        State.EmitterStates.Where(state => state.Historizing),
                        State.AllActiveIds.ToList(), token);
                }
            }), Task.Run(async () =>
            {
                await historyReader.FrontfillData(
                    State.NodeStates.Where(state => state.Historizing).ToList(), token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillData(
                        State.NodeStates.Where(state => state.Historizing).ToList(), token);
                }
            }));
        }

        private async Task PushersLoop(CancellationToken token)
        {
            var failingPushers = pushers.Where(pusher => pusher.DataFailing || pusher.EventsFailing || !pusher.Initialized).ToList();
            var passingPushers = pushers.Except(failingPushers).ToList();
            while (!token.IsCancellationRequested)
            {
                if (failingPushers.Any())
                {
                    var result = await Task.WhenAll(failingPushers.Select(pusher => pusher.TestConnection(config, token)));
                    var recovered = result.Select((res, idx) => (result: res, pusher: failingPushers.ElementAt(idx)))
                        .Where(x => x.result == true).ToList();

                    if (recovered.Any(pair => !pair.pusher.Initialized))
                    {
                        var toInit = recovered.Select(pair => pair.pusher).Where(pusher => !pusher.Initialized);
                        var (nodes, timeseries) = SortNodes(activeNodes.Values);
                        await Task.WhenAll(toInit.Select(pusher => PushNodes(nodes, timeseries, pusher, true, token)));
                    }
                    foreach (var pair in recovered)
                    {
                        if (pair.pusher.Initialized)
                        {
                            failingPushers.Remove(pair.pusher);
                            passingPushers.Add(pair.pusher);
                        }
                    }
                }


                var waitTask = Task.Delay(config.Extraction.DataPushDelay, token);
                var results = await Task.WhenAll(Task.Run(async () =>
                        await Streamer.PushDataPoints(passingPushers, failingPushers, token), token),
                    Task.Run(async () => await Streamer.PushEvents(passingPushers, failingPushers, token), token));

                if (results.Any(res => res))
                {
                    triggerHistoryRestart.Set();
                }

                foreach (var pusher in passingPushers.Where(pusher =>
                    pusher.DataFailing && Streamer.AllowData || pusher.EventsFailing && Streamer.AllowEvents || !pusher.Initialized).ToList())
                {
                    failingPushers.Add(pusher);
                    passingPushers.Remove(pusher);
                }

                await waitTask;
                nextPushFlag = true;

            }
        }

        private async Task StoreStateLoop(CancellationToken token)
        {
            var delay = TimeSpan.FromSeconds(config.StateStorage.Interval);
            while (!token.IsCancellationRequested)
            {
                await Task.WhenAll(
                    Task.Delay(delay, token),
                    StateStorage.StoreExtractionState(State.NodeStates.Where(state => state.Historizing), StateStorage.VariableStates, token),
                    StateStorage.StoreExtractionState(State.EmitterStates.Where(state => state.Historizing), StateStorage.EmitterStates, token)
                );
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
                if (quit)
                {
                    log.Warning("Manually quitting extractor due to error in subsystem");
                    throw new ExtractorFailureException("Manual exit due to error in subsystem");
                }
                if (restart)
                {
                    log.Information("Restarting extractor...");
                    extraNodesToBrowse.Clear();
                    tasks.Add(Task.Run(async () =>
                    {
                        Started = false;
                        await historyReader.Terminate(token, 30);
                        await uaClient.WaitForOperations();
                        ConfigureExtractor(token);
                        uaClient.ResetVisitedNodes();
                        await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, token);
                        var synchTasks = await MapUAToDestinations(token);
                        await Task.WhenAll(synchTasks);
                        Started = true;
                        log.Information("Successfully restarted extractor");
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

            foreach (var state in State.NodeStates)
            {
                state.ResetStreamingState();
                if (!config.History.Enabled || !config.History.Backfill)
                {
                    state.BackfillDone = true;
                }
            }

            foreach (var state in State.EmitterStates)
            {
                state.ResetStreamingState();
                if (!config.History.Enabled || !config.History.Backfill)
                {
                    state.BackfillDone = true;
                }
            }

            if (config.Events.EmitterIds != null
                && config.Events.EventIds != null
                && config.Events.EmitterIds.Any()
                && config.Events.EventIds.Any())
            {
                Streamer.AllowEvents = true;
                var emitters = config.Events.EmitterIds.Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)).ToList();
                foreach (var id in emitters)
                {
                    State.SetEmitterState(new EventExtractionState(id));
                }
                if (config.Events.HistorizingEmitterIds != null && config.Events.HistorizingEmitterIds.Any())
                {
                    var histEmitters = config.Events.HistorizingEmitterIds.Select(proto =>
                        proto.ToNodeId(uaClient, ObjectIds.Server)).ToList();
                    foreach (var id in histEmitters)
                    {
                        var state = State.GetEmitterState(id);
                        if (state == null) throw new ConfigurationException("Historical emitter not in emitter list");
                        state.Historizing = true;
                    }
                }
                var eventFields = uaClient.GetEventFields(config.Events.EventIds.Select(proto => proto.ToNodeId(uaClient, ObjectTypeIds.BaseEventType)).ToList(), token);
                foreach (var field in eventFields)
                {
                    State.ActiveEvents[field.Key] = field.Value;
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
                nodeMap.Add(GetUniqueId(buffer.Id), buffer);
            }
            log.Information("Getting data for {NumVariables} variables and {NumObjects} objects", 
                rawVariables.Count, objects.Count);
            uaClient.ReadNodeData(objects.Concat(rawVariables), token);
            foreach (var node in objects)
            {
                log.Debug(node.ToDebugDescription());
                activeNodes[(node.Id, -1)] = node;
            }

            foreach (var node in rawVariables)
            {
                log.Debug(node.ToDebugDescription());
                if (AllowTSMap(node))
                {
                    variables.Add(node);
                    var state = new NodeExtractionState(node);
                    activeNodes[(node.Id, node.Index)] = node;
                    if (node.ArrayDimensions != null && node.ArrayDimensions.Count > 0 && node.ArrayDimensions[0] > 0)
                    {
                        for (int i = 0; i < node.ArrayDimensions[0]; i++)
                        {
                            var ts = new BufferedVariable(node, i);
                            timeseries.Add(ts);
                            activeNodes[(ts.Id, ts.Index)] = ts;
                            var uniqueId = GetUniqueId(node.Id, i);
                            State.SetNodeState(state, uniqueId);
                            State.RegisterNode(node.Id, uniqueId);
                        }
                        objects.Add(node);
                    }
                    else
                    {
                        State.SetNodeState(state);
                        timeseries.Add(node);
                    }
                }
            }
        }

        private static (IEnumerable<BufferedNode>, IEnumerable<BufferedVariable>) SortNodes(IEnumerable<BufferedNode> nodes)
        {
            var timeseries = new List<BufferedVariable>();
            var objects = new List<BufferedNode>();
            foreach (var node in nodes)
            {
                if (node.IsVariable && node is BufferedVariable variable)
                {
                    if (variable.ArrayDimensions != null && variable.ArrayDimensions.Count > 0 &&
                        variable.ArrayDimensions[0] > 0 && variable.Index == -1)
                    {
                        objects.Add(variable);
                    }
                    else
                    {
                        timeseries.Add(variable);
                    }
                }
                else
                {
                    objects.Add(node);
                }
            }

            return (objects, timeseries);
        }
        private async Task PushNodes(IEnumerable<BufferedNode> objects, IEnumerable<BufferedVariable> timeseries,
            IPusher pusher, bool initial, CancellationToken token)
        {
            if (pusher.NoInit)
            {
                log.Warning("Skipping pushing on pusher with index {idx}", pusher.Index);
                pusher.Initialized = false;
                pusher.NoInit = false;
                return;
            }
            var result = await pusher.PushNodes(objects, timeseries, token);
            if (!result)
            {
                log.Error("Failed to push nodes on pusher with index {idx}", pusher.Index);
                pusher.Initialized = false;
                pusher.DataFailing = true;
                pusher.EventsFailing = true;
                return;
            }

            if (pusher.BaseConfig.ReadExtractedRanges)
            {
                var statesToSync = timeseries
                    .Select(ts => ts.Id)
                    .Distinct()
                    .Select(id => State.GetNodeState(id))
                    .Where(state => state.Historizing);
                var results = await Task.WhenAll(pusher.InitExtractedRanges(statesToSync, config.History.Backfill, token), 
                    pusher.InitExtractedEventRanges(State.EmitterStates.Where(state => state.Historizing),
                        timeseries.Concat(objects).Select(ts => ts.Id).Distinct(),
                        config.History.Backfill,
                        token));
                if (!results.All(res => res))
                {
                    log.Error("Initialization of extracted ranges failed for pusher with index {idx}", pusher.Index);
                    pusher.Initialized = false;
                    pusher.DataFailing = true;
                    pusher.EventsFailing = true;
                    return;
                }
            }

            pusher.Initialized |= initial;
        }
        /// <summary>
        /// Push given lists of nodes to pusher destinations, and fetches latest timestamp for relevant nodes.
        /// </summary>
        /// <param name="objects">Objects to synchronize with destinations</param>
        /// <param name="timeseries">Variables to synchronize with destinations</param>
        private async Task PushNodes(IEnumerable<BufferedNode> objects, IEnumerable<BufferedVariable> timeseries, CancellationToken token)
        {
            var newStates = timeseries
                .Select(ts => ts.Id)
                .Distinct()
                .Select(id => State.GetNodeState(id));

            bool initial = objects.Count() + timeseries.Count() == activeNodes.Count;

            var pushTasks = pushers.Select(pusher => PushNodes(objects, timeseries, pusher, initial, token));

            if (StateStorage != null && config.StateStorage.Interval > 0)
            {
                if (Streamer.AllowEvents)
                {
                    pushTasks = pushTasks.Append(StateStorage.ReadExtractionStates(
                        State.EmitterStates.Where(state => state.Historizing),
                        StateStorage.EmitterStates,
                        false,
                        token));
                }

                if (Streamer.AllowData)
                {
                    pushTasks = pushTasks.Append(StateStorage.ReadExtractionStates(
                        newStates.Where(state => state.Historizing),
                        StateStorage.VariableStates,
                        false,
                        token));
                }
            }

            pushTasks = pushTasks.ToList();
            await Task.WhenAll(pushTasks);

            if (initial)
            {
                trackedAssets.Set(objects.Count());
                trackedTimeseres.Set(timeseries.Count());
            }
            else
            {
                trackedAssets.Inc(objects.Count());
                trackedTimeseres.Inc(timeseries.Count());
            }

            foreach (var state in newStates.Concat<BaseExtractionState>(State.EmitterStates))
            {
                state.FinalizeRangeInit(config.History.Backfill);
            }
        }

        private async Task SynchronizeEvents(IEnumerable<NodeId> nodes, CancellationToken token)
        {
            await Task.Run(() => uaClient.SubscribeToEvents(State.EmitterStates.Select(state => state.Id), 
                nodes, Streamer.EventSubscriptionHandler, token));
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.Historizing), nodes, token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillEvents(State.EmitterStates.Where(state => state.Historizing), nodes, token);
                }
            }
        }

        private async Task SynchronizeNodes(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            await Task.Run(() => uaClient.SubscribeToNodes(states, Streamer.DataSubscriptionHandler, token));
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillData(states.Where(state => state.Historizing), token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillData(states.Where(state => state.Historizing), token);
                }
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
            var states = variables.Select(ts => ts.Id).Distinct().Select(id => State.GetNodeState(id));

            log.Information("Synchronize {NumNodesToSynch} nodes", variables.Count());
            var tasks = new List<Task>();
            // Create tasks to subscribe to nodes, then start history read. We might lose data if history read finished before subscriptions were created.
            if (states.Any())
            {
                tasks.Add(SynchronizeNodes(states, token));
            }
            if (State.EmitterStates.Any())
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
                log.Information("Mapping resulted in no new nodes");
                return Array.Empty<Task>();
            }

            Streamer.AllowData = State.NodeStates.Any();

            await PushNodes(objects, timeseries, token);

            foreach (var node in variables.Concat(objects).Select(node => node.Id))
            {
                State.AddManagedNode(node);
            }

            var historyTasks = Synchronize(variables, objects, token);
            Starting.Set(0);
            return historyTasks;
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
            log.Verbose("HandleNode {parent} {node}", parentId, node);

            if (node.NodeClass == NodeClass.Object)
            {
                var bufferedNode = new BufferedNode(uaClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                log.Verbose("HandleNode Object {name}", bufferedNode.DisplayName);
                State.RegisterNode(bufferedNode.Id, GetUniqueId(bufferedNode.Id));
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
                State.RegisterNode(bufferedNode.Id, GetUniqueId(bufferedNode.Id));
                log.Verbose("HandleNode Variable {name}", bufferedNode.DisplayName);
                commonQueue.Enqueue(bufferedNode);
            }
        }
        

        /// <summary>
        /// Handle subscription callback for audit events (AddReferences/AddNodes). Triggers partial re-browse when necessary
        /// </summary>
        private void AuditEventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (!(eventArgs.NotificationValue is EventFieldList triggeredEvent))
            {
                log.Warning("No event in event subscription notification: {}", item.StartNodeId);
                return;
            }

            var eventFields = triggeredEvent.EventFields;
            if (!(item.Filter is EventFilter filter))
            {
                log.Warning("Triggered event without filter");
                return;
            }
            int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                       && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                log.Warning("Triggered event has no type, ignoring");
                return;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            if (eventType == null || eventType != ObjectTypeIds.AuditAddNodesEventType && eventType != ObjectTypeIds.AuditAddReferencesEventType)
            {
                log.Warning("Non-audit event triggered on audit event listener");
                return;
            }

            if (eventType == ObjectTypeIds.AuditAddNodesEventType)
            {
                // This is a neat way to get the contents of the event, which may be fairly complicated (variant of arrays of extensionobjects)
                using var e = new AuditAddNodesEventState(null);
                e.Update(uaClient.GetSystemContext(), filter.SelectClauses, triggeredEvent);
                if (e.NodesToAdd?.Value == null)
                {
                    log.Warning("Missing NodesToAdd object on AddNodes event");
                    return;
                }

                var addedNodes = e.NodesToAdd.Value;

                var relevantIds = addedNodes.Where(added => added != null &&
                    (added.NodeClass == NodeClass.Variable || added.NodeClass == NodeClass.Object)
                    && (added.TypeDefinition != VariableTypeIds.PropertyType)
                    && (State.IsMappedNode(uaClient.ToNodeId(added.ParentNodeId))))
                    .Select(added => uaClient.ToNodeId(added.ParentNodeId))
                    .Distinct();
                if (!relevantIds.Any()) return;
                log.Information("Trigger rebrowse on {numnodes} node ids due to addNodes event", relevantIds.Count());

                foreach (var id in relevantIds)
                {
                    extraNodesToBrowse.Enqueue(id);
                }
                triggerUpdateOperations.Set();
                return;
            }

            using var ev = new AuditAddReferencesEventState(null);
            ev.Update(uaClient.GetSystemContext(), filter.SelectClauses, triggeredEvent);

            if (ev.ReferencesToAdd?.Value == null)
            {
                log.Warning("Missing ReferencesToAdd object on AddReferences event");
                return;
            }

            var addedReferences = ev.ReferencesToAdd.Value;

            var relevantRefIds = addedReferences.Where(added =>
                (added.IsForward && State.IsMappedNode(uaClient.ToNodeId(added.SourceNodeId))))
                .Select(added => uaClient.ToNodeId(added.SourceNodeId))
                .Distinct();

            if (!relevantRefIds.Any()) return;

            log.Information("Trigger rebrowse on {numnodes} node ids due to addReference event", relevantRefIds.Count());

            foreach (var id in relevantRefIds)
            {
                extraNodesToBrowse.Enqueue(id);
            }
            triggerUpdateOperations.Set();
        }
        #endregion

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            Starting.Set(0);
            triggerUpdateOperations?.Dispose();
            StateStorage?.Dispose();
            triggerHistoryRestart?.Dispose();
        }
    }
}

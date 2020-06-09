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
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
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
        public Looper Looper { get; }
        public FailureBuffer FailureBuffer { get; }
        public StateStorage StateStorage { get; }
        public State State { get; }
        public Streamer Streamer { get; }

        private readonly HistoryReader historyReader;
        public NodeId RootNode { get; private set; }
        private readonly IEnumerable<IPusher> pushers;
        private readonly ConcurrentQueue<BufferedNode> commonQueue = new ConcurrentQueue<BufferedNode>();
        private readonly ConcurrentQueue<NodeId> extraNodesToBrowse = new ConcurrentQueue<NodeId>();

        // Concurrent reading of properties
        private readonly HashSet<NodeId> pendingProperties = new HashSet<NodeId>();
        private readonly object propertySetLock = new object();
        private readonly List<Task> propertyReadTasks = new List<Task>();

        private HashSet<NodeId> ignoreDataTypes;

        public bool Started { get; private set; }
        public bool Pushing { get; private set; }

        private int subscribed;
        private bool subscribeFlag = false;


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

        private readonly ILogger log = Log.Logger.ForContext(typeof(Extractor));

        /// <summary>
        /// Construct extractor with list of pushers
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
            Looper = new Looper(this, config, pushers);
        }

        /// <summary>
        /// Construct extractor with single pusher.
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pusher">Pusher to be used</param>
        /// <param name="UAClient">UAClient to use</param>
        public Extractor(FullConfig config, IPusher pusher, UAClient uaClient) : this(config, new List<IPusher> { pusher }, uaClient) { }
        #region Interface

        /// <summary>
        /// Run the extractor, this browses, starts mapping, then waits for the looper main task and any history.
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
            if (quitAfterMap) return;
            Pushing = true;
            await Looper.InitTaskLoop(synchTasks, token);

        }
        /// <summary>
        /// Initializes restart of the extractor. Waits for history, reset states, then schedule restart on the looper.
        /// </summary>
        public void RestartExtractor()
        {
            subscribed = 0;
            subscribeFlag = false;
            historyReader.Terminate(CancellationToken.None, 30).Wait();
            foreach (var state in State.NodeStates) {
                state.ResetStreamingState();
            }

            foreach (var state in State.EmitterStates)
            {
                state.ResetStreamingState();
            }

            Looper.WaitForNextPush().Wait();
            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }
            Starting.Set(1);
            Looper.Restart();
        }
        /// <summary>
        /// Task for the actual extractor restart, performing it directly.
        /// Stops history, waits for UAClient to terminate, resets the extractor, rebrowses, then schedules history.
        /// </summary>
        public async Task FinishExtractorRestart(CancellationToken token)
        {
            log.Information("Restarting extractor...");
            extraNodesToBrowse.Clear();
            Started = false;
            await historyReader.Terminate(token, 30);
            await uaClient.WaitForOperations();
            ConfigureExtractor(token);
            uaClient.ResetVisitedNodes();
            await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, token);
            var synchTasks = await MapUAToDestinations(token);
            Looper.ScheduleTasks(synchTasks);
            Started = true;
            log.Information("Successfully restarted extractor");
        }
        /// <summary>
        /// Push nodes from the extraNodesToBrowse queue.
        /// </summary>
        public async Task PushExtraNodes(CancellationToken token)
        {
            if (extraNodesToBrowse.Any())
            {
                var nodesToBrowse = new List<NodeId>();
                while (extraNodesToBrowse.TryDequeue(out NodeId id))
                {
                    nodesToBrowse.Add(id);
                }
                await uaClient.BrowseNodeHierarchy(nodesToBrowse.Distinct(), HandleNode, token);
                var historyTasks = await MapUAToDestinations(token);
                Looper.ScheduleTasks(historyTasks);
            }
        }
        /// <summary>
        /// Terminate the extractor hard, causing a failure.
        /// </summary>
        public void QuitExtractorInternally()
        {
            Looper.Quit();
        }
        /// <summary>
        /// Terminate history, waiting for timeout seconds
        /// </summary>
        /// <param name="timeout">Seconds to wait before returning failure</param>
        /// <returns>True if history was terminated successfully</returns>
        public Task<bool> TerminateHistory(int timeout, CancellationToken token)
        {
            return historyReader.Terminate(token, timeout);
        }
        /// <summary>
        /// Closes the extractor, mainly just shutting down the opcua client and waiting for a clean loss of connection.
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
        /// <summary>
        /// Get uniqueId either from the uaClient or from the state
        /// </summary>
        /// <param name="id">NodeId to convert</param>
        /// <param name="index">Index to use for uniqueId</param>
        /// <returns>Converted uniqueId</returns>
        public string GetUniqueId(NodeId id, int index = -1)
        {
            if (index == -1)
            {
                return State.GetUniqueId(id) ?? uaClient.GetUniqueId(id, -1);
            }

            return uaClient.GetUniqueId(id, index);
        }
        /// <summary>
        /// Calls the ConvertToString method on UAClient. This uses the namespaceTable, so it cannot be static.
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <returns>Converted value</returns>
        public string ConvertToString(object value)
        {
            return uaClient.ConvertToString(value);
        }
        /// <summary>
        /// Read properties for the given list of BufferedNode. This is intelligent,
        /// and keeps track of which properties are in the process of being read,
        /// to prevent multiple pushers from starting PropertyRead operations at the same time.
        /// If this is called on a given node twice in short time, the second call
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
        /// Returns true if the timeseries may be mapped based on rules of array size and datatypes.
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
        /// Returns a task running history for both data and events.
        /// </summary>
        public async Task RestartHistory(CancellationToken token)
        {
            if (!config.History.Enabled) return;
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
        /// <summary>
        /// Redo browse, then schedule history on the looper.
        /// </summary>
        public async Task Rebrowse(CancellationToken token)
        {
            await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, token);
            var historyTasks = await MapUAToDestinations(token);
            Looper.ScheduleTasks(historyTasks);
        }

        public async Task WaitForSubscriptions(int timeout = 100)
        {
            int time = 0;
            while (!subscribeFlag && subscribed < 2 && time++ < timeout) await Task.Delay(100);
            if (time >= timeout && !subscribeFlag && subscribed < 2)
            {
                throw new TimeoutException("Waiting for push timed out");
            }
            log.Debug("Waited {s} milliseconds for subscriptions", time * 100);
        }
        #endregion

        #region Mapping
        /// <summary>
        /// Empties the node queue, pushing nodes to each destination, and starting subscriptions and history.
        /// This is the entry point for mapping on the extractor.
        /// </summary>
        /// <returns>A list of history tasks</returns>
        private async Task<IEnumerable<Task>> MapUAToDestinations(CancellationToken token)
        {
            GetNodesFromQueue(token, out var objects, out var timeseries, out var variables);

            if (!objects.Any() && !timeseries.Any() && !variables.Any())
            {
                log.Information("Mapping resulted in no new nodes");
                return Array.Empty<Task>();
            }

            log.Information("Map {obj} objects, {var} variables to destinations", objects.Count, variables.Count);

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
        /// <summary>
        /// Set up extractor once UAClient is started. This resets the internal state of the extractor.
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
                if (config.Events.HistorizingEmitterIds != null && config.Events.HistorizingEmitterIds.Any() && config.History.Enabled)
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
        private void GetNodesFromQueue(CancellationToken token, out List<BufferedNode> objects,
            out List<BufferedVariable> timeseries, out List<BufferedVariable> variables)
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
                State.AddActiveNode(node);
            }

            foreach (var node in rawVariables)
            {
                if (AllowTSMap(node))
                {
                    log.Debug(node.ToDebugDescription());
                    variables.Add(node);
                    var state = new NodeExtractionState(node);
                    State.AddActiveNode(node);
                    if (node.ArrayDimensions != null && node.ArrayDimensions.Count > 0 && node.ArrayDimensions[0] > 0)
                    {
                        for (int i = 0; i < node.ArrayDimensions[0]; i++)
                        {
                            var ts = new BufferedVariable(node, i);
                            timeseries.Add(ts);
                            State.AddActiveNode(ts);
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
        /// <summary>
        /// Push nodes to given pusher
        /// </summary>
        /// <param name="objects">Object type nodes to push</param>
        /// <param name="timeseries">Variable type nodes to push</param>
        /// <param name="pusher">Destination to push to</param>
        /// <param name="initial">True if this counts as initialization of the pusher</param>
        public async Task PushNodes(IEnumerable<BufferedNode> objects, IEnumerable<BufferedVariable> timeseries,
            IPusher pusher, bool initial, CancellationToken token)
        {
            if (pusher == null) throw new ArgumentNullException(nameof(pusher));
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

            bool initial = objects.Count() + timeseries.Count() == State.ActiveNodes.Count();

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
        /// <summary>
        /// Subscribe to event changes, then run history.
        /// </summary>
        /// <param name="nodes">Nodes to subscribe to events for</param>
        private async Task SynchronizeEvents(IEnumerable<NodeId> nodes, CancellationToken token)
        {
            await Task.Run(() => uaClient.SubscribeToEvents(State.EmitterStates.Select(state => state.Id), 
                nodes, Streamer.EventSubscriptionHandler, token));
            Interlocked.Increment(ref subscribed);
            if (!State.NodeStates.Any() || subscribed > 1) subscribeFlag = true;
            if (!config.History.Enabled) return;
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.Historizing), nodes, token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillEvents(State.EmitterStates.Where(state => state.Historizing), nodes, token);
                }
            }
            else
            {
                log.Information("Skipping event history due to no initialized pushers");
            }
        }
        /// <summary>
        /// Subscribe to data changes, then run history.
        /// </summary>
        /// <param name="states">States to subscribe to</param>
        private async Task SynchronizeNodes(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            await Task.Run(() => uaClient.SubscribeToNodes(states, Streamer.DataSubscriptionHandler, token));
            Interlocked.Increment(ref subscribed);
            if (!State.EmitterStates.Any() || subscribed > 1) subscribeFlag = true;
            if (!config.History.Enabled) return;
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillData(states.Where(state => state.Historizing), token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillData(states.Where(state => state.Historizing), token);
                }
            }
            else
            {
                log.Information("Skipping datapoints history due to no initialized pushers");
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
                if (!relevantIds.Any())
                {
                    log.Debug("No relevant nodes in addNodes audit event");
                    return;
                }
                log.Information("Trigger rebrowse on {numnodes} node ids due to addNodes event", relevantIds.Count());

                foreach (var id in relevantIds)
                {
                    extraNodesToBrowse.Enqueue(id);
                }
                Looper.ScheduleUpdate();
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

            if (!relevantRefIds.Any())
            {
                log.Debug("No relevant nodes in addReferences audit event");
                return;
            }

            log.Information("Trigger rebrowse on {numnodes} node ids due to addReference event", relevantRefIds.Count());

            foreach (var id in relevantRefIds)
            {
                extraNodesToBrowse.Enqueue(id);
            }
            Looper.ScheduleUpdate();
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
            StateStorage?.Dispose();
            Looper?.Dispose();
        }
    }
}

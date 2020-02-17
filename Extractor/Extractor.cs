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

        private readonly ConcurrentDictionary<string, NodeExtractionState> nodeStatesByExtId =
            new ConcurrentDictionary<string, NodeExtractionState>();

        public ConcurrentDictionary<NodeId, EventExtractionState> EmitterStates { get; } =
            new ConcurrentDictionary<NodeId, EventExtractionState>();

        private readonly ConcurrentDictionary<string, EventExtractionState> emitterStatesByExtId =
            new ConcurrentDictionary<string, EventExtractionState>();

        private readonly ConcurrentQueue<NodeId> extraNodesToBrowse = new ConcurrentQueue<NodeId>();
        private bool restart;
        private bool quit;
        private readonly AutoResetEvent triggerUpdateOperations = new AutoResetEvent(false);

        private readonly object managedNodesLock = new object();
        private HashSet<NodeId> managedNodes = new HashSet<NodeId>();

        private HashSet<NodeId> ignoreDataTypes;

        public ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> ActiveEvents { get; }
            = new ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>>();

        public ConcurrentQueue<BufferedDataPoint> DataPointQueue { get; }
            = new ConcurrentQueue<BufferedDataPoint>();

        public ConcurrentQueue<BufferedEvent> EventQueue { get; }
            = new ConcurrentQueue<BufferedEvent>();

        private bool pushEvents;
        private bool pushData;

        public bool Started { get; private set; }
        public bool Pushing { get; private set; }

        private bool nextPushFlag;


        private static readonly Gauge startTime = Metrics
            .CreateGauge("opcua_start_time", "Start time for the extractor");

        public static readonly Counter BadDataPoints = Metrics
            .CreateCounter("opcua_bad_datapoints", "Datapoints skipped due to bad status");

        public static readonly Gauge Starting = Metrics
            .CreateGauge("opcua_extractor_starting", "1 if the extractor is in the startup phase");

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
            FailureBuffer = new FailureBuffer(config.FailureBuffer, this);
            if (!string.IsNullOrWhiteSpace(config.StateStorage.Location))
            {
                StateStorage = new StateStorage(this, config);
            }
            this.uaClient.Extractor = this;
            historyReader = new HistoryReader(uaClient, this, pushers, config.History);
            log.Information("Building extractor with {NumPushers} pushers", pushers.Count());
            if (config.Extraction.IdPrefix == "events.")
            {
                throw new ConfigurationException("Avoid using events. as IdPrefix, as it is used internally");
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

            Pushing = true;

            var tasks = new List<Task>
                {
                    Task.Run(() => PushersLoop(token)),
                    Task.Run(() => ExtraTaskLoop(token))
                }.Concat(synchTasks);

            if (config.Extraction.AutoRebrowsePeriod > 0)
            {
                tasks = tasks.Append(Task.Run(() => RebrowseLoop(token)));
            }

            if (StateStorage != null && config.StateStorage.Interval > 0)
            {
                tasks = tasks.Append(Task.Run(() => StoreStateLoop(token)));
            }

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
            foreach (var state in NodeStates.Values) {
                state.ResetStreamingState();
            }

            foreach (var state in EmitterStates.Values)
            {
                state.ResetStreamingState();
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
            nextPushFlag = false;
            int time = 0;
            while (!nextPushFlag && time++ < timeout) await Task.Delay(100);
            if (time >= timeout && !nextPushFlag)
            {
                throw new TimeoutException("Waiting for push timed out");
            }
        }
        #endregion
        #region Loops

        private async Task PushDataPoints(CancellationToken token)
        {
            if (!pushData) return;
            var dataPointList = new List<BufferedDataPoint>();
            var pointRanges = new Dictionary<string, TimeRange>();

            while (DataPointQueue.TryDequeue(out BufferedDataPoint dp))
            {
                dataPointList.Add(dp);
                if (!pointRanges.ContainsKey(dp.Id))
                {
                    pointRanges[dp.Id] = new TimeRange(dp.Timestamp, dp.Timestamp);
                    continue;
                }

                var range = pointRanges[dp.Id];
                if (dp.Timestamp < range.Start)
                {
                    range.Start = dp.Timestamp;
                }
                else if (dp.Timestamp > range.End)
                {
                    range.End = dp.Timestamp;
                }
            }

            var results = await Task.WhenAll(pushers.Select(pusher =>
                pusher.DataFailing ? pusher.TestConnection(token) : pusher.PushDataPoints(dataPointList, token)));

            if (results.Any(failed => failed == false))
            {
                var failed = results.Select((res, idx) => (result: res, Index: idx)).Where(x => x.result == false).ToList();
                foreach (var pair in failed)
                {
                    var pusher = pushers.ElementAt(pair.Index);
                    pusher.DataFailing = true;
                }
                // Write to failurebuffer, also remember to register which indices are failed

            }
            else
            {
                foreach (var kvp in pointRanges)
                {
                    var state = GetNodeState(kvp.Key);
                    state.UpdateDestinationRange(kvp.Value);
                }
                if (FailureBuffer.Any)
                {
                    // Read from buffer, trigger history for relevant states...
                }
            }
        }

        private async Task PushEvents(CancellationToken token)
        {
            if (!pushEvents) return;
            var eventList = new List<BufferedEvent>();
            var eventRanges = new Dictionary<NodeId, TimeRange>();
            while (EventQueue.TryDequeue(out BufferedEvent evt))
            {
                eventList.Add(evt);
                if (!eventRanges.ContainsKey(evt.EmittingNode))
                {
                    eventRanges[evt.EmittingNode] = new TimeRange(evt.Time, evt.Time);
                    continue;
                }

                var range = eventRanges[evt.EmittingNode];
                if (evt.Time < range.Start)
                {
                    range.Start = evt.Time;
                }
                else if (evt.Time > range.End)
                {
                    range.End = evt.Time;
                }
            }
            var results = await Task.WhenAll(pushers.Select(pusher =>
                pusher.EventsFailing ? pusher.TestConnection(token) : pusher.PushEvents(eventList, token)));
            if (results.Any(failed => failed == false))
            {
                // Write to failurebuffer, also remember to register which indices are failed

            }
            else
            {
                foreach (var kvp in eventRanges)
                {
                    var state = GetEmitterState(kvp.Key);
                    state.UpdateDestinationRange(kvp.Value);
                }
                if (FailureBuffer.Any)
                {
                    // Read from buffer, trigger history for relevant states...
                }
            }
        }

        private async Task PushersLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                await Task.WhenAll(PushDataPoints(token), PushEvents(token), Task.Delay(config.Extraction.DataPushDelay, token));
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
                    StateStorage.StoreExtractionState(NodeStates.Values, StateStorage.VariableStates, token),
                    StateStorage.StoreExtractionState(EmitterStates.Values, StateStorage.EmitterStates, token)
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

            foreach (var state in NodeStates.Values)
            {
                state.ResetStreamingState();
            }

            if (config.Events.EmitterIds != null
                && config.Events.EventIds != null
                && config.Events.EmitterIds.Any()
                && config.Events.EventIds.Any())
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
                    var histEmitters = config.Events.HistorizingEmitterIds.Select(proto =>
                        proto.ToNodeId(uaClient, ObjectIds.Server)).ToList();
                    foreach (var id in histEmitters)
                    {
                        if (!EmitterStates.ContainsKey(id)) throw new ConfigurationException(
                            "Historical emitter not in emitter list");
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
            log.Information("Getting data for {NumVariables} variables and {NumObjects} objects", 
                rawVariables.Count, objects.Count);
            uaClient.ReadNodeData(objects.Concat(rawVariables), token);
            foreach (var node in objects.Concat(rawVariables))
            {
                log.Debug(node.ToDebugDescription());
            }

            foreach (var node in rawVariables)
            {
                if (AllowTSMap(node))
                {
                    variables.Add(node);
                    NodeStates[node.Id] = new NodeExtractionState(node);
                    if (node.ArrayDimensions != null && node.ArrayDimensions.Count > 0 && node.ArrayDimensions[0] > 0)
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
            if (!pushResult.All(res => res)) throw new ExtractorFailureException("Pushing nodes failed");

            var statesToSync = timeseries
                .Select(ts => ts.Id)
                .Distinct()
                .Select(id => NodeStates[id])
                .Where(state => state.Historizing);

            var getRangePushes = pushers
                .Where(pusher => pusher.BaseConfig.ReadExtractedRanges)
                .Select(pusher => pusher.InitExtractedRanges(statesToSync, config.History.Backfill, token));

            if (EmitterStates.Any(kvp => kvp.Value.Historizing))
            {
                var getEventRanges = pushers
                    .Where(pusher => pusher.BaseConfig.ReadExtractedRanges)
                    .Select(pusher => pusher.InitExtractedEventRanges(
                        EmitterStates.Values.Where(state => state.Historizing),
                        timeseries.Concat(objects).Select(ts => ts.Id).Distinct(),
                        config.History.Backfill,
                        token)
                    );
                getRangePushes = getRangePushes.Concat(getEventRanges);
            }

            if (StateStorage != null && config.StateStorage.Interval > 0)
            {
                getRangePushes = getRangePushes
                    .Append(StateStorage.ReadExtractionStates(
                        statesToSync, 
                        StateStorage.VariableStates,
                        timeseries.Count() * Math.Log(timeseries.Count(), 2) < NodeStates.Count(kvp => kvp.Value.Historizing),
                        token)
                    ).Append(StateStorage.ReadExtractionStates(
                        EmitterStates.Values.Where(state => state.Historizing),
                        StateStorage.EmitterStates, 
                        false, 
                        token)
                    );
            }


            var getRangeResult = await Task.WhenAll(getRangePushes);
            if (!getRangeResult.All(res => res)) throw new ExtractorFailureException("Getting latest timestamp failed");
            // If any nodes are still at default values, meaning no pusher can initialize them, initialize to default values.
            foreach (var state in statesToSync)
            {
                state.FinalizeRangeInit(config.History.Backfill);
            }

            foreach (var state in EmitterStates.Values.Where(state => state.Historizing))
            {
                state.FinalizeRangeInit(config.History.Backfill);
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

            log.Information("Synchronize {NumNodesToSynch} nodes", variables.Count());
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
                log.Information("Mapping resulted in no new nodes");
                return Array.Empty<Task>();
            }

            pushData = NodeStates.Any();

            await PushNodes(objects, timeseries, token);

            lock (managedNodesLock)
            {
                managedNodes = managedNodes.Concat(variables.Concat(objects).Select(node => node.Id)).ToHashSet();
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
                log.Verbose("HandleNode Variable {name}", bufferedNode.DisplayName);
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
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (variable == null) throw new ArgumentNullException(nameof(value));

            if (variable.ArrayDimensions != null && variable.ArrayDimensions.Count > 0 && variable.ArrayDimensions[0] > 0)
            {
                var ret = new List<BufferedDataPoint>();
                if (!(value.Value is Array))
                {
                    BadDataPoints.Inc();
                    log.Debug("Bad array datapoint: {BadPointName} {BadPointValue}", uniqueId, value.Value.ToString());
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
                    log.Debug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId, datapoint.SourceTimestamp);
                    continue;
                }
                var buffDps = ToDataPoint(datapoint, node, uniqueId);
                node.UpdateFromStream(buffDps);
                if (!node.IsStreaming) return;
                foreach (var buffDp in buffDps)
                {
                    log.Verbose("Subscription DataPoint {dp}", buffDp.ToDebugDescription());
                }
                foreach (var buffDp in buffDps)
                { 
                    DataPointQueue.Enqueue(buffDp);
                }
            }
        }


        /// <summary>
        /// Construct event from filter and collection of event fields
        /// </summary>
        /// <param name="filter">Filter that resulted in this event</param>
        /// <param name="eventFields">Fields for a single event</param>
        public BufferedEvent ConstructEvent(EventFilter filter, VariantCollection eventFields, NodeId emitter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            if (eventFields == null) throw new ArgumentNullException(nameof(eventFields));

            int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                       && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                log.Warning("Triggered event has no type, ignoring.");
                return null;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            // Many servers don't handle filtering on history data.
            if (eventType == null || !ActiveEvents.ContainsKey(eventType))
            {
                log.Verbose("Invalid event type: {eventType}", eventType);
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

            if (!(extractedProperties.GetValueOrDefault("EventId") is byte[] rawEventId))
            {
                log.Verbose("Event of type {type} lacks id", eventType);
                return null;
            }

            var eventId = Convert.ToBase64String(rawEventId);
            var sourceNode = extractedProperties.GetValueOrDefault("SourceNode");
            if (sourceNode == null || !managedNodes.Contains(sourceNode))
            {
                log.Verbose("Invalid source node, type: {type}, id: {id}", eventType, eventId);
                return null;
            }

            var time = extractedProperties.GetValueOrDefault("Time");
            if (time == null)
            {
                log.Verbose("Event lacks specified time, type: {type}", eventType, eventId);
                return null;
            }
            var buffEvent = new BufferedEvent
            {
                Message = uaClient.ConvertToString(extractedProperties.GetValueOrDefault("Message")),
                EventId = config.Extraction.IdPrefix + eventId,
                SourceNode = extractedProperties.GetValueOrDefault("SourceNode") as NodeId,
                Time = (DateTime)time,
                EventType = eventType,
                MetaData = extractedProperties
                    .Where(kvp => kvp.Key != "Message" && kvp.Key != "EventId" && kvp.Key != "SourceNode"
                                  && kvp.Key != "Time" && kvp.Key != "EventType")
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                EmittingNode = emitter,
                ReceivedTime = DateTime.UtcNow,
            };
            return buffEvent;
        }
        /// <summary>
        /// Handle subscription callback for events
        /// </summary>
        private void EventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
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
            var buffEvent = ConstructEvent(filter, eventFields, item.ResolvedNodeId);
            if (buffEvent == null) return;
            var eventState = EmitterStates[item.ResolvedNodeId];
            eventState.UpdateFromStream(buffEvent);

            // Either backfill/frontfill is done, or we are not outside of each respective bound
            if (!((eventState.IsStreaming || buffEvent.Time < eventState.SourceExtractedRange.End)
                  && (eventState.BackfillDone || buffEvent.Time > eventState.SourceExtractedRange.Start))) return;

            log.Verbose(buffEvent.ToDebugDescription());
            EventQueue.Enqueue(buffEvent);
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
                    && (managedNodes.Contains(uaClient.ToNodeId(added.ParentNodeId))))
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
                (added.IsForward && managedNodes.Contains(uaClient.ToNodeId(added.SourceNodeId))))
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
        }
    }
}

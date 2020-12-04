/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.StateStorage;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Main extractor class, tying together the <see cref="uaClient"/> and CDF client.
    /// </summary>
    public class UAExtractor : IDisposable
    {
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        public Looper Looper { get; }
        public FailureBuffer FailureBuffer { get; }
        public IExtractionStateStore StateStorage { get; }
        public State State { get; }
        public Streamer Streamer { get; }
        public DataTypeManager DataTypeManager => uaClient.DataTypeManager;

        private readonly HistoryReader historyReader;
        private readonly ReferenceTypeManager referenceTypeManager;
        public NodeId RootNode { get; private set; }
        private readonly IEnumerable<IPusher> pushers;
        private readonly ConcurrentQueue<BufferedNode> commonQueue = new ConcurrentQueue<BufferedNode>();
        private readonly ConcurrentQueue<NodeId> extraNodesToBrowse = new ConcurrentQueue<NodeId>();

        // Concurrent reading of properties
        private readonly HashSet<NodeId> pendingProperties = new HashSet<NodeId>();
        private readonly object propertySetLock = new object();
        private readonly List<Task> propertyReadTasks = new List<Task>();

        public bool Started { get; private set; }
        public bool Pushing { get; private set; }

        private int subscribed;
        private bool subscribeFlag;

        private readonly Regex propertyNameFilter;
        private readonly Regex propertyIdFilter;

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

        private readonly ILogger log = Log.Logger.ForContext(typeof(UAExtractor));

        private readonly CancellationTokenSource source;

        /// <summary>
        /// Construct extractor with list of pushers
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pushers">List of pushers to be used</param>
        /// <param name="uaClient">UAClient to be used</param>
        public UAExtractor(FullConfig config,
            IEnumerable<IPusher> pushers,
            UAClient uaClient,
            IExtractionStateStore stateStore,
            CancellationToken token)
        {
            this.pushers = pushers ?? throw new ArgumentNullException(nameof(pushers));
            this.uaClient = uaClient ?? throw new ArgumentNullException(nameof(uaClient));
            this.config = config ?? throw new ArgumentNullException(nameof(config));

            State = new State(this);
            Streamer = new Streamer(this, config);
            StateStorage = stateStore;
            if (config.Extraction.Relationships.Enabled)
            {
                referenceTypeManager = new ReferenceTypeManager(uaClient, this);
            }

            source = CancellationTokenSource.CreateLinkedTokenSource(token);

            if (config.FailureBuffer.Enabled)
            {
                FailureBuffer = new FailureBuffer(config, this, pushers.FirstOrDefault(pusher => pusher is InfluxPusher) as InfluxPusher);
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

            propertyNameFilter = CreatePropertyFilterRegex(config.Extraction.PropertyNameFilter);
            propertyIdFilter = CreatePropertyFilterRegex(config.Extraction.PropertyIdFilter);
        }

        private static Regex CreatePropertyFilterRegex(string regex)
        {
            if (string.IsNullOrEmpty(regex))
            {
                return null;
            }
            return new Regex(regex, RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.CultureInvariant);
        }

        /// <summary>
        /// Construct extractor with single pusher.
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pusher">Pusher to be used</param>
        /// <param name="uaClient">UAClient to use</param>
        public UAExtractor(FullConfig config,
            IPusher pusher,
            UAClient uaClient,
            IExtractionStateStore stateStore,
            CancellationToken token)
            : this(config, new List<IPusher> { pusher }, uaClient, stateStore, token) { }
        #region Interface

        /// <summary>
        /// Run the extractor, this browses, starts mapping, then waits for the looper main task and any history.
        /// </summary>
        /// <param name="quitAfterMap">If true, terminate the extractor after first map iteration</param>
        public async Task RunExtractor(bool quitAfterMap = false)
        {
            Starting.Set(1);
            if (!uaClient.Started)
            {
                log.Information("Start UAClient");
                try
                {
                    await uaClient.Run(source.Token);
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

            ConfigureExtractor();
            await DataTypeManager.GetDataTypeStructureAsync(source.Token);

            Started = true;
            startTime.Set(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds());

            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }

            log.Debug("Begin mapping directory");
            await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, source.Token);
            log.Debug("End mapping directory");

            IEnumerable<Task> synchTasks;
            try
            {
                synchTasks = await MapUAToDestinations();
            }
            catch (Exception ex)
            {
                ExtractorUtils.LogException(ex, "Unexpected error in MapUAToDestinations", 
                    "Handled service result exception in MapUAToDestinations");
                throw;
            }

            if (config.FailureBuffer.Enabled)
            {
                await FailureBuffer.InitializeBufferStates(State.NodeStates, State.EmitterStates, source.Token);
            }
            if (quitAfterMap) return;
            Pushing = true;
            await Looper.InitTaskLoop(synchTasks, source.Token);

        }
        /// <summary>
        /// Initializes restart of the extractor. Waits for history, reset states, then schedule restart on the looper.
        /// </summary>
        public void RestartExtractor()
        {
            subscribed = 0;
            subscribeFlag = false;
            historyReader.Terminate(source.Token, 30).Wait();
            foreach (var state in State.NodeStates) {
                state.RestartHistory();
            }

            foreach (var state in State.EmitterStates)
            {
                state.RestartHistory();
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
        public async Task FinishExtractorRestart()
        {
            log.Information("Restarting extractor...");
            extraNodesToBrowse.Clear();
            Started = false;
            await historyReader.Terminate(source.Token, 30);
            await uaClient.WaitForOperations();
            ConfigureExtractor();
            uaClient.ResetVisitedNodes();
            await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, source.Token);
            var synchTasks = await MapUAToDestinations();
            Looper.ScheduleTasks(synchTasks);
            Started = true;
            log.Information("Successfully restarted extractor");
        }
        /// <summary>
        /// Push nodes from the extraNodesToBrowse queue.
        /// </summary>
        public async Task PushExtraNodes()
        {
            if (extraNodesToBrowse.Any())
            {
                var nodesToBrowse = new List<NodeId>();
                while (extraNodesToBrowse.TryDequeue(out NodeId id))
                {
                    nodesToBrowse.Add(id);
                }
                await uaClient.BrowseNodeHierarchy(nodesToBrowse.Distinct(), HandleNode, source.Token);
                var historyTasks = await MapUAToDestinations();
                Looper.ScheduleTasks(historyTasks);
            }
        }
        /// <summary>
        /// Terminate history, waiting for timeout seconds
        /// </summary>
        /// <param name="timeout">Seconds to wait before returning failure</param>
        /// <returns>True if history was terminated successfully</returns>
        public Task<bool> TerminateHistory(int timeout)
        {
            return historyReader.Terminate(source.Token, timeout);
        }
        /// <summary>
        /// Closes the extractor, mainly just shutting down the opcua client and waiting for a clean loss of connection.
        /// </summary>
        public void Close()
        {
            source.Cancel();
            if (!uaClient.Started) return;
            try
            {
                uaClient.Close();
            }
            catch (ServiceResultException e)
            {
                ExtractorUtils.LogException(
                    ExtractorUtils.HandleServiceResult(e, ExtractorUtils.SourceOp.CloseSession),
                    "",
                    "");
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
        /// Get the unique reference id, on the form [prefix][reference-name];[sourceId];[targetId]
        /// </summary>
        /// <param name="reference">Reference to get id for</param>
        /// <returns>String reference id</returns>
        public string GetRelationshipId(BufferedReference reference)
        {
            return uaClient.GetRelationshipId(reference);
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
        public async Task ReadProperties(IEnumerable<BufferedNode> nodes)
        {
            Task newTask = null;
            List<Task> tasksToWaitFor;
            lock (propertySetLock)
            {
                nodes = nodes.Where(node => !pendingProperties.Contains(node.Id) && !node.PropertiesRead).ToList();
                if (nodes.Any())
                {
                    newTask = Task.Run(() => uaClient.GetNodeProperties(nodes, source.Token));
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
        /// Returns a task running history for both data and events.
        /// </summary>
        public async Task RestartHistory()
        {
            if (!config.History.Enabled && !config.Events.History) return;
            await Task.WhenAll(Task.Run(async () =>
            {
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.IsFrontfilling), source.Token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillEvents(State.EmitterStates.Where(state => state.IsBackfilling), source.Token);
                }
            }), Task.Run(async () =>
            {
                await historyReader.FrontfillData(
                    State.NodeStates.Where(state => state.IsFrontfilling).ToList(), source.Token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillData(
                        State.NodeStates.Where(state => state.IsBackfilling).ToList(), source.Token);
                }
            }));
        }
        /// <summary>
        /// Redo browse, then schedule history on the looper.
        /// </summary>
        public async Task Rebrowse()
        {
            // If we are updating we want to re-discover nodes in order to run them through mapping again.
            await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, source.Token,
                !config.Extraction.Update.AnyUpdate);
            var historyTasks = await MapUAToDestinations();
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
        private async Task<IEnumerable<Task>> MapUAToDestinations()
        {
            var nodes = await GetNodesFromQueue();

            IEnumerable<BufferedReference> references = null;
            if (config.Extraction.Relationships.Enabled)
            {
                references = await referenceTypeManager.GetReferencesAsync(nodes.RawObjects.Concat(nodes.RawVariables).Select(node => node.Id),
                    ReferenceTypeIds.NonHierarchicalReferences, source.Token);
                await referenceTypeManager.GetReferenceTypeDataAsync(source.Token);
                references = State.AddReferences(references);
            }

            if (!nodes.Objects.Any() && !nodes.Timeseries.Any() && !nodes.Variables.Any())
            {
                log.Information("Mapping resulted in no new nodes");
                return Array.Empty<Task>();
            }

            nodes.ClearRaw();

            log.Information("Map {obj} objects, and {ts} destination timeseries, representing {var} variables, to destinations",
                nodes.Objects.Count, nodes.Timeseries.Count, nodes.Variables.Count);

            Streamer.AllowData = State.NodeStates.Any();

            await PushNodes(nodes.Objects, nodes.Timeseries, references);

            foreach (var node in nodes.Variables.Concat(nodes.Objects).Select(node => node.Id))
            {
                State.AddManagedNode(node);
            }

            // Changed flag means that it already existed, so we avoid synchronizing these.
            var historyTasks = Synchronize(nodes.Variables.Where(var => !var.Changed));
            Starting.Set(0);
            return historyTasks;
        }
        /// <summary>
        /// Set up extractor once UAClient is started. This resets the internal state of the extractor.
        /// </summary>
        private void ConfigureExtractor()
        {
            RootNode = config.Extraction.RootNode.ToNodeId(uaClient, ObjectIds.ObjectsFolder);

            DataTypeManager.Configure();

            if (config.Extraction.NodeMap != null)
            {
                foreach (var kvp in config.Extraction.NodeMap)
                {
                    uaClient.AddNodeOverride(kvp.Value.ToNodeId(uaClient), kvp.Key);
                }
            }

            foreach (var state in State.NodeStates)
            {
                state.RestartHistory();
            }

            foreach (var state in State.EmitterStates)
            {
                state.RestartHistory();
            }
            if (config.Events.Enabled)
            {
                Streamer.AllowEvents = true;
                var eventFields = uaClient.GetEventFields(source.Token);
                foreach (var field in eventFields)
                {
                    State.ActiveEvents[field.Key] = field.Value;
                }
                if (config.Events.EmitterIds != null && config.Events.EmitterIds.Any())
                {
                    var histEmitterIds = new HashSet<NodeId>((config.Events.HistorizingEmitterIds ?? Array.Empty<ProtoNodeId>())
                        .Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)));
                    foreach (var id in config.Events.EmitterIds.Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)))
                    {
                        var history = (histEmitterIds.Contains(id)) && config.Events.History;
                        State.SetEmitterState(new EventExtractionState(this, id, history,
                            history && config.History.Backfill, StateStorage != null && config.StateStorage.Interval > 0));
                    }
                }
                var serverNode = uaClient.GetServerNode(source.Token);
                if ((serverNode.EventNotifier & EventNotifiers.SubscribeToEvents) != 0)
                {
                    var history = (serverNode.EventNotifier & EventNotifiers.HistoryRead) != 0 && config.Events.History;
                    State.SetEmitterState(new EventExtractionState(this, serverNode.Id, history,
                        history && config.History.Backfill, StateStorage != null && config.StateStorage.Interval > 0));
                }
            }
        }

        private class BrowseResult
        {
            /// <summary>
            /// All nodes that should be mapped to destination objects
            /// </summary>
            public List<BufferedNode> Objects { get;} = new List<BufferedNode>();
            /// <summary>
            /// All nodes that should be mapped to destination variables
            /// </summary>
            public List<BufferedVariable> Timeseries { get; } = new List<BufferedVariable>();
            /// <summary>
            /// All source system variables that should be read from
            /// </summary>
            public List<BufferedVariable> Variables { get; } = new List<BufferedVariable>();
            /// <summary>
            /// All source system objects
            /// </summary>
            public List<BufferedNode> RawObjects { get; private set; } = new List<BufferedNode>();
            /// <summary>
            /// All source system variables
            /// </summary>
            public List<BufferedVariable> RawVariables { get; private set; } = new List<BufferedVariable>();
            /// <summary>
            /// Helps free up memory, especially on re-browse
            /// </summary>
            public void ClearRaw()
            {
                RawObjects = null;
                RawVariables = null;
            }
        }

        private async Task GetNodeData(UpdateConfig update, BrowseResult result)
        {
            log.Information("Getting data for {NumVariables} variables and {NumObjects} objects",
                result.RawVariables.Count, result.RawObjects.Count);

            var nodes = result.RawObjects.Concat(result.RawVariables);

            if (update.Objects.Metadata || update.Variables.Metadata)
            {
                var toReadProperties = nodes
                    .Where(node => State.IsMappedNode(node.Id)
                        && (update.Objects.Metadata && !node.IsVariable
                            || update.Variables.Metadata && node.IsVariable))
                    .ToList();
                if (toReadProperties.Any())
                {
                    await ReadProperties(toReadProperties);
                }
            }
            await Task.Run(() => uaClient.ReadNodeData(nodes, source.Token));
            await DataTypeManager.GetDataTypeMetadataAsync(result.RawVariables.Select(variable => variable.DataType.Raw).ToHashSet(), source.Token);
        }

        private IEnumerable<BufferedNode> FilterObjects(UpdateConfig update, IEnumerable<BufferedNode> rawObjects)
        {
            foreach (var node in rawObjects)
            {
                if (update.AnyUpdate)
                {
                    var old = State.GetActiveNode(node.Id);
                    if (old != null)
                    {
                        node.CheckForUpdates(old, update.Objects, config.Extraction.DataTypes.DataTypeMetadata);
                        if (node.Changed)
                        {
                            State.AddActiveNode(node);
                            yield return node;
                        }
                        continue;
                    }
                }
                log.Verbose(node.ToDebugDescription());
                State.AddActiveNode(node);
                yield return node;
            }
        }

        private void InitEventStates(BrowseResult result)
        {
            foreach (var node in result.RawObjects.Concat(result.RawVariables))
            {
                if ((node.EventNotifier & EventNotifiers.SubscribeToEvents) == 0) continue;
                if (State.GetEmitterState(node.Id) != null) continue;
                bool history = (node.EventNotifier & EventNotifiers.HistoryRead) != 0 && config.Events.History;
                var eventState = new EventExtractionState(this, node.Id, history, history && config.History.Backfill,
                    StateStorage != null && config.StateStorage.Interval > 0);
                State.SetEmitterState(eventState);
            }
        }

        private void FilterVariables(UpdateConfig update, BrowseResult result)
        {
            foreach (var node in result.RawVariables)
            {
                if (!DataTypeManager.AllowTSMap(node)) continue;
                if (update.AnyUpdate)
                {
                    var old = State.GetActiveNode(node.Id);
                    if (old != null && old is BufferedVariable variable)
                    {
                        node.CheckForUpdates(old, update.Variables, config.Extraction.DataTypes.DataTypeMetadata);

                        if (node.Changed)
                        {
                            var oldState = State.GetNodeState(node.Id);
                            State.AddActiveNode(node);
                            if (oldState.IsArray)
                            {
                                if (update.Variables.Name && old.DisplayName != node.DisplayName && !string.IsNullOrWhiteSpace(node.DisplayName))
                                {
                                    var children = node.CreateArrayChildren();
                                    foreach (var child in children)
                                    {
                                        child.Changed = true;
                                        result.Timeseries.Add(child);
                                    }
                                }
                                result.Objects.Add(node);
                            }
                            else
                            {
                                result.Timeseries.Add(node);
                                result.Variables.Add(node);
                            }
                        }

                        continue;
                    }
                }
                log.Verbose(node.ToDebugDescription());
                result.Variables.Add(node);
                var state = new NodeExtractionState(this, node, node.Historizing, node.Historizing && config.History.Backfill,
                    StateStorage != null && config.StateStorage.Interval > 0);

                State.AddActiveNode(node);
                if (state.IsArray)
                {
                    var children = node.CreateArrayChildren();
                    foreach (var child in children)
                    {
                        result.Timeseries.Add(child);
                        var uniqueId = GetUniqueId(child.Id, child.Index);
                        State.SetNodeState(state, uniqueId);
                        State.RegisterNode(node.Id, uniqueId);
                    }
                    result.Objects.Add(node);
                }
                else
                {
                    State.SetNodeState(state);
                    result.Timeseries.Add(node);
                }
            }
        }

        /// <summary>
        /// Read nodes from commonQueue and sort them into lists of context objects, destination timeseries and source variables
        /// </summary>
        private async Task<BrowseResult> GetNodesFromQueue()
        {
            var result = new BrowseResult();
            var nodeMap = new Dictionary<NodeId, BufferedNode>();

            while (commonQueue.TryDequeue(out BufferedNode buffer))
            {
                if (buffer.IsVariable && buffer is BufferedVariable buffVar)
                {
                    if (buffVar.IsProperty)
                    {
                        nodeMap.TryGetValue(buffVar.ParentId, out BufferedNode parent);
                        if (parent == null) continue;
                        if (parent.Properties == null)
                        {
                            parent.Properties = new List<BufferedVariable>();
                        }
                        parent.Properties.Add(buffVar);
                    }
                    else
                    {
                        result.RawVariables.Add(buffVar);
                    }
                }
                else
                {
                    result.RawObjects.Add(buffer);
                }
                nodeMap.Add(buffer.Id, buffer);
            }

            var update = config.Extraction.Update;
            
            await GetNodeData(update, result);

            result.Objects.AddRange(FilterObjects(update, result.RawObjects));
            InitEventStates(result);
            FilterVariables(update, result);

            return result;
        }
        /// <summary>
        /// Push nodes to given pusher
        /// </summary>
        /// <param name="objects">Object type nodes to push</param>
        /// <param name="timeseries">Variable type nodes to push</param>
        /// <param name="pusher">Destination to push to</param>
        /// <param name="initial">True if this counts as initialization of the pusher</param>
        /// <param name="initMissing">Whether or not to initialize nodes with missing ranges to empty</param>
        public async Task PushNodes(
            IEnumerable<BufferedNode> objects,
            IEnumerable<BufferedVariable> timeseries,
            IEnumerable<BufferedReference> references,
            IPusher pusher, bool initial, bool initMissing)
        {
            if (pusher == null) throw new ArgumentNullException(nameof(pusher));
            if (pusher.NoInit)
            {
                log.Warning("Skipping pushing on pusher {name}", pusher.GetType());
                pusher.Initialized = false;
                pusher.NoInit = false;
                return;
            }

            var tasks = new List<Task<bool>>();
            if (objects.Any() || timeseries.Any())
            {
                tasks.Add(pusher.PushNodes(objects, timeseries, config.Extraction.Update, source.Token));
            }
            if (references != null && references.Any())
            {
                tasks.Add(pusher.PushReferences(references, source.Token));
            }

            var results = await Task.WhenAll(tasks);
            var result = results.All(res => res);
            if (!result)
            {
                log.Error("Failed to push nodes on pusher {name}", pusher.GetType());
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
                    .Where(state => state.FrontfillEnabled);
                var initResults = await Task.WhenAll(pusher.InitExtractedRanges(statesToSync, config.History.Backfill, initMissing, source.Token), 
                    pusher.InitExtractedEventRanges(State.EmitterStates.Where(state => state.FrontfillEnabled),
                        config.History.Backfill,
                        initMissing,
                        source.Token));
                if (!initResults.All(res => res))
                {
                    log.Error("Initialization of extracted ranges failed for pusher {name}", pusher.GetType());
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
        /// <param name="references">References to synchronize with destinations</param>
        private async Task PushNodes(IEnumerable<BufferedNode> objects,
            IEnumerable<BufferedVariable> timeseries,
            IEnumerable<BufferedReference> references)
        {
            var newStates = timeseries
                .Select(ts => ts.Id)
                .Distinct()
                .Select(id => State.GetNodeState(id));

            bool initial = objects.Count() + timeseries.Count() == State.ActiveNodes.Count();

            var pushTasks = pushers.Select(pusher => PushNodes(objects, timeseries, references, pusher, initial, false));

            if (StateStorage != null && config.StateStorage.Interval > 0)
            {
                if (Streamer.AllowEvents)
                {
                    pushTasks = pushTasks.Append(StateStorage.RestoreExtractionState(
                        State.EmitterStates.Where(state => state.FrontfillEnabled).ToDictionary(state => state.Id),
                        config.StateStorage.EventStore,
                        false,
                        source.Token));
                }

                if (Streamer.AllowData)
                {
                    pushTasks = pushTasks.Append(StateStorage.RestoreExtractionState(
                        newStates.Where(state => state.FrontfillEnabled).ToDictionary(state => state.Id),
                        config.StateStorage.VariableStore,
                        false,
                        source.Token));
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

            foreach (var state in newStates.Concat<UAHistoryExtractionState>(State.EmitterStates))
            {
                state.FinalizeRangeInit();
            }
        }
        /// <summary>
        /// Subscribe to event changes, then run history.
        /// </summary>
        /// <param name="nodes">Nodes to subscribe to events for</param>
        private async Task SynchronizeEvents()
        {
            await Task.Run(() => uaClient.SubscribeToEvents(State.EmitterStates,
                Streamer.EventSubscriptionHandler, source.Token));
            Interlocked.Increment(ref subscribed);
            if (!State.NodeStates.Any() || subscribed > 1) subscribeFlag = true;
            if (!config.Events.History) return;
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.IsFrontfilling), source.Token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillEvents(State.EmitterStates.Where(state => state.IsBackfilling), source.Token);
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
        private async Task SynchronizeNodes(IEnumerable<NodeExtractionState> states)
        {
            await Task.Run(() => uaClient.SubscribeToNodes(states, Streamer.DataSubscriptionHandler, source.Token));
            Interlocked.Increment(ref subscribed);
            if (!State.EmitterStates.Any() || subscribed > 1) subscribeFlag = true;
            if (!config.History.Enabled) return;
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillData(states.Where(state => state.IsFrontfilling), source.Token);
                if (config.History.Backfill)
                {
                    await historyReader.BackfillData(states.Where(state => state.IsBackfilling), source.Token);
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
        private IEnumerable<Task> Synchronize(IEnumerable<BufferedVariable> variables)
        {
            var states = variables.Select(ts => ts.Id).Distinct().Select(id => State.GetNodeState(id));

            log.Information("Synchronize {NumNodesToSynch} nodes", variables.Count());
            var tasks = new List<Task>();
            // Create tasks to subscribe to nodes, then start history read. We might lose data if history read finished before subscriptions were created.
            if (states.Any())
            {
                tasks.Add(SynchronizeNodes(states));
            }
            if (State.EmitterStates.Any())
            {
                tasks.Add(SynchronizeEvents());
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
                
                if (IsProperty(node))
                {
                    bufferedNode.IsProperty = true;
                    // Properties do not have children themselves in OPC-UA,
                    // but mapped variables might.
                    bufferedNode.PropertiesRead = node.TypeDefinition == VariableTypeIds.PropertyType;
                }
                State.RegisterNode(bufferedNode.Id, GetUniqueId(bufferedNode.Id));
                log.Verbose("HandleNode Variable {name}", bufferedNode.DisplayName);
                commonQueue.Enqueue(bufferedNode);
            }
        }

        private bool IsProperty(ReferenceDescription node)
        {
            if (node.TypeDefinition == VariableTypeIds.PropertyType)
            {
                return true;
            }

            var name = node.DisplayName?.Text;
            if (propertyNameFilter != null
                && name != null
                && propertyNameFilter.IsMatch(name))
            {
                return true;
            }

            if (propertyIdFilter != null
                && node.NodeId.IdType == IdType.String
                && node.NodeId.Identifier is string id
                && propertyIdFilter.IsMatch(id))
            {
                return true;
            }

            return false;
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
                using (var e = new AuditAddNodesEventState(null))
                {
                    e.Update(uaClient.SystemContext, filter.SelectClauses, triggeredEvent);
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
                }

                Looper.ScheduleUpdate();
                return;
            }

            using (var ev = new AuditAddReferencesEventState(null))
            {
                ev.Update(uaClient.SystemContext, filter.SelectClauses, triggeredEvent);

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
            if (disposing)
            {
                Starting.Set(0);
                source?.Cancel();
                source?.Dispose();
                Looper?.Dispose();
            }
        }
    }
}

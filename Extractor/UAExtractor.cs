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

using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Main extractor class, tying together the <see cref="uaClient"/> and CDF client.
    /// </summary>
    public class UAExtractor : IDisposable, IUAClientAccess
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
        private readonly ConcurrentQueue<UANode> commonQueue = new ConcurrentQueue<UANode>();
        private readonly ConcurrentQueue<NodeId> extraNodesToBrowse = new ConcurrentQueue<NodeId>();
        private readonly ConcurrentQueue<(ReferenceDescription Desc, NodeId ParentId)> referenceQueue =
            new ConcurrentQueue<(ReferenceDescription, NodeId)>();

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

        public static readonly Counter BadEvents = Metrics
            .CreateCounter("opcua_bad_events", "Events skipped due to bad fields received");

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

            this.uaClient.OnServerReconnect += UaClient_OnServerReconnect;
            this.uaClient.OnServerDisconnect += UaClient_OnServerDisconnect;

            State = new State();
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

        private void UaClient_OnServerDisconnect(object sender, EventArgs e)
        {
            if (config.Source.ForceRestart && !source.IsCancellationRequested)
            {
                Close();
            }
        }

        private void UaClient_OnServerReconnect(object sender, EventArgs e)
        {
            var client = sender as UAClient;
            if (config.Source.RestartOnReconnect && !source.IsCancellationRequested)
            {
                client.DataTypeManager.Configure();
                client.ClearNodeOverrides();
                client.ClearEventFields();
                client.ResetVisitedNodes();
                RestartExtractor();
            }
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
                    ExtractorUtils.LogException(log, ex, "Unexpected error starting UAClient",
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
            try
            {
                await uaClient.BrowseNodeHierarchy(RootNode, HandleNode, source.Token);
            }
            catch (Exception ex)
            {
                ExtractorUtils.LogException(log, ex, "Unexpected error browsing node hierarchy",
                    "Handled service result exception browsing node hierarchy");
                throw;
            }
            log.Debug("End mapping directory");

            IEnumerable<Task> synchTasks;
            try
            {
                synchTasks = await MapUAToDestinations();
            }
            catch (Exception ex)
            {
                ExtractorUtils.LogException(log, ex, "Unexpected error in MapUAToDestinations",
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
            foreach (var state in State.NodeStates)
            {
                state.RestartHistory();
            }

            foreach (var state in State.EmitterStates)
            {
                state.RestartHistory();
            }

            Looper.WaitForNextPush(true).Wait();
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
        public void Close(bool closeClient = true)
        {
            source.Cancel();
            if (!uaClient.Started || !closeClient) return;
            try
            {
                uaClient.Close();
            }
            catch (ServiceResultException e)
            {
                ExtractorUtils.LogException(log,
                    ExtractorUtils.HandleServiceResult(log, e, ExtractorUtils.SourceOp.CloseSession),
                    "",
                    "");
            }
            uaClient.WaitForOperations().Wait(10000);
            log.Information("Extractor closed");
        }
        /// <summary>
        /// Get uniqueId from uaClient
        /// </summary>
        /// <param name="id">NodeId to convert</param>
        /// <param name="index">Index to use for uniqueId</param>
        /// <returns>Converted uniqueId</returns>
        public string GetUniqueId(ExpandedNodeId id, int index = -1)
        {
            return uaClient.GetUniqueId(id, index);
        }

        /// <summary>
        /// Get the unique reference id, on the form [prefix][reference-name];[sourceId];[targetId]
        /// </summary>
        /// <param name="reference">Reference to get id for</param>
        /// <returns>String reference id</returns>
        public string GetRelationshipId(UAReference reference)
        {
            return uaClient.GetRelationshipId(reference);
        }
        /// <summary>
        /// Calls the ConvertToString method on UAClient. This uses the namespaceTable, so it cannot be static.
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <returns>Converted value</returns>
        public string ConvertToString(object value, IDictionary<long, string> enumValues = null, TypeInfo typeInfo = null)
        {
            return uaClient.ConvertToString(value, enumValues);
        }
        /// <summary>
        /// Read properties for the given list of BufferedNode. This is intelligent,
        /// and keeps track of which properties are in the process of being read,
        /// to prevent multiple pushers from starting PropertyRead operations at the same time.
        /// If this is called on a given node twice in short time, the second call
        /// waits on the first.
        /// </summary>
        /// <param name="nodes">Nodes to get properties for</param>
        public async Task ReadProperties(IEnumerable<UANode> nodes)
        {
            Task newTask = null;
            List<Task> tasksToWaitFor;
            lock (propertySetLock)
            {
                nodes = nodes.Where(node => !pendingProperties.Contains(node.Id) && !node.PropertiesRead).ToList();
                if (nodes.Any())
                {
                    newTask = Task.Run(async () => await uaClient.GetNodeProperties(nodes, source.Token));
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
                !config.Extraction.Update.AnyUpdate && !config.Extraction.Relationships.Enabled);
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
        public Dictionary<string, string> GetExtraMetadata(UANode node)
        {
            if (node == null) return null;
            Dictionary<string, string> fields = null;
            if (node is UAVariable variable)
            {
                fields = DataTypeManager.GetAdditionalMetadata(variable);
            }
            if (config.Extraction.NodeTypes.Metadata)
            {
                fields ??= new Dictionary<string, string>();
                if (node.NodeType?.Name != null)
                {
                    fields["TypeDefinition"] = node.NodeType.Name;
                }
            }
            return fields;

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

            IEnumerable<UAReference> references = null;
            if (config.Extraction.Relationships.Enabled)
            {
                references = await GetRelationshipData(nodes);
            }
            nodes.ClearRaw();

            if (!nodes.Objects.Any() && !nodes.Timeseries.Any() && !nodes.Variables.Any() && (references == null || !references.Any()))
            {
                log.Information("Mapping resulted in no new nodes");
                return Array.Empty<Task>();
            }


            log.Information("Map {obj} objects, and {ts} destination timeseries, representing {var} variables, to destinations",
                nodes.Objects.Count, nodes.Timeseries.Count, nodes.Variables.Count);

            Streamer.AllowData = State.NodeStates.Any();

            await PushNodes(nodes.Objects, nodes.Timeseries, references);

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
                    State.RegisterNode(field.Key, uaClient.GetUniqueId(field.Key));
                }
                if (config.Events.EmitterIds != null && config.Events.EmitterIds.Any())
                {
                    var histEmitterIds = new HashSet<NodeId>((config.Events.HistorizingEmitterIds ?? Array.Empty<ProtoNodeId>())
                        .Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)));
                    foreach (var id in config.Events.EmitterIds.Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)))
                    {
                        var history = (histEmitterIds.Contains(id)) && config.Events.History;
                        State.SetEmitterState(new EventExtractionState(this, id, history,
                            history && config.History.Backfill));
                    }
                }
                var serverNode = uaClient.GetServerNode(source.Token);
                if ((serverNode.EventNotifier & EventNotifiers.SubscribeToEvents) != 0)
                {
                    var history = (serverNode.EventNotifier & EventNotifiers.HistoryRead) != 0 && config.Events.History;
                    State.SetEmitterState(new EventExtractionState(this, serverNode.Id, history,
                        history && config.History.Backfill));
                }
            }
        }

        private class BrowseResult
        {
            /// <summary>
            /// All nodes that should be mapped to destination objects
            /// </summary>
            public List<UANode> Objects { get; } = new List<UANode>();
            /// <summary>
            /// All nodes that should be mapped to destination variables
            /// </summary>
            public List<UAVariable> Timeseries { get; } = new List<UAVariable>();
            /// <summary>
            /// All source system variables that should be read from
            /// </summary>
            public List<UAVariable> Variables { get; } = new List<UAVariable>();
            /// <summary>
            /// All source system objects
            /// </summary>
            public List<UANode> RawObjects { get; private set; } = new List<UANode>();
            /// <summary>
            /// All source system variables
            /// </summary>
            public List<UAVariable> RawVariables { get; private set; } = new List<UAVariable>();
            /// <summary>
            /// Helps free up memory, especially on re-browse
            /// </summary>
            public void ClearRaw()
            {
                RawObjects = null;
                RawVariables = null;
            }
        }

        private async Task GetNodeData(UpdateConfig update, IEnumerable<UANode> nodes)
        {
            var variables = new List<UAVariable>();
            var objects = new List<UANode>();
            foreach (var node in nodes)
            {
                if (node is UAVariable variable)
                {
                    variables.Add(variable);
                }
                else
                {
                    objects.Add(node);
                }
            }

            log.Information("Getting data for {NumVariables} variables and {NumObjects} objects",
                variables.Count, objects.Count);

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

            var extraMetaTasks = new List<Task>();
            extraMetaTasks.Add(DataTypeManager.GetDataTypeMetadataAsync(variables.Select(variable => variable.DataType.Raw).ToHashSet(), source.Token));
            if (config.Extraction.NodeTypes.Metadata)
            {
                extraMetaTasks.Add(uaClient.ObjectTypeManager.GetObjectTypeMetadataAsync(source.Token));
            }
            await Task.WhenAll(extraMetaTasks);
        }

        private IEnumerable<UANode> FilterObjects(UpdateConfig update, IEnumerable<UANode> rawObjects)
        {
            foreach (var node in rawObjects)
            {
                if (update.AnyUpdate)
                {
                    var oldChecksum = State.GetNodeChecksum(node.Id);
                    if (oldChecksum != null)
                    {
                        node.Changed = oldChecksum != node.GetUpdateChecksum(update.Objects, config.Extraction.DataTypes.DataTypeMetadata,
                            config.Extraction.NodeTypes.Metadata);
                        if (node.Changed)
                        {

                            State.AddActiveNode(node, update.Objects, config.Extraction.DataTypes.DataTypeMetadata,
                                config.Extraction.NodeTypes.Metadata);
                            yield return node;
                        }
                        continue;
                    }
                }
                log.Verbose(node.ToString());

                State.AddActiveNode(node, update.Objects, config.Extraction.DataTypes.DataTypeMetadata,
                    config.Extraction.NodeTypes.Metadata);
                yield return node;
            }
        }

        private void InitEventStates(BrowseResult result)
        {
            foreach (var node in result.Objects.Concat(result.Variables))
            {
                if ((node.EventNotifier & EventNotifiers.SubscribeToEvents) == 0) continue;
                if (State.GetEmitterState(node.Id) != null) continue;
                bool history = (node.EventNotifier & EventNotifiers.HistoryRead) != 0 && config.Events.History;
                var eventState = new EventExtractionState(this, node.Id, history, history && config.History.Backfill);
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
                    var oldChecksum = State.GetNodeChecksum(node.Id);
                    if (oldChecksum != null)
                    {
                        node.Changed = oldChecksum != node.GetUpdateChecksum(update.Variables, config.Extraction.DataTypes.DataTypeMetadata,
                            config.Extraction.NodeTypes.Metadata);

                        if (node.Changed)
                        {
                            var oldState = State.GetNodeState(node.Id);
                            State.AddActiveNode(node, update.Variables, config.Extraction.DataTypes.DataTypeMetadata,
                                config.Extraction.NodeTypes.Metadata);
                            if (oldState.IsArray)
                            {
                                var children = node.CreateArrayChildren();
                                foreach (var child in children)
                                {
                                    child.Changed = true;
                                    result.Timeseries.Add(child);
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
                log.Verbose(node.ToString());
                result.Variables.Add(node);
                var state = new VariableExtractionState(this, node, node.Historizing, node.Historizing && config.History.Backfill);

                State.AddActiveNode(node, update.Variables, config.Extraction.DataTypes.DataTypeMetadata,
                    config.Extraction.NodeTypes.Metadata);
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
            var nodeMap = new Dictionary<NodeId, UANode>();

            while (commonQueue.TryDequeue(out UANode node))
            {
                if (node.Ignore) continue;
                nodeMap[node.Id] = node;
            }

            var update = config.Extraction.Update;
            await GetNodeData(update, nodeMap.Values);

            foreach (var node in nodeMap.Values)
            {
                if (nodeMap.TryGetValue(node.ParentId, out var parent))
                {
                    node.Parent = parent;
                }

                // Transformations here

                if (node.Ignore) continue;

                if (node.IsProperty)
                {
                    if (node.Parent == null) continue;
                    if (node.Parent.Properties == null)
                    {
                        node.Parent.Properties = new List<UANode>();
                    }
                    node.Parent.Properties.Add(node);
                }
                else if (node.IsVariable && node is UAVariable variable)
                {
                    result.RawVariables.Add(variable);
                }
                else
                {
                    result.RawObjects.Add(node);
                }
            }

            result.Objects.AddRange(FilterObjects(update, result.RawObjects));
            InitEventStates(result);
            FilterVariables(update, result);

            return result;
        }


        private void PushNodesFailure(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> timeseries,
            IEnumerable<UAReference> references,
            bool nodesPassed,
            bool referencesPassed,
            bool dpRangesPassed,
            IPusher pusher)
        {
            pusher.Initialized = false;
            pusher.DataFailing = true;
            pusher.EventsFailing = true;
            if (!nodesPassed)
            {
                pusher.PendingNodes.AddRange(objects);
                pusher.PendingNodes.AddRange(timeseries);
            }
            else if (!dpRangesPassed)
            {
                pusher.PendingNodes.AddRange(timeseries
                    .DistinctBy(ts => ts.Id)
                    .Where(ts => State.GetNodeState(ts.Id)?.FrontfillEnabled ?? false));
            }
            if (!referencesPassed && references != null)
            {
                pusher.PendingReferences.AddRange(references);
            }
        }
        /// <summary>
        /// Push nodes to given pusher
        /// </summary>
        /// <param name="objects">Object type nodes to push</param>
        /// <param name="timeseries">Variable type nodes to push</param>
        /// <param name="pusher">Destination to push to</param>
        /// <param name="initial">True if this counts as initialization of the pusher</param>
        public async Task PushNodes(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> timeseries,
            IEnumerable<UAReference> references,
            IPusher pusher, bool initial)
        {
            if (pusher == null) throw new ArgumentNullException(nameof(pusher));
            if (pusher.NoInit)
            {
                log.Warning("Skipping pushing on pusher {name}", pusher.GetType());
                PushNodesFailure(objects, timeseries, references, false, false, false, pusher);
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
                int idx = 0;
                bool nodesPassed = objects.Any() && timeseries.Any() && results[idx++];
                bool referencesPassed = references != null && references.Any() && results[idx];
                PushNodesFailure(objects, timeseries, references, nodesPassed, referencesPassed, false, pusher);
                return;
            }

            if (pusher.BaseConfig.ReadExtractedRanges)
            {
                var statesToSync = timeseries
                    .Select(ts => ts.Id)
                    .Distinct()
                    .Select(id => State.GetNodeState(id))
                    .Where(state => state != null && state.FrontfillEnabled && !state.Initialized);

                var eventStatesToSync = State.EmitterStates.Where(state => state.FrontfillEnabled && !state.Initialized);

                var initResults = await Task.WhenAll(
                    pusher.InitExtractedRanges(statesToSync, config.History.Backfill, source.Token),
                    pusher.InitExtractedEventRanges(eventStatesToSync, config.History.Backfill, source.Token));

                if (!initResults.All(res => res))
                {
                    log.Error("Initialization of extracted ranges failed for pusher {name}", pusher.GetType());
                    PushNodesFailure(objects, timeseries, references, true, true, initResults[0], pusher);
                    return;
                }
            }

            pusher.Initialized |= initial;
        }

        private async Task<IEnumerable<UAReference>> GetRelationshipData(BrowseResult nodes)
        {
            var references = await referenceTypeManager.GetReferencesAsync(nodes.RawObjects.Concat(nodes.RawVariables).DistinctBy(node => node.Id),
                    ReferenceTypeIds.NonHierarchicalReferences, source.Token);

            if (config.Extraction.Relationships.Hierarchical)
            {
                var nodeMap = nodes.Objects.Concat(nodes.Variables)
                    .Where(node => !(node is UAVariable variable) || variable.Index == -1)
                    .DistinctBy(node => node.Id)
                    .ToDictionary(node => node.Id);
                var hierarchicalReferences = new List<UAReference>();

                while (referenceQueue.TryDequeue(out var pair))
                {
                    // The child should always be in the list of mapped nodes here
                    var nodeId = uaClient.ToNodeId(pair.Desc.NodeId);
                    if (!nodeMap.TryGetValue(nodeId, out var childNode)) continue;
                    if (childNode == null || childNode is UAVariable childVar && childVar.IsProperty) continue;

                    bool childIsTs = childNode is UAVariable cVar && !cVar.IsArray;

                    hierarchicalReferences.Add(new UAReference(
                        pair.Desc.ReferenceTypeId,
                        true,
                        pair.ParentId,
                        childNode.Id,
                        false,
                        childIsTs,
                        referenceTypeManager));

                    if (config.Extraction.Relationships.InverseHierarchical)
                    {
                        hierarchicalReferences.Add(new UAReference(
                            pair.Desc.ReferenceTypeId,
                            false,
                            childNode.Id,
                            pair.ParentId,
                            childIsTs,
                            true,
                            referenceTypeManager));
                    }
                }
                references = references.Concat(hierarchicalReferences);
            }

            await referenceTypeManager.GetReferenceTypeDataAsync(source.Token);
            return references.Distinct();
        }
        /// <summary>
        /// Push given lists of nodes to pusher destinations, and fetches latest timestamp for relevant nodes.
        /// </summary>
        /// <param name="objects">Objects to synchronize with destinations</param>
        /// <param name="timeseries">Variables to synchronize with destinations</param>
        /// <param name="references">References to synchronize with destinations</param>
        private async Task PushNodes(IEnumerable<UANode> objects,
            IEnumerable<UAVariable> timeseries,
            IEnumerable<UAReference> references)
        {
            var newStates = timeseries
                .Select(ts => ts.Id)
                .Distinct()
                .Select(id => State.GetNodeState(id));

            bool initial = objects.Count() + timeseries.Count() >= State.NumActiveNodes;

            var pushTasks = pushers.Select(pusher => PushNodes(objects, timeseries, references, pusher, initial));

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
        private async Task SynchronizeNodes(IEnumerable<VariableExtractionState> states)
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
        private IEnumerable<Task> Synchronize(IEnumerable<UAVariable> variables)
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
        /// Callback for the browse operation, creates <see cref="UANode"/>s and enqueues them.
        /// </summary>
        /// <param name="node">Description of the node to be handled</param>
        /// <param name="parentId">Id of the parent node</param>
        private void HandleNode(ReferenceDescription node, NodeId parentId)
        {
            bool mapped = false;
            log.Verbose("HandleNode {parent} {node}", parentId, node);

            if (node.NodeClass == NodeClass.Object)
            {
                var bufferedNode = new UANode(uaClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);

                if (node.TypeDefinition != null && !node.TypeDefinition.IsNull)
                {
                    bufferedNode.NodeType = uaClient.ObjectTypeManager.GetObjectType(uaClient.ToNodeId(node.TypeDefinition), false);
                }
                log.Verbose("HandleNode Object {name}", bufferedNode.DisplayName);
                State.RegisterNode(bufferedNode.Id, GetUniqueId(bufferedNode.Id));
                commonQueue.Enqueue(bufferedNode);
                mapped = true;
            }
            else if (node.NodeClass == NodeClass.Variable)
            {
                var bufferedNode = new UAVariable(uaClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);

                if (IsProperty(node))
                {
                    bufferedNode.IsProperty = true;
                    // Properties do not have children themselves in OPC-UA,
                    // but mapped variables might.
                    bufferedNode.PropertiesRead = node.TypeDefinition == VariableTypeIds.PropertyType;
                }
                else
                {
                    mapped = true;
                    if (node.TypeDefinition != null && !node.TypeDefinition.IsNull)
                    {
                        bufferedNode.NodeType = uaClient.ObjectTypeManager.GetObjectType(uaClient.ToNodeId(node.TypeDefinition), true);
                    }
                }
                State.RegisterNode(bufferedNode.Id, GetUniqueId(bufferedNode.Id));
                log.Verbose("HandleNode Variable {name}", bufferedNode.DisplayName);
                commonQueue.Enqueue(bufferedNode);
            }

            if (mapped && config.Extraction.Relationships.Enabled && config.Extraction.Relationships.Hierarchical)
            {
                if (parentId == null || parentId.IsNullNodeId) return;
                referenceQueue.Enqueue((node, parentId));
            }
        }

        public bool IsProperty(ReferenceDescription node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
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
                uaClient.OnServerDisconnect -= UaClient_OnServerDisconnect;
                uaClient.OnServerReconnect -= UaClient_OnServerReconnect;
            }
        }
    }
    public interface IUAClientAccess
    {
        string GetUniqueId(ExpandedNodeId id, int index = -1);
        string ConvertToString(object value, IDictionary<long, string> enumValues = null, TypeInfo typeInfo = null);
        string GetRelationshipId(UAReference reference);
    }
}

/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

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
using Cognite.OpcUa.History;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers;
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
using System.Threading;
using System.Threading.Tasks;


[assembly: CLSCompliant(false)]
namespace Cognite.OpcUa
{
    /// <summary>
    /// Main extractor class, tying together the <see cref="uaClient"/> and CDF client.
    /// </summary>
    public class UAExtractor : IDisposable, IUAClientAccess
    {
        private readonly UAClient uaClient;
        private readonly FullConfig config;
        public FullConfig FullConfig => config;
        public Looper Looper { get; }
        public FailureBuffer? FailureBuffer { get; }
        public IExtractionStateStore? StateStorage { get; }
        public State State { get; }
        public Streamer Streamer { get; }
        public DataTypeManager DataTypeManager => uaClient.DataTypeManager;

        private readonly HistoryReader historyReader;
        public ReferenceTypeManager? ReferenceTypeManager { get; private set; }
        public IEnumerable<NodeId> RootNodes { get; private set; } = null!;
        private readonly IEnumerable<IPusher> pushers;
        private readonly ConcurrentQueue<NodeId> extraNodesToBrowse = new ConcurrentQueue<NodeId>();

        // Concurrent reading of properties
        private readonly HashSet<NodeId> pendingProperties = new HashSet<NodeId>();
        private readonly object propertySetLock = new object();
        private readonly List<Task> propertyReadTasks = new List<Task>();
        public IEnumerable<NodeTransformation>? Transformations { get; private set; }
        public StringConverter StringConverter => uaClient.StringConverter;

        public bool Started { get; private set; }
        public bool Pushing { get; private set; }

        private int subscribed;
        private bool subscribeFlag;

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
            IExtractionStateStore? stateStore,
            CancellationToken token)
        {
            this.uaClient = uaClient;
            this.pushers = pushers;
            this.config = config;

            this.uaClient.OnServerReconnect += UaClient_OnServerReconnect;
            this.uaClient.OnServerDisconnect += UaClient_OnServerDisconnect;

            State = new State();
            Streamer = new Streamer(this, config);
            StateStorage = stateStore;
            if (config.Extraction.Relationships.Enabled)
            {
                ReferenceTypeManager = new ReferenceTypeManager(uaClient, this);
            }

            source = CancellationTokenSource.CreateLinkedTokenSource(token);

            if (config.FailureBuffer.Enabled)
            {
                FailureBuffer = new FailureBuffer(config, this, pushers.OfType<InfluxPusher>().FirstOrDefault());
            }
            historyReader = new HistoryReader(uaClient, this, config.History, source.Token);
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
        /// Event handler for UAClient disconnect
        /// </summary>
        /// <param name="sender">UAClient that generated this event</param>
        /// <param name="e">EventArgs for this event</param>
        private void UaClient_OnServerDisconnect(object sender, EventArgs e)
        {
            if (config.Source.ForceRestart && !source.IsCancellationRequested)
            {
                Close();
            }
        }

        /// <summary>
        /// Event handler for UAClient reconnect
        /// </summary>
        /// <param name="sender">UAClient that generated this event</param>
        /// <param name="e">EventArgs for this event</param>
        private void UaClient_OnServerReconnect(object sender, EventArgs e)
        {
            if (sender is UAClient client && config.Source.RestartOnReconnect && !source.IsCancellationRequested)
            {
                client.DataTypeManager.Configure();
                client.ClearNodeOverrides();
                client.ClearEventFields();
                client.Browser.ResetVisitedNodes();
                RestartExtractor();
            }
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
            if (config.Source.NodeSetSource == null
                || (!config.Source.NodeSetSource.NodeSets?.Any() ?? false)
                || !config.Source.NodeSetSource.Types)
            {
                await DataTypeManager.GetDataTypeStructureAsync(source.Token);
            }

            Started = true;
            startTime.Set(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds());

            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }

            var synchTasks = await RunMapping(RootNodes, true, true);

            if (config.FailureBuffer.Enabled && FailureBuffer != null)
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
            historyReader?.Terminate(source.Token, 30).Wait();
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
            await uaClient.WaitForOperations(source.Token);
            ConfigureExtractor();
            uaClient.Browser.ResetVisitedNodes();

            var synchTasks = await RunMapping(RootNodes, true, false);

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
                var historyTasks = await RunMapping(nodesToBrowse.Distinct(), true, false);

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
            uaClient.WaitForOperations(source.Token).Wait();
            log.Information("Extractor closed");
        }

        /// <summary>
        /// Get uniqueId from uaClient
        /// </summary>
        /// <param name="id">NodeId to convert</param>
        /// <param name="index">Index to use for uniqueId</param>
        /// <returns>Converted uniqueId</returns>
        public string? GetUniqueId(ExpandedNodeId id, int index = -1)
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
        /// Read properties for the given list of BufferedNode. This is intelligent,
        /// and keeps track of which properties are in the process of being read,
        /// to prevent multiple pushers from starting PropertyRead operations at the same time.
        /// If this is called on a given node twice in short time, the second call
        /// waits on the first.
        /// </summary>
        /// <param name="nodes">Nodes to get properties for</param>
        public async Task ReadProperties(IEnumerable<UANode> nodes)
        {
            Task? newTask = null;
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
                    node.Attributes.PropertiesRead = true;
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
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.IsFrontfilling));
                if (config.History.Backfill)
                {
                    await historyReader.BackfillEvents(State.EmitterStates.Where(state => state.IsBackfilling));
                }
            }), Task.Run(async () =>
            {
                await historyReader.FrontfillData(State.NodeStates.Where(state => state.IsFrontfilling).ToList());
                if (config.History.Backfill)
                {
                    await historyReader.BackfillData(State.NodeStates.Where(state => state.IsBackfilling).ToList());
                }
            }));
        }

        /// <summary>
        /// Redo browse, then schedule history on the looper.
        /// </summary>
        public async Task Rebrowse()
        {
            // If we are updating we want to re-discover nodes in order to run them through mapping again.
            var historyTasks = await RunMapping(RootNodes,
                !config.Extraction.Update.AnyUpdate && !config.Extraction.Relationships.Enabled,
                false);
            Looper.ScheduleTasks(historyTasks);
        }

        /// <summary>
        /// Used for testing, wait for subscriptions to be created, with given timeout.
        /// </summary>
        /// <param name="timeout">Timeout in 10ths of a second</param>
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

        private async Task<IEnumerable<Task>> RunMapping(IEnumerable<NodeId> nodesToBrowse, bool ignoreVisited, bool initial)
        {
            bool readFromOpc = true;

            NodeSourceResult? result = null;
            IEventFieldSource? eventSource = null;
            if ((config.Cognite?.RawNodeBuffer?.Enable ?? false) && initial)
            {
                log.Debug("Begin fetching data from CDF");
                var handler = new CDFNodeSource(config, this, uaClient, pushers.OfType<CDFPusher>().First());
                await handler.ReadRawNodes(source.Token);

                result = await handler.ParseResults(source.Token);

                if (result == null || !result.DestinationObjects.Any() && !result.DestinationVariables.Any())
                {
                    if (!config.Cognite.RawNodeBuffer.BrowseOnEmpty)
                    {
                        throw new ExtractorFailureException("Found no nodes in CDF, restarting");
                    }
                    log.Information("Found no nodes in CDF, reading from OPC-UA server");
                }
                else
                {
                    readFromOpc = false;
                }
            }

            if ((config.Source.NodeSetSource?.NodeSets?.Any() ?? false) && initial
                && (config.Source.NodeSetSource.Instance || config.Source.NodeSetSource.Types))
            {
                log.Debug("Begin fetching data from internal node set");
                var handler = new NodeSetSource(config, this, uaClient);
                handler.BuildNodes(nodesToBrowse);

                if (config.Source.NodeSetSource.Instance)
                {
                    result = await handler.ParseResults(source.Token);
                    readFromOpc = false;
                }
                if (config.Source.NodeSetSource.Types)
                {
                    eventSource = handler;
                }
            }

            if (config.Events.Enabled)
            {
                var eventFields = await uaClient.GetEventFields(eventSource, source.Token);
                foreach (var field in eventFields)
                {
                    State.ActiveEvents[field.Key] = field.Value;
                    State.RegisterNode(field.Key, uaClient.GetUniqueId(field.Key));
                }
            }

            if (readFromOpc)
            {
                log.Debug("Begin mapping directory");
                var handler = new UANodeSource(config, this, uaClient);
                try
                {
                    await uaClient.Browser.BrowseNodeHierarchy(nodesToBrowse, handler.Callback, source.Token, ignoreVisited);
                }
                catch (Exception ex)
                {
                    ExtractorUtils.LogException(log, ex, "Unexpected error browsing node hierarchy",
                        "Handled service result exception browsing node hierarchy");
                    throw;
                }
                result = await handler.ParseResults(source.Token);
                log.Debug("End mapping directory");
            }

            try
            {
                return await MapUAToDestinations(result);
            }
            catch (Exception ex)
            {
                ExtractorUtils.LogException(log, ex, "Unexpected error in MapUAToDestinations",
                    "Handled service result exception in MapUAToDestinations");
                throw;
            }
        }

        /// <summary>
        /// Empties the node queue, pushing nodes to each destination, and starting subscriptions and history.
        /// This is the entry point for mapping on the extractor.
        /// </summary>
        /// <returns>A list of history tasks</returns>
        private async Task<IEnumerable<Task>> MapUAToDestinations(NodeSourceResult? result)
        {
            if (result == null) return Enumerable.Empty<Task>();

            Streamer.AllowData = State.NodeStates.Any();
            await PushNodes(result.DestinationObjects, result.DestinationVariables, result.DestinationReferences);
            // Changed flag means that it already existed, so we avoid synchronizing these.
            var historyTasks = Synchronize(result.SourceVariables.Where(var => !var.Changed));
            Starting.Set(0);
            return historyTasks;
        }

        /// <summary>
        /// Build transformations from configured list and deprecated filter properties.
        /// </summary>
        private void BuildTransformations()
        {
            var transformations = new List<NodeTransformation>();
            int idx = 0;

            if (config.Extraction.Transformations != null)
            {
                foreach (var raw in config.Extraction.Transformations)
                {
                    transformations.Add(new NodeTransformation(raw, idx++));
                }
            }

            if (!string.IsNullOrEmpty(config.Extraction.PropertyIdFilter))
            {
                log.Warning("Property Id filter is deprecated, use transformations instead");
                transformations.Add(new NodeTransformation(new RawNodeTransformation
                {
                    Filter = new RawNodeFilter
                    {
                        Id = config.Extraction.PropertyIdFilter
                    },
                    Type = TransformationType.Property
                }, idx++));
            }
            if (!string.IsNullOrEmpty(config.Extraction.PropertyNameFilter))
            {
                log.Warning("Property Name filter is deprecated, use transformations instead");
                transformations.Add(new NodeTransformation(new RawNodeTransformation
                {
                    Filter = new RawNodeFilter
                    {
                        Name = config.Extraction.PropertyNameFilter
                    },
                    Type = TransformationType.Property
                }, idx++));
            }
            if (config.Extraction.IgnoreName != null && config.Extraction.IgnoreName.Any())
            {
                log.Warning("Ignore name is deprecated, use transformations instead");
                var filterStr = string.Join('|', config.Extraction.IgnoreName.Select(str => $"^{str}$"));
                transformations.Add(new NodeTransformation(new RawNodeTransformation
                {
                    Filter = new RawNodeFilter
                    {
                        Name = filterStr
                    },
                    Type = TransformationType.Ignore
                }, idx++));
            }
            if (config.Extraction.IgnoreNamePrefix != null && config.Extraction.IgnoreNamePrefix.Any())
            {
                log.Warning("Ignore name prefix is deprecated, use transformations instead: {cnf}", string.Join(',', config.Extraction.IgnoreNamePrefix));
                var filterStr = string.Join('|', config.Extraction.IgnoreNamePrefix.Select(str => $"^{str}"));
                transformations.Add(new NodeTransformation(new RawNodeTransformation
                {
                    Filter = new RawNodeFilter
                    {
                        Name = filterStr
                    },
                    Type = TransformationType.Ignore
                }, idx++));
            }
            foreach (var trans in transformations)
            {
                log.Debug(trans.ToString());
            }
            
            uaClient.Browser.IgnoreFilters = transformations.Where(trans => trans.Type == TransformationType.Ignore).Select(trans => trans.Filter).ToList();
            Transformations = transformations;
        }

        /// <summary>
        /// Set up extractor once UAClient is started. This resets the internal state of the extractor.
        /// </summary>
        private void ConfigureExtractor()
        {
            RootNodes = config.Extraction.GetRootNodes(uaClient);

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
                if (config.Events.EmitterIds != null && config.Events.EmitterIds.Any()
                    || config.Events.HistorizingEmitterIds != null && config.Events.HistorizingEmitterIds.Any())
                {
                    var histEmitterIds = new HashSet<NodeId>((config.Events.HistorizingEmitterIds ?? Enumerable.Empty<ProtoNodeId>())
                        .Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)));
                    var emitterIds = new HashSet<NodeId>((config.Events.EmitterIds ?? Enumerable.Empty<ProtoNodeId>())
                        .Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)));
                    var eventEmitterIds = new HashSet<NodeId>(histEmitterIds.Concat(emitterIds));

                    foreach (var id in eventEmitterIds)
                    {
                        var history = (histEmitterIds.Contains(id)) && config.Events.History;
                        var subscription = emitterIds.Contains(id);
                        State.SetEmitterState(new EventExtractionState(this, id, history, history && config.History.Backfill, subscription));
                    }
                }
                if (config.Events.ReadServer)
                {
                    var serverNode = uaClient.GetServerNode(source.Token);
                    if (serverNode.EventNotifier != 0)
                    {
                        var history = (serverNode.EventNotifier & EventNotifiers.HistoryRead) != 0 && config.Events.History;
                        var subscription = (serverNode.EventNotifier & EventNotifiers.SubscribeToEvents) != 0;
                        State.SetEmitterState(new EventExtractionState(this, serverNode.Id, history, history && config.History.Backfill, subscription));
                    }
                }
            }
            BuildTransformations();

            var helper = new ServerInfoHelper(uaClient);
            helper.LimitConfigValues(config, source.Token);
        }

        /// <summary>
        /// Called when pushing nodes fail, to properly add the nodes not yet pushed to
        /// PendingNodes and PendingReferences on the pusher.
        /// </summary>
        /// <param name="objects">Objects pushed</param>
        /// <param name="timeseries">Timeseries pushed</param>
        /// <param name="references">References pushed</param>
        /// <param name="nodesPassed">True if nodes were successfully pushed</param>
        /// <param name="referencesPassed">True if references were successfully pushed</param>
        /// <param name="dpRangesPassed">True if datapoint ranges were pushed.</param>
        /// <param name="pusher">Pusher pushed to</param>
        private void PushNodesFailure(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> timeseries,
            IEnumerable<UAReference>? references,
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
        /// <param name="references">References to push</param>
        /// <param name="pusher">Destination to push to</param>
        /// <param name="initial">True if this counts as initialization of the pusher</param>
        public async Task PushNodes(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> timeseries,
            IEnumerable<UAReference> references,
            IPusher pusher, bool initial)
        {
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
                    .SelectNonNull(id => State.GetNodeState(id))
                    .Where(state => state.FrontfillEnabled && !state.Initialized);

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
                        newStates.Where(state => state != null && state.FrontfillEnabled).ToDictionary(state => state?.Id),
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

            foreach (var state in newStates.Concat<UAHistoryExtractionState?>(State.EmitterStates))
            {
                state?.FinalizeRangeInit();
            }
        }

        /// <summary>
        /// Subscribe to event changes, then run history.
        /// </summary>
        private async Task SynchronizeEvents()
        {
            if (config.Subscriptions.Events)
            {
                var subscribeStates = State.EmitterStates.Where(state => state.ShouldSubscribe);
                await Task.Run(() => uaClient.SubscribeToEvents(subscribeStates,
                    Streamer.EventSubscriptionHandler, source.Token));
            }

            Interlocked.Increment(ref subscribed);
            if (!State.NodeStates.Any() || subscribed > 1) subscribeFlag = true;
            if (!config.Events.History) return;
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.IsFrontfilling));
                if (config.History.Backfill)
                {
                    await historyReader.BackfillEvents(State.EmitterStates.Where(state => state.IsBackfilling));
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
            if (config.Subscriptions.DataPoints)
            {
                var subscribeStates = states.Where(state => state.ShouldSubscribe);
                await Task.Run(() => uaClient.SubscribeToNodes(subscribeStates, Streamer.DataSubscriptionHandler, source.Token));
            }

            Interlocked.Increment(ref subscribed);
            if (!State.EmitterStates.Any() || subscribed > 1) subscribeFlag = true;
            if (!config.History.Enabled) return;
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillData(states.Where(state => state.IsFrontfilling));
                if (config.History.Backfill)
                {
                    await historyReader.BackfillData(states.Where(state => state.IsBackfilling));
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
        /// <returns>Two tasks, one for data and one for events</returns>
        private IEnumerable<Task> Synchronize(IEnumerable<UAVariable> variables)
        {
            var states = variables.Select(ts => ts.Id).Distinct().SelectNonNull(id => State.GetNodeState(id));

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
        string? GetUniqueId(ExpandedNodeId id, int index = -1);
        StringConverter StringConverter { get; }
        string GetRelationshipId(UAReference reference);
    }
}

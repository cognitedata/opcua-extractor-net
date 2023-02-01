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
using Cognite.Extractor.Utils;
using Cognite.OpcUa.History;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.PubSub;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;


[assembly: CLSCompliant(false)]
namespace Cognite.OpcUa
{
    /// <summary>
    /// Main extractor class, tying together the <see cref="uaClient"/> and CDF client.
    /// </summary>
    public class UAExtractor : BaseExtractor<FullConfig>, IUAClientAccess
    {
        private readonly UAClient uaClient;
        public Looper Looper { get; private set; } = null!;
        public FailureBuffer? FailureBuffer { get; }
        public IExtractionStateStore? StateStorage { get; }
        public State State { get; }
        public Streamer Streamer { get; }
        public DataTypeManager DataTypeManager => uaClient.DataTypeManager;

        private HistoryReader historyReader = null!;
        public ReferenceTypeManager? ReferenceTypeManager { get; private set; }
        public IEnumerable<NodeId> RootNodes { get; private set; } = null!;
        private readonly IEnumerable<IPusher> pushers;
        private readonly ConcurrentQueue<NodeId> extraNodesToBrowse = new ConcurrentQueue<NodeId>();
        public IEnumerable<NodeTransformation>? Transformations { get; private set; }
        public StringConverter StringConverter => uaClient.StringConverter;
        private readonly PubSubManager? pubSubManager;

        private NodeSetSource? nodeSetSource;

        private readonly DeletesManager? deletesManager;

        public bool ShouldStartLooping { get; set; } = true;

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

        private readonly ILogger<UAExtractor> log;

        private readonly RebrowseTriggerManager? rebrowseTriggerManager;

        public static readonly DateTime StartTime = DateTime.UtcNow;

        /// <summary>
        /// Construct extractor with list of pushers
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pushers">List of pushers to be used</param>
        /// <param name="uaClient">UAClient to be used</param>
        public UAExtractor(FullConfig config,
            IServiceProvider provider,
            IEnumerable<IPusher> pushers,
            UAClient uaClient,
            IExtractionStateStore? stateStore,
            ExtractionRun? run = null,
            RemoteConfigManager<FullConfig>? configManager = null) : base(config, provider, null, run, configManager)
        {
            this.uaClient = uaClient;
            this.pushers = pushers.Where(pusher => pusher != null).ToList();
            log = provider.GetRequiredService<ILogger<UAExtractor>>();

            log.LogDebug("Config:{NewLine}{Config}", Environment.NewLine, ExtractorUtils.ConfigToString(Config));

            this.uaClient.OnServerReconnect += UaClient_OnServerReconnect;
            this.uaClient.OnServerDisconnect += UaClient_OnServerDisconnect;

            State = new State();
            Streamer = new Streamer(provider.GetRequiredService<ILogger<Streamer>>(), this, config);
            StateStorage = stateStore;
            if (config.Extraction.Relationships.Enabled)
            {
                ReferenceTypeManager = new ReferenceTypeManager(Config, provider.GetRequiredService<ILogger<ReferenceTypeManager>>(), uaClient, this);
            }

            if (config.FailureBuffer.Enabled)
            {
                FailureBuffer = new FailureBuffer(provider.GetRequiredService<ILogger<FailureBuffer>>(),
                    config, this, this.pushers.OfType<InfluxPusher>().FirstOrDefault());
            }
            if (run != null) run.Continuous = true;

            log.LogInformation("Building extractor with {NumPushers} pushers", this.pushers.Count());

            if (Config.PubSub.Enabled)
            {
                pubSubManager = new PubSubManager(provider.GetRequiredService<ILogger<PubSubManager>>(), uaClient, this, Config.PubSub);
            }

            foreach (var pusher in this.pushers)
            {
                pusher.Extractor = this;
            }

            if (configManager != null)
            {
                configManager.UpdatePeriod = new BasicTimeSpanProvider(TimeSpan.FromMinutes(2));
                OnConfigUpdate += OnNewConfig;
            }

            if (config.Extraction.RebrowseTriggers is not null)
            {
                rebrowseTriggerManager = new RebrowseTriggerManager(
                    provider.GetRequiredService<ILogger<RebrowseTriggerManager>>(),
                    uaClient, config.Extraction.RebrowseTriggers,
                    this
                );
            }

            if (config.Extraction.Deletes.Enabled)
            {
                if (stateStore != null)
                {
                    deletesManager = new DeletesManager(stateStore, this, provider.GetRequiredService<ILogger<DeletesManager>>(), config);
                }
                else
                {
                    log.LogWarning("Deletes are enabled, but no state store is configured. Detecting deleted nodes will not work.");
                }
            }
        }

        private void OnNewConfig(object sender, FullConfig newConfig, int revision)
        {
            log.LogInformation("New remote configuration file obtained, restarting extractor");
            // Trigger close, we can just fire-and-forget this.
            _ = Close();
        }

        /// <summary>
        /// Event handler for UAClient disconnect
        /// </summary>
        /// <param name="sender">UAClient that generated this event</param>
        /// <param name="e">EventArgs for this event</param>
        private void UaClient_OnServerDisconnect(object sender, EventArgs e)
        {
            /* if (Config.Source.ForceRestart && !Source.IsCancellationRequested)
            {
                Close().Wait();
            } */
        }

        /// <summary>
        /// Event handler for UAClient reconnect
        /// </summary>
        /// <param name="sender">UAClient that generated this event</param>
        /// <param name="e">EventArgs for this event</param>
        private void UaClient_OnServerReconnect(object sender, EventArgs e)
        {
            if (sender is not UAClient client || Source.IsCancellationRequested) return;

            Scheduler.ScheduleTask(null, async t =>
            {
                await EnsureSubscriptions();
                if (Config.Source.RestartOnReconnect)
                {
                    client.DataTypeManager.Configure();
                    client.ClearNodeOverrides();
                    client.ClearEventFields();
                    client.Browser.ResetVisitedNodes();
                    await RestartExtractor();
                }
                else
                {
                    if (historyReader != null)
                    {
                        await historyReader.Terminate(Source.Token);
                        foreach (var state in State.NodeStates)
                        {
                            state.RestartHistory();
                        }

                        foreach (var state in State.EmitterStates)
                        {
                            state.RestartHistory();
                        }

                        await RestartHistory();
                    }
                }
            });
        }
        #region Interface

        protected override void Init(CancellationToken token)
        {
            base.Init(token);
            if (historyReader != null)
            {
                historyReader.Dispose();
            }
            Looper = new Looper(Provider.GetRequiredService<ILogger<Looper>>(), Scheduler, this, Config, pushers);
            historyReader = new HistoryReader(Provider.GetRequiredService<ILogger<HistoryReader>>(),
                uaClient, this, Config.History, Source.Token);
        }

        public void InitExternal(CancellationToken token)
        {
            Init(token);
        }

        private async Task RunExtractorInternal(int startTimeout = -1)
        {
            Starting.Set(1);

            if (Config.HighAvailability != null)
            {
                await RunWithHighAvailabilityAndWait(Config.HighAvailability);
            }

            if (!uaClient.Started && Config.Source.EndpointUrl != null)
            {
                log.LogInformation("Start UAClient");
                try
                {
                    await uaClient.Run(Source.Token, startTimeout);
                }
                catch (OperationCanceledException)
                {
                    log.LogWarning("Connecting to OPC-UA server was cancelled");
                    throw;
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
            else if (Config.Source.EndpointUrl == null)
            {
                log.LogWarning("No endpointUrl specified, extractor will attempt to run without OPC-UA context. " +
                    "This mode is experimental, most configuration options will not work.");

                if (Config.Source.NodeSetSource == null) throw new ConfigurationException("Extractor configured without both EndpointUrl and NodeSetSource");
                Config.Subscriptions.Events = false;
                Config.Subscriptions.DataPoints = false;
                Config.History.Enabled = false;
                Config.Source.LimitToServerConfig = false;
                Config.Source.NodeSetSource.Types = true;
                Config.Source.NodeSetSource.Instance = true;
                nodeSetSource = new NodeSetSource(Provider.GetRequiredService<ILogger<NodeSetSource>>(), Config, this, uaClient);
                nodeSetSource.Build();
            }

            await ConfigureExtractor();
            if (Config.Source.NodeSetSource == null
                || (!Config.Source.NodeSetSource.NodeSets?.Any() ?? false)
                || !Config.Source.NodeSetSource.Types)
            {
                await DataTypeManager.GetDataTypeStructureAsync(Source.Token);
            }

            Started = true;

            if (rebrowseTriggerManager is not null)
            {
                await rebrowseTriggerManager.EnableCustomServerSubscriptions(Source.Token);
            }

            startTime.Set(new DateTimeOffset(UAExtractor.StartTime).ToUnixTimeMilliseconds());

            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }

            var synchTasks = await RunMapping(RootNodes, true, true, true);

            if (Config.FailureBuffer.Enabled && FailureBuffer != null)
            {
                await FailureBuffer.InitializeBufferStates(State.NodeStates, State.EmitterStates, Source.Token);
            }
            foreach (var task in synchTasks)
            {
                Scheduler.ScheduleTask(null, task);
            }

            if (pubSubManager != null)
            {
                Scheduler.ScheduleTask(null, StartPubSub);
            }
        }

        /// <summary>
        /// Used for running the extractor without calling Start(token) and getting locked
        /// into waiting for cancellation.
        /// </summary>
        /// <param name="quitAfterMap">False to wait for cancellation</param>
        /// <returns></returns>
        public async Task RunExtractor(bool quitAfterMap = false, int startTimeout = -1)
        {
            await RunExtractorInternal(startTimeout);
            if (!quitAfterMap)
            {
                Looper.Run();
                await Scheduler.WaitForAll();
            }
        }

        protected override async Task Start()
        {
            log.LogInformation("Starting OPC UA Extractor version {Version}",
                Extractor.Metrics.Version.GetVersion(Assembly.GetExecutingAssembly()));
            log.LogInformation("Revision information: {Status}",
                Extractor.Metrics.Version.GetDescription(Assembly.GetExecutingAssembly()));

            await RunExtractorInternal();
            Looper.Run();
        }

        protected override async Task OnStop()
        {
            await Close();
        }
        /// <summary>
        /// Initializes restart of the extractor. Waits for history, reset states, then schedule restart on the looper.
        /// </summary>
        public async Task RestartExtractor()
        {
            subscribed = 0;
            subscribeFlag = false;
            if (historyReader != null)
            {
                await historyReader.Terminate(Source.Token, 30);
                foreach (var state in State.NodeStates)
                {
                    state.RestartHistory();
                }

                foreach (var state in State.EmitterStates)
                {
                    state.RestartHistory();
                }
            }

            await Looper.WaitForNextPush(true);
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
            log.LogInformation("Restarting extractor...");
            extraNodesToBrowse.Clear();
            Started = false;

            await historyReader.Terminate(Source.Token, 30);
            await uaClient.WaitForOperations(Source.Token);
            await ConfigureExtractor();

            uaClient.Browser.ResetVisitedNodes();

            var synchTasks = await RunMapping(RootNodes, true, false, true);

            foreach (var task in synchTasks)
            {
                Looper.Scheduler.ScheduleTask(null, task);
            }
            Started = true;
            log.LogInformation("Successfully restarted extractor");
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
                var historyTasks = await RunMapping(nodesToBrowse.Distinct(), true, false, false);

                foreach (var task in historyTasks)
                {
                    Looper.Scheduler.ScheduleTask(null, task);
                }
            }
        }

        /// <summary>
        /// Terminate history, waiting for timeout seconds
        /// </summary>
        /// <param name="timeout">Seconds to wait before returning failure</param>
        /// <returns>True if history was terminated successfully</returns>
        public Task<bool> TerminateHistory(int timeout)
        {
            return historyReader.Terminate(Source.Token, timeout);
        }

        /// <summary>
        /// Closes the extractor, mainly just shutting down the opcua client and waiting for a clean loss of connection.
        /// </summary>
        public async Task Close(bool closeClient = true)
        {
            Source?.Cancel();
            if (!uaClient.Started || !closeClient) return;
            try
            {
                await uaClient.Close(CancellationToken.None);
            }
            catch (Exception e)
            {
                ExtractorUtils.LogException(log,
                    ExtractorUtils.HandleServiceResult(log, e, ExtractorUtils.SourceOp.CloseSession),
                    "",
                    "");
            }
            await uaClient.WaitForOperations(CancellationToken.None);
            log.LogInformation("Extractor closed");
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
        /// Returns a task running history for both data and events.
        /// </summary>
        public async Task RestartHistory()
        {
            if (!Config.History.Enabled && !Config.Events.History) return;
            await Task.WhenAll(Task.Run(async () =>
            {
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.IsFrontfilling));
                if (Config.History.Backfill)
                {
                    await historyReader.BackfillEvents(State.EmitterStates.Where(state => state.IsBackfilling));
                }
            }), Task.Run(async () =>
            {
                await historyReader.FrontfillData(State.NodeStates.Where(state => state.IsFrontfilling).ToList());
                if (Config.History.Backfill)
                {
                    await historyReader.BackfillData(State.NodeStates.Where(state => state.IsBackfilling).ToList());
                }
            }));
        }

        private bool ShouldFullyRebrowse()
        {
            // If there are any updates, we need to do a full mapping
            if (Config.Extraction.Update.AnyUpdate) return true;
            // If relationships are enabled we need to fully map, other wise we won't be able to consistently discover new relationships.
            if (Config.Extraction.Relationships.Enabled) return true;
            // If deletes are enabled we want a full map as well, to discover deleted nodes.
            if (deletesManager != null) return true;

            return false;
        }

        /// <summary>
        /// Redo browse, then schedule history on the looper.
        /// </summary>
        public async Task Rebrowse()
        {
            var isFull = ShouldFullyRebrowse();
            // If we are updating we want to re-discover nodes in order to run them through mapping again.
            var historyTasks = await RunMapping(RootNodes,
                ignoreVisited: !isFull,
                initial: false,
                isFull: isFull);

            foreach (var task in historyTasks)
            {
                Looper.Scheduler.ScheduleTask(null, task);
            }
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
            log.LogDebug("Waited {TimeS} milliseconds for subscriptions", time * 100);
        }
        #endregion

        #region Mapping

        private async Task<IEnumerable<Func<CancellationToken, Task>>> RunMapping(IEnumerable<NodeId> nodesToBrowse, bool ignoreVisited, bool initial, bool isFull)
        {
            bool readFromOpc = true;

            NodeSourceResult? result = null;
            IEventFieldSource? eventSource = null;
            if ((Config.Cognite?.RawNodeBuffer?.Enable ?? false) && initial)
            {
                log.LogDebug("Begin fetching data from CDF");
                var handler = new CDFNodeSource(Provider.GetRequiredService<ILogger<CDFNodeSource>>(),
                    Config, this, uaClient, pushers.OfType<CDFPusher>().First());
                await handler.ReadRawNodes(Source.Token);

                result = await handler.ParseResults(Source.Token);

                if (result == null || !result.DestinationObjects.Any() && !result.DestinationVariables.Any())
                {
                    if (!Config.Cognite.RawNodeBuffer.BrowseOnEmpty)
                    {
                        throw new ExtractorFailureException("Found no nodes in CDF, restarting");
                    }
                    log.LogInformation("Found no nodes in CDF, reading from OPC-UA server");
                }
                else
                {
                    readFromOpc = false;
                }
            }

            if ((Config.Source.NodeSetSource?.NodeSets?.Any() ?? false) && initial
                && (Config.Source.NodeSetSource.Instance || Config.Source.NodeSetSource.Types))
            {
                log.LogDebug("Begin fetching data from internal node set");
                if (nodeSetSource == null)
                {
                    nodeSetSource = new NodeSetSource(Provider.GetRequiredService<ILogger<NodeSetSource>>(), Config, this, uaClient);
                }

                var handler = nodeSetSource;
                handler.BuildNodes(nodesToBrowse, isFull);

                if (Config.Source.NodeSetSource.Instance)
                {
                    result = await handler.ParseResults(Source.Token);
                    readFromOpc = false;
                }
                if (Config.Source.NodeSetSource.Types)
                {
                    eventSource = handler;
                }
                nodeSetSource = null;
            }

            if (Config.Events.Enabled)
            {
                var eventFields = await uaClient.GetEventFields(eventSource, Source.Token);
                foreach (var field in eventFields)
                {
                    State.ActiveEvents[field.Key] = field.Value;
                    State.RegisterNode(field.Key, uaClient.GetUniqueId(field.Key));
                }
            }

            if (readFromOpc)
            {
                log.LogInformation("Begin mapping main directory");
                var handler = new UANodeSource(Provider.GetRequiredService<ILogger<UANodeSource>>(), Config, this, uaClient, isFull);
                try
                {
                    await uaClient.Browser.BrowseNodeHierarchy(nodesToBrowse, handler.Callback, Source.Token, ignoreVisited,
                        "the main instance hierarchy");
                }
                catch (Exception ex)
                {
                    ExtractorUtils.LogException(log, ex, "Unexpected error browsing node hierarchy",
                        "Handled service result exception browsing node hierarchy");
                    throw;
                }
                result = await handler.ParseResults(Source.Token);
                log.LogInformation("End mapping main directory");
            }

            try
            {
                var tasks = await MapUAToDestinations(result);
                if (initial && !readFromOpc && Config.Source.AltSourceBackgroundBrowse)
                {
                    tasks = tasks.Append(async token =>
                    {
                        var tasks = await RunMapping(RootNodes, false, false, true);
                        foreach (var task in tasks)
                        {
                            Looper.Scheduler.ScheduleTask(null, task);
                        }
                    });
                }
                return tasks;
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
        private async Task<IEnumerable<Func<CancellationToken, Task>>> MapUAToDestinations(NodeSourceResult? result)
        {
            if (result == null) return Enumerable.Empty<Func<CancellationToken, Task>>();

            Streamer.AllowData = State.NodeStates.Any();

            var toPush = await PusherInput.FromNodeSourceResult(result, deletesManager, Source.Token);

            await PushNodes(toPush);
            // Changed flag means that it already existed, so we avoid synchronizing these.
            var historyTasks = Synchronize(result.SourceVariables.Where(var => !var.Changed));
            Starting.Set(0);
            return historyTasks;
        }

        private async Task EnsureSubscriptions()
        {
            if (Config.Subscriptions.Events)
            {
                var subscribeStates = State.EmitterStates.Where(state => state.ShouldSubscribe);

                await uaClient.SubscribeToEvents(subscribeStates, Streamer.EventSubscriptionHandler, Source.Token);
            }

            if (Config.Subscriptions.DataPoints)
            {
                var subscribeStates = State.NodeStates.Where(state => state.ShouldSubscribe);

                await uaClient.SubscribeToNodes(subscribeStates, Streamer.DataSubscriptionHandler, Source.Token);
            }

            if (rebrowseTriggerManager is not null)
            {
                await rebrowseTriggerManager.EnableCustomServerSubscriptions(Source.Token);
            }
        }

        /// <summary>
        /// Build transformations from configured list and deprecated filter properties.
        /// </summary>
        private void BuildTransformations()
        {
            var transformations = new List<NodeTransformation>();
            int idx = 0;

            if (Config.Extraction.Transformations != null)
            {
                foreach (var raw in Config.Extraction.Transformations)
                {
                    transformations.Add(new NodeTransformation(raw, idx++));
                }
            }

            if (!string.IsNullOrEmpty(Config.Extraction.PropertyIdFilter))
            {
                log.LogWarning("Property Id filter is deprecated, use transformations instead");
                transformations.Add(new NodeTransformation(new RawNodeTransformation
                {
                    Filter = new RawNodeFilter
                    {
                        Id = Config.Extraction.PropertyIdFilter
                    },
                    Type = TransformationType.Property
                }, idx++));
            }
            if (!string.IsNullOrEmpty(Config.Extraction.PropertyNameFilter))
            {
                log.LogWarning("Property Name filter is deprecated, use transformations instead");
                transformations.Add(new NodeTransformation(new RawNodeTransformation
                {
                    Filter = new RawNodeFilter
                    {
                        Name = Config.Extraction.PropertyNameFilter
                    },
                    Type = TransformationType.Property
                }, idx++));
            }
            if (Config.Extraction.IgnoreName != null && Config.Extraction.IgnoreName.Any())
            {
                log.LogWarning("Ignore name is deprecated, use transformations instead");
                var filterStr = string.Join('|', Config.Extraction.IgnoreName.Select(str => $"^{str}$"));
                transformations.Add(new NodeTransformation(new RawNodeTransformation
                {
                    Filter = new RawNodeFilter
                    {
                        Name = filterStr
                    },
                    Type = TransformationType.Ignore
                }, idx++));
            }
            if (Config.Extraction.IgnoreNamePrefix != null && Config.Extraction.IgnoreNamePrefix.Any())
            {
                log.LogWarning("Ignore name prefix is deprecated, use transformations instead: {Prefix}", string.Join(',', Config.Extraction.IgnoreNamePrefix));
                var filterStr = string.Join('|', Config.Extraction.IgnoreNamePrefix.Select(str => $"^{str}"));
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
                log.LogDebug("{Transformation}", trans.ToString());
            }

            uaClient.Browser.IgnoreFilters = transformations.Where(trans => trans.Type == TransformationType.Ignore).Select(trans => trans.Filter).ToList();
            Transformations = transformations;
        }

        /// <summary>
        /// Set up extractor once UAClient is started. This resets the internal state of the extractor.
        /// </summary>
        private async Task ConfigureExtractor()
        {
            RootNodes = Config.Extraction.GetRootNodes(uaClient);

            DataTypeManager.Configure();

            if (Config.Extraction.NodeMap != null)
            {
                foreach (var kvp in Config.Extraction.NodeMap)
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
            if (Config.Extraction.Transformations?.Any(trans => trans.Type == TransformationType.AsEvents) ?? false)
            {
                Streamer.AllowEvents = true;
            }

            if (Config.Events.Enabled)
            {
                Streamer.AllowEvents = true;
                if (Config.Events.EmitterIds != null && Config.Events.EmitterIds.Any()
                    || Config.Events.HistorizingEmitterIds != null && Config.Events.HistorizingEmitterIds.Any())
                {
                    var histEmitterIds = new HashSet<NodeId>((Config.Events.HistorizingEmitterIds ?? Enumerable.Empty<ProtoNodeId>())
                        .Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)));
                    var emitterIds = new HashSet<NodeId>((Config.Events.EmitterIds ?? Enumerable.Empty<ProtoNodeId>())
                        .Select(proto => proto.ToNodeId(uaClient, ObjectIds.Server)));
                    var eventEmitterIds = new HashSet<NodeId>(histEmitterIds.Concat(emitterIds));

                    foreach (var id in eventEmitterIds)
                    {
                        var history = (histEmitterIds.Contains(id)) && Config.Events.History;
                        var subscription = emitterIds.Contains(id);
                        State.SetEmitterState(new EventExtractionState(this, id, history, history && Config.History.Backfill, subscription));
                        State.RegisterNode(id, uaClient.GetUniqueId(id));
                    }
                }
                if (Config.Events.ReadServer)
                {
                    var serverNode = await uaClient.GetServerNode(Source.Token);
                    if (serverNode.EventNotifier != 0)
                    {
                        var history = (serverNode.EventNotifier & EventNotifiers.HistoryRead) != 0 && Config.Events.History;
                        var subscription = (serverNode.EventNotifier & EventNotifiers.SubscribeToEvents) != 0 && Config.Subscriptions.Events;
                        State.SetEmitterState(new EventExtractionState(this, serverNode.Id, history, history && Config.History.Backfill, subscription));
                    }
                }
            }
            BuildTransformations();

            var helper = new ServerInfoHelper(Provider.GetRequiredService<ILogger<ServerInfoHelper>>(), uaClient);
            await helper.LimitConfigValues(Config, Source.Token);
        }

        /// <summary>
        /// Called when pushing nodes fail, to properly add the nodes not yet pushed to
        /// PendingNodes and PendingReferences on the pusher.
        /// </summary>
        /// <param name="input">Nodes that failed to push</param>
        /// <param name="pusher">Pusher pushed to</param>
        private static void PushNodesFailure(
            PusherInput input,
            FullPushResult result,
            IPusher pusher)
        {
            pusher.Initialized = false;
            pusher.DataFailing = true;
            pusher.EventsFailing = true;

            pusher.AddPendingNodes(input, result);
        }
        /// <summary>
        /// Push nodes to given pusher
        /// </summary>
        /// <param name="input">Nodes to push</param>
        /// <param name="pusher">Destination to push to</param>
        /// <param name="initial">True if this counts as initialization of the pusher</param>
        public async Task PushNodes(
            PusherInput? input,
            IPusher pusher, bool initial)
        {
            if (input == null)
            {
                log.LogWarning("No input given to pusher {Name}, not initializing", pusher.GetType());
                return;
            }

            var result = new FullPushResult();
            if (pusher.NoInit)
            {
                log.LogWarning("Skipping pushing on pusher {Name}", pusher.GetType());
                PushNodesFailure(input, result, pusher);
                return;
            }

            log.LogInformation("Executing pushes on pusher {Type}", pusher.GetType());

            if (input.Objects.Any() || input.Variables.Any() || input.References.Any())
            {
                var pushResult = await pusher.PushNodes(input.Objects, input.Variables, input.References, Config.Extraction.Update, Source.Token);
                result.Apply(pushResult);
                if (!result.Variables || !result.Objects || !result.References)
                {
                    log.LogError("Failed to push nodes on pusher {Name}", pusher.GetType());
                    PushNodesFailure(input, result, pusher);
                    return;
                }
            }

            if (pusher.BaseConfig.ReadExtractedRanges)
            {
                var statesToSync = input
                    .Variables
                    .Select(ts => ts.Id)
                    .Distinct()
                    .SelectNonNull(id => State.GetNodeState(id))
                    .Where(state => state.FrontfillEnabled && !state.Initialized);

                var eventStatesToSync = State.EmitterStates.Where(state => state.FrontfillEnabled && !state.Initialized);

                var initResults = await Task.WhenAll(
                    pusher.InitExtractedRanges(statesToSync, Config.History.Backfill, Source.Token),
                    pusher.InitExtractedEventRanges(eventStatesToSync, Config.History.Backfill, Source.Token));

                result.Ranges = initResults[0];

                if (!initResults.All(res => res))
                {
                    log.LogError("Initialization of extracted ranges failed for pusher {Name}", pusher.GetType());
                    PushNodesFailure(input, result, pusher);
                    return;
                }
            }

            if (input.Deletes != null)
            {
                var delResult = await pusher.ExecuteDeletes(input.Deletes, Source.Token);
                result.Deletes = delResult;
                if (!delResult)
                {
                    log.LogError("Executing soft deletes failed for pusher {Name}", pusher.GetType());
                    PushNodesFailure(input, result, pusher);
                    return;
                }
            }

            log.LogInformation("Successfully pushed nodes on pusher {Name} {Initial}", pusher.GetType(), initial);

            pusher.Initialized |= initial;
        }

        /// <summary>
        /// Push given lists of nodes to pusher destinations, and fetches latest timestamp for relevant nodes.
        /// </summary>
        /// <param name="input">Nodes to push</param>
        private async Task PushNodes(PusherInput input)
        {
            var newStates = input.Variables
                .Select(ts => ts.Id)
                .Distinct()
                .Select(id => State.GetNodeState(id));

            bool initial = input.Variables.Count() + input.Objects.Count() >= State.NumActiveNodes;

            var pushTasks = pushers.Select(pusher => PushNodes(input, pusher, initial));

            if (StateStorage != null && Config.StateStorage.IntervalValue.Value != Timeout.InfiniteTimeSpan)
            {
                if (Streamer.AllowEvents)
                {
                    pushTasks = pushTasks.Append(StateStorage.RestoreExtractionState(
                        State.EmitterStates.Where(state => state.FrontfillEnabled).ToDictionary(state => state.Id),
                        Config.StateStorage.EventStore,
                        false,
                        Source.Token));
                }

                if (Streamer.AllowData)
                {
                    pushTasks = pushTasks.Append(StateStorage.RestoreExtractionState(
                        newStates.Where(state => state != null && state.FrontfillEnabled).ToDictionary(state => state?.Id!, state => state!),
                        Config.StateStorage.VariableStore,
                        false,
                        Source.Token));
                }
            }

            pushTasks = pushTasks.ToList();
            log.LogInformation("Waiting for pushes on pushers");
            await Task.WhenAll(pushTasks);

            if (initial)
            {
                trackedAssets.Set(input.Objects.Count());
                trackedTimeseres.Set(input.Variables.Count());
            }
            else
            {
                trackedAssets.Inc(input.Objects.Count());
                trackedTimeseres.Inc(input.Variables.Count());
            }

            foreach (var state in newStates.Concat<UAHistoryExtractionState?>(State.EmitterStates))
            {
                state?.FinalizeRangeInit();
            }
        }

        /// <summary>
        /// Subscribe to event changes, then run history.
        /// </summary>
        private async Task SynchronizeEvents(CancellationToken token)
        {
            if (Config.Subscriptions.Events)
            {
                var subscribeStates = State.EmitterStates.Where(state => state.ShouldSubscribe);

                await uaClient.SubscribeToEvents(subscribeStates, Streamer.EventSubscriptionHandler, Source.Token);
            }

            Interlocked.Increment(ref subscribed);
            if (!State.NodeStates.Any() || subscribed > 1) subscribeFlag = true;
            if (!Config.Events.History) return;
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillEvents(State.EmitterStates.Where(state => state.IsFrontfilling));
                if (Config.History.Backfill)
                {
                    await historyReader.BackfillEvents(State.EmitterStates.Where(state => state.IsBackfilling));
                }
            }
            else
            {
                log.LogInformation("Skipping event history due to no initialized pushers");
            }
        }

        /// <summary>
        /// Subscribe to data changes, then run history.
        /// </summary>
        /// <param name="states">States to subscribe to</param>
        private async Task SynchronizeNodes(IEnumerable<VariableExtractionState> states, CancellationToken token)
        {
            if (Config.Subscriptions.DataPoints)
            {
                var subscribeStates = states.Where(state => state.ShouldSubscribe);

                await uaClient.SubscribeToNodes(subscribeStates, Streamer.DataSubscriptionHandler, token);
            }

            Interlocked.Increment(ref subscribed);
            if (!State.EmitterStates.Any() || subscribed > 1) subscribeFlag = true;
            if (!Config.History.Enabled) return;
            if (pushers.Any(pusher => pusher.Initialized))
            {
                await historyReader.FrontfillData(states.Where(state => state.IsFrontfilling));
                if (Config.History.Backfill)
                {
                    await historyReader.BackfillData(states.Where(state => state.IsBackfilling));
                }
            }
            else
            {
                log.LogInformation("Skipping datapoints history due to no initialized pushers");
            }
        }

        /// <summary>
        /// Start synchronization of given list of variables with the server.
        /// </summary>
        /// <param name="variables">Variables to synchronize</param>
        /// <returns>Two tasks, one for data and one for events</returns>
        private IEnumerable<Func<CancellationToken, Task>> Synchronize(IEnumerable<UAVariable> variables)
        {
            var states = variables.Select(ts => ts.Id).Distinct().SelectNonNull(id => State.GetNodeState(id));

            log.LogInformation("Synchronize {NumNodesToSynch} nodes", variables.Count());
            var tasks = new List<Func<CancellationToken, Task>>();
            // Create tasks to subscribe to nodes, then start history read. We might lose data if history read finished before subscriptions were created.
            if (states.Any())
            {
                tasks.Add(token => SynchronizeNodes(states, token));
            }
            if (State.EmitterStates.Any())
            {
                tasks.Add(SynchronizeEvents);
            }

            if (Config.Extraction.EnableAuditDiscovery)
            {
                tasks.Add(token => uaClient.SubscribeToAuditEvents(AuditEventSubscriptionHandler, token));
            }
            return tasks;
        }

        private async Task StartPubSub(CancellationToken token)
        {
            if (pubSubManager == null) return;
            log.LogInformation("Begin starting pubsub client");
            try
            {
                await pubSubManager.Start(token);
            }
            catch (Exception ex)
            {
                ExtractorUtils.LogException(log, ex, "Failed to launch PubSub client", "Failed to launch PubSub client");
            }
            log.LogInformation("PubSub manager started");
        }

        #endregion

        #region Handlers
        /// <summary>
        /// Handle subscription callback for audit events (AddReferences/AddNodes). Triggers partial re-browse when necessary
        /// </summary>
        private void AuditEventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (eventArgs.NotificationValue is not EventFieldList triggeredEvent)
            {
                log.LogWarning("No event in event subscription notification: {}", item.StartNodeId);
                return;
            }

            var eventFields = triggeredEvent.EventFields;
            if (item.Filter is not EventFilter filter)
            {
                log.LogWarning("Triggered event without filter");
                return;
            }
            int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                       && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                log.LogWarning("Triggered event has no type, ignoring");
                return;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            if (eventType == null || eventType != ObjectTypeIds.AuditAddNodesEventType && eventType != ObjectTypeIds.AuditAddReferencesEventType)
            {
                log.LogWarning("Non-audit event triggered on audit event listener");
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
                        log.LogWarning("Missing NodesToAdd object on AddNodes event");
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
                        log.LogDebug("No relevant nodes in addNodes audit event");
                        return;
                    }
                    log.LogInformation("Trigger rebrowse on {NumNodes} node ids due to addNodes event", relevantIds.Count());

                    foreach (var id in relevantIds)
                    {
                        extraNodesToBrowse.Enqueue(id);
                    }
                }
                Looper.Scheduler.TryTriggerTask("ExtraTasks");
                return;
            }

            using (var ev = new AuditAddReferencesEventState(null))
            {
                ev.Update(uaClient.SystemContext, filter.SelectClauses, triggeredEvent);

                if (ev.ReferencesToAdd?.Value == null)
                {
                    log.LogWarning("Missing ReferencesToAdd object on AddReferences event");
                    return;
                }

                var addedReferences = ev.ReferencesToAdd.Value;

                var relevantRefIds = addedReferences.Where(added =>
                    (added.IsForward && State.IsMappedNode(uaClient.ToNodeId(added.SourceNodeId))))
                    .Select(added => uaClient.ToNodeId(added.SourceNodeId))
                    .Distinct();

                if (!relevantRefIds.Any())
                {
                    log.LogDebug("No relevant nodes in addReferences audit event");
                    return;
                }

                log.LogInformation("Trigger rebrowse on {NumNodes} node ids due to addReference event", relevantRefIds.Count());

                foreach (var id in relevantRefIds)
                {
                    extraNodesToBrowse.Enqueue(id);
                }
            }

            Looper.Scheduler.TryTriggerTask("ExtraTasks");
        }
        #endregion

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Starting.Set(0);
                historyReader?.Dispose();
                uaClient.OnServerDisconnect -= UaClient_OnServerDisconnect;
                uaClient.OnServerReconnect -= UaClient_OnServerReconnect;
                pubSubManager?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
    public interface IUAClientAccess
    {
        string? GetUniqueId(ExpandedNodeId id, int index = -1);
        StringConverter StringConverter { get; }
        string GetRelationshipId(UAReference reference);
    }
}

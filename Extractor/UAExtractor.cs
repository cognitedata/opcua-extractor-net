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

using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.PubSub;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.Tasks;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Cognite.OpcUa.Utils;
using CogniteSdk.Alpha;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Nito.Disposables.Internals;
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
    public class UAExtractor : Extractor.Utils.Unstable.BaseExtractor<FullConfig>, IUAClientAccess, IClientCallbacks
    {
        private readonly UAClient uaClient;
        public FailureBuffer? FailureBuffer { get; }
        public IExtractionStateStore? StateStorage { get; }
        public State State { get; }
        public Streamer Streamer { get; }
        private HistoryReader? historyReader;
        public IEnumerable<NodeId> RootNodes { get; private set; } = null!;
        private readonly IPusher pusher;
        public IPusher Pusher => pusher;
        public TransformationCollection? Transformations { get; private set; }
        public TypeConverter TypeConverter => uaClient.TypeConverter;
        private PubSubManager? pubSubManager;
        public NamespaceTable? NamespaceTable => uaClient.NamespaceTable;

        public TypeManager TypeManager => uaClient.TypeManager;
        public SessionContext Context => uaClient.Context;
        public int PublishingInterval => Config.Source.PublishingInterval;

        private NodeSetNodeSource? nodeSetSource;

        private readonly DeletesManager? deletesManager;
        public DeletesManager? DeletesManager => deletesManager;

        public bool ShouldStartLooping { get; set; } = true;

        public bool Started { get; private set; }

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

        public SourceInformation SourceInfo => uaClient?.SourceInfo ?? SourceInformation.Default();

        public bool AllowUpdateState => GetAllowUpdateState();

        private readonly SubscriptionTask? subscriptionTask;
        private readonly BrowseTask browseTask;
        private readonly PusherTask pusherTask;

        // Active subscriptions, used in tests for WaitForSubscription().
        private readonly HashSet<SubscriptionName> activeSubscriptions = new();

        public PeriodicScheduler PeriodicScheduler { get; private set; } = null!;

        /// <summary>
        /// Construct extractor with list of pushers
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pushers">List of pushers to be used</param>
        /// <param name="uaClient">UAClient to be used</param>
        public UAExtractor(
            ConfigWrapper<FullConfig> config,
            IServiceProvider provider,
            ExtractorTaskScheduler taskScheduler,
            IPusher pusher,
            UAClient uaClient,
            IIntegrationSink sink,
            IExtractionStateStore? stateStore) : base(config, provider, taskScheduler, sink)
        {
            this.uaClient = uaClient;
            // Fallback to other pusher... This is bad, but it's fundamentally a design flaw
            // in .NET dependency injection that you can register a services as null and
            // it is considered to exist...
            if (pusher == null)
            {
                pusher = provider.GetServices<IPusher>().WhereNotNull().FirstOrDefault();
            }

            this.pusher = pusher ?? throw new ConfigurationException("Missing cognite configuration");
            this.uaClient.Callbacks = this;
            log = provider.GetRequiredService<ILogger<UAExtractor>>();

            log.LogDebug("Config:{NewLine}{Config}", Environment.NewLine, ExtractorUtils.ConfigToString(Config));

            State = new State();
            Streamer = new Streamer(provider.GetRequiredService<ILogger<Streamer>>(), this, Config);
            StateStorage = stateStore;

            if (Config.FailureBuffer.Enabled)
            {
                FailureBuffer = new FailureBuffer(provider.GetRequiredService<ILogger<FailureBuffer>>(),
                    Config, this);
            }

            log.LogInformation("Building extractor");

            if (Config.PubSub.Enabled)
            {
                pubSubManager = new PubSubManager(provider.GetRequiredService<ILogger<PubSubManager>>(), uaClient, this, Config);
            }

            if (Config.Subscriptions.Events || Config.Subscriptions.DataPoints || Config.Extraction.EnableAuditDiscovery)
            {
                subscriptionTask = new SubscriptionTask(this, uaClient, Config);
            }
            browseTask = new BrowseTask(this, uaClient, Config, Provider);
            pusherTask = new PusherTask(this, Config, pusher, Provider.GetRequiredService<ILogger<PusherTask>>());

            pusher.Extractor = this;

            if (Config.Extraction.RebrowseTriggers is not null)
            {
                rebrowseTriggerManager = new RebrowseTriggerManager(
                    provider.GetRequiredService<ILogger<RebrowseTriggerManager>>(),
                    uaClient, Config.Extraction.RebrowseTriggers,
                    Config.StateStorage.NamespacePublicationDateStore,
                    this
                );
            }

            if (Config.Extraction.Deletes.Enabled)
            {
                if (stateStore != null)
                {
                    deletesManager = new DeletesManager(stateStore, this, provider.GetRequiredService<ILogger<DeletesManager>>(), Config);
                }
                else
                {
                    log.LogWarning("Deletes are enabled, but no state store is configured. Detecting deleted nodes will not work.");
                }
            }
        }

        private bool GetAllowUpdateState()
        {
            if (historyReader != null && historyReader.CurrentHistoryRunIsBad) return false;
            if (!Config.Source.Redundancy.MonitorServiceLevel) return true;

            return uaClient.SessionManager.CurrentServiceLevel >= Config.Source.Redundancy.ServiceLevelThreshold;
        }

        /// <summary>
        /// Event handler for UAClient disconnect
        /// </summary>
        /// <param name="sender">UAClient that generated this event</param>
        /// <param name="e">EventArgs for this event</param>
        public void OnServerDisconnect(UAClient source)
        {
            historyReader?.AddIssue(HistoryReader.StateIssue.ServerConnection);
        }

        public Task OnServiceLevelAboveThreshold(UAClient source)
        {
            if (Source.IsCancellationRequested) return Task.CompletedTask;

            log.LogInformation("Service level went above threshold");
            historyReader?.RemoveIssue(HistoryReader.StateIssue.ServiceLevel);

            return Task.CompletedTask;
        }

        public Task OnServicelevelBelowThreshold(UAClient source)
        {
            if (Source.IsCancellationRequested) return Task.CompletedTask;

            log.LogInformation("Service level dropped below threshold");

            historyReader?.AddIssue(HistoryReader.StateIssue.ServiceLevel);

            return Task.CompletedTask;
        }

        public void OnDataPushFailure()
        {
            historyReader?.AddIssue(HistoryReader.StateIssue.DataPushFailing);
        }

        public void OnEventsPushFailure()
        {
            historyReader?.AddIssue(HistoryReader.StateIssue.EventsPushFailing);
        }

        public void OnDataPushRecovery()
        {
            historyReader?.RemoveIssue(HistoryReader.StateIssue.DataPushFailing);
        }

        public void OnEventsPushRecovery()
        {
            historyReader?.RemoveIssue(HistoryReader.StateIssue.EventsPushFailing);
        }

        public void OnNodeHierarchyRead()
        {
            historyReader?.RemoveIssue(HistoryReader.StateIssue.NodeHierarchyRead);
            Starting.Set(0);
        }

        public void ScheduleRebrowse()
        {
            browseTask.AddNodesToBrowse(RootNodes, isFull: true);
            TaskScheduler.ScheduleTaskNow(browseTask.Name, reScheduleIfRunning: true);
        }

        public void OnQueueOverflow()
        {
            pusherTask.TriggerPushNow();
        }

        public void OnSubscriptionsEstablished()
        {
            // Called when subscriptions are established.
            TaskScheduler.ScheduleTaskNow(pusherTask.Name);
        }

        /// <summary>
        /// Event handler for UAClient reconnect
        /// </summary>
        /// <param name="sender">UAClient that generated this event</param>
        /// <param name="e">EventArgs for this event</param>
        public async Task OnServerReconnect(UAClient source)
        {
            if (Source.IsCancellationRequested) return;

            await EnsureSubscriptions();
            if (Config.Source.RestartOnReconnect)
            {
                await RestartExtractor();
            }
            else
            {
                historyReader?.RemoveIssue(HistoryReader.StateIssue.ServerConnection);
                TaskScheduler.Notify();
            }
        }

        private async Task RunPeriodicTasks(CancellationToken token)
        {
            var scheduler = new PeriodicScheduler(token);
            if (StateStorage != null && !Config.DryRun)
            {
                var interval = Config.StateStorage.IntervalValue.Value;
                scheduler.SchedulePeriodicTask(nameof(StoreState), interval, StoreState, interval != Timeout.InfiniteTimeSpan);
            }
            await scheduler.WaitForAll();
        }

        public async Task StoreState(CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
            if (StateStorage == null) return;
            await Task.WhenAll(
                StateStorage.StoreExtractionState(State.NodeStates
                    .Where(state => state.FrontfillEnabled), Config.StateStorage.VariableStore, token),
                StateStorage.StoreExtractionState(State.EmitterStates
                    .Where(state => state.FrontfillEnabled), Config.StateStorage.EventStore, token)
            );
        }
        #region Interface

        protected override Task InitTasks()
        {
            historyReader?.Dispose();

            if (Config.History.Enabled)
            {
                historyReader = new HistoryReader(Provider.GetRequiredService<ILogger<HistoryReader>>(),
                    uaClient, this, TypeManager, Config, Source.Token);
            }

            log.LogInformation("Starting OPC UA Extractor version {Version}",
                Extractor.Metrics.Version.GetVersion(Assembly.GetExecutingAssembly()));
            log.LogInformation("Revision information: {Status}",
                Extractor.Metrics.Version.GetDescription(Assembly.GetExecutingAssembly()));

            if (subscriptionTask != null)
            {
                TaskScheduler.AddScheduledTask(subscriptionTask, false);
            }
            TaskScheduler.AddScheduledTask(browseTask, true);
            TaskScheduler.AddScheduledTask(pusherTask, false);

            AddMonitoredTask(token => InitialConnection(), ExtractorTaskResult.Expected, "Initial Connection");
            AddMonitoredTask(RunPeriodicTasks, ExtractorTaskResult.Unexpected, "Run Periodic Tasks");
            // TEMP: Rewrite as a proper task!
            if (historyReader != null)
            {
                AddMonitoredTask(token => historyReader.Run(token), ExtractorTaskResult.Unexpected, "History Reader");
            }

            return Task.CompletedTask;
        }

        protected override ExtractorId GetExtractorVersion()
        {
            var version = Extractor.Metrics.Version.GetVersion(Assembly.GetExecutingAssembly());
            Console.WriteLine(version);
            return new ExtractorId
            {
                ExternalId = "cognite-opcua",
                Version = string.IsNullOrWhiteSpace(version) ? null : version.Truncate(32),
            };
        }

        public void InitExternal(CancellationToken token)
        {
            Init(token);
        }

        private async Task InitialConnection(int startTimeout = -1)
        {
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
                nodeSetSource = new NodeSetNodeSource(
                    Provider.GetRequiredService<ILogger<NodeSetNodeSource>>(),
                    Config, this, uaClient, TypeManager);
                await nodeSetSource.Initialize(Source.Token);
            }

            await ConfigureExtractor();

            TaskScheduler.Notify();

            Started = true;
            startTime.Set(new DateTimeOffset(StartTime ?? DateTime.UtcNow).ToUnixTimeMilliseconds());

            pusher.Reset();

            if (rebrowseTriggerManager is not null)
            {
                await rebrowseTriggerManager.EnableCustomServerSubscriptions(Source.Token);
            }

            if (pubSubManager != null)
            {
                AddMonitoredTask(StartPubSub, ExtractorTaskResult.Expected, "Start PubSub");
            }
        }

        /// <summary>
        /// Used for running the extractor without calling Start(token) and getting locked
        /// into waiting for cancellation.
        /// </summary>
        /// <param name="quitAfterMap">False to wait for cancellation</param>
        /// <returns></returns>
        /* public async Task RunExtractor(bool quitAfterMap = false, int startTimeout = -1)
        {
            await RunExtractorInternal(startTimeout);
            if (!quitAfterMap)
            {
                Looper.Run();
                await Scheduler.WaitForAll();
            }
        }*/

        public void ScheduleTask(Func<CancellationToken, Task> task, ExtractorTaskResult staticResult, string name)
        {
            AddMonitoredTask(task, staticResult, name);
        }

        /// <summary>
        /// Initializes restart of the extractor. Waits for history, reset states, then schedule restart on the looper.
        /// </summary>
        public async Task RestartExtractor()
        {
            lock (activeSubscriptions)
            {
                activeSubscriptions.Clear();
            }

            historyReader?.AddIssue(HistoryReader.StateIssue.NodeHierarchyRead);

            await pusherTask.WaitForNextPush(true);
            pusher.Reset();
            Starting.Set(1);
            AddMonitoredTask(token => FinishExtractorRestart(), ExtractorTaskResult.Expected, "Finish Extractor Restart");
        }

        /// <summary>
        /// Task for the actual extractor restart, performing it directly.
        /// Stops history, waits for UAClient to terminate, resets the extractor, rebrowses, then schedules history.
        /// </summary>
        public async Task FinishExtractorRestart()
        {
            log.LogInformation("Restarting extractor...");
            Started = false;

            await uaClient.WaitForOperations(Source.Token);
            await ConfigureExtractor();
            TypeManager.Reset();

            browseTask!.AddNodesToBrowse(RootNodes, isFull: true);
            TaskScheduler.ScheduleTaskNow(browseTask!.Name, reScheduleIfRunning: true);

            log.LogInformation("Successfully restarted extractor");
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
        /// Used for testing, wait for subscriptions to be created, with given timeout.
        /// </summary>
        /// <param name="timeout">Timeout in 10ths of a second</param>
        public async Task WaitForSubscription(SubscriptionName name, int timeout = 100)
        {
            int time = 0;
            while (time++ < timeout)
            {
                lock (activeSubscriptions)
                {
                    if (activeSubscriptions.Contains(name))
                    {
                        log.LogDebug("Waited {TimeS} milliseconds for subscriptions", time * 100);

                        return;
                    }
                }
                await Task.Delay(100);
            }
            throw new TimeoutException("Waiting for subscriptions timed out");
        }

        public void TriggerHistoryRestart()
        {
            historyReader?.RequestRestart();
        }

        public async Task RestartHistoryWaitForStop()
        {
            if (historyReader != null)
            {
                await historyReader.RequestRestartWaitForTermination(Source.Token);
            }
        }

        public void OnSubscriptionFailure(SubscriptionName subscription)
        {
            switch (subscription)
            {
                case SubscriptionName.Events:
                    historyReader?.AddIssue(HistoryReader.StateIssue.EventSubscription);
                    break;
                case SubscriptionName.DataPoints:
                    historyReader?.AddIssue(HistoryReader.StateIssue.DataPointSubscription);
                    break;
            }
        }

        public void OnCreatedSubscription(SubscriptionName subscription)
        {
            lock (activeSubscriptions)
            {
                activeSubscriptions.Add(subscription);
            }
            switch (subscription)
            {
                case SubscriptionName.Events:
                    historyReader?.RemoveIssue(HistoryReader.StateIssue.EventSubscription);
                    break;
                case SubscriptionName.DataPoints:
                    historyReader?.RemoveIssue(HistoryReader.StateIssue.DataPointSubscription);
                    break;
            }
        }

        public void RemoveKnownSubscription(SubscriptionName name)
        {
            lock (activeSubscriptions)
            {
                activeSubscriptions.Remove(name);
            }
        }

        public void ClearKnownSubscriptions()
        {
            lock (activeSubscriptions)
            {
                activeSubscriptions.Clear();
            }
        }
        #endregion

        #region Mapping

        private async Task EnsureSubscriptions()
        {
            if (uaClient.SubscriptionManager == null) throw new InvalidOperationException("Client not yet initialized");

            if (Config.Subscriptions.Events)
            {
                var subscribeStates = State.EmitterStates.Where(state => state.ShouldSubscribe);

                uaClient.SubscriptionManager.EnqueueTask(new EventSubscriptionTask(
                    Streamer.EventSubscriptionHandler,
                    subscribeStates,
                    uaClient.BuildEventFilter(TypeManager.EventFields),
                    this));
            }

            if (Config.Subscriptions.DataPoints)
            {
                var subscribeStates = State.NodeStates.Where(state => state.ShouldSubscribe);

                uaClient.SubscriptionManager.EnqueueTask(new DataPointSubscriptionTask(
                    Streamer.DataSubscriptionHandler,
                    subscribeStates,
                    this));
            }

            if (rebrowseTriggerManager is not null)
            {
                await rebrowseTriggerManager.EnableCustomServerSubscriptions(Source.Token);
            }

            if (Config.Source.Redundancy.MonitorServiceLevel)
            {
                uaClient.SessionManager.EnsureServiceLevelSubscription();
            }

            await uaClient.SubscriptionManager.WaitForAllCurrentlyPendingTasks(Source.Token);
        }

        /// <summary>
        /// Build transformations from configured list.
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

            foreach (var trans in transformations)
            {
                log.LogDebug("{Transformation}", trans.ToString());
            }

            var tfs = new TransformationCollection(transformations);

            if (tfs.NoEarlyFiltering)
            {
                log.LogWarning("Transformations contain a non-trivial include filter after an ignore filter, early filtering will not be possible.");
            }
            uaClient.Browser.Transformations = tfs;
            Transformations = tfs;
        }

        /// <summary>
        /// Set up extractor once UAClient is started. This resets the internal state of the extractor.
        /// </summary>
        private async Task ConfigureExtractor()
        {
            RootNodes = Config.Extraction.GetRootNodes(uaClient.Context);

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
                    var histEmitterIds = Config.Events.GetHistorizingEmitterIds(uaClient.Context);
                    var emitterIds = Config.Events.GetEmitterIds(uaClient.Context);
                    var eventEmitterIds = new HashSet<NodeId>(histEmitterIds.Concat(emitterIds));

                    foreach (var id in eventEmitterIds)
                    {
                        var history = histEmitterIds.Contains(id) && Config.Events.History;
                        var subscription = emitterIds.Contains(id);
                        State.SetEmitterState(new EventExtractionState(this, id, history, history && Config.History.Backfill, subscription));
                        State.RegisterNode(id, uaClient.GetUniqueId(id));
                    }
                }
                if (Config.Events.ReadServer)
                {
                    var serverNode = await uaClient.GetServerNode(Source.Token) as UAObject ?? throw new ExtractorFailureException("Server node is not an object, or does not exist");
                    if (serverNode.FullAttributes.EventNotifier != 0)
                    {
                        var history = serverNode.FullAttributes.ShouldReadEventHistory(Config);
                        var subscription = serverNode.FullAttributes.ShouldSubscribeToEvents(Config);
                        State.SetEmitterState(new EventExtractionState(this, serverNode.Id, history, history && Config.History.Backfill, subscription));
                        State.RegisterNode(serverNode.Id, uaClient.GetUniqueId(serverNode.Id));
                    }
                }
            }
            BuildTransformations();

            var helper = new ServerInfoHelper(Provider.GetRequiredService<ILogger<ServerInfoHelper>>(), uaClient);
            var oldHistoryPar = Config.History.Throttling.MaxNodeParallelism;
            var oldBrowsePar = Config.Source.BrowseThrottling.MaxNodeParallelism;
            await helper.LimitConfigValues(Config, Source.Token);

            if (historyReader != null && oldHistoryPar != Config.History.Throttling.MaxNodeParallelism)
            {
                historyReader.MaxNodeParallelismChanged();
            }
            if (oldBrowsePar != Config.Source.BrowseThrottling.MaxNodeParallelism)
            {
                uaClient.Browser.MaxNodeParallelismChanged();
            }
        }

        /// <summary>
        /// Called when pushing nodes fail, to properly add the nodes not yet pushed to
        /// PendingNodes and PendingReferences on the pusher.
        /// </summary>
        /// <param name="input">Nodes that failed to push</param>
        /// <param name="pusher">Pusher pushed to</param>
        private void PushNodesFailure(
            PusherInput input,
            FullPushResult result,
            IPusher pusher)
        {
            pusher.Initialized = false;
            pusher.DataFailing = true;
            pusher.EventsFailing = true;

            pusher.AddPendingNodes(input, result, Config);
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

            if (input.Deletes == null) result.Deletes = true;

            log.LogInformation("Executing pushes on pusher {Type}", pusher.GetType());

            if (input.Objects.Any() || input.Variables.Any() || input.References.Any())
            {
                var pushResult = await pusher.PushNodes(input.Objects, input.Variables, input.References, Source.Token);
                result.Apply(pushResult);
                if (!result.Variables || !result.Objects || !result.References)
                {
                    log.LogError("Failed to push nodes on pusher {Name}", pusher.GetType());
                    PushNodesFailure(input, result, pusher);
                    return;
                }
            }
            else
            {
                result.Variables = true;
                result.Objects = true;
                result.References = true;
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
        public async Task PushNodes(PusherInput input, bool initial)
        {
            var newStates = input.Variables
                .Select(ts => ts.Id)
                .Distinct()
                .Select(id => State.GetNodeState(id));

            if (initial && !input.Variables.Any() && Config.Subscriptions.DataPoints)
            {
                log.LogWarning("No variables found, the extractor can run without any variables, but will not read history. " +
                    "There may be issues reported at the debug log level, or this may be a configuration issue. " +
                    "If this is intentional, and you do not want to read datapoints at all, you should disable " +
                    "data point subscriptions by setting `subscriptions.data-points` to false");
            }

            var pushTasks = new List<Task> {
                PushNodes(input, pusher, initial)
            };

            if (Config.DryRun)
            {
                log.LogInformation("Dry run is enabled");
                log.LogInformation("Would push {Count} nodes without dry run:", input.Variables.Count() + input.Objects.Count());
                foreach (var node in input.Variables.Concat(input.Objects))
                {
                    if (Source.IsCancellationRequested) break;
                    log.LogDebug("{Node}", node);
                }
                log.LogInformation("Would push {Count} references without dry run:", input.References.Count());
                foreach (var rf in input.References)
                {
                    if (Source.IsCancellationRequested) break;
                    log.LogDebug("{Ref}", rf);
                }
                if (input.Deletes != null)
                {
                    log.LogInformation("Would delete {Count} nodes and {Count} references without dry run:", input.Deletes.Variables.Count() + input.Deletes.Objects.Count(), input.Deletes.References.Count());
                    foreach (var node in input.Deletes.Variables)
                    {
                        if (Source.IsCancellationRequested) break;
                        log.LogDebug("Delete variable {Node}", node);
                    }
                    foreach (var node in input.Deletes.Objects)
                    {
                        if (Source.IsCancellationRequested) break;
                        log.LogDebug("Delete object {Node}", node);
                    }
                    foreach (var rf in input.Deletes.References)
                    {
                        if (Source.IsCancellationRequested) break;
                        log.LogDebug("Delete reference {Node}", rf);
                    }
                }
            }

            if (Config.StateStorage.IsEnabled && StateStorage != null)
            {
                if (Streamer.AllowEvents)
                {
                    pushTasks.Add(StateStorage.RestoreExtractionState(
                        State.EmitterStates.Where(state => state.FrontfillEnabled).ToDictionary(state => state.Id),
                        Config.StateStorage.EventStore,
                        false,
                        Source.Token));
                }

                if (Streamer.AllowData)
                {
                    pushTasks.Add(StateStorage.RestoreExtractionState(
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
        /// Start synchronization of given list of variables with the server.
        /// </summary>
        /// <param name="variables">Variables to synchronize</param>
        /// <returns>Two tasks, one for data and one for events</returns>
        public void CreateSubscriptions(IEnumerable<UAVariable> variables)
        {
            var states = variables.Select(ts => ts.Id).Distinct().SelectNonNull(id => State.GetNodeState(id));

            historyReader?.AddStates(states, State.EmitterStates);

            subscriptionTask?.AddVariables(states);
            subscriptionTask?.AddEmitters(State.EmitterStates);

            if (subscriptionTask != null) TaskScheduler.ScheduleTaskNow(subscriptionTask.Name, reScheduleIfRunning: true);
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
        public void AuditEventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
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

                    if (browseTask.AddNodesToBrowse(relevantIds, isFull: false))
                    {
                        TaskScheduler.ScheduleTaskNow(browseTask.Name, reScheduleIfRunning: true);
                    }
                }
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

                if (browseTask.AddNodesToBrowse(relevantRefIds, isFull: false))
                {
                    TaskScheduler.ScheduleTaskNow(browseTask.Name, reScheduleIfRunning: true);
                }
            }
        }
        #endregion

        protected override async ValueTask DisposeAsyncCore()
        {
            await Close(true);

            Starting.Set(0);
            historyReader?.Dispose();
            historyReader = null;
            pubSubManager?.Dispose();
            pubSubManager = null;

            await base.DisposeAsyncCore();
        }
    }

    public interface IUAClientAccess
    {
        string? GetUniqueId(ExpandedNodeId id, int index = -1);
        TypeConverter TypeConverter { get; }
        string GetRelationshipId(UAReference reference);
        NamespaceTable? NamespaceTable { get; }
        public SessionContext Context { get; }
        public SourceInformation SourceInfo { get; }
        public int PublishingInterval { get; }
    }
}

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.Utils;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;

namespace Cognite.OpcUa.Connect
{
    enum SessionManagerState
    {
        Connecting,
        Connected
    }

    abstract class SessionManagerEvent
    {
    }

    class InitialEvent : SessionManagerEvent { }
    class KeepAliveFailedEvent : SessionManagerEvent { }
    class NotifyServiceLevel : SessionManagerEvent
    {
        public byte? ServiceLevel { get; }
        public NotifyServiceLevel(byte? sl)
        {
            ServiceLevel = sl;
        }
    }


    enum TransitionType
    {
        High,
        Low
    }

    class ServiceLevelUpdateResult
    {
        public TransitionType? Transition { get; set; }
        public bool TriggerReconnect { get; set; }
    }

    class DeferredCallbackActions
    {
        public TransitionType? Transition { get; set; }
        public bool Reconnected { get; set; }

        public async Task Call(UAClient client)
        {
            if (Reconnected) await client.Callbacks.OnServerReconnect(client);
            if (Transition == TransitionType.High) await client.Callbacks.OnServiceLevelAboveThreshold(client);
            else if (Transition == TransitionType.Low) await client.Callbacks.OnServicelevelBelowThreshold(client);
        }
    }

    class ManagerActions
    {
        public SessionManagerState? StateChange { get; set; }
        public byte? ServiceLevelUpdate { get; set; }
        public bool RecheckServiceLevel { get; set; }

        public void Reset()
        {
            StateChange = null;
            ServiceLevelUpdate = null;
            RecheckServiceLevel = false;
        }
    }


    public class SessionManager2
    {
        private readonly ManagerActions pendingActions = new ManagerActions();

        private readonly AsyncLock stateLock = new AsyncLock();
        private readonly AsyncConditionVariable onStateChange;
        private readonly AsyncConditionVariable onNewPendingActions;

        private readonly ILogger log;
        private readonly UAClient client;
        public UAClient Client => client;
        private readonly SourceConfig config;
        private readonly IConnectionSource connectionSource;

        private static readonly Counter connects = Metrics
            .CreateCounter("opcua_connects", "Number of times the client has connected to and mapped the opcua server");
        private static readonly Gauge connected = Metrics
            .CreateGauge("opcua_connected", "Whether or not the client is currently connected to the opcua server");

        private SessionManagerState state = SessionManagerState.Connecting;
        private bool running;

        private DateTime? lastLowSLConnectAttempt = null;

        public byte CurrentServiceLevel { get; private set; } = 255;
        public SessionContext Context { get; }

        private Connection? connection;

        public ISession? Session => connection?.Session;

        private int timeout;

        // Initialized late, will be non-null after Initialize is called.
        private ApplicationConfiguration appConfig = null!;


        public SessionManager2(UAClient client, FullConfig config, ILogger logger)
        {
            Context = new SessionContext(config, logger);
            log = logger;
            onStateChange = new AsyncConditionVariable(stateLock);
            onNewPendingActions = new AsyncConditionVariable(stateLock);
            this.client = client;
            this.config = config.Source;

            connectionSource = IConnectionSource.FromConfig(this, this.config, logger);
        }

        public static void IncConnects()
        {
            connects.Inc();
        }

        public async Task<ISession> WaitForSession(CancellationToken token = default)
        {
            using (await stateLock.LockAsync(token))
            {
                while (state != SessionManagerState.Connected || connection == null)
                {
                    await onStateChange.WaitAsync(token);
                }
                return connection.Session;
            }
        }

        public async Task RegisterExternalNamespaces(string[] table, CancellationToken token = default)
        {
            using (await stateLock.LockAsync(token))
            {
                Context.AddExternalNamespaces(table);
            }
        }

        public async Task Close(CancellationToken token)
        {
            try
            {
                if (connection != null && !connection.Session.Disposed)
                {
                    await CloseSession(connection.Session, token);
                    connection = null;
                }
            }
            finally
            {
                connected.Set(0);
            }
        }

        public void Initialize(ApplicationConfiguration appConfig, int timeout)
        {
            this.timeout = timeout;
            this.appConfig = appConfig;
        }


        public async Task Run(CancellationToken token)
        {
            if (running) throw new ExtractorFailureException("Session manager has already been started");
            running = true;
            pendingActions.Reset();
            state = SessionManagerState.Connecting;

            try
            {
                await RunInner(token);
            }
            finally
            {
                running = false;
            }
        }

        public void EnsureServiceLevelSubscription()
        {
            client.SubscriptionManager!.EnqueueTask(new ServiceLevelSubscriptionTask(OnServiceLevelUpdate, client.Callbacks));
        }

        private async Task RunInner(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                using (await stateLock.LockAsync(token))
                {
                    // First check if the state has changed.
                    if (pendingActions.StateChange != null)
                    {
                        state = pendingActions.StateChange.Value;
                    }

                    // Next, if we aren't connected, ensure that we are.
                    DeferredCallbackActions? actions = null;
                    if (state == SessionManagerState.Connecting)
                    {
                        actions = await TryWithBackoff(() => EnsureConnected(token), 6, token);
                    }
                    // If we're not recently connected, 
                    else if (pendingActions.ServiceLevelUpdate != null || pendingActions.RecheckServiceLevel)
                    {
                        var res = UpdateServiceLevel(pendingActions.ServiceLevelUpdate ?? CurrentServiceLevel);
                        actions = new DeferredCallbackActions
                        {
                            Transition = res.Transition
                        };
                        if (res.TriggerReconnect)
                        {
                            ConnectResult r;
                            try
                            {
                                r = await connectionSource.Connect(connection, true, appConfig, token);
                            }
                            catch (Exception ex)
                            {
                                log.LogError(ex, "Unexpected error attempting reconnect after service level change");
                                state = SessionManagerState.Connecting;
                                continue;
                            }

                            try
                            {
                                actions = await HandleConnectResult(r, actions, token);
                            }
                            catch (Exception ex)
                            {
                                log.LogError(ex, "Unexpected error when handling connect result after service level change");
                                await CloseSession(r.Connection.Session, token);
                                state = SessionManagerState.Connecting;
                                continue;
                            }
                        }
                    }
                    if (actions != null)
                    {
                        // Don't run the callbacks inside the session manager loop, since
                        // if we lose connection while the callbacks are running,
                        // we need the loop to recover.
                        client.Callbacks.TaskScheduler.ScheduleTask(null, token => actions.Call(client));
                    }

                    await onNewPendingActions.WaitAsync(token);
                }
            }
        }

        private async Task<T> TryWithBackoff<T>(Func<Task<T>> method, int maxBackoff, CancellationToken token)
        {
            int iter = 0;
            var start = DateTime.UtcNow;
            TimeSpan backoff;
            while (true)
            {
                token.ThrowIfCancellationRequested();
                try
                {
                    return await method();
                }
                catch
                {
                    iter++;
                    iter = Math.Min(iter, maxBackoff);
                    backoff = TimeSpan.FromSeconds(Math.Pow(2, iter));

                    if (timeout >= 0 && (DateTime.UtcNow - start).TotalSeconds > timeout)
                    {
                        throw;
                    }
                    if (!token.IsCancellationRequested)
                        log.LogWarning("Failed to connect, retrying in {Backoff}", backoff);
                    try { await Task.Delay(backoff, token); } catch (TaskCanceledException) { }
                }
            }

        }

        public static async Task<byte> ReadServiceLevel(ISession session, CancellationToken token)
        {
            var res = await session.ReadAsync(null, 0, TimestampsToReturn.Neither, new ReadValueIdCollection
            {
                new ReadValueId
                {
                    NodeId = VariableIds.Server_ServiceLevel,
                    AttributeId = Attributes.Value
                }
            }, token);
            var dv = res.Results[0];
            return dv.GetValue<byte>(0);
        }

        private async Task<DeferredCallbackActions> EnsureConnected(CancellationToken token)
        {
            bool isConnected = state == SessionManagerState.Connecting;
            if (isConnected) return new DeferredCallbackActions();

            var result = await connectionSource.Connect(connection, isConnected, appConfig, token);

            try
            {
                return await HandleConnectResult(result, new DeferredCallbackActions(), token);
            }
            catch
            {
                await CloseSession(result.Connection.Session, token);
                state = SessionManagerState.Connecting;
                connection = null;
                throw;
            }
        }

        private ServiceLevelUpdateResult UpdateServiceLevel(byte newLevel)
        {
            var oldServiceLevel = CurrentServiceLevel;

            if (newLevel != oldServiceLevel)
            {
                log.LogDebug("Server ServiceLevel updated {From} -> {To}", oldServiceLevel, newLevel);
            }

            var result = new ServiceLevelUpdateResult();

            if (newLevel < config.Redundancy.ServiceLevelThreshold)
            {
                result.TriggerReconnect = true;

                // We don't want to reconnect too frequently
                if (lastLowSLConnectAttempt != null)
                {
                    var timeSinceLastAttempt = DateTime.UtcNow - lastLowSLConnectAttempt.Value + TimeSpan.FromSeconds(5);
                    if (config.Redundancy.ReconnectIntervalValue.Value != Timeout.InfiniteTimeSpan
                        && timeSinceLastAttempt < config.Redundancy.ReconnectIntervalValue.Value)
                    {
                        result.TriggerReconnect = false;
                    }
                }

                if (oldServiceLevel >= config.Redundancy.ServiceLevelThreshold)
                {
                    log.LogWarning("Service level dropped below threshold. Until it is recovered, the extractor will not update history state");
                    result.Transition = TransitionType.Low;
                }
            }
            else if (oldServiceLevel < config.Redundancy.ServiceLevelThreshold)
            {
                log.LogInformation("New service level {Level} is a above threshold {Threshold}, triggering callback", newLevel, config.Redundancy.ServiceLevelThreshold);
                result.Transition = TransitionType.High;
            }

            CurrentServiceLevel = newLevel;

            return result;
        }

        private async Task<DeferredCallbackActions> HandleConnectResult(ConnectResult result, DeferredCallbackActions actions, CancellationToken token)
        {
            // No reconnect, do nothing here.
            if (result.Type == ConnectType.None) return actions;

            actions.Reconnected = true;


            // Ensure that we're correctly monitoring service level.
            if (config.Redundancy.MonitorServiceLevel)
            {
                EnsureServiceLevelSubscription();
                if (config.Redundancy.ReconnectIntervalValue.Value != Timeout.InfiniteTimeSpan
                    && !client.Callbacks.TaskScheduler.ContainsTask("CheckServiceLevel"))
                {
                    client.Callbacks.TaskScheduler.SchedulePeriodicTask("CheckServiceLevel",
                        config.Redundancy.ReconnectIntervalValue,
                        async (token) =>
                        {
                            using (await stateLock.LockAsync(token))
                            {
                                pendingActions.RecheckServiceLevel = true;
                                onNewPendingActions.NotifyAll();
                            }
                        }
                    );
                }
            }

            // Fetch the current service level as well, and trigger an update.
            if (config.IsRedundancyEnabled || config.Redundancy.MonitorServiceLevel)
            {
                var sl = await ReadServiceLevel(result.Connection.Session, token);
                var slResult = UpdateServiceLevel(sl);
                // Ignore trigger reconnect here, we never want to reconnect off the back of a
                // connection change.
                actions.Transition = slResult.Transition;
            }

            if (connection?.Session != result.Connection.Session)
            {
                // Make sure the session has the correct callbacks set.
                result.Connection.Session.KeepAliveInterval = config.KeepAliveInterval;
                result.Connection.Session.KeepAlive -= ClientKeepAlive;
                result.Connection.Session.KeepAlive += ClientKeepAlive;
                result.Connection.Session.PublishError -= OnPublishError;
                result.Connection.Session.PublishError += OnPublishError;

                log.LogInformation("Registered new session with ID: {Id}", result.Connection.Session.SessionId);

                if (connection != null)
                {
                    await CloseSession(connection.Session, token);
                }
                connection = result.Connection;
            }

            if (result.Connection.Session.SubscriptionCount > 0)
            {
                foreach (var sub in result.Connection.Session.Subscriptions)
                {
                    log.LogDebug("Session already has subscription: {Sub} with {Count} monitored items", sub.DisplayName, sub.MonitoredItemCount);
                }
            }

            state = SessionManagerState.Connected;
            Context.UpdateFromSession(connection.Session);
            onStateChange.NotifyAll();

            return actions;
        }


        public async Task CloseSession(ISession toClose, CancellationToken token)
        {
            try
            {
                var closeTask = toClose.CloseAsync(token);
                var resultTask = await Task.WhenAny(Task.Delay(5000, token), closeTask);
                if (closeTask != resultTask)
                {
                    log.LogWarning("Failed to close session, timed out");
                }
                else
                {
                    log.LogInformation("Successfully closed connection to server");
                }
            }
            catch (Exception ex)
            {
                log.LogWarning("Failed to close connection to server, proceeding assuming it is closed: {Message}", ex.Message);
            }

            lock (toClose)
            {
                if (!toClose.Disposed)
                {
                    toClose.KeepAlive -= ClientKeepAlive;
                    toClose.PublishError -= OnPublishError;
                    toClose.Dispose();
                }
            }
        }

        private static readonly uint[] statusCodesToAbandon = new[] {
            StatusCodes.BadSessionIdInvalid,
            StatusCodes.BadSessionNotActivated,
            StatusCodes.BadSessionClosed
        };

        public static bool ShouldAbandonReconnect(Exception ex)
        {
            if (ex is AggregateException aex)
            {
                return ShouldAbandonReconnect(aex.InnerException);
            }
            if (ex is ServiceResultException e)
            {
                return statusCodesToAbandon.Contains(e.StatusCode);
            }
            return false;
        }


        private void OnPublishError(ISession session, PublishErrorEventArgs e)
        {
            string symId = StatusCode.LookupSymbolicId(e.Status.Code);

            var sub = session.Subscriptions.FirstOrDefault(sub => sub.Id == e.SubscriptionId);

            if (sub != null)
            {
                log.LogError("Unexpected error on publish: {Code}, subscription: {Name}", symId, sub.DisplayName);
            }
        }

        private void ClientKeepAlive(ISession sender, KeepAliveEventArgs eventArgs)
        {
            client.LogDump("Keep Alive", eventArgs);
            if (eventArgs.Status == null || !ServiceResult.IsNotGood(eventArgs.Status)) return;
            log.LogWarning("Keep alive failed: {Status}", eventArgs.Status);
            using (stateLock.Lock())
            {
                if (state == SessionManagerState.Connecting || pendingActions.StateChange == SessionManagerState.Connecting)
                {
                    log.LogTrace("Session already connecting, skipping");
                    return;
                }
                if (!running)
                {
                    log.LogWarning("Session manager is closed, not attempting to reconnect");
                    return;
                }

                pendingActions.StateChange = SessionManagerState.Connecting;
                log.LogWarning("--- RECONNECTING ---");
                client.Callbacks.OnServerDisconnect(client);
                connected.Set(0);
                onNewPendingActions.NotifyAll();
            }
        }

        private void OnServiceLevelUpdate(MonitoredItem item, MonitoredItemNotificationEventArgs _)
        {
            try
            {
                var values = item.DequeueValues();
                var value = values.FirstOrDefault()
                    ?.GetValue<byte>(0);
                if (value == null)
                {
                    log.LogWarning("Received null or invalid ServiceLevel");
                    return;
                }
                using (stateLock.Lock())
                {
                    pendingActions.ServiceLevelUpdate = value;
                    onNewPendingActions.NotifyAll();
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Unexpected error handling ServiceLevel trigger");
            }
        }
    }
}
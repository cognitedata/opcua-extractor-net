using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.Utils;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using Opc.Ua;
using Opc.Ua.Client;

namespace Cognite.OpcUa.Connect
{
    enum SessionManagerState
    {
        Connecting,
        Connected,
        Closed
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


    public class SessionManager : IDisposable
    {
        /// <summary>
        /// Actions the main session manager loop should execute once
        /// resumed.
        /// </summary>
        private readonly ManagerActions pendingActions = new ManagerActions();

        /// <summary>
        /// Lock covering `pendingActions`, `state`, and `CurrentServiceLevel`.
        /// `onStateChange` and `onNewPendingActions` are tied to this lock.
        /// </summary>
        private readonly AsyncLock stateLock = new AsyncLock();
        private SessionManagerState state = SessionManagerState.Closed;
        /// <summary>
        /// Notified when the state has changed, and a new session is available.
        /// </summary>
        private readonly AsyncConditionVariable onStateChange;
        /// <summary>
        /// Notified when pendingActions have been updated.
        /// </summary>
        private readonly AsyncConditionVariable onNewPendingActions;

        private readonly ILogger log;
        private readonly UAClient client;
        public UAClient Client => client;
        private readonly SourceConfig config;
        private IConnectionSource connectionSource = null!;


        public bool Running { get; private set; }
        public Task<SchedulerTaskResult>? RunningTask { get; private set; }

        private DateTime? lastConnectionSwitch = null;

        public byte CurrentServiceLevel { get; private set; } = 255;
        public SessionContext Context { get; }

        private Connection? connection;

        public ISession? Session => connection?.Session;
        public string? EndpointUrl => connection?.EndpointUrl;

        private int timeout;

        // Initialized late, will be non-null after Initialize is called.
        private ApplicationConfiguration appConfig = null!;
        private bool disposedValue;

        public SessionManager(UAClient client, FullConfig config, ILogger logger)
        {
            Context = new SessionContext(config, logger);
            log = logger;
            onStateChange = new AsyncConditionVariable(stateLock);
            onNewPendingActions = new AsyncConditionVariable(stateLock);
            this.client = client;
            this.config = config.Source;
        }

        /// <summary>
        /// Wait for a session to be available, guaranteed to
        /// return a non-null connected session. Of course, the
        /// returned session may immediately lose connection again.
        /// </summary>
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

        public void RegisterExternalNamespaces(string[] table)
        {
            Context.AddExternalNamespaces(table);
        }

        public async Task Close(CancellationToken token)
        {
            try
            {
                using (await stateLock.LockAsync(token))
                {
                    if (connection != null && !connection.Session.Disposed)
                    {
                        await CloseSession(connection.Session, token);
                        connection = null;
                    }
                    pendingActions.StateChange = SessionManagerState.Closed;
                    onNewPendingActions.NotifyAll();
                }
            }
            finally
            {
                ConnectionUtils.Connected.Set(0);
            }
        }

        public void Initialize(ApplicationConfiguration appConfig, int timeout)
        {
            this.timeout = timeout;
            this.appConfig = appConfig;
            connectionSource = IConnectionSource.FromConfig(this, config, log);
        }


        public async Task<SchedulerTaskResult> Run(CancellationToken token)
        {
            if (Running) throw new ExtractorFailureException("Session manager has already been started");
            Running = true;
            pendingActions.Reset();
            pendingActions.StateChange = SessionManagerState.Connecting;

            log.LogInformation("Starting session manager");

            try
            {
                RunningTask = RunInner(token);
                return await RunningTask;
            }
            catch (Exception ex)
            {
                if (ex is not OperationCanceledException)
                {
                    log.LogError("Session manager failed fatally: {Message}", ex.Message);
                }
                throw;
            }
            finally
            {
                state = SessionManagerState.Closed;
                Running = false;
            }
        }

        public void EnsureServiceLevelSubscription()
        {
            client.SubscriptionManager!.EnqueueTask(new ServiceLevelSubscriptionTask(OnServiceLevelUpdate, client.Callbacks));
        }

        private async Task<SchedulerTaskResult> RunInner(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                using (await stateLock.LockAsync(token))
                {
                    bool isInitial = state == SessionManagerState.Closed;

                    // First check if the state has changed.
                    if (pendingActions.StateChange == SessionManagerState.Connecting)
                    {
                        if (state == SessionManagerState.Connected)
                        {
                            client.Callbacks.OnServerDisconnect(client);
                        }
                        state = SessionManagerState.Connecting;
                    }
                    // If the target state is set to Closed, exit the session manager cleanly.
                    else if (pendingActions.StateChange == SessionManagerState.Closed)
                    {
                        state = SessionManagerState.Closed;
                        pendingActions.Reset();
                        return SchedulerTaskResult.Expected;
                    }

                    byte? updateServiceLevel = pendingActions.ServiceLevelUpdate;
                    if (pendingActions.RecheckServiceLevel)
                    {
                        updateServiceLevel = CurrentServiceLevel;
                    }

                    // Reset the pending actions here so that
                    // any errant "continue;"'s in the following code won't
                    // cause us to end up in a state where it isn't clean at the start
                    // of the loop.
                    pendingActions.Reset();

                    bool newConnection = false;
                    // Next, if we aren't connected, ensure that we are.
                    if (state == SessionManagerState.Connecting)
                    {
                        newConnection = await ConnectionUtils.TryWithBackoff(
                            () => EnsureConnected(token), 6, timeout, log, token);
                        lastConnectionSwitch = DateTime.UtcNow;
                    }
                    // Attempt to update the service level, but only if we're not also reconnecting.
                    // If we reconnect we fetch the service level anyway.
                    else if (updateServiceLevel != null)
                    {
                        var res = await UpdateServiceLevel(updateServiceLevel.Value, false);
                        if (res)
                        {
                            ConnectResult r;
                            try
                            {
                                r = await connectionSource.Connect(connection, true, appConfig, token);
                                lastConnectionSwitch = DateTime.UtcNow;
                            }
                            catch (Exception ex)
                            {
                                ExtractorUtils.LogException(log, ex, "Unexpected error attempting reconnect after service level change");
                                state = SessionManagerState.Connecting;
                                continue;
                            }

                            try
                            {
                                newConnection = await HandleConnectResult(r, token);
                            }
                            catch (Exception ex)
                            {
                                ExtractorUtils.LogException(log, ex, "Unexpected error when handling connect result after service level change");
                                await CloseSession(r.Connection.Session, token);
                                state = SessionManagerState.Connecting;
                                continue;
                            }
                        }
                    }
                    if (newConnection && !isInitial)
                    {
                        // Don't run the callbacks inside the session manager loop, since
                        // if we lose connection while the callbacks are running,
                        // we need the loop to recover.
                        client.Callbacks.ScheduleTask(_ => client.Callbacks.OnServerReconnect(client), SchedulerTaskResult.Expected, "OnServerReconnect");
                    }

                    await onNewPendingActions.WaitAsync(token);
                }
            }
            return SchedulerTaskResult.Unexpected;
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
            if (res.Results.Count != 1)
            {
                throw new ExtractorFailureException(
                    "Incorrect number of results returned when reading service level, this is a bug in the server"
                );
            }
            var dv = res.Results[0];
            if (!StatusCode.IsGood(dv.StatusCode))
            {
                var sym = StatusCode.LookupSymbolicId(dv.StatusCode.Code);
                throw new ExtractorFailureException(
                    $"Service level has a bad status code: {sym}"
                );
            }
            return dv.GetValue<byte>(0);
        }

        private async Task<bool> EnsureConnected(CancellationToken token)
        {
            if (state == SessionManagerState.Connected) return false;

            var result = await connectionSource.Connect(connection, false, appConfig, token);

            try
            {
                return await HandleConnectResult(result, token);
            }
            catch
            {
                await CloseSession(result.Connection.Session, token);
                state = SessionManagerState.Connecting;
                connection = null;
                throw;
            }
        }

        private async Task<bool> UpdateServiceLevel(byte newLevel, bool fromConnection)
        {
            var oldServiceLevel = CurrentServiceLevel;

            if (newLevel != oldServiceLevel)
            {
                log.LogDebug("Server ServiceLevel updated {From} -> {To}", oldServiceLevel, newLevel);
            }
            // If service level equals old service level, we may still need to check
            // for reconnect, if this is part of the periodic re-check.

            bool triggerReconnect = false;
            if (newLevel < config.Redundancy.ServiceLevelThreshold)
            {
                triggerReconnect = true;

                if (fromConnection)
                {
                    triggerReconnect = false;
                }

                // We don't want to reconnect too frequently
                if (lastConnectionSwitch != null && !fromConnection)
                {
                    var timeSinceLastAttempt = DateTime.UtcNow - lastConnectionSwitch.Value + TimeSpan.FromSeconds(5);
                    if (config.Redundancy.ReconnectIntervalValue.Value != Timeout.InfiniteTimeSpan
                        && timeSinceLastAttempt < config.Redundancy.ReconnectIntervalValue.Value)
                    {
                        triggerReconnect = false;
                        log.LogDebug(
                            "Last switch was too recent ({Last}), waiting until the next trigger after {Next}",
                            lastConnectionSwitch.Value, lastConnectionSwitch + config.Redundancy.ReconnectIntervalValue.Value);
                    }
                }

                if (oldServiceLevel >= config.Redundancy.ServiceLevelThreshold)
                {
                    log.LogWarning("Service level dropped below threshold. Until it is recovered, the extractor will not update history state");
                    await client.Callbacks.OnServicelevelBelowThreshold(client);
                }
            }
            else if (oldServiceLevel < config.Redundancy.ServiceLevelThreshold)
            {
                log.LogInformation("New service level {Level} is a above threshold {Threshold}, triggering callback", newLevel, config.Redundancy.ServiceLevelThreshold);
                await client.Callbacks.OnServiceLevelAboveThreshold(client);
            }

            CurrentServiceLevel = newLevel;
            ConnectionUtils.ServiceLevel.Set(CurrentServiceLevel);

            return triggerReconnect;
        }

        private async Task<bool> HandleConnectResult(ConnectResult result, CancellationToken token)
        {
            // No reconnect, do nothing here.
            if (result.Type == ConnectType.None) return false;
            ConnectionUtils.Connected.Set(1);

            // Ensure that we're correctly monitoring service level.
            if (config.Redundancy.MonitorServiceLevel)
            {
                EnsureServiceLevelSubscription();
                if (config.Redundancy.ReconnectIntervalValue.Value != Timeout.InfiniteTimeSpan
                    && !client.Callbacks.PeriodicScheduler.ContainsTask("CheckServiceLevel"))
                {
                    client.Callbacks.PeriodicScheduler.SchedulePeriodicTask("CheckServiceLevel",
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
                var slResult = UpdateServiceLevel(sl, true);
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

            return true;
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
            // Using a sync lock is safe here since we're in a thread spawned by
            // the OPC-UA SDK, not the main task loop.
            using (stateLock.Lock())
            {
                if (state == SessionManagerState.Connecting || pendingActions.StateChange == SessionManagerState.Connecting)
                {
                    log.LogTrace("Session already connecting, skipping");
                    return;
                }
                if (!Running)
                {
                    log.LogWarning("Session manager is closed, not attempting to reconnect");
                    return;
                }
                log.LogWarning("Keep alive failed: {Status}", eventArgs.Status);

                if (sender != connection?.Session)
                {
                    log.LogDebug("Keep alive on dead session, ignoring");
                    return;
                }

                pendingActions.StateChange = SessionManagerState.Connecting;
                log.LogWarning("--- RECONNECTING ---");
                ConnectionUtils.Connected.Set(0);
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
                // Using a sync lock is safe here since we're in a thread spawned by
                // the OPC-UA SDK, not the main task loop.
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

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    connectionSource?.Dispose();
                    if (connection?.Session != null)
                    {
                        try
                        {
                            connection.Session.Close();
                        }
                        catch { }
                        connection.Session.KeepAlive -= ClientKeepAlive;
                        connection.Session.PublishError -= OnPublishError;
                        connection.Session.Dispose();
                    }
                    connection = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

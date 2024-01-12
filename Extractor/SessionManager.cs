using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Subscriptions;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Metrics = Prometheus.Metrics;

namespace Cognite.OpcUa
{

    public class SessionManager : IDisposable
    {
        private readonly SourceConfig config;
        private readonly FullConfig fullConfig;
        private readonly UAClient client;
        private ReverseConnectManager? reverseConnectManager;
        // Initialized late, will be non-null after Initialize is called.
        private ApplicationConfiguration appConfig = null!;
        private ILogger log;

        public byte CurrentServiceLevel { get; private set; } = 255;
        private DateTime? lastLowSLConnectAttempt = null;
        private readonly SemaphoreSlim redundancyReconnectLock = new SemaphoreSlim(1);

        private ISession? session;
        private bool currentSessionIsDead;
        private CancellationToken liveToken;
        private bool disposedValue;
        private int timeout;

        private static readonly Counter connects = Metrics
            .CreateCounter("opcua_connects", "Number of times the client has connected to and mapped the opcua server");
        private static readonly Gauge connected = Metrics
            .CreateGauge("opcua_connected", "Whether or not the client is currently connected to the opcua server");
        public ISession? Session => session;

        public string? EndpointUrl { get; private set; }

        private readonly SemaphoreSlim sessionWaitLock = new SemaphoreSlim(0);
        private int sessionWaiters = 0;
        private readonly object sessionWaitExtLock = new();

        public SessionContext Context { get; }


        public SessionManager(FullConfig config, UAClient parent, ILogger log)
        {
            client = parent;
            this.config = config.Source;
            fullConfig = config;
            this.log = log;
            Context = new SessionContext(config, log);
        }

        public async Task<ISession> WaitForSession()
        {
            lock (sessionWaitExtLock)
            {
                if (session != null) return session;
                sessionWaiters++;
            }
            await sessionWaitLock.WaitAsync(liveToken);
            if (session == null)
            {
                throw new ExtractorFailureException("Session does not exist after waiting, extractor is unable to continue");
            }
            return session;
        }

        public void RegisterExternalNamespaces(string[] table)
        {
            lock (sessionWaitExtLock)
            {
                Context.AddExternalNamespaces(table);
            }
        }

        private async Task TryWithBackoff(Func<Task> method, int maxBackoff, CancellationToken token)
        {
            int iter = 0;
            var start = DateTime.UtcNow;
            TimeSpan backoff;
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await method();
                    break;
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
                    if (!liveToken.IsCancellationRequested)
                        log.LogWarning("Failed to connect, retrying in {Backoff}", backoff);
                    try { await Task.Delay(backoff, token); } catch (TaskCanceledException) { }
                }
            }
        }

        private void SetNewSession(ISession session)
        {
            session.KeepAliveInterval = config.KeepAliveInterval;
            session.KeepAlive -= ClientKeepAlive;
            session.KeepAlive += ClientKeepAlive;
            session.PublishError -= OnPublishError;
            session.PublishError += OnPublishError;
            lock (sessionWaitExtLock)
            {
                this.session = session;
                currentSessionIsDead = false;
                Context.UpdateFromSession(session);
                for (; sessionWaiters > 0; sessionWaiters--)
                {
                    sessionWaitLock.Release();
                }
            }
        }

        public void Initialize(ApplicationConfiguration appConfig, CancellationToken token, int timeout)
        {
            liveToken = token;
            this.timeout = timeout;
            this.appConfig = appConfig;
        }

        public async Task Connect()
        {
            Func<Task> generator = async () =>
            {
                ISession newSession;
                if (!string.IsNullOrEmpty(config.ReverseConnectUrl))
                {
                    if (session != null && EndpointUrl != config.EndpointUrl)
                    {
                        await Close(liveToken);
                    }
                    newSession = await WaitForReverseConnect(session);
                    EndpointUrl = config.EndpointUrl;
                }
                else if (config.IsRedundancyEnabled)
                {
                    var result = await CreateSessionWithRedundancy(config.AltEndpointUrls!.Prepend(config.EndpointUrl!),
                        session == null || EndpointUrl == null
                        ? null
                        : new SessionRedundancyResult(session, EndpointUrl, 0));
                    newSession = result.Session;
                    EndpointUrl = result.EndpointUrl;
                    await UpdateServiceLevel(result.ServiceLevel, true);
                }
                else
                {
                    if (session != null && EndpointUrl != config.EndpointUrl)
                    {
                        await Close(liveToken);
                    }
                    newSession = await CreateSessionDirect(config.EndpointUrl!, session);
                    EndpointUrl = config.EndpointUrl;
                }
                SetNewSession(newSession);
            };
            await TryWithBackoff(generator, 6, liveToken);
            if (!liveToken.IsCancellationRequested)
            {
                log.LogInformation("Successfully connected to server");
                connects.Inc();
                connected.Set(1);
            }

            if (config.Redundancy.MonitorServiceLevel)
            {
                EnsureServiceLevelSubscription();
                if (config.Redundancy.ReconnectIntervalValue.Value != System.Threading.Timeout.InfiniteTimeSpan
                    && !client.Callbacks.TaskScheduler.ContainsTask("CheckServiceLevel"))
                {
                    client.Callbacks.TaskScheduler.SchedulePeriodicTask("CheckServiceLevel",
                        config.Redundancy.ReconnectIntervalValue, async (_) => await RecheckServiceLevel());
                }
                // When starting with redundancy we read the service level as part of the startup process.
                // If not running with redundancy, we want to make sure that we still get the servicelevel on startup.
                if (!config.IsRedundancyEnabled)
                {
                    var sl = await ReadServiceLevel(Session!);
                    await UpdateServiceLevel(sl, true);
                }
            }
        }

        /// <summary>
        /// Event triggered when a publish request fails.
        /// </summary>
        private void OnPublishError(ISession session, PublishErrorEventArgs e)
        {
            string symId = StatusCode.LookupSymbolicId(e.Status.Code);

            var sub = session.Subscriptions.FirstOrDefault(sub => sub.Id == e.SubscriptionId);

            if (sub != null)
            {
                log.LogError("Unexpected error on publish: {Code}, subscription: {Name}", symId, sub.DisplayName);
            }
        }

        private async Task<ISession> WaitForReverseConnect(ISession? oldSession)
        {
            if (oldSession != null && reverseConnectManager != null)
            {
                log.LogInformation("Attempting to reconnect to the server, before creating a new connection");
                try
                {
                    var reconn = await reverseConnectManager.WaitForConnection(new Uri(oldSession.Endpoint.EndpointUrl), oldSession.Endpoint.Server.ApplicationUri, liveToken);
                    if (reconn == null)
                    {
                        log.LogError("Reverse connect failed, no connection established");
                        throw new ExtractorFailureException("Failed to obtain reverse connection from server");
                    }
                    await Task.Run(() => oldSession.Reconnect(reconn), liveToken);
                    return oldSession;
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to reconnect to server at {Url}: {Message}", oldSession.Endpoint.EndpointUrl, ex.Message);
                    if (ShouldAbandonReconnect(ex) || config.ForceRestart)
                    {
                        await Close(liveToken);
                    }
                    else throw;
                }
            }

            reverseConnectManager?.Dispose();

            appConfig.ClientConfiguration.ReverseConnect = new ReverseConnectClientConfiguration
            {
                WaitTimeout = 300000,
                HoldTime = 30000
            };

            reverseConnectManager = new ReverseConnectManager();
            var endpointUrl = new Uri(config.EndpointUrl);
            var reverseUrl = new Uri(config.ReverseConnectUrl);
            reverseConnectManager.AddEndpoint(reverseUrl);
            reverseConnectManager.StartService(appConfig);

            log.LogInformation("Waiting for reverse connection from: {EndpointURL}", config.EndpointUrl);
            var connection = await reverseConnectManager.WaitForConnection(endpointUrl, null);
            if (connection == null)
            {
                log.LogError("Reverse connect failed, no connection established");
                throw new ExtractorFailureException("Failed to obtain reverse connection from server");
            }
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(appConfig, connection, config.Secure, 30000);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.SelectEndpoint);
            }
            var endpointConfiguration = EndpointConfiguration.Create(appConfig);
            client.LogDump("Reverse connect endpoint configuration", endpointConfiguration);

            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            client.LogDump("Reverse connect endpoint", endpoint);

            var identity = AuthenticationUtils.GetUserIdentity(config);
            log.LogInformation("Attempt to connect to endpoint with security: {SecurityPolicyUri} using user identity {Identity}",
                endpoint.Description.SecurityPolicyUri,
                identity.DisplayName);

            try
            {
                connection = await reverseConnectManager.WaitForConnection(endpointUrl, null);
                if (connection == null)
                {
                    log.LogError("Reverse connect failed, no connection established");
                    throw new ExtractorFailureException("Failed to obtain reverse connection from server");
                }


                var session = await Opc.Ua.Client.Session.Create(
                    appConfig,
                    connection,
                    endpoint,
                    false,
                    false,
                    ".NET OPC-UA Extractor Client",
                    0,
                    identity,
                    null);
                session.DeleteSubscriptionsOnClose = true;
                return session;
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }

        private static uint[] statusCodesToAbandon = new[] {
            StatusCodes.BadSessionIdInvalid,
            StatusCodes.BadSessionNotActivated,
            StatusCodes.BadSessionClosed
        };

        private bool ShouldAbandonReconnect(Exception ex)
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

        private async Task<ISession> CreateSessionDirect(string endpointUrl, ISession? oldSession)
        {
            if (oldSession != null)
            {
                log.LogInformation("Attempting to reconnect to the server, before creating a new connection");
                try
                {
                    await Task.Run(() => oldSession.Reconnect(), liveToken);
                    return oldSession;
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to reconnect to server at {Url}: {Message}", endpointUrl, ex.Message);
                    if (ShouldAbandonReconnect(ex) || config.ForceRestart)
                    {
                        await Close(liveToken);
                    }
                    else throw;
                }
            }

            log.LogInformation("Attempt to select endpoint from: {EndpointURL}", CoreClientUtils.GetDiscoveryUrl(endpointUrl));

            var identity = AuthenticationUtils.GetUserIdentity(config);
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(endpointUrl, config.Secure);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.SelectEndpoint);
            }
            if (config.EndpointDetails != null && !string.IsNullOrWhiteSpace(config.EndpointDetails.OverrideEndpointUrl))
            {
                log.LogInformation("Discovered endpoint is {Url}, using override {Override} instead",
                    selectedEndpoint.EndpointUrl, config.EndpointDetails.OverrideEndpointUrl);
                selectedEndpoint.EndpointUrl = config.EndpointDetails.OverrideEndpointUrl;
            }

            var endpointConfiguration = EndpointConfiguration.Create(appConfig);
            client.LogDump("Endpoint configuration", endpointConfiguration);

            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            client.LogDump("Endpoint", endpoint);
            log.LogInformation("Attempt to connect to endpoint at {Endpoint} with security: {SecurityPolicyUri} using user identity {Identity}",
                endpoint.Description.EndpointUrl,
                endpoint.Description.SecurityPolicyUri,
                identity.DisplayName);
            try
            {
                var session = await Opc.Ua.Client.Session.Create(
                    appConfig,
                    endpoint,
                    false,
                    ".NET OPC-UA Extractor Client",
                    0,
                    identity,
                    null
                );
                session.DeleteSubscriptionsOnClose = true;
                return session;
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }

        private class SessionRedundancyResult
        {
            public ISession Session { get; set; }
            public string EndpointUrl { get; }
            public byte ServiceLevel { get; set; }
            public SessionRedundancyResult(ISession session, string endpointUrl, byte serviceLevel)
            {
                Session = session;
                EndpointUrl = endpointUrl;
                ServiceLevel = serviceLevel;
            }
        }

        private async Task<SessionRedundancyResult> CreateSessionWithRedundancy(IEnumerable<string> endpointUrls, SessionRedundancyResult? initial)
        {
            endpointUrls = endpointUrls.Distinct().ToList();
            log.LogInformation("Create session with redundant connections to {Urls}", string.Join(", ", endpointUrls));

            var exceptions = new List<Exception>();

            // First check if we can reuse the current session
            if (initial != null)
            {
                try
                {
                    if (currentSessionIsDead)
                    {
                        log.LogInformation("Attempting to reconnect to the current server before switching to another");
                        initial.Session = await CreateSessionDirect(initial.EndpointUrl, initial.Session);
                    }
                    initial.ServiceLevel = await ReadServiceLevel(initial.Session);
                    if (initial.ServiceLevel >= config.Redundancy.ServiceLevelThreshold)
                    {
                        log.LogInformation("Service level on current server is above threshold ({Val}), not switching", initial.ServiceLevel);
                        return initial;
                    }
                }
                catch (Exception ex)
                {
                    var hEx = ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
                    log.LogWarning("Failed to reconnect to current session: {Message}", hEx.Message);
                }
            }

            SessionRedundancyResult? current = initial;
            foreach (var url in endpointUrls)
            {
                try
                {
                    if (initial != null && url == initial.EndpointUrl) continue;

                    var session = await CreateSessionDirect(url, null);
                    var serviceLevel = await ReadServiceLevel(session);

                    if (serviceLevel > (current?.ServiceLevel ?? 0))
                    {
                        if (current != null)
                        {
                            await current.Session.CloseAsync();
                            current.Session.KeepAlive -= ClientKeepAlive;
                            current.Session.PublishError -= OnPublishError;
                            current.Session.Dispose();
                        }
                        current = new SessionRedundancyResult(session, url, serviceLevel);
                    }
                }
                catch (Exception ex)
                {
                    log.LogError("Failed to connect to endpoint {Url}: {Error}", url, ex.Message);
                    exceptions.Add(ex);
                }
            }

            if (current == null)
            {
                throw new AggregateException("Failed to connect to any configured endpoint", exceptions);
            }

            liveToken.ThrowIfCancellationRequested();
            log.LogInformation("Successfully connected to server with endpoint: {Endpoint}, ServiceLevel: {Level}", current.EndpointUrl, current.ServiceLevel);

            return current;
        }

        private async Task<byte> ReadServiceLevel(ISession session)
        {
            var res = await session.ReadAsync(null, 0, TimestampsToReturn.Neither, new ReadValueIdCollection
            {
                new ReadValueId
                {
                    NodeId = VariableIds.Server_ServiceLevel,
                    AttributeId = Attributes.Value
                }
            }, liveToken);
            var dv = res.Results[0];
            return dv.GetValue<byte>(0);
        }

        /// <summary>
        /// Called on client keep alive, handles the case where the server has stopped responding and the connection timed out.
        /// </summary>
        private void ClientKeepAlive(ISession sender, KeepAliveEventArgs eventArgs)
        {
            client.LogDump("Keep Alive", eventArgs);
            if (eventArgs.Status == null || !ServiceResult.IsNotGood(eventArgs.Status)) return;
            log.LogWarning("Keep alive failed: {Status}", eventArgs.Status);
            if (liveToken.IsCancellationRequested) return;
            connected.Set(0);

            log.LogWarning("--- RECONNECTING ---");
            if (!liveToken.IsCancellationRequested)
            {
                currentSessionIsDead = true;
                client.Callbacks.TaskScheduler.ScheduleTask(null, async (_) =>
                {
                    log.LogInformation("Attempting to reconnect to server");
                    if (!liveToken.IsCancellationRequested)
                    {
                        await Connect();
                        await client.Callbacks.OnServerReconnect(client);
                        connects.Inc();
                        connected.Set(1);
                    }
                });
            }
            client.Callbacks.TaskScheduler.ScheduleTask(null, async (_) => await client.Callbacks.OnServerDisconnect(client));
        }

        public async Task Close(CancellationToken token)
        {
            try
            {
                if (session != null && !session.Disposed)
                {
                    var closeTask = session.CloseAsync(token);
                    var resultTask = await Task.WhenAny(Task.Delay(5000, token), closeTask);
                    if (closeTask != resultTask)
                    {
                        log.LogWarning("Failed to close session, timed out");
                    }
                    else
                    {
                        log.LogInformation("Successfully closed connection to server");
                    }
#pragma warning disable CA1508 // Avoid dead conditional code. Threading may cause this method to be called multiple times in parallel...
                    if (session != null)
                    {
                        session.KeepAlive -= ClientKeepAlive;
                        session.PublishError -= OnPublishError;
                        session.Dispose();
                        session = null;
                    }
#pragma warning restore CA1508 // Avoid dead conditional code
                }
            }
            finally
            {
                connected.Set(0);
            }
        }

        private async Task UpdateServiceLevel(byte newLevel, bool fromConnectAttempt)
        {
            // Ensure that UpdateServiceLevel is only called once in parallel.
            await redundancyReconnectLock.WaitAsync();
            try
            {
                await UpdateServiceLevelInner(newLevel, fromConnectAttempt);
            }
            catch (Exception ex)
            {
                ExtractorUtils.LogException(log, ex, "Failed to update service level");
            }
            finally
            {
                redundancyReconnectLock.Release();
            }
        }

        private async Task UpdateServiceLevelInner(byte newLevel, bool fromConnectionChange)
        {
            if (!config.Redundancy.MonitorServiceLevel) return;
            // The rule is as follows:
            // If we are below the threshold service level we should reconnect.
            // Unless this update comes from a reconnect, or we reconnected too recently.

            var oldServiceLevel = CurrentServiceLevel;
            CurrentServiceLevel = newLevel;

            bool shouldReconnect = false;
            if (newLevel < config.Redundancy.ServiceLevelThreshold)
            {
                // We're not reconnecting off the back of a connect attempt.
                shouldReconnect = true;
                if (fromConnectionChange)
                {
                    shouldReconnect = false;
                }

                // We don't want to reconnect too frequently
                if (lastLowSLConnectAttempt != null)
                {
                    var timeSinceLastAttempt = (DateTime.UtcNow - lastLowSLConnectAttempt.Value) + TimeSpan.FromSeconds(5);
                    if (config.Redundancy.ReconnectIntervalValue.Value != System.Threading.Timeout.InfiniteTimeSpan
                        && timeSinceLastAttempt < config.Redundancy.ReconnectIntervalValue.Value)
                    {
                        shouldReconnect = false;
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
                // We have moved from being at low SL to high, so we should restart history before we set the service level.
                log.LogInformation("New service level {Level} is a above threshold {Threshold}, triggering callback", newLevel, config.Redundancy.ServiceLevelThreshold);
                await client.Callbacks.OnServiceLevelAboveThreshold(client);
            }

            if (newLevel != oldServiceLevel)
            {
                log.LogDebug("Server ServiceLevel updated {From} -> {To}", oldServiceLevel, newLevel);
            }

            if (shouldReconnect && config.IsRedundancyEnabled)
            {
                log.LogWarning("ServiceLevel is low ({Level}), attempting background reconnect", CurrentServiceLevel);
                lastLowSLConnectAttempt = DateTime.UtcNow;

                client.Callbacks.TaskScheduler.ScheduleTask(null, async (token) =>
                {
                    await TryWithBackoff(async () =>
                    {
                        var current = session != null ? new SessionRedundancyResult(session, EndpointUrl!, CurrentServiceLevel) : null;
                        var result = await CreateSessionWithRedundancy(config.AltEndpointUrls!.Prepend(config.EndpointUrl!), current);
                        if (result.EndpointUrl == EndpointUrl)
                        {
                            log.LogWarning("Attempted reconnect due to low service level resulted in same server {Url}. Proceeding with ServiceLevel {Level}", result.EndpointUrl, result.ServiceLevel);
                            await UpdateServiceLevel(result.ServiceLevel, false);
                            if (result.Session != session)
                            {
                                log.LogError("Error! Returned endpoint was the same, but session had changed. This is a bug!");
                            }
                        }
                        else
                        {
                            log.LogInformation("Reconnect due to low ServiceLevel resulted in new server {Url} (Old {Old}) with ServiceLevel {Level}",
                                result.EndpointUrl,
                                EndpointUrl,
                                result.ServiceLevel);
                            EndpointUrl = result.EndpointUrl;
                            SetNewSession(result.Session);
                            await UpdateServiceLevel(result.ServiceLevel, true);
                            await client.Callbacks.OnServerReconnect(client);
                        }
                    }, 6, token);
                });
            }
        }

        public async Task RecheckServiceLevel()
        {
            await UpdateServiceLevel(CurrentServiceLevel, false);
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
                client.Callbacks.TaskScheduler.ScheduleTask(null, async (_) => await UpdateServiceLevel(value.Value, false));
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Unexpected error handling ServiceLevel trigger");
            }
        }

        public void EnsureServiceLevelSubscription()
        {
            client.SubscriptionManager!.EnqueueTask(new ServiceLevelSubscriptionTask(OnServiceLevelUpdate, client.Callbacks));
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    reverseConnectManager?.Dispose();
                    reverseConnectManager = null;
                    if (session != null)
                    {
                        try
                        {
                            session.Close();
                        }
                        catch { }
                        session.KeepAlive -= ClientKeepAlive;
                        session.PublishError -= OnPublishError;
                    }
                    session?.Dispose();
                    session = null;
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

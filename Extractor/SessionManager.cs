using Microsoft.Identity.Client;
using Opc.Ua.Client;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Threading;
using Prometheus;
using Metrics = Prometheus.Metrics;
using System.Linq;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using System.Net;

namespace Cognite.OpcUa
{
    public class SessionManager : IDisposable
    {
        private SourceConfig config;
        private UAClient client;
        private ReverseConnectManager? reverseConnectManager;
        private SessionReconnectHandler? reconnectHandler;
        private ApplicationConfiguration appConfig;
        private ILogger log;

        public byte CurrentServiceLevel { get; private set; } = 255;
        private DateTime? lastLowSLConnectAttempt = null;
        private SemaphoreSlim redundancyReconnectLock = new SemaphoreSlim(1);

        private ISession? session;
        private CancellationToken liveToken;
        private bool disposedValue;
        public int Timeout { get; set; }

        private static readonly Counter connects = Metrics
            .CreateCounter("opcua_connects", "Number of times the client has connected to and mapped the opcua server");
        private static readonly Gauge connected = Metrics
            .CreateGauge("opcua_connected", "Whether or not the client is currently connected to the opcua server");
        public ISession? Session => session;

        public string? EndpointUrl { get; private set; }

        public SessionManager(SourceConfig config, UAClient parent, ApplicationConfiguration appConfig, ILogger log, CancellationToken token, int timeout = -1)
        {
            client = parent;
            this.config = config;
            this.appConfig = appConfig;
            this.log = log;
            liveToken = token;
            Timeout = timeout;
            EndpointUrl = config.EndpointUrl;
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

                    if (Timeout >= 0 && (DateTime.UtcNow - start).TotalSeconds > Timeout)
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
            session.KeepAlive += ClientKeepAlive;
            session.PublishError += OnPublishError;
            this.session = session;
        }

        public async Task Connect()
        {
            if (session != null)
            {
                try
                {
                    session.Close();
                }
                catch { }
                session.KeepAlive -= ClientKeepAlive;
                session.PublishError -= OnPublishError;
                session.Dispose();
                session = null;
            }
            if (reconnectHandler != null) reconnectHandler.Dispose();
            session = null;

            Func<Task> generator = async () =>
            {
                ISession newSession;
                if (!string.IsNullOrEmpty(config.ReverseConnectUrl))
                {
                    newSession = await WaitForReverseConnect();
                }
                else if (config.IsRedundancyEnabled)
                {
                    var result = await CreateSessionWithRedundancy(config.EndpointUrl!, config.AltEndpointUrls!, null, null, liveToken);
                    newSession = result.Session;
                    EndpointUrl = result.EndpointUrl;
                    await UpdateServiceLevel(result.ServiceLevel, true);
                }
                else
                {
                    newSession = await CreateSessionDirect(config.EndpointUrl!);
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
                await EnsureServiceLevelSubscription();
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

        private async Task<Session> WaitForReverseConnect()
        {
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
                EndpointUrl = config.EndpointUrl;
                return session;
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }

        private async Task<Session> CreateSessionDirect(string endpointUrl)
        {
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
                EndpointUrl = endpointUrl;
                return session;
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }

        private class SessionRedundancyResult
        {
            public ISession Session { get; }
            public string EndpointUrl { get; }
            public byte ServiceLevel { get; }
            public SessionRedundancyResult(ISession session, string endpointUrl, byte serviceLevel)
            {
                Session = session;
                EndpointUrl = endpointUrl;
                ServiceLevel = serviceLevel;
            }
        }

        private async Task<SessionRedundancyResult> CreateSessionWithRedundancy(string initialUrl, IEnumerable<string> endpointUrls, ISession? initialSession, byte? initialServiceLevel, CancellationToken token)
        {
            string? bestUrl = null;
            byte bestServiceLevel = 0;

            endpointUrls = endpointUrls.Prepend(initialUrl).Distinct().ToList();
            log.LogInformation("Create session with redundant connections to {Urls}", string.Join(", ", endpointUrls));

            ISession? activeSession = initialSession;

            if (activeSession != null)
            {
                if (initialServiceLevel == null) throw new InvalidOperationException("InitialServiceLevel required when initialSession is non-null");
                if (EndpointUrl == null) throw new InvalidOperationException("EndpointUrl must be set if initialSession is passed");
                bestUrl = EndpointUrl;
                bestServiceLevel = initialServiceLevel.Value;
            }

            string? activeEndpoint = null;
            var exceptions = new List<Exception>();

            foreach (var url in endpointUrls)
            {
                try
                {
                    ISession session;
                    if (url == bestUrl && activeSession != null)
                    {
                        session = activeSession;
                    }
                    else
                    {
                        session = await CreateSessionDirect(url);
                    }

                    var serviceLevel = await ReadServiceLevel(session);

                    if (serviceLevel > bestServiceLevel)
                    {
                        if (activeSession != null && url != activeEndpoint)
                        {
                            activeSession.Close();
                            activeSession.Dispose();
                        }
                        activeSession = session;
                        bestServiceLevel = serviceLevel;
                        bestUrl = url;
                    }

                    log.LogInformation("Connected to session with endpoint {Endpoint}. ServiceLevel: {Level}", url, serviceLevel);
                }
                catch (Exception ex)
                {
                    log.LogError("Failed to connect to endpoint {Url}: {Error}", url, ex.Message);
                    exceptions.Add(ex);
                }
            }

            if (activeSession == null)
            {
                throw new AggregateException("Failed to connect to any configured endpoint", exceptions);
            }

            liveToken.ThrowIfCancellationRequested();
            log.LogInformation("Successfully connected to server with endpoint: {Endpoint}, ServiceLevel: {Level}", bestUrl, bestServiceLevel);

            return new SessionRedundancyResult(activeSession, bestUrl!, bestServiceLevel);
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
        /// Event triggered after a succesfull reconnect.
        /// </summary>
        private void ClientReconnectComplete(object sender, EventArgs eventArgs)
        {
            if (!ReferenceEquals(sender, reconnectHandler)) return;
            if (reconnectHandler == null) return;
            session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            log.LogWarning("--- RECONNECTED ---");

            client.Callbacks.TaskScheduler.ScheduleTask(null, async (_) =>
            {
                await client.Callbacks.OnServerReconnect(client);
            });

            connects.Inc();
            connected.Set(1);
            reconnectHandler = null;
        }

        /// <summary>
        /// Called on client keep alive, handles the case where the server has stopped responding and the connection timed out.
        /// </summary>
        private void ClientKeepAlive(ISession sender, KeepAliveEventArgs eventArgs)
        {
            client.LogDump("Keep Alive", eventArgs);
            if (eventArgs.Status == null || !ServiceResult.IsNotGood(eventArgs.Status)) return;
            log.LogWarning("Keep alive failed: {Status}", eventArgs.Status);
            if (reconnectHandler != null || liveToken.IsCancellationRequested) return;
            connected.Set(0);

#pragma warning disable CA1508 // Avoid dead conditional code
            if (reconnectHandler != null) return;
#pragma warning restore CA1508 // Avoid dead conditional code
            log.LogWarning("--- RECONNECTING ---");
            if (!config.ForceRestart && !liveToken.IsCancellationRequested)
            {
                reconnectHandler = new SessionReconnectHandler();
                if (reverseConnectManager != null)
                {
                    reconnectHandler.BeginReconnect(sender, reverseConnectManager, 5000, ClientReconnectComplete);
                }
                else
                {
                    reconnectHandler.BeginReconnect(sender, 5000, ClientReconnectComplete);
                }
            }
            else
            {
                try
                {
                    session?.Close();
                }
                catch
                {
                    log.LogWarning("Client failed to close");
                }
                finally
                {
                    if (session != null)
                    {
                        session.KeepAlive -= ClientKeepAlive;
                        session.PublishError -= OnPublishError;
                        session = null;
                    }
                }
                if (!liveToken.IsCancellationRequested)
                {
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
            }
            client.Callbacks.TaskScheduler.ScheduleTask(null, async (_) => await client.Callbacks.OnServerDisconnect(client));
        }

        public async Task Close(CancellationToken token)
        {
            reconnectHandler?.Dispose();
            reconnectHandler = null;
            try
            {
                if (session != null && !session.Disposed)
                {
                    var closeTask = session.CloseSessionAsync(null, true, token);
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
                    var timeSinceLastAttempt = DateTime.UtcNow - lastLowSLConnectAttempt.Value;
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
            else if (oldServiceLevel < config.Redundancy.ServiceLevelThreshold && !fromConnectionChange)
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
                    var result = await CreateSessionWithRedundancy(config.EndpointUrl!, config.AltEndpointUrls!, session, CurrentServiceLevel, token);
                    if (result.EndpointUrl == EndpointUrl)
                    {
                        log.LogWarning("Attempted reconnect due to low service level resulted in same server {Url}. Proceeding with ServiceLevel {Level}", result.EndpointUrl, result.ServiceLevel);
                        await UpdateServiceLevel(result.ServiceLevel, false);
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
                });
            }
        }

        public async Task RecheckServiceLevel()
        {
            await UpdateServiceLevel(CurrentServiceLevel, false);
        }

        public async Task EnsureServiceLevelSubscription()
        {
            var state = new UAHistoryExtractionState(client, VariableIds.Server_ServiceLevel, false, false);
            await client.AddSubscriptions(new[] { state }, "ServiceLevel", (MonitoredItem item, MonitoredItemNotificationEventArgs _) =>
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
            }, state => new MonitoredItem
            {
                StartNodeId = state.SourceId,
                SamplingInterval = 1000,
                DisplayName = "Value " + state.Id,
                QueueSize = 1,
                DiscardOldest = true,
                AttributeId = Attributes.Value,
                NodeClass = NodeClass.Variable,
                CacheQueueSize = 1
            }, liveToken, "service level");
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    reverseConnectManager?.Dispose();
                    reverseConnectManager = null;
                    reconnectHandler?.Dispose();
                    reconnectHandler = null;
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

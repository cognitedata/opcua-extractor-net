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

namespace Cognite.OpcUa
{
    public class SessionManager : IDisposable
    {
        private UAClientConfig config;
        private UAClient client;
        private ReverseConnectManager? reverseConnectManager;
        private SessionReconnectHandler? reconnectHandler;
        private ApplicationConfiguration appConfig;
        private ILogger log;

        private Session? session;
        private CancellationToken liveToken;
        private bool disposedValue;
        public int Timeout { get; set; }

        private static readonly Counter connects = Metrics
            .CreateCounter("opcua_connects", "Number of times the client has connected to and mapped the opcua server");
        private static readonly Gauge connected = Metrics
            .CreateGauge("opcua_connected", "Whether or not the client is currently connected to the opcua server");
        public Session? Session => session;

        public string? EndpointUrl { get; private set; }

        public SessionManager(UAClientConfig config, UAClient parent, ApplicationConfiguration appConfig, ILogger log, CancellationToken token, int timeout = -1)
        {
            client = parent;
            this.config = config;
            this.appConfig = appConfig;
            this.log = log;
            liveToken = token;
            this.Timeout = timeout;
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

        public async Task Connect()
        {
            if (session != null)
            {
                try
                {
                    session.Close();
                } catch { }
                session.KeepAlive -= ClientKeepAlive;
                session.PublishError -= OnPublishError;
                session.Dispose();
                session = null;
            }
            if (reconnectHandler != null) reconnectHandler.Dispose();
            session = null;

            Func<Task> generator = async () =>
            {
                Session newSession;
                if (!string.IsNullOrEmpty(config.ReverseConnectUrl))
                {
                    newSession = await WaitForReverseConnect();
                }
                else if (config.AltEndpointUrls != null && config.AltEndpointUrls.Any())
                {
                    newSession = await CreateSessionWithRedundancy(config.EndpointUrl!, config.AltEndpointUrls);
                }
                else
                {
                    newSession = await CreateSessionDirect(config.EndpointUrl!);
                }
                newSession.KeepAliveInterval = config.KeepAliveInterval;
                newSession.KeepAlive += ClientKeepAlive;
                newSession.PublishError += OnPublishError;
                session = newSession;
            };
            await TryWithBackoff(generator, 6, liveToken);
            if (!liveToken.IsCancellationRequested)
            {
                log.LogInformation("Successfully connected to server");
                connects.Inc();
                connected.Set(1);
            }
        }

        /// <summary>
        /// Event triggered when a publish request fails.
        /// </summary>
        private void OnPublishError(Session session, PublishErrorEventArgs e)
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


                var session = await Session.Create(
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
            log.LogInformation("Attempt to select endpoint from: {EndpointURL}", endpointUrl);
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(endpointUrl, config.Secure);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.SelectEndpoint);
            }
            var endpointConfiguration = EndpointConfiguration.Create(appConfig);
            client.LogDump("Endpoint configuration", endpointConfiguration);

            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            client.LogDump("Endpoint", endpoint);

            var identity = AuthenticationUtils.GetUserIdentity(config);
            log.LogInformation("Attempt to connect to endpoint with security: {SecurityPolicyUri} using user identity {Identity}",
                endpoint.Description.SecurityPolicyUri,
                identity.DisplayName);
            try
            {
                var session = await Session.Create(
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

        public async Task<Session> CreateSessionWithRedundancy(string initialUrl, IEnumerable<string> endpointUrls)
        {
            string? bestUrl = null;
            byte bestServiceLevel = 0;

            endpointUrls = endpointUrls.Prepend(initialUrl).Distinct().ToList();

            Session? activeSession = null;
            var exceptions = new List<Exception>();

            foreach (var url in endpointUrls)
            {
                try
                {
                    var session = await CreateSessionDirect(url);
                    var res = await session.ReadAsync(null, 0, TimestampsToReturn.Neither, new ReadValueIdCollection
                    {
                        new ReadValueId
                        {
                            NodeId = VariableIds.Server_ServiceLevel,
                            AttributeId = Attributes.Value
                        }
                    }, liveToken);                    
                    var dv = res.Results[0];
                    byte value = dv.GetValue<byte>(0);

                    if (value > bestServiceLevel)
                    {
                        if (activeSession != null)
                        {
                            activeSession.Close();
                            activeSession.Dispose();
                        }
                        activeSession = session;
                        bestServiceLevel = value;
                        bestUrl = url;
                    }

                    log.LogInformation("Connected to session with endpoint {Endpoint}. ServiceLevel: {Level}", url, value);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if (activeSession == null)
            {
                throw new AggregateException("Failed to connect to any configured endpoint", exceptions);
            }

            liveToken.ThrowIfCancellationRequested();
            EndpointUrl = bestUrl;
            log.LogInformation("Successfully connected to server with endpoint: {Endpoint}, ServiceLevel: {Level}", bestUrl, bestServiceLevel);
            return activeSession;
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

            client.TriggerOnServerReconnect();

            connects.Inc();
            connected.Set(1);
            reconnectHandler = null;
        }

        /// <summary>
        /// Called on client keep alive, handles the case where the server has stopped responding and the connection timed out.
        /// </summary>
        private void ClientKeepAlive(Session sender, KeepAliveEventArgs eventArgs)
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
                    var _ =Task.Run(async () => {
                        log.LogInformation("Attempting to reconnect to server");
                        await Connect();
                        if (!liveToken.IsCancellationRequested)
                        {
                            client.TriggerOnServerReconnect();
                            connects.Inc();
                            connected.Set(1);
                        }
                    }, liveToken);
                }
            }
            client.TriggerOnServerDisconnect();
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
                    session.KeepAlive -= ClientKeepAlive;
                    session.PublishError -= OnPublishError;
                    session.Dispose();
                    session = null;
                }
            }
            finally
            {
                connected.Set(0);
            }
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

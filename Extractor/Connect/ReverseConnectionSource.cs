using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Utils;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;

namespace Cognite.OpcUa.Connect
{
    public class ReverseConnectionSource : IConnectionSource
    {
        private readonly string endpointUrl;
        private ReverseConnectManager? connectManager;
        private bool disposedValue;
        private readonly SourceConfig config;
        private readonly ILogger log;
        private readonly SessionManager sessionManager;

        public ReverseConnectionSource(string endpointUrl, SourceConfig config, ILogger log, SessionManager sessionManager)
        {
            this.endpointUrl = endpointUrl;
            this.config = config;
            this.log = log;
            this.sessionManager = sessionManager;
        }

        public async Task<ConnectResult> Connect(Connection? oldConnection, bool isConnected, ApplicationConfiguration appConfig, CancellationToken token)
        {
            if (oldConnection != null && isConnected) return new ConnectResult(oldConnection, ConnectType.None);

            if (oldConnection != null && connectManager != null)
            {
                log.LogInformation("Attempting to reconnect to the server, before creating a new connection");
                try
                {
                    var reconn = await connectManager.WaitForConnection(
                        new Uri(oldConnection.Session.Endpoint.EndpointUrl),
                        oldConnection.Session.Endpoint.Server.ApplicationUri,
                        token
                    );
                    ConnectionUtils.Connects.Inc();

                    if (reconn == null)
                    {
                        log.LogError("Reverse connect failed, no connection established");
                        throw new ExtractorFailureException("Failed to obtain reverse connection from server");
                    }
                    await oldConnection.Session.ReconnectAsync(reconn, token);
                    return new ConnectResult(oldConnection, ConnectType.Reconnect);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to reconnect to server at {Url}: {Message}", oldConnection.Session.Endpoint.EndpointUrl, ex.Message);
                    if (ConnectionUtils.ShouldAbandonReconnect(ex) || config.ForceRestart)
                    {
                        await sessionManager.CloseSession(oldConnection.Session, token);
                    }
                    else throw;
                }
            }
            connectManager?.Dispose();

            appConfig.ClientConfiguration.ReverseConnect = new ReverseConnectClientConfiguration
            {
                WaitTimeout = 300000,
                HoldTime = 30000
            };

            connectManager = new ReverseConnectManager();
            var url = new Uri(endpointUrl);
            var reverseUrl = new Uri(config.ReverseConnectUrl);
            connectManager.AddEndpoint(reverseUrl);
            connectManager.StartService(appConfig);

            log.LogInformation("Waiting for reverse connection from: {EndpointURL}", config.EndpointUrl);
            var connection = await connectManager.WaitForConnection(url, null, token);
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
            sessionManager.Client.LogDump("Reverse connect endpoint configuration", endpointConfiguration);

            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            sessionManager.Client.LogDump("Reverse connect endpoint", endpoint);

            var identity = AuthenticationUtils.GetUserIdentity(config);
            log.LogInformation("Attempt to connect to endpoint with security: {SecurityPolicyUri} using user identity {Identity}",
                endpoint.Description.SecurityPolicyUri,
                identity.DisplayName);

            try
            {
                connection = await connectManager.WaitForConnection(url, null);
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
                ConnectionUtils.Connects.Inc();
                session.DeleteSubscriptionsOnClose = true;
                return new ConnectResult(new Connection(session, endpointUrl), ConnectType.NewSession);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    connectManager?.Dispose();
                    connectManager = null;
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
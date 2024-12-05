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
    public class DirectConnectionSource : IConnectionSource
    {
        private readonly string endpointUrl;
        public string EndpointUrl => endpointUrl;
        private readonly SourceConfig config;
        private readonly ILogger log;
        private readonly SessionManager sessionManager;

        public DirectConnectionSource(
            string endpointUrl,
            SourceConfig config,
            ILogger log,
            SessionManager sessionManager)
        {
            this.endpointUrl = endpointUrl;
            this.config = config;
            this.log = log;
            this.sessionManager = sessionManager;
        }

        private async Task<bool> ReconnectDirect(Connection oldConnection, CancellationToken token)
        {
            try
            {
                await oldConnection.Session.ReconnectAsync(token);
                ConnectionUtils.Connects.Inc();
                log.LogInformation("Successfully reconnected to the server");
                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to reconnect to server at {Url}: {Message}", oldConnection.EndpointUrl, ex.Message);
                if (ConnectionUtils.ShouldAbandonReconnect(ex) || config.ForceRestart)
                {
                    await sessionManager.CloseSession(oldConnection.Session, token);
                }
                else throw;
            }

            return false;
        }


        public async Task<ConnectResult> Connect(
            Connection? oldConnection,
            bool isConnected,
            ApplicationConfiguration appConfig,
            CancellationToken token)
        {
            if (oldConnection != null)
            {
                if (isConnected) return new ConnectResult(oldConnection, ConnectType.None);

                var res = await ReconnectDirect(oldConnection, token);
                if (res) return new ConnectResult(oldConnection, ConnectType.Reconnect);
            }

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
            sessionManager.Client.LogDump("Endpoint configuration", endpointConfiguration);

            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            sessionManager.Client.LogDump("Endpoint", endpoint);
            log.LogInformation("Attempt to connect to endpoint at {Endpoint} with security: {SecurityPolicyUri} using user identity {Identity}",
                endpoint.Description.EndpointUrl,
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
                ConnectionUtils.Connects.Inc();
                session.DeleteSubscriptionsOnClose = true;
                return new ConnectResult(new Connection(session, endpointUrl), ConnectType.NewSession);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }

        public void Dispose()
        {
        }
    }
}

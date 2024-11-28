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
    public class RedundantConnectionSource : IConnectionSource
    {
        private readonly SourceConfig config;
        private readonly ILogger log;
        private readonly SessionManager2 sessionManager;

        private readonly Dictionary<string, DirectConnectionSource> sources;

        public RedundantConnectionSource(
            SourceConfig config,
            ILogger log,
            SessionManager2 sessionManager)
        {
            var endpointUrls = config.AltEndpointUrls!.Prepend(config.EndpointUrl!).Distinct().ToList();
            sources = endpointUrls.Select(v => new DirectConnectionSource(v, config, log, sessionManager)).ToDictionary(v => v.EndpointUrl);
            this.config = config;
            this.log = log;
            this.sessionManager = sessionManager;
        }

        private async Task<(byte, ConnectResult)> TrySession(
            Connection? oldConnection,
            bool isConnected,
            ApplicationConfiguration appConfig,
            DirectConnectionSource source,
            CancellationToken token)
        {
            var res = await source.Connect(oldConnection, isConnected, appConfig, token);
            byte sl;
            try
            {
                sl = await SessionManager2.ReadServiceLevel(res.Connection.Session, token);
            }
            catch
            {
                await sessionManager.CloseSession(res.Connection.Session, token);
                throw;
            }
            return (sl, res);
        }

        public async Task<ConnectResult> Connect(Connection? oldConnection, bool isConnected, ApplicationConfiguration appConfig, CancellationToken token)
        {
            log.LogInformation("Create session with redundant connections to {Urls}", string.Join(", ", sources.Keys));

            IEnumerable<string> endpointUrlsOrdered = sources.Keys;

            byte bestServiceLevel = 0;
            Connection? currentConnection = oldConnection;
            var exceptions = new List<Exception>();
            if (oldConnection != null)
            {
                endpointUrlsOrdered = endpointUrlsOrdered
                    .Except(new[] { oldConnection.EndpointUrl });

                log.LogInformation("Attempting to reconnect to the current server before switching to another");

                try
                {
                    var (sl, res) = await TrySession(oldConnection, isConnected, appConfig, sources[oldConnection.EndpointUrl], token);
                    bestServiceLevel = sl;
                    if (sl >= config.Redundancy.ServiceLevelThreshold)
                    {
                        log.LogInformation("Service level on current server is above threshold ({Val}), not switching", sl);
                        return res;
                    }
                }
                catch (Exception ex)
                {
                    var hEx = ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
                    log.LogWarning("Failed to reconnect to current session: {Message}", hEx.Message);
                    await sessionManager.CloseSession(oldConnection.Session, token);
                    currentConnection = null;
                }
            }



            foreach (var url in endpointUrlsOrdered)
            {
                try
                {
                    var (sl, res) = await TrySession(null, false, appConfig, sources[url], token);
                    if (sl > bestServiceLevel)
                    {
                        if (currentConnection != null)
                        {
                            await sessionManager.CloseSession(currentConnection.Session, token);
                        }
                        bestServiceLevel = sl;
                        currentConnection = res.Connection;
                    }
                }
                catch (Exception ex)
                {
                    var hEx = ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
                    log.LogError("Failed to connect to endpoint {Url}: {Error}", url, hEx.Message);
                    exceptions.Add(hEx);
                }
            }

            if (currentConnection == null)
            {
                throw new AggregateException("Failed to connect to any configured endpoint", exceptions);
            }

            token.ThrowIfCancellationRequested();
            log.LogInformation("Successfully connected to server with endpoint: {Endpoint}, ServiceLevel: {Level}", currentConnection.EndpointUrl, bestServiceLevel);

            return new ConnectResult(currentConnection, ConnectType.NewSession);
        }
    }
}
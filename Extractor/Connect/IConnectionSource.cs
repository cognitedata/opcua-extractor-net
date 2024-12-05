using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;

namespace Cognite.OpcUa.Connect
{
    public enum ConnectType
    {
        None,
        Reconnect,
        NewSession,
    }

    public class ConnectResult
    {
        public ConnectType Type { get; }
        public Connection Connection { get; }

        public ConnectResult(Connection connection, ConnectType type)
        {
            Type = type;
            Connection = connection;
        }
    }

    public class Connection
    {
        public ISession Session { get; }
        public string EndpointUrl { get; }
        public Connection(ISession session, string endpointUrl)
        {
            Session = session;
            EndpointUrl = endpointUrl;
        }
    }

    public interface IConnectionSource : IDisposable
    {
        public Task<ConnectResult> Connect(
            Connection? oldConnection,
            bool isConnected,
            ApplicationConfiguration appConfig,
            CancellationToken token
        );

        public static IConnectionSource FromConfig(SessionManager manager, SourceConfig config, ILogger log)
        {
            if (!string.IsNullOrEmpty(config.ReverseConnectUrl))
            {
                return new ReverseConnectionSource(config.EndpointUrl!, config, log, manager);
            }
            else if (config.IsRedundancyEnabled)
            {
                return new RedundantConnectionSource(config, log, manager);
            }
            else
            {
                return new DirectConnectionSource(config.EndpointUrl!, config, log, manager);
            }
        }
    }
}

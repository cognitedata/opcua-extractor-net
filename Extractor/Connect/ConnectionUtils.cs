using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Prometheus;

namespace Cognite.OpcUa.Connect
{
    public static class ConnectionUtils
    {
        public static Counter Connects { get; } = Metrics
            .CreateCounter("opcua_connects", "Number of times the client has connected to and mapped the opcua server");
        public static Gauge Connected { get; } = Metrics
            .CreateGauge("opcua_connected", "Whether or not the client is currently connected to the opcua server");
        public static Gauge ServiceLevel { get; } = Metrics
            .CreateGauge("opcua_service_level", "Value of the ServiceLevel on the server");

        private static readonly uint[] statusCodesToAbandon = new[] {
            StatusCodes.BadSessionIdInvalid,
            StatusCodes.BadSessionNotActivated,
            StatusCodes.BadSessionClosed,
            StatusCodes.BadServiceUnsupported,
            StatusCodes.BadInternalError,
        };

        public static bool ShouldReconnect(Exception ex)
        {
            if (ex is AggregateException aex)
            {
                return ShouldReconnect(aex.InnerException);
            }
            if (ex is ServiceResultException e)
            {
                return !statusCodesToAbandon.Contains(e.StatusCode);
            }
            return false;
        }

        public static async Task<T> TryWithBackoff<T>(Func<Task<T>> method, int maxBackoff, int timeoutSeconds, ILogger log, CancellationToken token)
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

                    if (timeoutSeconds >= 0 && (DateTime.UtcNow - start).TotalSeconds > timeoutSeconds)
                    {
                        throw;
                    }
                    if (!token.IsCancellationRequested)
                        log.LogWarning("Failed to connect, retrying in {Backoff}", backoff);
                    try { await Task.Delay(backoff, token); } catch (TaskCanceledException) { }
                }
            }
        }
    }
}

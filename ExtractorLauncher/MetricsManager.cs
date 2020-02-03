using System;
using System.Collections.Generic;
using System.Linq;
using Prometheus.Client;
using Prometheus.Client.MetricPusher;
using Prometheus.Client.MetricServer;
using Serilog;

namespace Cognite.OpcUa
{
    public static class MetricsManager
    {
        private static ILogger _logger;
        private static bool _isFirstRun = true;

        public static void Setup(MetricsConfig config)
        {
            _logger = Log.Logger.ForContext(typeof(MetricsManager));

            // Added this to be able to restart the extractor from the windows service, as setting this twice generates an exception
            if (_isFirstRun)
            {
                Prometheus.Client.Collectors.DefaultCollectors.UseDefaultCollectors(Metrics.DefaultCollectorRegistry);
            }

            SetupMetricsServer(config?.Server);

            IEnumerable<IMetricPusher> pushers = config?.PushGateways?.Select(SetupMetricsPush).Where(p => p != null);
            if (pushers != null)
            {
                var metricsWorker = new MetricPushServer(pushers.ToArray(), TimeSpan.FromSeconds(10));
                metricsWorker.Start();
            }

            _isFirstRun = false;
        }

        /// <summary>
        /// Start a local metrics server.
        /// </summary>
        /// <param name="config"></param>
        private static void SetupMetricsServer(MetricsServerConfig config)
        {
            if (config?.Host == null || config.Port == 0)
            {
                _logger.Information("Metrics server disabled");
                return;
            }
            var metricServer = new MetricServer(
                Metrics.DefaultCollectorRegistry,
                new MetricServerOptions { Host = config.Host, Port = config.Port });
            metricServer.Start();
            _logger.Information("Metrics server started at {MetricsServerHost}:{MetricsServerPort}", config.Host, config.Port);
        }

        /// <summary>
        /// Configure metrics to use pushgateway in order to push metrics to a prometheus server.
        /// </summary>
        /// <param name="config"></param>
        private static MetricPusher SetupMetricsPush(PushGatewayConfig config)
        {
            if (config.Host == null || config.Job == null)
            {
                _logger.Warning("Invalid metrics push destination (missing Host or Job)");
                return null;
            }

            _logger.Information("Pushing metrics to {PushgatewayHost} with job name {PushgatewayJob}", config.Host, config.Job);

            var additionalHeaders = new Dictionary<string, string>();
            if (config.Username != null && config.Password != null)
            {
                // Create the basic auth header
                string encoded = Convert.ToBase64String(
                    System.Text.Encoding
                        .GetEncoding("ISO-8859-1")
                        .GetBytes(config.Username + ":" + config.Password));
                additionalHeaders.Add("Authorization", "Basic " + encoded);
            }

            var pusher = new MetricPusher(config.Host, config.Job, additionalHeaders);
            try
            {
                pusher.PushAsync().GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                _logger.Warning("Failed to push metrics to {PushgatewayHost} with job name {PushgatewayJob}: {Message}", config.Host, config.Job, e.Message);
            }
            return pusher;
        }
    }
}

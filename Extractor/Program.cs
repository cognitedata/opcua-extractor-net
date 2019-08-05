using System;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using System.Net.Http;
using System.Collections.Generic;
using Polly;
using Prometheus.Client.MetricPusher;
using Opc.Ua;
using Fusion.Api;
using Polly.Extensions.Http;

namespace Cognite.OpcUa
{
    class Program
    {
        static MetricPushServer worker;
        /// <summary>
        /// Load config, start the <see cref="Logger"/>, start the <see cref="Extractor"/> then wait for exit signal
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        static int Main(string[] args)
        {
            var configDir = Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR");
            configDir = string.IsNullOrEmpty(configDir) ? "config/" : configDir;
            FullConfig fullConfig = Utils.GetConfig($"{configDir}/config.yml");
            fullConfig.UAConfig.ConfigRoot = configDir;
            if (fullConfig == null) return -1;
            try
            {
                ValidateConfig(fullConfig);
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to load config");
                Console.WriteLine(e.Message);
                return -1;
            }

            Logger.Startup(fullConfig.LoggerConfig);

            var services = new ServiceCollection();
            Configure(services);
            var provider = services.BuildServiceProvider();

            CDFPusher pusher = new CDFPusher(provider, fullConfig);
            UAClient client = new UAClient(fullConfig);
            Extractor extractor = new Extractor(fullConfig, pusher, client);
            try
            {
                SetupMetrics(fullConfig.MetricsConfig);
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to start metrics pusher");
                Logger.LogException(e);
            }

            Run(extractor);
            Logger.LogInfo("Interrupted, shutting down extractor...");
            extractor.Close();
            worker?.Stop();
            Logger.Shutdown();

			return 0;
        }
        /// <summary>
        /// Tests that the config is correct and valid
        /// </summary>
        /// <param name="config">The config object</param>
        /// <exception cref="Exception">On invalid config</exception>
        private static void ValidateConfig(FullConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.UAConfig.EndpointURL)) throw new Exception("Invalid EndpointURL");
            if (string.IsNullOrWhiteSpace(config.UAConfig.GlobalPrefix)) throw new Exception("Invalid GlobalPrefix");
            if (config.UAConfig.PollingInterval < 0) throw new Exception("PollingInterval must be a positive number");
            if (string.IsNullOrWhiteSpace(config.CogniteConfig.Project)) throw new Exception("Invalid Project");
            if (string.IsNullOrWhiteSpace(config.CogniteConfig.ApiKey)) throw new Exception("Invalid api-key");
        }
        private static void Configure(IServiceCollection services)
        {
            services.AddHttpClient<Client>()
                .AddPolicyHandler(GetRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
        }

        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .WaitAndRetryAsync(6, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
        }
        private static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy()
        {
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(120));
        }
        /// <summary>
        /// Starts prometheus pushgateway client
        /// </summary>
        /// <param name="config">The metrics config object</param>
        private static void SetupMetrics(MetricsConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.URL) || string.IsNullOrWhiteSpace(config.Job))
            {
                Logger.LogWarning("Unable to start metrics, missing URL or Job");
                return;
            }
            var additionalHeaders = new Dictionary<string, string>();
            if (!string.IsNullOrWhiteSpace(config.Username) && !string.IsNullOrWhiteSpace(config.Password))
            {
                string encoded = Convert.ToBase64String(
                    System.Text.Encoding
                        .GetEncoding("ISO-8859-1")
                        .GetBytes($"{config.Username}:{config.Password}")
                );
                additionalHeaders.Add("Authorization", $"Basic {encoded}");
            }
            var pusher = new MetricPusher(config.URL, config.Job, config.Instance, additionalHeaders);
            worker = new MetricPushServer(pusher, TimeSpan.FromMilliseconds(config.PushInterval));
            worker.Start();
        }
        private static void Run(Extractor extractor)
        {
            using (var quitEvent = new ManualResetEvent(false))
            {
                Console.CancelKeyPress += (sender, eArgs) =>
                {
                    quitEvent.Set();
                    eArgs.Cancel = true;
                };
                Console.WriteLine("Press ^C to exit");
                while (true)
                {
                    bool failed = false;
                    if (extractor.Start())
                    {
                        try
                        {
                            extractor.MapUAToCDF().Wait();
                        }
                        catch (Exception e)
                        {
                            Logger.LogError("Failed to map directory");
                            Logger.LogException(e);
                            failed = true;
                        }

                        if (quitEvent.WaitOne(failed ? 4000 : -1)) return;
                    }
                    else
                    {
                        if (quitEvent.WaitOne(4000)) return;
                    }
                }
            }
        }
    }
    public class UAClientConfig
    {
        public string ConfigRoot { get; set; } = "config";
        public string EndpointURL { get; set; }
        public bool Autoaccept { get; set; } = false;
        public int PollingInterval { get; set; } = 500;
        public string GlobalPrefix { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public bool Secure { get; set; }
        public string IgnorePrefix { get; set; }
        public int HistoryGranularity { get; set; }
    }
    public class CogniteClientConfig
    {
        public string Project { get; set; }
        public string ApiKey { get; set; }
        public long RootAssetId { get; set; }
        public ProtoNodeId RootNode { get; set; }
        public int DataPushDelay { get; set; }
        public bool Debug { get; set; }
        public bool BufferOnFailure { get; set; }
        public string BufferFile { get; set; }
    }
    public class FullConfig
    {
        public Dictionary<string, string> NSMaps { get; set; }
        public UAClientConfig UAConfig { get; set; }
        public CogniteClientConfig CogniteConfig { get; set; }
        public LoggerConfig LoggerConfig { get; set; }
        public MetricsConfig MetricsConfig { get; set; }
        public BulkSizes BulkSizes { get; set; }
    }
    public class LoggerConfig
    {
        public string LogFolder { get; set; }
        public bool LogData { get; set; }
        public bool LogNodes { get; set; }
        public bool LogConsole { get; set; }
    }
    public class MetricsConfig
    {
        public string URL { get; set; }
        public string Job { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public int PushInterval { get; set; }
        public string Instance { get; set; }
    }
    public class ProtoNodeId
    {
        public string NamespaceUri { get; set; }
        public string NodeId { get; set; }
        public NodeId ToNodeId(UAClient client)
        {
            return client.ToNodeId(NodeId, NamespaceUri);
        }
    }
    public class BulkSizes
    {
        public int CDFAssets { get; set; }
        public int CDFTimeseries { get; set; }
        public int UABrowse { get; set; }
        private int _uaHistoryReadPoints;
        public int UAHistoryReadPoints { get { return _uaHistoryReadPoints; } set { _uaHistoryReadPoints = Math.Max(0, value); } }
        private int _uaHistoryReadNodes;
        public int UAHistoryReadNodes { get { return _uaHistoryReadNodes; } set { _uaHistoryReadNodes = Math.Max(1, value); } }
        public int UAAttributes { get; set; }
    }
}

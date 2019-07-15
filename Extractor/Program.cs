using System;
using System.Threading;
using YamlDotNet.Serialization;
using System.IO;
using YamlDotNet.RepresentationModel;
using Microsoft.Extensions.DependencyInjection;
using System.Net.Http;
using System.Collections.Generic;
using Prometheus.Client.MetricPusher;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    class Program
    {
        /// <summary>
        /// Load config, start the <see cref="Logger"/>, start the <see cref="Extractor"/> then wait for exit signal
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        static int Main(string[] args)
        {
            FullConfig fullConfig = Utils.GetConfig(args.Length > 0 ? args[0] : "config.yml");
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

            CDFPusher pusher = new CDFPusher(provider.GetRequiredService<IHttpClientFactory>(), fullConfig.CogniteConfig);
            UAClient client = new UAClient(fullConfig);
            Extractor extractor = new Extractor(fullConfig, pusher, client);

            if (!extractor.Started)
            {
                Logger.Shutdown();
                return -1;
            }
            try
            {
                SetupMetrics(fullConfig.MetricsConfig);
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to start metrics pusher");
                Logger.LogException(e);
            }

            var quitEvent = new ManualResetEvent(false);
			Task runtask = Task.Run(() =>
            {
                try
                {
                    extractor.MapUAToCDF();
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to map directory");
                    Logger.LogException(e);
                    quitEvent.Set();
                }
            });
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                quitEvent.Set();
                eArgs.Cancel = true;
            };
            Console.WriteLine("Press ^C to exit");
            quitEvent.WaitOne(-1);
            Logger.LogInfo("Shutting down extractor...");
            extractor.Close();
            Logger.Shutdown();
            if (runtask != null && runtask.IsFaulted)
            {
                Logger.Shutdown();
                extractor.Close();
                return -1;
            }
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
        public static void Configure(IServiceCollection services)
        {
            services.AddHttpClient();
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
                        .GetBytes(config.Username + ":" + config.Password)
                );
                additionalHeaders.Add("Authorization", "Basic " + encoded);
            }
            var pusher = new MetricPusher(config.URL, config.Job, config.Instance, additionalHeaders);
            var worker = new MetricPushServer(pusher, TimeSpan.FromMilliseconds(config.PushInterval));
            worker.Start();
        }
    }
    public class UAClientConfig
    {
        public int ReconnectPeriod { get; set; } = 1000;
        public string EndpointURL { get; set; }
        public bool Autoaccept { get; set; } = false;
        public uint MaxResults { get; set; } = 100;
        public int PollingInterval { get; set; } = 500;
        public string GlobalPrefix { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public bool Secure { get; set; }
        public string IgnorePrefix { get; set; }

    }
    public class CogniteClientConfig
    {
        public string Project { get; set; }
        public string ApiKey { get; set; }
        public long RootAssetId { get; set; }
        public string RootNodeNamespace { get; set; }
        public string RootNodeId { get; set; }
        public int DataPushDelay { get; set; }
        public bool Debug { get; set; }
        public bool BufferOnFailure { get; set; }
        public string BufferFile { get; set; }
    }
    public class FullConfig
    {
        public YamlMappingNode NSMaps { get; set; }
        public UAClientConfig UAConfig { get; set; }
        public CogniteClientConfig CogniteConfig { get; set; }
        public LoggerConfig LoggerConfig { get; set; }
        public MetricsConfig MetricsConfig { get; set; }
    }
    public class LoggerConfig
    {
        public string LogFolder { get; set; }
        public bool LogData { get; set; }
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
}

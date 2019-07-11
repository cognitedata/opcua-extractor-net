using System;
using System.Threading;
using YamlDotNet.Serialization;
using System.IO;
using YamlDotNet.RepresentationModel;
using Microsoft.Extensions.DependencyInjection;
using System.Net.Http;
using System.Collections.Generic;
using Prometheus.Client.MetricPusher;

namespace Cognite.OpcUa
{
    class Program
    {
        static int Main()
        {
            FullConfig fullConfig = GetConfig();
            if (fullConfig == null) return -1;

            var services = new ServiceCollection();
            Configure(services);
            var provider = services.BuildServiceProvider();
            Extractor extractor = new Extractor(fullConfig, provider.GetRequiredService<IHttpClientFactory>());
			Logger.Startup(fullConfig.LoggerConfig);
            try
            {
                SetupMetrics(fullConfig.MetricsConfig);
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to start metrics pusher");
                Logger.LogException(e);
            }
            try
			{
				extractor.MapUAToCDF();
			}
            catch (Exception e)
			{
				Logger.LogError("Failed to map directory");
				Logger.LogException(e);
				return -1;
			}

            var quitEvent = new ManualResetEvent(false);
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                quitEvent.Set();
                eArgs.Cancel = true;
            };
            Console.WriteLine("Press ^C to exit");
            quitEvent.WaitOne(-1);
            Logger.LogInfo("Shutting down extractor");
            extractor.Close();
			return 0;
        }
        private static FullConfig GetConfig()
        {
            var config = ReadConfig();
            FullConfig fullConfig = null;
            try
            {
                var clientCfg = config.Children[new YamlScalarNode("client")];
                var nsmaps = (YamlMappingNode)config.Children[new YamlScalarNode("nsmaps")];
                var cogniteConfig = config.Children[new YamlScalarNode("cognite")];
                var loggerConfig = config.Children[new YamlScalarNode("logging")];
                var metricsConfig = config.Children[new YamlScalarNode("metrics")];
                fullConfig = new FullConfig
                {
                    NSMaps = nsmaps,
                    UAConfig = DeserializeNode<UAClientConfig>(clientCfg),
                    CogniteConfig = DeserializeNode<CogniteClientConfig>(cogniteConfig),
                    LoggerConfig = DeserializeNode<LoggerConfig>(loggerConfig),
                    MetricsConfig = DeserializeNode<MetricsConfig>(metricsConfig)
                };
                ValidateConfig(fullConfig);
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to load config");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return fullConfig;
        }
        private static YamlMappingNode ReadConfig()
        {
            string document = File.ReadAllText("config.yml");
            StringReader input = new StringReader(document);
            YamlStream stream = new YamlStream();
            stream.Load(input);

            return (YamlMappingNode)stream.Documents[0].RootNode;
        }

        private static T DeserializeNode<T>(YamlNode node)
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var reader = new StreamReader(stream))
            {
                new YamlStream(new YamlDocument[] { new YamlDocument(node) }).Save(writer);
                writer.Flush();
                stream.Position = 0;
                try
                {
                    return new Deserializer().Deserialize<T>(reader);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to load config: " + node);
                    throw e;
                }
            }
        }
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

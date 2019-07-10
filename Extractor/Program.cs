using System;
using System.Threading;
using YamlDotNet.Serialization;
using System.IO;
using YamlDotNet.RepresentationModel;
using Microsoft.Extensions.DependencyInjection;
using System.Net.Http;

namespace Cognite.OpcUa
{
    class Program
    {
        static int Main()
        {
            var config = ReadConfig();
            FullConfig fullConfig;
			try
			{
				YamlMappingNode clientCfg = (YamlMappingNode)config.Children[new YamlScalarNode("client")];
				YamlMappingNode nsmaps = (YamlMappingNode)config.Children[new YamlScalarNode("nsmaps")];
				YamlMappingNode cogniteConfig = (YamlMappingNode)config.Children[new YamlScalarNode("cognite")];
				YamlMappingNode loggerConfig = (YamlMappingNode)config.Children[new YamlScalarNode("logging")];
				fullConfig = new FullConfig
				{
					Nsmaps = nsmaps,
					Uaconfig = DeserializeNode<UAClientConfig>(clientCfg),
					CogniteConfig = DeserializeNode<CogniteClientConfig>(cogniteConfig),
                    LoggerConfig = DeserializeNode<LoggerConfig>(loggerConfig)
				};
				ValidateConfig(fullConfig);
			}
            catch (Exception e)
			{
				Console.WriteLine(e.Message);
				return -1;
			}

            ServiceCollection services = new ServiceCollection();
            Configure(services);
            ServiceProvider provider = services.BuildServiceProvider();

            Extractor extractor = new Extractor(fullConfig, provider.GetRequiredService<IHttpClientFactory>());
			Logger.Startup(fullConfig.LoggerConfig);

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

            ManualResetEvent quitEvent = new ManualResetEvent(false);
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
                return new Deserializer().Deserialize<T>(reader);
            }
        }
        private static void ValidateConfig(FullConfig config)
        {
            if (config.Uaconfig.ReconnectPeriod < 100)
            {
                throw new Exception("Too short reconnect period (<100ms)");
            }
            if (string.IsNullOrEmpty(config.Uaconfig.EndpointURL))
            {
                throw new Exception("Invalid EndpointURL");
            }
            if (string.IsNullOrEmpty(config.Uaconfig.GlobalPrefix))
            {
                throw new Exception("Invalid GlobalPrefix");
            }
            if (config.Uaconfig.PollingInterval < 0)
            {
                throw new Exception("PollingInterval must be a positive number");
            }
            if (string.IsNullOrEmpty(config.CogniteConfig.Project))
            {
                throw new Exception("Invalid Project");
            }
            if (string.IsNullOrEmpty(config.CogniteConfig.ApiKey))
            {
                throw new Exception("Invalid api-key");
            }
        }
        public static void Configure(IServiceCollection services)
        {
            services.AddHttpClient();
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

    }
    public class CogniteClientConfig
    {
        public string Project { get; set; }
        public string ApiKey { get; set; }
        public long RootAssetId { get; set; }
        public string RootNodeNamespace { get; set; }
        public string RootNodeId { get; set; }
        public int DataPushDelay { get; set; }
        public int NodePushDelay { get; set; }
        public bool Debug { get; set; }
    }
    public class FullConfig
    {
        public YamlMappingNode Nsmaps { get; set; }
        public UAClientConfig Uaconfig { get; set; }
        public CogniteClientConfig CogniteConfig { get; set; }
        public LoggerConfig LoggerConfig { get; set; }
    }
    public class LoggerConfig
    {
        public string LogFolder { get; set; }
        public bool LogData { get; set; }
    }
}

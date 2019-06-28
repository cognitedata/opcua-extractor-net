using System;
using Opc.Ua;
using System.Threading;
using YamlDotNet.Serialization;
using System.IO;
using YamlDotNet.RepresentationModel;
using Microsoft.Extensions.DependencyInjection;
using Cognite.Sdk.Api;

namespace opcua_extractor_net
{
    class Program
    {
        static void Main()
        {
            var config = ReadConfig();
            YamlMappingNode clientCfg = (YamlMappingNode)config.Children[new YamlScalarNode("client")];
            YamlMappingNode nsmaps = (YamlMappingNode)config.Children[new YamlScalarNode("nsmaps")];
            FullConfig fullConfig = new FullConfig()
            {
                nsmaps = nsmaps,
                uaconfig = clientCfg
            };
            Extractor extractor = new Extractor();
            UAClient client = new UAClient(DeserializeNode<UAClientConfig>(clientCfg), nsmaps, extractor);
            extractor.Client = client;

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddHttpClient<Client>()
                .SetHandlerLifetime(TimeSpan.MaxValue);

            client.Run().Wait();
            client.DebugBrowseDirectory(ObjectIds.ObjectsFolder);

            ServiceProvider services = serviceCollection.BuildServiceProvider();

            Client cogniteClient = services.GetRequiredService<Client>();


            ManualResetEvent quitEvent = new ManualResetEvent(false);
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                quitEvent.Set();
                eArgs.Cancel = true;
            };

            quitEvent.WaitOne(-1);
        }
        static YamlMappingNode ReadConfig()
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
    }
    public class UAClientConfig
    {
        public int ReconnectPeriod { get; set; } = 1000;
        public string EndpointURL { get; set; }
        public bool Autoaccept { get; set; } = false;
        public uint MaxResults { get; set; } = 100;
        public int PollingInterval { get; set; } = 500;
        public string GlobalPrefix { get; set; }
    }
    public class FullConfig
    {
        public YamlMappingNode nsmaps { get; set; }
        public UAClientConfig uaconfig { get; set; }
    }
}

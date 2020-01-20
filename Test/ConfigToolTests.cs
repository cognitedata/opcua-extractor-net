using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Xunit;
using Xunit.Abstractions;
using Cognite.OpcUa.Config;
using Serilog;

namespace Test
{
    [CollectionDefinition("Pusher_tests", DisableParallelization = true)]
    public class ConfigToolTests : MakeConsoleWork
    {
        public ConfigToolTests(ITestOutputHelper output) : base(output) { }


        [Trait("Server", "all")]
        [Trait("Target", "ConfigTool")]
        [Trait("Test", "fulltest")]
        [InlineData(ServerName.Basic)]
        [InlineData(ServerName.Full)]
        [InlineData(ServerName.Array)]
        [InlineData(ServerName.Events)]
        [InlineData(ServerName.Audit)]
        [Theory]
        public async Task DoConfigToolTest(ServerName server)
        {
            Log.Information("Loading config from config.config - tool - test.yml");

            var fullConfig = Utils.GetConfig("config.config-tool-test.yml");
            var baseConfig = Utils.GetConfig("config.config-tool-test.yml");
            Logger.Configure(fullConfig.Logging);

            fullConfig.Source.EndpointURL = ExtractorTester._hostNames[server];
            baseConfig.Source.EndpointURL = ExtractorTester._hostNames[server];

            var explorer = new UAServerExplorer(fullConfig, baseConfig);

            using var source = new CancellationTokenSource();

            await explorer.GetEndpoints(source.Token);
            Assert.False(baseConfig.Source.Secure);

            await explorer.GetBrowseChunkSizes(source.Token);
            Assert.Equal(1000, baseConfig.Source.BrowseChunk);
            Assert.Equal(1000, baseConfig.Source.BrowseNodesChunk);

            await explorer.GetAttributeChunkSizes(source.Token);
            Assert.True(baseConfig.Source.AttributesChunk >= 1000);

            explorer.ReadCustomTypes(source.Token);

            await explorer.IdentifyDataTypeSettings(source.Token);
            Assert.True(baseConfig.Extraction.AllowStringVariables);
            if (server == ServerName.Array)
            {
                Assert.Equal(4, baseConfig.Extraction.MaxArraySize);
            }
            else
            {
                Assert.Equal(0, baseConfig.Extraction.MaxArraySize);
            }


            await explorer.GetSubscriptionChunkSizes(source.Token);
            Assert.Equal(1000, baseConfig.Source.SubscriptionChunk);

            await explorer.GetHistoryReadConfig();
            Assert.Equal(100, baseConfig.History.DataNodesChunk);

            await explorer.GetEventConfig(source.Token);
            if (server == ServerName.Events)
            {
                Assert.Contains(baseConfig.Events.EventIds, proto => proto.NodeId == "i=12");
                Assert.Contains(baseConfig.Events.EventIds, proto => proto.NodeId == "i=16");
                Assert.Contains(baseConfig.Events.EventIds, proto => proto.NodeId == "i=17");
                Assert.Contains(baseConfig.Events.EventIds, proto => proto.NodeId == "i=18");
                Assert.Contains(baseConfig.Events.EmitterIds, proto => proto.NodeId == "i=2253");
                Assert.Contains(baseConfig.Events.EmitterIds, proto => proto.NodeId == "i=1");
                Assert.Contains(baseConfig.Events.EmitterIds, proto => proto.NodeId == "i=2");
                Assert.Contains(baseConfig.Events.HistorizingEmitterIds, proto => proto.NodeId == "i=2253");
            }
            else if (server == ServerName.Audit)
            {
                Assert.True(baseConfig.Extraction.EnableAuditDiscovery);
            }

            explorer.GetNamespaceMap();

            Assert.True(baseConfig.Extraction.NamespaceMap.ContainsKey("http://opcfoundation.org/UA/")
                && baseConfig.Extraction.NamespaceMap["http://opcfoundation.org/UA/"] == "base");
            Assert.True(baseConfig.Extraction.NamespaceMap.ContainsKey("http://examples.freeopcua.github.io")
                && baseConfig.Extraction.NamespaceMap["http://examples.freeopcua.github.io"] == "efg");

            explorer.LogSummary();

            ToolUtil.ConfigResultToString(baseConfig);

            explorer.Close();
        }
    }
}

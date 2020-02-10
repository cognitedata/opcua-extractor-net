/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using System;
using System.Collections.Generic;
using System.Linq;
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
        private static readonly ILogger log = Log.Logger.ForContext(typeof(ConfigToolTests));

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
            log.Information("Loading config from config.config - tool - test.yml");

            var fullConfig = ExtractorUtils.GetConfig("config.config-tool-test.yml");
            var baseConfig = ExtractorUtils.GetConfig("config.config-tool-test.yml");
            Logger.Configure(fullConfig.Logging);

            fullConfig.Source.EndpointURL = ExtractorTester.hostNames[server];
            baseConfig.Source.EndpointURL = ExtractorTester.hostNames[server];

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
                && baseConfig.Extraction.NamespaceMap["http://opcfoundation.org/UA/"] == "base:");
            Assert.True(baseConfig.Extraction.NamespaceMap.ContainsKey("http://examples.freeopcua.github.io")
                && baseConfig.Extraction.NamespaceMap["http://examples.freeopcua.github.io"] == "efg:");

            explorer.LogSummary();

            ToolUtil.ConfigResultToString(baseConfig);

            explorer.Close();
        }

        [Trait("Server", "basic")]
        [Trait("Target", "ExtractorRuntime")]
        [Trait("Test", "extractorruntime")]
        [Fact]
        public async Task TestExtractorRuntime()
        {
            var fullConfig = ExtractorUtils.GetConfig("config.influxtest.yml");
            Logger.Configure(fullConfig.Logging);

            fullConfig.Source.EndpointURL = ExtractorTester.hostNames[ServerName.Basic];
            fullConfig.Pushers = new List<PusherConfig>();

            var runTime = new ExtractorRuntime(fullConfig);

            using var source = new CancellationTokenSource();

            runTime.Configure();
            var runTask = runTime.Run(source);

            await Task.Delay(2000);

            Assert.False(runTask.IsFaulted);

            source.Cancel();

            try
            {
                await runTask;
            }
            catch (Exception ex)
            {
                if (!CommonTestUtils.TestRunResult(ex)) throw;
            }
        }
        [Trait("Server", "basic")]
        [Trait("Target", "ExtractorRuntime")]
        [Trait("Test", "extractorruntimefailure")]
        [Fact]
        public async Task TestExtractorRuntimeFailure()
        {
            var fullConfig = ExtractorUtils.GetConfig("config.test.yml");
            Logger.Configure(fullConfig.Logging);

            fullConfig.Source.EndpointURL = ExtractorTester.hostNames[ServerName.Basic];
            fullConfig.Pushers.First().Critical = true;
            fullConfig.Logging.ConsoleLevel = "debug";

            var runTime = new ExtractorRuntime(fullConfig);

            using var source = new CancellationTokenSource();

            runTime.Configure();
            var runTask = runTime.Run(source);

            for (int i = 0; i < 10; i++)
            {
                if (runTask.IsFaulted) break;
                await Task.Delay(1000);
            }

            source.Cancel();

            try
            {
                await runTask;
            }
            catch (Exception ex)
            {
                ExtractorFailureException efe = null;
                switch (ex)
                {
                    case ExtractorFailureException exception:
                        efe = exception;
                        break;
                    case AggregateException aex:
                        efe = ExtractorUtils.GetRootExceptionOfType<ExtractorFailureException>(aex);
                        break;
                }
                if (efe == null || efe.Message != "Critical pusher failed to connect") throw;
            }
        }
        [Trait("Server", "basic")]
        [Trait("Target", "ConfigToolRuntime")]
        [Trait("Test", "configtoolruntime")]
        [Fact]
        public async Task TestConfigToolRuntime()
        {
            var fullConfig = ExtractorUtils.GetConfig("config.config-tool-test.yml");
            var baseConfig = ExtractorUtils.GetConfig("config.config-tool-test.yml");

            Logger.Configure(fullConfig.Logging);

            fullConfig.Source.EndpointURL = ExtractorTester.hostNames[ServerName.Basic];
            baseConfig.Source.EndpointURL = ExtractorTester.hostNames[ServerName.Basic];

            var runTime = new ConfigToolRuntime(fullConfig, baseConfig, "config.config-tool-output.yml");

            var runTask = runTime.Run();

            try
            {
                await runTask;
            }
            catch (Exception ex)
            {
                if (!CommonTestUtils.TestRunResult(ex)) throw;
            }
        }
    }
}

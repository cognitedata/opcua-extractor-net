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
using Server;

namespace Test
{
    [CollectionDefinition("Pusher_tests", DisableParallelization = true)]
    public class ConfigToolTests : MakeConsoleWork
    {
        // private readonly ILogger log = Log.Logger.ForContext(typeof(ConfigToolTests));

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
        public async Task DoConfigToolTest(ServerName serverName)
        {
            var fullConfig = ExtractorUtils.GetConfig("config.config-tool-test.yml");
            var baseConfig = ExtractorUtils.GetConfig("config.config-tool-test.yml");
            Logger.Configure(fullConfig.Logging);

            fullConfig.Source.EndpointURL = ExtractorTester.HostName;
            baseConfig.Source.EndpointURL = ExtractorTester.HostName;

            using var server = new ServerController(new[] { ExtractorTester.SetupMap[serverName] });
            await server.Start();

            if (serverName == ServerName.Events)
            {
                server.PopulateEvents();
            }

            if (serverName == ServerName.Array)
            {
                server.PopulateArrayHistory();
            }

            if (serverName == ServerName.Basic)
            {
                server.PopulateBaseHistory();
            }

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
            if (serverName != ServerName.Audit)
            {
                Assert.True(baseConfig.Extraction.AllowStringVariables);
            }

            if (serverName == ServerName.Array)
            {
                Assert.Equal(4, baseConfig.Extraction.MaxArraySize);
                Assert.Equal(2, baseConfig.Extraction.CustomNumericTypes.Count());
                Assert.Contains(baseConfig.Extraction.CustomNumericTypes, proto => proto.NodeId.NodeId == "i=6");
                Assert.Contains(baseConfig.Extraction.CustomNumericTypes, proto => proto.NodeId.NodeId == "i=7");
            }
            else
            {
                Assert.Equal(0, baseConfig.Extraction.MaxArraySize);
            }


            await explorer.GetSubscriptionChunkSizes(source.Token);
            Assert.Equal(1000, baseConfig.Source.SubscriptionChunk);

            await explorer.GetHistoryReadConfig();
            Assert.Equal(100, baseConfig.History.DataNodesChunk);
            if (serverName == ServerName.Audit || serverName == ServerName.Full || serverName == ServerName.Events)
            {
                Assert.False(baseConfig.History.Enabled);
            }
            else
            {
                Assert.True(baseConfig.History.Enabled);
            }

            await explorer.GetEventConfig(source.Token);
            if (serverName == ServerName.Events)
            {
                Assert.Contains(baseConfig.Events.EventIds, proto => proto.NodeId == "i=7");
                Assert.Contains(baseConfig.Events.EventIds, proto => proto.NodeId == "i=11");
                Assert.Contains(baseConfig.Events.EventIds, proto => proto.NodeId == "i=12");
                Assert.Contains(baseConfig.Events.EventIds, proto => proto.NodeId == "i=13");
                Assert.Contains(baseConfig.Events.EmitterIds, proto => proto.NodeId == "i=2253");
                Assert.Contains(baseConfig.Events.EmitterIds, proto => proto.NodeId == "i=2");
                Assert.Contains(baseConfig.Events.EmitterIds, proto => proto.NodeId == "i=3");
                Assert.Contains(baseConfig.Events.HistorizingEmitterIds, proto => proto.NodeId == "i=2253");
                Assert.Contains(baseConfig.Events.HistorizingEmitterIds, proto => proto.NodeId == "i=2");
                Assert.True(baseConfig.History.Enabled);
            }
            else if (serverName == ServerName.Audit)
            {
                Assert.True(baseConfig.Extraction.EnableAuditDiscovery);
            }

            explorer.GetNamespaceMap();

            Assert.True(baseConfig.Extraction.NamespaceMap.ContainsKey("http://opcfoundation.org/UA/")
                && baseConfig.Extraction.NamespaceMap["http://opcfoundation.org/UA/"] == "base:");
            Assert.True(baseConfig.Extraction.NamespaceMap.ContainsKey("opc.tcp://test.localhost")
                && baseConfig.Extraction.NamespaceMap["opc.tcp://test.localhost"] == "tl:");

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

            fullConfig.Source.EndpointURL = ExtractorTester.HostName;
            fullConfig.Pushers = new List<PusherConfig>();

            using var server = new ServerController(new[] { PredefinedSetup.Base });
            await server.Start();

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
        [Trait("Target", "ConfigToolRuntime")]
        [Trait("Test", "configtoolruntime")]
        [Fact]
        public async Task TestConfigToolRuntime()
        {
            var fullConfig = ExtractorUtils.GetConfig("config.config-tool-test.yml");
            var baseConfig = ExtractorUtils.GetConfig("config.config-tool-test.yml");

            Logger.Configure(fullConfig.Logging);

            using var server = new ServerController(new[] { PredefinedSetup.Base });
            await server.Start();

            fullConfig.Source.EndpointURL = ExtractorTester.HostName;
            baseConfig.Source.EndpointURL = ExtractorTester.HostName;

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

        [Fact]
        [Trait("Server", "none")]
        [Trait("Target", "ConfigTool")]
        [Trait("Test", "namespacemapping")]
        public void TestNamespaceMapping()
        {
            var namespaces = new List<string>
            {
                "opc.tcp://test.namespace.onet",
                "test.namespace.twot",
                "test.namespace.duplicateone",
                "test.namespace.duplicatetwo",
                "http://test.namespace.http",
                "http://opcfoundation.org/UA/",
                "test.Upper.Case.Duplicate",
                "test.Upper.Case.Duplicatetwo"
            };

            var expectedKeys = new []
            {
                "tno:",
                "tnt:",
                "tnd:",
                "tnd1:",
                "tnh:",
                "base:",
                "tucd:",
                "tucd1:"
            };

            var dict = UAServerExplorer.GenerateNamespaceMap(namespaces);
            var keys = namespaces.Select(ns => dict[ns]).ToArray();
            for (int i = 0; i < keys.Length; i++)
            {
                Assert.Equal(expectedKeys[i], keys[i]);
            }
        }
    }
}

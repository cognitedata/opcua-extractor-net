/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

using Cognite.Extractor.Configuration;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [Collection("Extractor tests")]
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
            var fullConfig = ConfigurationUtils.Read<FullConfig>("config.config-tool-test.yml");
            var baseConfig = ConfigurationUtils.Read<FullConfig>("config.config-tool-test.yml");
            Console.WriteLine($"{baseConfig.Source.BrowseNodesChunk}");
            fullConfig.GenerateDefaults();
            baseConfig.GenerateDefaults();

            fullConfig.Source.EndpointUrl = ExtractorTester.HostName;
            baseConfig.Source.EndpointUrl = ExtractorTester.HostName;

            using var server = new ServerController(new[] { ExtractorTester.SetupMap[serverName] });
            await server.Start();

            if (serverName == ServerName.Events)
            {
                server.PopulateEvents();
            }

            if (serverName == ServerName.Array)
            {
                server.PopulateCustomHistory();
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
                Assert.True(baseConfig.Extraction.DataTypes.AllowStringVariables);
            }

            if (serverName == ServerName.Array)
            {
                Assert.Equal(4, baseConfig.Extraction.DataTypes.MaxArraySize);
                Assert.Equal(4, baseConfig.Extraction.DataTypes.CustomNumericTypes.Count());
                Assert.Contains(baseConfig.Extraction.DataTypes.CustomNumericTypes, proto => proto.NodeId.NodeId == "i=6");
                Assert.Contains(baseConfig.Extraction.DataTypes.CustomNumericTypes, proto => proto.NodeId.NodeId == "i=7");
                Assert.Contains(baseConfig.Extraction.DataTypes.CustomNumericTypes, proto => proto.Enum);
            }
            else
            {
                Assert.Equal(0, baseConfig.Extraction.DataTypes.MaxArraySize);
            }


            await explorer.GetSubscriptionChunkSizes(source.Token);
            Assert.Equal(1000, baseConfig.Source.SubscriptionChunk);

            await explorer.GetHistoryReadConfig(source.Token);
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
                Assert.True(baseConfig.Events.Enabled);
                Assert.True(baseConfig.Events.History);
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

            var result = ToolUtil.ConfigResultToString(baseConfig);

            ConfigurationUtils.TryReadConfigFromString<FullConfig>(result, 1);

            explorer.Close();
        }

        [Trait("Server", "basic")]
        [Trait("Target", "ConfigToolRuntime")]
        [Trait("Test", "configtoolruntime")]
        [Fact]
        public async Task TestConfigToolRuntime()
        {
            var fullConfig = ConfigurationUtils.Read<FullConfig>("config.config-tool-test.yml");
            fullConfig.GenerateDefaults();
            var baseConfig = ConfigurationUtils.Read<FullConfig>("config.config-tool-test.yml");
            baseConfig.GenerateDefaults();

            using var server = new ServerController(new[] { PredefinedSetup.Base });
            await server.Start();

            fullConfig.Source.EndpointUrl = ExtractorTester.HostName;
            baseConfig.Source.EndpointUrl = ExtractorTester.HostName;

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

            var expectedKeys = new[]
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

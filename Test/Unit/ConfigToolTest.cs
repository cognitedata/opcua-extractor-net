using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public class ConfigToolTestFixture
    {
        public UAServerExplorer Explorer { get; }
        public FullConfig Config { get; }
        public FullConfig BaseConfig { get; }
        public ServerController Server { get; }
        public CancellationTokenSource Source { get; protected set; }
        public ConfigToolTestFixture()
        {
            var services = new ServiceCollection();
            Config = services.AddConfig<FullConfig>("config.test.yml", 1);
            Console.WriteLine($"Add logger: {Config.Logger}");
            Config.Source.EndpointUrl = $"opc.tcp://localhost:63500";
            BaseConfig = ConfigurationUtils.Read<FullConfig>("config.test.yml");

            services.AddLogger();
            LoggingUtils.Configure(Config.Logger);

            Server = new ServerController(new[] {
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, 63500);
            Server.Start().Wait();

            Explorer = new UAServerExplorer(Config, BaseConfig);
            Source = new CancellationTokenSource();
            Explorer.Run(Source.Token).Wait();
        }
    }
    public class ConfigToolTest : MakeConsoleWork, IClassFixture<ConfigToolTestFixture>
    {
        private readonly ConfigToolTestFixture tester;
        public ConfigToolTest(ITestOutputHelper output, ConfigToolTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        [Fact]
        public async Task TestEndpointDiscovery()
        {
            // Test while connected
            await tester.Explorer.GetEndpoints(tester.Source.Token);
            var summary = tester.Explorer.GetSummary();
            Assert.True(summary.Secure);
            Assert.Equal(7, summary.Endpoints.Count);

            // Test failure to connect at all
            tester.Explorer.Close();
            tester.Explorer.ResetSummary();
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:60000";
            await Assert.ThrowsAsync<FatalException>(() => tester.Explorer.GetEndpoints(tester.Source.Token));
            summary = tester.Explorer.GetSummary();
            Assert.False(summary.Secure);
            Assert.Null(summary.Endpoints);

            // Test connect from explorer
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:63500";
            await tester.Explorer.GetEndpoints(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.True(summary.Secure);
            Assert.Equal(7, summary.Endpoints.Count);

            // Test with secure set to true
            tester.Explorer.ResetSummary();
            tester.Config.Source.Secure = true;
            await tester.Explorer.GetEndpoints(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.True(summary.Secure);
            Assert.Equal(7, summary.Endpoints.Count);
        }
        [Fact]
        public async Task TestGetBrowseChunkSizes()
        {
            // Test normal run
            await tester.Explorer.GetBrowseChunkSizes(tester.Source.Token);
            var summary = tester.Explorer.GetSummary();
            Assert.Equal(1000, summary.BrowseChunk);
            Assert.False(summary.BrowseNextWarning);
            Assert.Equal(1000, summary.BrowseNodesChunk);

            // Test with adjusted initial settings
            tester.Explorer.ResetSummary();
            tester.Config.Source.BrowseChunk = 100;
            tester.Config.Source.BrowseNodesChunk = 100;
            await tester.Explorer.GetBrowseChunkSizes(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.Equal(100, summary.BrowseChunk);
            Assert.False(summary.BrowseNextWarning);
            Assert.Equal(100, summary.BrowseNodesChunk);
            Assert.Equal(100, tester.BaseConfig.Source.BrowseChunk);
            Assert.Equal(100, tester.BaseConfig.Source.BrowseNodesChunk);

            // Test with zero browse chunk
            tester.Explorer.ResetSummary();
            tester.Config.Source.BrowseChunk = 0;
            tester.Config.Source.BrowseNodesChunk = 100;
            await tester.Explorer.GetBrowseChunkSizes(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.Equal(0, summary.BrowseChunk);
            Assert.False(summary.BrowseNextWarning);
            Assert.Equal(100, summary.BrowseNodesChunk);
            Assert.Equal(0, tester.BaseConfig.Source.BrowseChunk);
            Assert.Equal(100, tester.BaseConfig.Source.BrowseNodesChunk);

            tester.Config.Source.BrowseNodesChunk = 1000;
            tester.Config.Source.BrowseChunk = 1000;
        }
        [Fact]
        public void TestGetCustomDataTypes()
        {
            // Config doesn't really impact this
            tester.Explorer.ReadCustomTypes(tester.Source.Token);
            var summary = tester.Explorer.GetSummary();
            Assert.Equal(1, summary.CustomNumTypesCount);
            Assert.True(tester.BaseConfig.Extraction.DataTypes.AutoIdentifyTypes);
            Assert.True(summary.Enums);
            Assert.Single(tester.BaseConfig.Extraction.DataTypes.CustomNumericTypes);
        }
    }
}

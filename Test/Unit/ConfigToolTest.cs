using Cognite.Extractor.Configuration;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.DependencyInjection;
using Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public class ConfigToolTestFixture : LoggingTestFixture, IAsyncLifetime
    {
        public UAServerExplorer Explorer { get; private set; }
        public FullConfig Config { get; }
        public FullConfig BaseConfig { get; }
        public ServerController Server { get; }
        public CancellationTokenSource Source { get; protected set; }
        public ServiceProvider Provider { get; }
        public ConfigToolTestFixture()
        {
            var services = new ServiceCollection();
            Config = services.AddConfig<FullConfig>("config.test.yml", 1);
            Config.Source.Retries = new UARetryConfig
            {
                MaxTries = 1,
                MaxDelay = "100ms"
            };
            int port = CommonTestUtils.NextPort;
            Config.Source.EndpointUrl = $"opc.tcp://localhost:{port}";
            BaseConfig = ConfigurationUtils.Read<FullConfig>("config.test.yml");
            BaseConfig.GenerateDefaults();
            Configure(services);

            Provider = services.BuildServiceProvider();

            Server = new ServerController(new[] {
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, Provider, port);
        }

        public async Task InitializeAsync()
        {
            await Server.Start();

            Source = new CancellationTokenSource();
            Explorer = new UAServerExplorer(Provider, Config, BaseConfig, Source.Token);
            await Explorer.Run(Source.Token, 0);
        }

        public async Task DisposeAsync()
        {
            if (Source != null)
            {
                await Source.CancelAsync();
                Source.Dispose();
                Source = null;
            }
            await Explorer?.Close(CancellationToken.None);
            Explorer?.Dispose();
            Server?.Stop();
            Server?.Dispose();

            await Provider.DisposeAsync();
        }
    }
    public class ConfigToolTest : IClassFixture<ConfigToolTestFixture>
    {
        private readonly ConfigToolTestFixture tester;
        public ConfigToolTest(ITestOutputHelper output, ConfigToolTestFixture tester)
        {
            this.tester = tester;
            tester.Init(output);
            tester.Explorer.TypeManager.Reset();
        }
        [Fact(Timeout = 10000)]
        public async Task TestEndpointDiscovery()
        {
            // Test while connected
            await tester.Explorer.GetEndpoints(tester.Source.Token);
            var summary = tester.Explorer.Summary;
            Assert.True(summary.Session.Secure);
            Assert.Equal(7, summary.Session.Endpoints.Count);

            var oldEP = tester.Config.Source.EndpointUrl;
            // Test failure to connect at all
            await tester.Explorer.Close(CancellationToken.None);
            tester.Explorer.ResetSummary();
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:60000";
            await Assert.ThrowsAsync<FatalException>(() => tester.Explorer.GetEndpoints(tester.Source.Token));
            summary = tester.Explorer.Summary;
            Assert.False(summary.Session.Secure);
            Assert.Empty(summary.Session.Endpoints);

            // Test connect from explorer
            tester.Config.Source.EndpointUrl = oldEP;
            await tester.Explorer.GetEndpoints(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.True(summary.Session.Secure);
            Assert.Equal(7, summary.Session.Endpoints.Count);

            // Test with secure set to true
            tester.Explorer.ResetSummary();
            tester.Config.Source.Secure = true;
            await tester.Explorer.GetEndpoints(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.True(summary.Session.Secure);
            Assert.Equal(7, summary.Session.Endpoints.Count);
        }
        [Fact(Timeout = 30000)]
        public async Task TestGetBrowseChunkSizes()
        {
            // Test normal run
            await tester.Explorer.GetBrowseChunkSizes(tester.Source.Token);
            var summary = tester.Explorer.Summary;
            Assert.Equal(1000, summary.Browse.BrowseChunk);
            Assert.False(summary.Browse.BrowseNextWarning);
            Assert.Equal(1000, summary.Browse.BrowseNodesChunk);

            // Test with adjusted initial settings
            tester.Explorer.ResetSummary();
            tester.Config.Source.BrowseChunk = 100;
            tester.Config.Source.BrowseNodesChunk = 100;
            await tester.Explorer.GetBrowseChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.Equal(100, summary.Browse.BrowseChunk);
            Assert.False(summary.Browse.BrowseNextWarning);
            Assert.Equal(100, summary.Browse.BrowseNodesChunk);
            Assert.Equal(100, tester.BaseConfig.Source.BrowseChunk);
            Assert.Equal(100, tester.BaseConfig.Source.BrowseNodesChunk);

            // Test with zero browse chunk
            tester.Explorer.ResetSummary();
            tester.Config.Source.BrowseChunk = 0;
            tester.Config.Source.BrowseNodesChunk = 100;
            await tester.Explorer.GetBrowseChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.Equal(0, summary.Browse.BrowseChunk);
            Assert.False(summary.Browse.BrowseNextWarning);
            Assert.Equal(100, summary.Browse.BrowseNodesChunk);
            Assert.Equal(0, tester.BaseConfig.Source.BrowseChunk);
            Assert.Equal(100, tester.BaseConfig.Source.BrowseNodesChunk);

            // Test with browseNext issues.
            tester.Explorer.ResetSummary();
            tester.Config.Source.BrowseNodesChunk = 1000;
            tester.Config.Source.BrowseChunk = 1000;
            tester.Server.Issues.MaxBrowseResults = 100;
            await tester.Explorer.GetBrowseChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.Equal(2, summary.Browse.BrowseNodesChunk);
            Assert.True(summary.Browse.BrowseNextWarning);
            Assert.Equal(100, summary.Browse.BrowseChunk);
            Assert.Equal(100, tester.BaseConfig.Source.BrowseChunk);
            Assert.Equal(2, tester.BaseConfig.Source.BrowseNodesChunk);

            // Test with browseNodes issues
            tester.Explorer.ResetSummary();
            tester.Config.Source.BrowseNodesChunk = 1000;
            tester.Config.Source.BrowseChunk = 1000;
            tester.Server.Issues.MaxBrowseResults = 0;
            tester.Server.Issues.MaxBrowseNodes = 10;
            await tester.Explorer.GetBrowseChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.Equal(10, summary.Browse.BrowseNodesChunk);
            Assert.False(summary.Browse.BrowseNextWarning);
            Assert.Equal(1000, summary.Browse.BrowseChunk);
            Assert.Equal(1000, tester.BaseConfig.Source.BrowseChunk);
            Assert.Equal(10, tester.BaseConfig.Source.BrowseNodesChunk);

            tester.Config.Source.BrowseNodesChunk = 1000;
            tester.Config.Source.BrowseChunk = 1000;
            tester.BaseConfig.Source.BrowseNodesChunk = 1000;
            tester.BaseConfig.Source.BrowseChunk = 1000;
            tester.Server.Issues.MaxBrowseNodes = 0;
        }
        [Fact(Timeout = 30000)]
        public async Task TestGetCustomDataTypes()
        {
            // Config doesn't really impact this
            await tester.Explorer.ReadCustomTypes(tester.Source.Token);
            var summary = tester.Explorer.Summary;
            Assert.Equal(1, summary.DataTypes.CustomNumTypesCount);
            Assert.True(tester.BaseConfig.Extraction.DataTypes.AutoIdentifyTypes);
            Assert.True(summary.DataTypes.Enums);
            Assert.Single(tester.BaseConfig.Extraction.DataTypes.CustomNumericTypes);
        }
        [Fact(Timeout = 30000)]
        public async Task TestAttributeChunkSizes()
        {
            tester.Config.Extraction.RootNode = null;
            // Test no root
            tester.Explorer.ResetNodes();
            await tester.Explorer.GetAttributeChunkSizes(tester.Source.Token);
            var summary = tester.Explorer.Summary;
            Assert.Equal(10000, summary.Attributes.ChunkSize);
            Assert.False(summary.Attributes.LimitWarning);

            // Test smaller root
            tester.Explorer.ResetSummary();
            tester.Explorer.ResetNodes();
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            await tester.Explorer.GetAttributeChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.Equal(1000, summary.Attributes.ChunkSize);
            Assert.True(summary.Attributes.LimitWarning);
            tester.Config.Extraction.RootNode = null;

            // Test max size issues
            tester.Explorer.ResetSummary();
            tester.Explorer.ResetNodes();
            tester.Config.Extraction.RootNode = tester.Server.Ids.Full.WideRoot.ToProtoNodeId(tester.Explorer);
            tester.Server.Issues.MaxAttributes = 100;
            await tester.Explorer.GetAttributeChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.Equal(100, summary.Attributes.ChunkSize);
            Assert.False(summary.Attributes.LimitWarning && (summary.Attributes.KnownCount < summary.Attributes.ChunkSize));

            tester.Config.Extraction.RootNode = null;
            tester.Server.Issues.MaxAttributes = 0;
            tester.Config.Source.AttributesChunk = 1000;
        }
        [Fact(Timeout = 30000)]
        public async Task TestGetDataTypeSettings()
        {
            // First for all nodes
            tester.Config.Extraction.RootNode = null;
            tester.Explorer.ResetNodes();
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            var summary = tester.Explorer.Summary;
            Assert.True(summary.DataTypes.StringVariables);
            Assert.Equal(4, summary.DataTypes.MaxArraySize);

            // Limit max array size a bit
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            tester.Config.Extraction.DataTypes.MaxArraySize = 2;
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.True(summary.DataTypes.StringVariables);
            Assert.Equal(2, summary.DataTypes.MaxArraySize);

            // Limit max array size more
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            tester.Config.Extraction.DataTypes.MaxArraySize = 1;
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.True(summary.DataTypes.StringVariables);
            Assert.Equal(0, summary.DataTypes.MaxArraySize);

            // Map base hierarchy
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.True(summary.DataTypes.StringVariables);
            Assert.Equal(0, summary.DataTypes.MaxArraySize);

            // Map event hierarchy
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            tester.Config.Extraction.RootNode = tester.Server.Ids.Event.Root.ToProtoNodeId(tester.Explorer);
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.False(summary.DataTypes.StringVariables);
            Assert.Equal(0, summary.DataTypes.MaxArraySize);

            tester.Config.Extraction.RootNode = null;
        }

        [Fact(Timeout = 30000)]
        public async Task TestGetSubscriptionChunkSizes()
        {
            bool generate = true;
            var generateDpsTask = Task.Run(async () =>
            {
                double counter = 0;
                while (!tester.Source.Token.IsCancellationRequested && generate)
                {
                    tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar1, counter++);
                    await Task.Delay(200);
                }
            });
            tester.Config.Extraction.RootNode = tester.Server.Ids.Full.WideRoot.ToProtoNodeId(tester.Explorer);
            // Test full hierarchy
            tester.Explorer.ResetNodes();
            await tester.Explorer.GetSubscriptionChunkSizes(tester.Source.Token);
            var summary = tester.Explorer.Summary;
            Assert.False(summary.Subscriptions.SilentWarning);
            Assert.Equal(1000, summary.Subscriptions.ChunkSize);
            Assert.False(summary.Subscriptions.LimitWarning);

            // Test only base hierarchy
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetSubscriptionChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.False(summary.Subscriptions.SilentWarning);
            Assert.Equal(1000, summary.Subscriptions.ChunkSize);
            Assert.True(summary.Subscriptions.LimitWarning);

            // Test only custom hierarchy
            tester.Config.Extraction.RootNode = tester.Server.Ids.Custom.Root.ToProtoNodeId(tester.Explorer);
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetSubscriptionChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.False(summary.Subscriptions.SilentWarning);
            Assert.Equal(1000, summary.Subscriptions.ChunkSize);
            Assert.True(summary.Subscriptions.LimitWarning);

            // Test issue with chunk sizes
            tester.Config.Extraction.RootNode = tester.Server.Ids.Full.WideRoot.ToProtoNodeId(tester.Explorer);
            tester.Server.Issues.MaxMonitoredItems = 100;
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetSubscriptionChunkSizes(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.False(summary.Subscriptions.SilentWarning);
            Assert.Equal(100, summary.Subscriptions.ChunkSize);
            Assert.False(summary.Subscriptions.LimitWarning);

            generate = false;
            tester.Server.WipeHistory(tester.Server.Ids.Base.DoubleVar1, 0);
            tester.Config.Source.SubscriptionChunk = 1000;
            tester.Server.Issues.MaxMonitoredItems = 0;
        }
        [Fact(Timeout = 30000)]
        public async Task TestGetHistoryChunkSizes()
        {
            tester.Explorer.ResetNodes();
            // Test for non-historizing nodes
            tester.Config.Extraction.RootNode = tester.Server.Ids.Event.Root.ToProtoNodeId(tester.Explorer);
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            var summary = tester.Explorer.Summary;
            Assert.True(summary.History.NoHistorizingNodes);

            // Test for regular analysis, with no data
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.False(summary.History.NoHistorizingNodes);
            Assert.True(summary.History.Enabled);
            Assert.Equal(100, summary.History.ChunkSize);
            Assert.Equal(TimeSpan.Zero, summary.History.Granularity);

            // Test with issues
            tester.Server.Issues.MaxHistoryNodes = 1;
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.False(summary.History.NoHistorizingNodes);
            Assert.True(summary.History.Enabled);
            Assert.Equal(1, summary.History.ChunkSize);
            Assert.Equal(TimeSpan.Zero, summary.History.Granularity);

            tester.Server.Issues.MaxHistoryNodes = 0;

            // Test with data
            tester.Explorer.ResetSummary();
            var now = DateTime.UtcNow;
            tester.Server.PopulateBaseHistory(now.AddSeconds(-100));
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.False(summary.History.NoHistorizingNodes);
            Assert.True(summary.History.Enabled);
            Assert.Equal(100, summary.History.ChunkSize);
            Assert.Equal(TimeSpan.FromSeconds(1), summary.History.Granularity);
            Assert.False(summary.History.BackfillRecommended);

            // Test with more data
            tester.Explorer.ResetSummary();
            tester.Server.PopulateBaseHistory(now.AddSeconds(-10000));
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.False(summary.History.NoHistorizingNodes);
            Assert.True(summary.History.Enabled);
            Assert.Equal(100, summary.History.ChunkSize);
            Assert.Equal(TimeSpan.FromSeconds(1), summary.History.Granularity);
            Assert.True(summary.History.BackfillRecommended);

            tester.Server.WipeHistory(tester.Server.Ids.Base.DoubleVar1, 0.0);
            tester.Server.WipeHistory(tester.Server.Ids.Base.DoubleVar1, 0.0);

        }
        [Fact(Timeout = 30000)]
        public async Task TestGetEventConfig()
        {
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();

            // Test no events
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            tester.Server.SetEventConfig(false, false, false);
            await tester.Explorer.GetEventConfig(tester.Source.Token);
            var summary = tester.Explorer.Summary;
            Assert.False(summary.Events.AnyEvents);
            Assert.False(summary.Events.Auditing);
            Assert.False(summary.Events.HistoricalEvents);
            Assert.Equal(0, summary.Events.NumEmitters);
            Assert.Equal(0, summary.Events.NumHistEmitters);

            // Test events and auditing set on server
            tester.Server.SetEventConfig(true, true, false);
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetEventConfig(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.True(summary.Events.AnyEvents);
            Assert.True(summary.Events.Auditing);
            Assert.True(summary.Events.HistoricalEvents);
            Assert.Equal(1, summary.Events.NumEmitters);
            Assert.Equal(1, summary.Events.NumHistEmitters);

            // Test auditing set on server
            tester.Server.SetEventConfig(false, false, true);
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetEventConfig(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.True(summary.Events.AnyEvents);
            Assert.True(summary.Events.Auditing);
            Assert.False(summary.Events.HistoricalEvents);
            Assert.Equal(0, summary.Events.NumEmitters);
            Assert.Equal(0, summary.Events.NumHistEmitters);

            // Test discover on event hierarchy
            tester.Server.SetEventConfig(false, false, false);
            tester.Config.Extraction.RootNode = tester.Server.Ids.Event.Root.ToProtoNodeId(tester.Explorer);
            tester.Explorer.ResetSummary();
            tester.Explorer.ResetNodes();
            await tester.Explorer.GetEventConfig(tester.Source.Token);
            summary = tester.Explorer.Summary;
            Assert.True(summary.Events.AnyEvents);
            Assert.False(summary.Events.Auditing);
            Assert.True(summary.Events.HistoricalEvents);
            Assert.Equal(2, summary.Events.NumEmitters);
            Assert.Equal(1, summary.Events.NumHistEmitters);
        }
        [Fact]
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
        [Fact(Timeout = 30000)]
        public async Task TestConfigToolRuntime()
        {
            var fullConfig = ConfigurationUtils.Read<FullConfig>("config.config-tool-test.yml");
            fullConfig.GenerateDefaults();
            var baseConfig = ConfigurationUtils.Read<FullConfig>("config.config-tool-test.yml");
            baseConfig.GenerateDefaults();

            fullConfig.Source.EndpointUrl = tester.Config.Source.EndpointUrl;
            baseConfig.Source.EndpointUrl = tester.Config.Source.EndpointUrl;

            var runTime = new ConfigToolRuntime(tester.Provider, fullConfig, baseConfig, "config.config-tool-output.yml");

            var runTask = runTime.Run(CancellationToken.None);

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

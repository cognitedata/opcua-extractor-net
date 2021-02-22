﻿using Cognite.Extractor.Configuration;
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
using System.Reflection;
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
            BaseConfig.GenerateDefaults();

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
        [Fact]
        public async Task TestAttributeChunkSizes()
        {
            // Test no root
            tester.Explorer.ResetNodes();
            await tester.Explorer.GetAttributeChunkSizes(tester.Source.Token);
            var summary = tester.Explorer.GetSummary();
            Assert.Equal(10000, summary.AttributeChunkSize);
            Assert.False(summary.VariableLimitWarning);

            // Test smaller root
            tester.Explorer.ResetSummary();
            tester.Explorer.ResetNodes();
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            await tester.Explorer.GetAttributeChunkSizes(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.Equal(1000, summary.AttributeChunkSize);
            Assert.True(summary.VariableLimitWarning);
            tester.Config.Extraction.RootNode = null;
        }
        [Fact]
        public async Task TestGetDataTypeSettings()
        {
            // First for all nodes
            tester.Explorer.ResetNodes();
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            var summary = tester.Explorer.GetSummary();
            Assert.True(summary.StringVariables);
            Assert.Equal(4, summary.MaxArraySize);
            bool history = (bool)tester.Explorer.GetType()
                .GetField("history", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(tester.Explorer);
            Assert.True(history);

            // Limit max array size a bit
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            tester.Config.Extraction.DataTypes.MaxArraySize = 2;
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.True(summary.StringVariables);
            Assert.Equal(2, summary.MaxArraySize);
            history = (bool)tester.Explorer.GetType()
                .GetField("history", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(tester.Explorer);
            Assert.True(history);

            // Limit max array size more
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            tester.Config.Extraction.DataTypes.MaxArraySize = 1;
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.True(summary.StringVariables);
            Assert.Equal(0, summary.MaxArraySize);
            history = (bool)tester.Explorer.GetType()
                .GetField("history", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(tester.Explorer);
            Assert.True(history);

            // Map base hierarchy
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.True(summary.StringVariables);
            Assert.Equal(0, summary.MaxArraySize);
            history = (bool)tester.Explorer.GetType()
                .GetField("history", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(tester.Explorer);
            Assert.True(history);

            // Map event hierarchy
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            tester.Config.Extraction.RootNode = tester.Server.Ids.Event.Root.ToProtoNodeId(tester.Explorer);
            await tester.Explorer.IdentifyDataTypeSettings(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.False(summary.StringVariables);
            Assert.Equal(0, summary.MaxArraySize);
            history = (bool)tester.Explorer.GetType()
                .GetField("history", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(tester.Explorer);
            Assert.False(history);

            tester.Config.Extraction.RootNode = null;
        }

        [Fact]
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
            // Test full hierarchy
            tester.Explorer.ResetNodes();
            await tester.Explorer.GetSubscriptionChunkSizes(tester.Source.Token);
            var summary = tester.Explorer.GetSummary();
            Assert.False(summary.SilentSubscriptionsWarning);
            Assert.Equal(1000, summary.SubscriptionChunkSize);
            Assert.False(summary.SubscriptionLimitWarning);

            // Test only base hierarchy
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetSubscriptionChunkSizes(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.False(summary.SilentSubscriptionsWarning);
            Assert.Equal(1000, summary.SubscriptionChunkSize);
            Assert.True(summary.SubscriptionLimitWarning);

            // Test only custom hierarchy
            tester.Config.Extraction.RootNode = tester.Server.Ids.Custom.Root.ToProtoNodeId(tester.Explorer);
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetSubscriptionChunkSizes(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.False(summary.SilentSubscriptionsWarning);
            Assert.Equal(1000, summary.SubscriptionChunkSize);
            Assert.True(summary.SubscriptionLimitWarning);

            generate = false;
            tester.Server.WipeHistory(tester.Server.Ids.Base.DoubleVar1, 0);
        }
        [Fact]
        public async Task TestGetHistoryChunkSizes()
        {
            tester.Explorer.ResetNodes();
            // Test for non-historizing nodes
            tester.Config.Extraction.RootNode = tester.Server.Ids.Event.Root.ToProtoNodeId(tester.Explorer);
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            var summary = tester.Explorer.GetSummary();
            Assert.True(summary.NoHistorizingNodes);

            // Test for regular analysis, with no data
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.False(summary.NoHistorizingNodes);
            Assert.True(summary.History);
            Assert.Equal(100, summary.HistoryChunkSize);
            Assert.Equal(TimeSpan.Zero, summary.HistoryGranularity);

            // Test with data
            tester.Explorer.ResetSummary();
            var now = DateTime.UtcNow;
            tester.Server.PopulateBaseHistory(now.AddSeconds(-100));
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.False(summary.NoHistorizingNodes);
            Assert.True(summary.History);
            Assert.Equal(100, summary.HistoryChunkSize);
            Assert.Equal(TimeSpan.FromSeconds(1), summary.HistoryGranularity);
            Assert.False(summary.BackfillRecommended);

            // Test with more data
            tester.Explorer.ResetSummary();
            tester.Server.PopulateBaseHistory(now.AddSeconds(-10000));
            await tester.Explorer.GetHistoryReadConfig(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.False(summary.NoHistorizingNodes);
            Assert.True(summary.History);
            Assert.Equal(100, summary.HistoryChunkSize);
            Assert.Equal(TimeSpan.FromSeconds(1), summary.HistoryGranularity);
            Assert.True(summary.BackfillRecommended);
        }
        [Fact]
        public async Task TestGetEventConfig()
        {
            tester.Explorer.ResetNodes();
            tester.Explorer.ResetSummary();

            // Test no events
            tester.Config.Extraction.RootNode = tester.Server.Ids.Base.Root.ToProtoNodeId(tester.Explorer);
            tester.Server.SetEventConfig(false, false, false);
            await tester.Explorer.GetEventConfig(tester.Source.Token);
            var summary = tester.Explorer.GetSummary();
            Assert.False(summary.AnyEvents);
            Assert.False(summary.Auditing);
            Assert.False(summary.HistoricalEvents);
            Assert.Equal(0, summary.NumEmitters);
            Assert.Equal(0, summary.NumHistEmitters);

            // Test events and auditing set on server
            tester.Server.SetEventConfig(true, true, false);
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetEventConfig(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.True(summary.AnyEvents);
            Assert.True(summary.Auditing);
            Assert.True(summary.HistoricalEvents);
            Assert.Equal(1, summary.NumEmitters);
            Assert.Equal(1, summary.NumHistEmitters);

            // Test auditing set on server
            tester.Server.SetEventConfig(false, false, true);
            tester.Explorer.ResetSummary();
            await tester.Explorer.GetEventConfig(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.True(summary.AnyEvents);
            Assert.True(summary.Auditing);
            Assert.False(summary.HistoricalEvents);
            Assert.Equal(0, summary.NumEmitters);
            Assert.Equal(0, summary.NumHistEmitters);

            // Test discover on event hierarchy
            tester.Server.SetEventConfig(false, false, false);
            tester.Config.Extraction.RootNode = tester.Server.Ids.Event.Root.ToProtoNodeId(tester.Explorer);
            tester.Explorer.ResetSummary();
            tester.Explorer.ResetNodes();
            await tester.Explorer.GetEventConfig(tester.Source.Token);
            summary = tester.Explorer.GetSummary();
            Assert.True(summary.AnyEvents);
            Assert.False(summary.Auditing);
            Assert.True(summary.HistoricalEvents);
            Assert.Equal(2, summary.NumEmitters);
            Assert.Equal(1, summary.NumHistEmitters);

        }
    }
}

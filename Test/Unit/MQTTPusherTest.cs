using Cognite.Bridge;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet.Client;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class MQTTPusherTestFixture : BaseExtractorTestFixture
    {
        private static int idCounter;
        public MQTTPusherTestFixture() : base()
        {
        }
        public (CDFMockHandler, MQTTBridge, MQTTPusher) GetPusher()
        {
            Services.AddCogniteClient("appid", null, true, true, false);
            var mqttConfig = ConfigurationUtils.Read<BridgeConfig>("config.bridge.yml");
            mqttConfig.Mqtt.ClientId = $"opcua-mqtt-pusher-test-bridge-{idCounter}";
            mqttConfig.GenerateDefaults();
            mqttConfig.Cognite.Update = true;
            CommonTestUtils.AddDummyProvider(Config.Cognite.Project, CDFMockHandler.MockMode.None, true, Services);

            Services.AddSingleton(mqttConfig.Cognite);
            Services.AddCogniteClient("MQTT-CDF Bridge", null, true, true, false);
            Services.AddMultiTestLogging(Serilog.Events.LogEventLevel.Debug);
            var provider = Services.BuildServiceProvider();
            Config.Mqtt.ClientId = $"opcua-mqtt-pusher-test-{idCounter}";
            idCounter++;
            var log = provider.GetRequiredService<ILogger<MQTTPusher>>();
            var pusher = new MQTTPusher(log, provider, Config.Mqtt);
            var bridge = new MQTTBridge(new Destination(mqttConfig.Cognite, provider), mqttConfig,
                provider.GetRequiredService<ILogger<MQTTBridge>>());

            var handler = provider.GetRequiredService<CDFMockHandler>();
            return (handler, bridge, pusher);
        }
    }
    public sealed class MQTTPusherTest : IClassFixture<MQTTPusherTestFixture>, IDisposable
    {
        private readonly MQTTPusherTestFixture tester;
        private readonly MQTTBridge bridge;
        private readonly CDFMockHandler handler;
        private readonly MQTTPusher pusher;
        private CancellationTokenSource bridgeSource;

        public MQTTPusherTest(ITestOutputHelper output, MQTTPusherTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
            (handler, bridge, pusher) = tester.GetPusher();
            bridgeSource = new CancellationTokenSource();
            try
            {
                bridge.StartBridge(bridgeSource.Token).Wait();
            }
            catch
            {
                bridgeSource.Cancel();
                throw;
            }
            tester.Client.TypeManager.Reset();
        }

        [Fact]
        public async Task TestTestConnection()
        {
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            await pusher.Disconnect();
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            // Test auto-reconnect
            var client = (IMqttClient)pusher.GetType().GetField("client", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(pusher);
            await client.DisconnectAsync();
            Assert.False(client.IsConnected);
            await TestUtils.WaitForCondition(() => client.IsConnected, 5, "Expected client to reconnect automatically");
        }
        [Fact]
        public async Task TestPushDatapoints()
        {
            CommonTestUtils.ResetMetricValues("opcua_datapoint_push_failures_mqtt",
                "opcua_datapoints_pushed_mqtt", "opcua_datapoint_pushes_mqtt");

            handler.MockTimeseries("test-ts-double");
            var stringTs = handler.MockTimeseries("test-ts-string");
            stringTs.isString = true;

            // Null input
            Assert.Null(await pusher.PushDataPoints(null, tester.Source.Token));

            // Test filtering out dps
            var invalidDps = new[]
            {
                new UADataPoint(DateTime.MinValue, "test-ts-double", 123, StatusCodes.Good),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.NaN, StatusCodes.Good),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.NegativeInfinity, StatusCodes.Good),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.PositiveInfinity, StatusCodes.Good),
            };
            Assert.Null(await pusher.PushDataPoints(invalidDps, tester.Source.Token));

            tester.Config.DryRun = true;

            var time = DateTime.UtcNow;

            var dps = new[]
            {
                new UADataPoint(time, "test-ts-double", 123, StatusCodes.Good),
                new UADataPoint(time.AddSeconds(1), "test-ts-double", 321, StatusCodes.Good),
                new UADataPoint(time, "test-ts-string", "string", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(1), "test-ts-string", "string2", StatusCodes.Good),
                new UADataPoint(time, "test-ts-missing", "value", StatusCodes.Good)
            };

            // Debug true
            Assert.Null(await pusher.PushDataPoints(dps, tester.Source.Token));

            tester.Config.DryRun = false;

            // Missing timeseries, but the others should succeed
            var waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushDataPoints(dps, tester.Source.Token));
            await waitTask;

            Assert.Equal(2, handler.Datapoints["test-ts-double"].NumericDatapoints.Count);
            Assert.Equal(2, handler.Datapoints["test-ts-string"].StringDatapoints.Count);

            Assert.Equal(time.ToUnixTimeMilliseconds(), handler.Datapoints["test-ts-double"].NumericDatapoints.First().Timestamp);
            Assert.Equal(123, handler.Datapoints["test-ts-double"].NumericDatapoints.First().Value);
            Assert.Equal(time.ToUnixTimeMilliseconds(), handler.Datapoints["test-ts-string"].StringDatapoints.First().Timestamp);
            Assert.Equal("string", handler.Datapoints["test-ts-string"].StringDatapoints.First().Value);

            Assert.Equal(2, handler.Datapoints.Count);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoints_pushed_mqtt", 5, tester.Log));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_pushes_mqtt", 1));

            // Mismatched timeseries, handled by bridge
            dps = new[]
            {
                new UADataPoint(time.AddSeconds(2), "test-ts-double", "string", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(3), "test-ts-double", "string2", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(2), "test-ts-string", "string3", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(3), "test-ts-string", "string4", StatusCodes.Good),
                new UADataPoint(time, "test-ts-missing", "value", StatusCodes.Good)
            };
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushDataPoints(dps, tester.Source.Token));
            await waitTask;

            Assert.Equal(2, handler.Datapoints["test-ts-double"].NumericDatapoints.Count);
            Assert.Equal(4, handler.Datapoints["test-ts-string"].StringDatapoints.Count);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_push_failures_mqtt", 0));
        }
        [Fact]
        public async Task TestPushEvents()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);

            CommonTestUtils.ResetMetricValues("opcua_event_push_failures_mqtt",
                "opcua_events_pushed_mqtt", "opcua_event_pushes_mqtt");

            Assert.Null(await pusher.PushEvents(null, tester.Source.Token));
            var invalidEvents = new[]
            {
                new UAEvent
                {
                    Time = DateTime.MinValue
                },
                new UAEvent
                {
                    Time = DateTime.MaxValue
                }
            };
            Assert.Null(await pusher.PushEvents(invalidEvents, tester.Source.Token));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_skipped_events_mqtt", 2));

            handler.MockAsset(tester.Client.GetUniqueId(new NodeId("source", 0)));

            var time = DateTime.UtcNow;

            var events = new[]
            {
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter", 0),
                    SourceNode = new NodeId("source", 0),
                    EventType = new UAObjectType(new NodeId("type", 0)),
                    EventId = "someid"
                },
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter", 0),
                    SourceNode = new NodeId("missingsource", 0),
                    EventType = new UAObjectType(new NodeId("type", 0)),
                    EventId = "someid2"
                }
            };

            tester.Config.DryRun = true;
            Assert.Null(await pusher.PushEvents(events, tester.Source.Token));
            tester.Config.DryRun = false;

            var waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.Events.Count);
            Assert.Equal(1, handler.Events.First().Value.assetIds.First());
            Assert.Empty(handler.Events.Last().Value.assetIds);

            events = events.Append(new UAEvent
            {
                Time = time,
                EmittingNode = new NodeId("emitter", 0),
                SourceNode = new NodeId("source", 0),
                EventType = new UAObjectType(new NodeId("type", 0)),
                EventId = "someid3"
            }).ToArray();

            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            await waitTask;
            Assert.Equal(3, handler.Events.Count);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_event_pushes_mqtt", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_events_pushed_mqtt", 5));

            Assert.True(CommonTestUtils.TestMetricValue("opcua_event_push_failures_mqtt", 0));
        }
        [Fact]
        public async Task TestCreateUpdateAssets()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_mqtt");
            tester.Config.Mqtt.RawMetadata = null;

            var tss = Enumerable.Empty<UAVariable>();
            var rels = Enumerable.Empty<UAReference>();
            var update = new UpdateConfig();
            Assert.True((await pusher.PushNodes(Enumerable.Empty<BaseUANode>(), tss, rels, update, tester.Source.Token)).Objects);

            // Test debug mode
            var node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            tester.Config.DryRun = true;
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);
            tester.Config.DryRun = false;
            Assert.Empty(handler.Assets);

            // Create the asset
            node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            var waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);
            await waitTask;
            Assert.Single(handler.Assets);

            // Do nothing here, due to no update configured.
            node.Attributes.Description = "description";
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            // Do nothing again, due to no changes on the node
            update.Objects.Context = true;
            update.Objects.Description = true;
            update.Objects.Metadata = true;
            update.Objects.Name = true;
            node.Attributes.Description = null;
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);
            await waitTask;

            // Create new node
            var node2 = new UAObject(tester.Server.Ids.Custom.Root, "CustomRoot", null, null, NodeId.Null, null);
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).Objects);
            await waitTask;
            Assert.Equal(2, handler.Assets.Count);
            Assert.Null(handler.Assets.First().Value.description);
            Assert.Null(handler.Assets.Last().Value.description);

            // Update both nodes
            node.Attributes.Description = "description";
            node2.Attributes.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).Objects);
            await waitTask;
            Assert.Equal(2, handler.Assets.Count);
            Assert.Equal("description", handler.Assets.First().Value.description);
            Assert.Equal("description", handler.Assets.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_mqtt", 0));
        }
        [Fact]
        public async Task TestCreateUpdateRawAssets()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);

            tester.Config.Mqtt.RawMetadata = new RawMetadataConfig
            {
                AssetsTable = "assets",
                Database = "metadata"
            };
            var node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();
            var rels = Enumerable.Empty<UAReference>();

            // Create one
            var waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);
            await waitTask;
            Assert.Single(handler.AssetsRaw);
            Assert.Equal("BaseRoot", handler.AssetsRaw.First().Value.GetProperty("name").GetString());

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UAObject(tester.Server.Ids.Custom.Root, "CustomRoot", null, null, NodeId.Null, null);
            node.Attributes.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).Objects);
            await waitTask;
            Assert.Equal(2, handler.AssetsRaw.Count);
            Assert.Null(handler.AssetsRaw.First().Value.GetProperty("description").GetString());
            Assert.Null(handler.AssetsRaw.Last().Value.GetProperty("description").GetString());

            // Try to create again, skip both
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).Objects);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);
            Assert.Equal(2, handler.AssetsRaw.Count);
            Assert.Null(handler.AssetsRaw.First().Value.GetProperty("description").GetString());
            Assert.Null(handler.AssetsRaw.Last().Value.GetProperty("description").GetString());

            // Update due to update settings
            update.Objects.Description = true;
            node2.Attributes.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).Objects);
            await waitTask;
            Assert.Equal(2, handler.AssetsRaw.Count);
            Assert.Equal("description", handler.AssetsRaw.First().Value.GetProperty("description").GetString());
            Assert.Equal("description", handler.AssetsRaw.Last().Value.GetProperty("description").GetString());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_mqtt", 0));
        }
        [Fact]
        public async Task TestCreateUpdateTimeseries()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_mqtt");
            tester.Config.Mqtt.RawMetadata = null;

            var dt = new UADataType(DataTypeIds.Double);

            var assets = Enumerable.Empty<BaseUANode>();
            var update = new UpdateConfig();
            var rels = Enumerable.Empty<UAReference>();

            handler.MockAsset(tester.Client.GetUniqueId(new NodeId("parent", 0)));

            // Test debug mode
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", null, null, new NodeId("parent", 0), null);
            node.FullAttributes.DataType = dt;
            tester.Config.DryRun = true;
            var waitTask = bridge.WaitForNextMessage(1);
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);
            tester.Config.DryRun = false;
            Assert.Empty(handler.Timeseries);

            // Create the timeseries
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);
            await waitTask;
            Assert.Single(handler.Timeseries);
            Assert.Equal(1, handler.Timeseries.First().Value.assetId);

            // Do nothing due to no configured update
            node.Attributes.Description = "description";
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            // Do nothing again due to no changes on the node
            update.Variables.Context = true;
            update.Variables.Description = true;
            update.Variables.Metadata = true;
            update.Variables.Name = true;
            node.Attributes.Description = null;
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);
            await waitTask;

            // Create new node
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", null, null, new NodeId("parent", 0), null);
            node2.FullAttributes.DataType = dt;
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).Variables);
            await waitTask;
            Assert.Equal(2, handler.Timeseries.Count);
            Assert.Null(handler.Timeseries.First().Value.description);
            Assert.Null(handler.Timeseries.Last().Value.description);

            // Update both nodes
            node2.Attributes.Description = "description";
            node.Attributes.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).Variables);
            await waitTask;
            Assert.Equal(2, handler.Timeseries.Count);
            Assert.Equal("description", handler.Timeseries.First().Value.description);
            Assert.Equal("description", handler.Timeseries.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_mqtt", 0));
        }
        [Fact]
        public async Task TestCreateUpdateRawTimeseries()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_mqtt");

            tester.Config.Mqtt.RawMetadata = new RawMetadataConfig
            {
                TimeseriesTable = "timeseries",
                Database = "metadata"
            };

            var dt = new UADataType(DataTypeIds.Double);

            var assets = Enumerable.Empty<BaseUANode>();
            var rels = Enumerable.Empty<UAReference>();
            var update = new UpdateConfig();
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", null, null, new NodeId("parent", 0), null);
            node.FullAttributes.DataType = dt;

            // Create one
            var waitTask = bridge.WaitForNextMessage(topic: tester.Config.Mqtt.RawTopic);
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);
            await waitTask;
            Assert.Single(handler.TimeseriesRaw);
            Assert.Equal("Variable 1", handler.TimeseriesRaw.First().Value.GetProperty("name").GetString());

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", null, null, new NodeId("parent", 0), null);
            node2.FullAttributes.DataType = dt;
            node.Attributes.Description = "description";
            waitTask = bridge.WaitForNextMessage(topic: tester.Config.Mqtt.RawTopic);
            Assert.True((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).Variables);
            await waitTask;
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Null(handler.TimeseriesRaw.First().Value.GetProperty("description").GetString());
            Assert.Null(handler.TimeseriesRaw.Last().Value.GetProperty("description").GetString());

            // Try to create again, skip both
            waitTask = bridge.WaitForNextMessage(5, tester.Config.Mqtt.RawTopic);
            Assert.True((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).Variables);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Null(handler.TimeseriesRaw.First().Value.GetProperty("description").GetString());
            Assert.Null(handler.TimeseriesRaw.Last().Value.GetProperty("description").GetString());

            // Update due to update settings
            update.Variables.Description = true;
            node2.Attributes.Description = "description";
            waitTask = bridge.WaitForNextMessage(topic: tester.Config.Mqtt.RawTopic);
            Assert.True((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).Variables);
            await waitTask;
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Equal("description", handler.TimeseriesRaw.First().Value.GetProperty("description").GetString());
            Assert.Equal("description", handler.TimeseriesRaw.Last().Value.GetProperty("description").GetString());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_mqtt", 0));
        }
        [Fact]
        public async Task TestCreateRelationships()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_mqtt");

            var organizes = tester.Client.TypeManager.GetReferenceType(ReferenceTypeIds.Organizes);

            var source = new UAObject(new NodeId("source", 0), "Source", "Source", null, NodeId.Null, null);
            var target = new UAObject(new NodeId("target", 0), "Target", "Target", null, NodeId.Null, null);
            var sourceVar = new UAVariable(new NodeId("source2", 0), "Source", "Source", null, NodeId.Null, null);
            var targetVar = new UAVariable(new NodeId("target2", 0), "Target", "Target", null, NodeId.Null, null);

            var assets = Enumerable.Empty<BaseUANode>();
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();

            tester.Config.Extraction.Relationships.Enabled = true;

            var uaSource = new UANodeSource(tester.Log, extractor, tester.Client, tester.Client.TypeManager);


            // Push none
            var waitTask = bridge.WaitForNextMessage(1);
            Assert.True((await pusher.PushNodes(assets, tss, Enumerable.Empty<UAReference>(), update, tester.Source.Token)).References);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            var references = new List<UAReference>
            {
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target),
            };
            await extractor.TypeManager.LoadTypeData(uaSource, tester.Source.Token);

            // Push successful
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).References);
            await waitTask;
            Assert.Equal(2, handler.Relationships.Count);

            // Push again, with duplicates
            var references2 = new List<UAReference>
            {
                new UAReference(organizes, true, source, target),
                new UAReference(organizes, false, sourceVar, targetVar),
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target)
            };
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(assets, tss, references2, update, tester.Source.Token)).References);
            await waitTask;
            Assert.Equal(4, handler.Relationships.Count);
            var ids = new List<string>
            {
                "gp.Organizes;base:s=source;base:s=target",
                "gp.OrganizedBy;base:s=source2;base:s=target2",
                "gp.Organizes;base:s=source;base:s=target2",
                "gp.OrganizedBy;base:s=source2;base:s=target",
            };
            Assert.All(ids, id => Assert.True(handler.Relationships.ContainsKey(id)));

            // Test pushing all duplicates
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).References);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_mqtt", 0));
        }
        [Fact]
        public async Task TestCreateRawRelationships()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_mqtt");

            var organizes = tester.Client.TypeManager.GetReferenceType(ReferenceTypeIds.Organizes);

            var source = new UAObject(new NodeId("source", 0), "Source", "Source", null, NodeId.Null, null);
            var target = new UAObject(new NodeId("target", 0), "Target", "Target", null, NodeId.Null, null);
            var sourceVar = new UAVariable(new NodeId("source2", 0), "Source", "Source", null, NodeId.Null, null);
            var targetVar = new UAVariable(new NodeId("target2", 0), "Target", "Target", null, NodeId.Null, null);

            tester.Config.Mqtt.RawMetadata = new RawMetadataConfig
            {
                RelationshipsTable = "relationships",
                Database = "metadata"
            };

            tester.Config.Extraction.Relationships.Enabled = true;

            var uaSource = new UANodeSource(tester.Log, extractor, tester.Client, tester.Client.TypeManager);

            var assets = Enumerable.Empty<BaseUANode>();
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();

            // Push none
            var waitTask = bridge.WaitForNextMessage(1);
            Assert.True((await pusher.PushNodes(assets, tss, Enumerable.Empty<UAReference>(), update, tester.Source.Token)).References);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            var references = new List<UAReference>
            {
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target),
            };
            await extractor.TypeManager.LoadTypeData(uaSource, tester.Source.Token);

            // Push successful
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).References);
            await waitTask;
            Assert.Equal(2, handler.RelationshipsRaw.Count);

            // Push again, with duplicates
            var references2 = new List<UAReference>
            {
                new UAReference(organizes, true, source, target),
                new UAReference(organizes, false, sourceVar, targetVar),
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target)
            };
            waitTask = bridge.WaitForNextMessage();
            Assert.True((await pusher.PushNodes(assets, tss, references2, update, tester.Source.Token)).References);
            await waitTask;
            Assert.Equal(4, handler.RelationshipsRaw.Count);
            var ids = new List<string>
            {
                "gp.Organizes;base:s=source;base:s=target",
                "gp.OrganizedBy;base:s=source2;base:s=target2",
                "gp.Organizes;base:s=source;base:s=target2",
                "gp.OrganizedBy;base:s=source2;base:s=target",
            };
            Assert.All(ids, id => Assert.True(handler.RelationshipsRaw.ContainsKey(id)));

            // Test pushing all duplicates
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).References);
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_mqtt", 0));
        }

        [Fact]
        public async Task TestNodesState()
        {
            try
            {
                File.Delete("mqtt-state-store-1.db");
            }
            catch { }

            var stateStoreConfig = new StateStoreConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = "mqtt-state-store-1.db"
            };
            using var stateStore = new LiteDBStateStore(stateStoreConfig, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());
            using var extractor = tester.BuildExtractor(true, stateStore, pusher);
            CommonTestUtils.ResetMetricValues("opcua_node_ensure_failures_mqtt", "opcua_created_assets_mqtt",
                "opcua_created_timeseries_mqtt");
            tester.Config.Mqtt.RawMetadata = null;
            tester.Config.Mqtt.LocalState = "mqtt_state";

            var dt = new UADataType(DataTypeIds.Double);

            var rels = Enumerable.Empty<UAReference>();

            var ts = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", null, null, new NodeId("parent", 0), null);
            ts.FullAttributes.DataType = dt;
            var ts2 = new UAVariable(tester.Server.Ids.Base.DoubleVar2, "Variable 2", null, null, new NodeId("parent", 0), null);
            ts2.FullAttributes.DataType = dt;
            var node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            var node2 = new UAObject(tester.Server.Ids.Custom.Root, "BaseRoot", null, null, NodeId.Null, null);

            await pusher.PushNodes(new[] { node, node2 }, new[] { ts, ts2 }, rels, new UpdateConfig(), tester.Source.Token);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_assets_mqtt", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_timeseries_mqtt", 2));

            var existingNodes = (HashSet<string>)pusher.GetType()
                .GetField("existingNodes", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);

            Assert.Equal(4, existingNodes.Count);
            existingNodes.Clear();

            await pusher.PushNodes(new[] { node, node2 }, new[] { ts, ts2 }, rels, new UpdateConfig(), tester.Source.Token);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_assets_mqtt", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_timeseries_mqtt", 2));
            Assert.Equal(4, existingNodes.Count);

            try
            {
                File.Delete("mqtt-state-store-1.db");
            }
            catch { }
        }

        [Fact]
        public async Task TestReferencesState()
        {
            try
            {
                File.Delete("mqtt-state-store-2.db");
            }
            catch { }

            var organizes = tester.Client.TypeManager.GetReferenceType(ReferenceTypeIds.Organizes);

            var source = new UAObject(new NodeId("source", 0), "Source", "Source", null, NodeId.Null, null);
            var target = new UAObject(new NodeId("target", 0), "Target", "Target", null, NodeId.Null, null);
            var sourceVar = new UAVariable(new NodeId("source2", 0), "Source", "Source", null, NodeId.Null, null);
            var targetVar = new UAVariable(new NodeId("target2", 0), "Target", "Target", null, NodeId.Null, null);

            var stateStoreConfig = new StateStoreConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = "mqtt-state-store-2.db"
            };

            var assets = Enumerable.Empty<BaseUANode>();
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();

            using var stateStore = new LiteDBStateStore(stateStoreConfig, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());

            using var extractor = tester.BuildExtractor(true, stateStore, pusher);

            var uaSource = new UANodeSource(tester.Log, extractor, tester.Client, tester.Client.TypeManager);

            CommonTestUtils.ResetMetricValues("opcua_node_ensure_failures_mqtt", "opcua_created_relationships_mqtt");
            tester.Config.Mqtt.LocalState = "mqtt_state";

            var references = new List<UAReference>
            {
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target),
            };
            await extractor.TypeManager.LoadTypeData(uaSource, tester.Source.Token);

            await pusher.PushNodes(assets, tss, references, update, tester.Source.Token);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_relationships_mqtt", 2));

            var existingNodes = (HashSet<string>)pusher.GetType()
                .GetField("existingNodes", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);
            Assert.Equal(2, existingNodes.Count);

            existingNodes.Clear();

            await pusher.PushNodes(assets, tss, references, update, tester.Source.Token);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_relationships_mqtt", 2));
            Assert.Equal(2, existingNodes.Count);

            try
            {
                File.Delete("mqtt-state-store-2.db");
            }
            catch { }
        }
        public void Dispose()
        {
            bridge?.Dispose();
            pusher?.Dispose();
            bridgeSource?.Cancel();
            bridgeSource?.Dispose();
            bridgeSource = null;
        }
    }
}

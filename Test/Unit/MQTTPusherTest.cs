using Cognite.Bridge;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.TypeCollectors;
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
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class MQTTPusherTestFixture : BaseExtractorTestFixture
    {
        private static int idCounter;
        public MQTTPusherTestFixture() : base(63100)
        {
        }
        public (CDFMockHandler, MQTTBridge, MQTTPusher) GetPusher()
        {
            Services.AddCogniteClient("appid", true, true, false);
            var mqttConfig = ConfigurationUtils.Read<BridgeConfig>("config.bridge.yml");
            mqttConfig.Mqtt.ClientId = $"opcua-mqtt-pusher-test-bridge-{idCounter}";
            mqttConfig.GenerateDefaults();
            mqttConfig.Cognite.Update = true;
            var handler = new CDFMockHandler(mqttConfig.Cognite.Project, CDFMockHandler.MockMode.None)
            {
                StoreDatapoints = true
            };
            CommonTestUtils.AddDummyProvider(handler, Services);
            Services.AddSingleton(mqttConfig.Cognite);
            Services.AddCogniteClient("MQTT-CDF Bridge", true, true, false);
            var provider = Services.BuildServiceProvider();
            Config.Mqtt.ClientId = $"opcua-mqtt-pusher-test-{idCounter}";
            idCounter++;
            var pusher = Config.Mqtt.ToPusher(provider) as MQTTPusher;
            var bridge = new MQTTBridge(new Destination(mqttConfig.Cognite, provider), mqttConfig);
            return (handler, bridge, pusher);
        }
    }
    public class MQTTPusherTest : MakeConsoleWork, IClassFixture<MQTTPusherTestFixture>
    {
        private MQTTPusherTestFixture tester;
        private MQTTBridge bridge;
        private CDFMockHandler handler;
        private MQTTPusher pusher;

        public MQTTPusherTest(ITestOutputHelper output, MQTTPusherTestFixture tester) : base(output)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
            this.tester = tester;
            (handler, bridge, pusher) = tester.GetPusher();
            bridge.StartBridge(tester.Source.Token).Wait();
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
            await CommonTestUtils.WaitForCondition(() => client.IsConnected, 5, "Expected client to reconnect automatically");
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
                new UADataPoint(DateTime.MinValue, "test-ts-double", 123),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.NaN),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.NegativeInfinity),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.PositiveInfinity),
            };
            Assert.Null(await pusher.PushDataPoints(invalidDps, tester.Source.Token));

            tester.Config.Mqtt.Debug = true;

            var time = DateTime.UtcNow;

            var dps = new[]
            {
                new UADataPoint(time, "test-ts-double", 123),
                new UADataPoint(time.AddSeconds(1), "test-ts-double", 321),
                new UADataPoint(time, "test-ts-string", "string"),
                new UADataPoint(time.AddSeconds(1), "test-ts-string", "string2"),
                new UADataPoint(time, "test-ts-missing", "value")
            };

            // Debug true
            Assert.Null(await pusher.PushDataPoints(dps, tester.Source.Token));

            tester.Config.Mqtt.Debug = false;

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
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoints_pushed_mqtt", 5));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_pushes_mqtt", 1));

            // Mismatched timeseries, handled by bridge
            dps = new[]
            {
                new UADataPoint(time.AddSeconds(2), "test-ts-double", "string"),
                new UADataPoint(time.AddSeconds(3), "test-ts-double", "string2"),
                new UADataPoint(time.AddSeconds(2), "test-ts-string", "string3"),
                new UADataPoint(time.AddSeconds(3), "test-ts-string", "string4"),
                new UADataPoint(time, "test-ts-missing", "value")
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

            handler.MockAsset(tester.Client.GetUniqueId(new NodeId("source")));

            var time = DateTime.UtcNow;

            var events = new[]
            {
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter"),
                    SourceNode = new NodeId("source"),
                    EventType = new NodeId("type"),
                    EventId = "someid"
                },
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter"),
                    SourceNode = new NodeId("missingsource"),
                    EventType = new NodeId("type"),
                    EventId = "someid2"
                }
            };

            tester.Config.Mqtt.Debug = true;
            Assert.Null(await pusher.PushEvents(events, tester.Source.Token));
            tester.Config.Mqtt.Debug = false;

            var waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.Events.Count);
            Assert.Equal(1, handler.Events.First().Value.assetIds.First());
            Assert.Empty(handler.Events.Last().Value.assetIds);

            events = events.Append(new UAEvent
            {
                Time = time,
                EmittingNode = new NodeId("emitter"),
                SourceNode = new NodeId("source"),
                EventType = new NodeId("type"),
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
            var update = new UpdateConfig();
            Assert.True(await pusher.PushNodes(Enumerable.Empty<UANode>(), tss, update, tester.Source.Token));

            // Test debug mode
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null);
            tester.Config.Mqtt.Debug = true;
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            tester.Config.Mqtt.Debug = false;
            Assert.Empty(handler.Assets);

            // Create the asset
            node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null);
            var waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            await waitTask;
            Assert.Single(handler.Assets);

            // Do nothing here, due to no update configured.
            node.Description = "description";
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            // Do nothing again, due to no changes on the node
            update.Objects.Context = true;
            update.Objects.Description = true;
            update.Objects.Metadata = true;
            update.Objects.Name = true;
            node.Description = null;
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            await waitTask;

            // Create new node
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", NodeId.Null);
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.Assets.Count);
            Assert.Null(handler.Assets.First().Value.description);
            Assert.Null(handler.Assets.Last().Value.description);

            // Update both nodes
            node.Description = "description";
            node2.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
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
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null);
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();

            // Create one
            var waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            await waitTask;
            Assert.Single(handler.AssetRaw);
            Assert.Equal("BaseRoot", handler.AssetRaw.First().Value.name);

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", NodeId.Null);
            node.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.AssetRaw.Count);
            Assert.Null(handler.AssetRaw.First().Value.description);
            Assert.Null(handler.AssetRaw.Last().Value.description);

            // Try to create again, skip both
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);
            Assert.Equal(2, handler.AssetRaw.Count);
            Assert.Null(handler.AssetRaw.First().Value.description);
            Assert.Null(handler.AssetRaw.Last().Value.description);

            // Update due to update settings
            update.Objects.Description = true;
            node2.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.AssetRaw.Count);
            Assert.Equal("description", handler.AssetRaw.First().Value.description);
            Assert.Equal("description", handler.AssetRaw.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_mqtt", 0));
            tester.Config.Mqtt.RawMetadata = null;
        }
        [Fact]
        public async Task TestCreateUpdateTimeseries()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_mqtt");
            tester.Config.Mqtt.RawMetadata = null;

            var dt = new UADataType(DataTypeIds.Double);

            var assets = Enumerable.Empty<UANode>();
            var update = new UpdateConfig();

            handler.MockAsset(tester.Client.GetUniqueId(new NodeId("parent")));

            // Test debug mode
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent")) { DataType = dt };
            tester.Config.Mqtt.Debug = true;
            var waitTask = bridge.WaitForNextMessage(1);
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);
            Assert.Equal(2, node.Properties.Count);
            tester.Config.Mqtt.Debug = false;
            Assert.Empty(handler.Timeseries);

            // Create the timeseries
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            await waitTask;
            Assert.Single(handler.Timeseries);
            Assert.Equal(1, handler.Timeseries.First().Value.assetId);

            // Do nothing due to no configured update
            node.Description = "description";
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            // Do nothing again due to no changes on the node
            update.Variables.Context = true;
            update.Variables.Description = true;
            update.Variables.Metadata = true;
            update.Variables.Name = true;
            node.Description = null;
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            await waitTask;

            // Create new node
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", new NodeId("parent")) { DataType = dt };
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.Timeseries.Count);
            Assert.Null(handler.Timeseries.First().Value.description);
            Assert.Null(handler.Timeseries.Last().Value.description);

            // Update both nodes
            node2.Description = "description";
            node.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
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

            var assets = Enumerable.Empty<UANode>();
            var update = new UpdateConfig();
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent")) { DataType = dt };

            // Create one
            var waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            await waitTask;
            Assert.Single(handler.TimeseriesRaw);
            Assert.Equal("Variable 1", handler.TimeseriesRaw.First().Value.name);

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", new NodeId("parent")) { DataType = dt };
            node.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Null(handler.TimeseriesRaw.First().Value.description);
            Assert.Null(handler.TimeseriesRaw.Last().Value.description);

            // Try to create again, skip both
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Null(handler.TimeseriesRaw.First().Value.description);
            Assert.Null(handler.TimeseriesRaw.Last().Value.description);

            // Update due to update settings
            update.Variables.Description = true;
            node2.Description = "description";
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Equal("description", handler.TimeseriesRaw.First().Value.description);
            Assert.Equal("description", handler.TimeseriesRaw.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_mqtt", 0));
            tester.Config.Mqtt.RawMetadata = null;
        }
        [Fact]
        public async Task TestCreateRelationships()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            var mgr = new ReferenceTypeManager(tester.Client, extractor);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_mqtt");

            // Push none
            var waitTask = bridge.WaitForNextMessage(1);
            Assert.True(await pusher.PushReferences(Enumerable.Empty<UAReference>(), tester.Source.Token));
            await Assert.ThrowsAsync<TimeoutException>(() => waitTask);

            var references = new List<UAReference>
            {
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target2"), true, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target"), false, true, mgr),
            };
            await mgr.GetReferenceTypeDataAsync(tester.Source.Token);

            // Push successful
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushReferences(references, tester.Source.Token));
            await waitTask;
            Assert.Equal(2, handler.Relationships.Count);

            // Push again, with duplicates
            var references2 = new List<UAReference>
            {
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), true, true, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target2"), false, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target2"), true, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target"), false, true, mgr)
            };
            waitTask = bridge.WaitForNextMessage();
            Assert.True(await pusher.PushReferences(references2, tester.Source.Token));
            await waitTask;
            Assert.Equal(4, handler.Relationships.Count);
            var ids = new List<string>
            {
                "gp.Organizes;base:s=source;base:s=target",
                "gp.OrganizedBy;base:s=source2;base:s=target2",
                "gp.Organizes;base:s=source;base:s=target2",
                "gp.OrganizedBy;base:s=source2;base:s=target",
            };
            Assert.All(ids, id => Assert.Contains(handler.Relationships, rel => rel.Key == id));

            // Test pushing all duplicates
            waitTask = bridge.WaitForNextMessage(1);
            Assert.True(await pusher.PushReferences(references, tester.Source.Token));
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

            var ts = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent")) { DataType = dt };
            var ts2 = new UAVariable(tester.Server.Ids.Base.DoubleVar2, "Variable 2", new NodeId("parent")) { DataType = dt };
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null);
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "BaseRoot", NodeId.Null);

            await pusher.PushNodes(new[] { node, node2 }, new[] { ts, ts2 }, new UpdateConfig(), tester.Source.Token);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_assets_mqtt", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_timeseries_mqtt", 2));

            var existingNodes = (HashSet<string>)pusher.GetType()
                .GetField("existingNodes", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);

            Assert.Equal(4, existingNodes.Count);
            existingNodes.Clear();

            await pusher.PushNodes(new[] { node, node2 }, new[] { ts, ts2 }, new UpdateConfig(), tester.Source.Token);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_assets_mqtt", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_timeseries_mqtt", 2));
            Assert.Equal(4, existingNodes.Count);

            tester.Config.Mqtt.LocalState = null;


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

            var stateStoreConfig = new StateStoreConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = "mqtt-state-store-2.db"
            };
            using var stateStore = new LiteDBStateStore(stateStoreConfig, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());

            using var extractor = tester.BuildExtractor(true, stateStore, pusher);
            var mgr = new ReferenceTypeManager(tester.Client, extractor);
            CommonTestUtils.ResetMetricValues("opcua_node_ensure_failures_mqtt", "opcua_created_relationships_mqtt");
            tester.Config.Mqtt.LocalState = "mqtt_state";

            var references = new List<UAReference>
            {
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target2"), true, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target"), false, true, mgr),
            };
            await mgr.GetReferenceTypeDataAsync(tester.Source.Token);

            await pusher.PushReferences(references, tester.Source.Token);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_relationships_mqtt", 2));

            var existingNodes = (HashSet<string>)pusher.GetType()
                .GetField("existingNodes", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);
            Assert.Equal(2, existingNodes.Count);

            existingNodes.Clear();

            await pusher.PushReferences(references, tester.Source.Token);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_relationships_mqtt", 2));
            Assert.Equal(2, existingNodes.Count);
            tester.Config.Mqtt.LocalState = null;

            try
            {
                File.Delete("mqtt-state-store-2.db");
            }
            catch { }
        }
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                bridge.Dispose();
                pusher.Dispose();
            }
        }
    }
}

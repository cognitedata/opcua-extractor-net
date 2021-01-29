using Cognite.Bridge;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet.Client;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class MQTTPusherTestFixture : BaseExtractorTestFixture
    {
        private static int idCounter;
        public MQTTPusherTestFixture() : base(62900)
        {
        }
        public (CDFMockHandler, MQTTBridge, MQTTPusher) GetPusher()
        {
            Services.AddCogniteClient("appid", true, true, false);
            var mqttConfig = ConfigurationUtils.Read<BridgeConfig>("config.bridge.yml");
            mqttConfig.Mqtt.ClientId = $"opcua-mqtt-pusher-test-bridge-{idCounter}";
            mqttConfig.GenerateDefaults();
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

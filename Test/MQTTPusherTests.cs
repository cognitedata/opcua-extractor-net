using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Cognite.OpcUa;

namespace Test
{
    /// <summary>
    /// Tests for the full MQTT pipeline to CDF mocker.
    /// </summary>
    [CollectionDefinition("MQTTPusher_Tests", DisableParallelization = true)]
    public class MQTTPusherTests : MakeConsoleWork
    {
        public MQTTPusherTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        [Trait("Server", "array")]
        [Trait("Target", "MQTTPusher")]
        [Trait("Test", "mqttpushdata")]
        public async Task TestMqttPushData()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                PusherConfig = ConfigName.Mqtt,
                ServerName = ServerName.Array,
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();
            
            await tester.StartServer();
            tester.Server.PopulateArrayHistory();
            
            tester.StartExtractor();
            
            tester.Config.Extraction.AllowStringVariables = true;
            tester.Config.Extraction.MaxArraySize = 4;

            await tester.Extractor.Looper.WaitForNextPush();

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => state.IsStreaming), 20);
            tester.Server.UpdateNode(tester.Server.Ids.Custom.StringArray, new[] { "test 1", "test 1" });

            var waitTask = tester.Bridge.WaitForNextMessage();
            await tester.Extractor.Looper.WaitForNextPush();
            await waitTask;

            await tester.TerminateRunTask();

            Assert.Equal(4, tester.Handler.Assets.Count);
            Assert.Equal(10, tester.Handler.Timeseries.Count);
            Assert.True(tester.Handler.Datapoints.ContainsKey("gp.tl:i=2[0]"));
            Assert.Equal(1000, tester.Handler.Datapoints["gp.tl:i=2[0]"].NumericDatapoints.DistinctBy(dp => dp.Timestamp).Count());
            Assert.True(tester.Handler.Datapoints.ContainsKey("gp.tl:i=3[0]"));
            Assert.Equal(2, tester.Handler.Datapoints["gp.tl:i=3[0]"].StringDatapoints.DistinctBy(dp => dp.Timestamp).Count());
        }
        [Fact]
        [Trait("Server", "events")]
        [Trait("Target", "MQTTPusher")]
        [Trait("Test", "mqttpushevents")]
        public async Task TestMqttPushEvents()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                LogLevel = "debug",
                ConfigName = ConfigName.Events,
                PusherConfig = ConfigName.Mqtt,
                ServerName = ServerName.Events,
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();

            await tester.StartServer();
            tester.Server.PopulateEvents();

            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();
            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => state.IsStreaming), 20);

            var waitTask = tester.Bridge.WaitForNextMessage();
            tester.Server.TriggerEvents(100);
            await tester.Extractor.Looper.WaitForNextPush();
            await waitTask;

            await tester.TerminateRunTask();

            var eventTypes = tester.Handler.Events.Select(evt => evt.Value.type).Distinct();
            Assert.Equal(3, eventTypes.Count());
            var eventAssets = tester.Handler.Events.SelectMany(evt => evt.Value.assetIds).Distinct();
            Assert.Equal(2, eventAssets.Count());
            Assert.True(tester.Handler.Events.Values.All(evt => evt.assetIds.Any()));
        }

        [Fact]
        [Trait("Server", "array")]
        [Trait("Target", "MQTTPusher")]
        [Trait("Test", "mqttstate")]
        public async Task TestMqttState()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                PusherConfig = ConfigName.Mqtt,
                ServerName = ServerName.Array,
                StoreDatapoints = true,
                MqttState = true
            });
            tester.Config.Extraction.AllowStringVariables = true;
            tester.Config.Extraction.MaxArraySize = 4;
            tester.Config.History.Enabled = false;

            await tester.StartServer();

            await tester.ClearPersistentData();
            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();

            tester.Config.Extraction.AllowStringVariables = true;
            tester.Config.Extraction.MaxArraySize = 4;

            var waitTask = tester.Bridge.WaitForNextMessage();
            tester.Server.UpdateNode(tester.Server.Ids.Custom.Array, new[] { 1000, 1000, 1000, 1000 });
            await tester.Extractor.Looper.WaitForNextPush();
            await waitTask;

            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_assets_mqtt", 4));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_timeseries_mqtt", 10));
            CommonTestUtils.ResetTestMetrics();

            tester.Extractor.RestartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();

            waitTask = tester.Bridge.WaitForNextMessage();
            tester.Server.UpdateNode(tester.Server.Ids.Custom.Array, new[] { 1001, 1001, 1001, 1001 });
            await tester.Extractor.Looper.WaitForNextPush();
            await waitTask;

            await tester.TerminateRunTask();

            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_assets_mqtt", 0));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_created_timeseries_mqtt", 0));
            Assert.Equal(4, tester.Handler.Assets.Count);
            Assert.Equal(10, tester.Handler.Timeseries.Count);
        }
    }
}

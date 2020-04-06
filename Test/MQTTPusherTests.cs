using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    /// <summary>
    /// Tests for the full MQTT pipeline to CDF mocker.
    /// </summary>
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
                LogLevel = "debug",
                PusherConfig = ConfigName.Mqtt,
                ServerName = ServerName.Array,
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();
            tester.StartExtractor();

            tester.Config.Extraction.AllowStringVariables = true;
            tester.Config.Extraction.MaxArraySize = 4;

            await tester.Extractor.Looper.WaitForNextPush();

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => state.IsStreaming), 20);

            var waitTask = tester.Bridge.WaitForNextMessage();
            await tester.Extractor.Looper.WaitForNextPush();
            await waitTask;

            await tester.TerminateRunTask();

            Assert.Equal(5, tester.Handler.assets.Count);
            Assert.Equal(10, tester.Handler.timeseries.Count);
            Assert.True(tester.Handler.datapoints.ContainsKey("gp.efg:i=2[0]"));
            Assert.True(tester.Handler.datapoints["gp.efg:i=2[0]"].Item1.Count > 0);
            Assert.True(tester.Handler.datapoints.ContainsKey("gp.efg:i=3[0]"));
            Assert.True(tester.Handler.datapoints["gp.efg:i=3[0]"].Item2.Count > 0);
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
            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => state.IsStreaming), 20);

            var waitTask = tester.Bridge.WaitForNextMessage();
            await tester.Extractor.Looper.WaitForNextPush();
            await waitTask;

            await tester.TerminateRunTask();

            var eventTypes = tester.Handler.events.Select(evt => evt.Value.type).Distinct();
            Assert.Equal(3, eventTypes.Count());
            var eventAssets = tester.Handler.events.SelectMany(evt => evt.Value.assetIds).Distinct();
            Assert.Equal(2, eventAssets.Count());
            Assert.True(tester.Handler.events.Values.All(evt => evt.assetIds.Any()));

        }
    }
}

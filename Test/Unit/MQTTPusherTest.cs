using Cognite.Bridge;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Pushers;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet.Client;
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

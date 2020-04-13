using System;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using MQTTnet.Client.Subscribing;
using Serilog;

namespace Cognite.Bridge
{
    public sealed class MQTTBridge : IDisposable
    {
        private readonly BridgeConfig config;
        private readonly IMqttClientOptions options;
        private readonly IMqttClient client;

        private readonly ILogger log = Log.ForContext(typeof(MQTTBridge));

        private readonly Destination destination;
        private bool recFlag = false;
        public MQTTBridge(Destination destination, BridgeConfig config)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.destination = destination;
            var builder = new MqttClientOptionsBuilder()
                .WithClientId(config.MQTT.ClientId)
                .WithTcpServer(config.MQTT.Host, config.MQTT.Port)
                .WithCleanSession();

            if (config.MQTT.UseTls)
            {
                builder = builder.WithTls();
            }
            if (!string.IsNullOrEmpty(config.MQTT.Username) && !string.IsNullOrEmpty(config.MQTT.Host))
            {
                builder = builder.WithCredentials(config.MQTT.Username, config.MQTT.Password);
            }

            options = builder.Build();
            client = new MqttFactory().CreateMqttClient();
        }

        public async Task WaitForNextMessage(int timeout = 10)
        {
            recFlag = false;
            for (int i = 0; i < timeout * 10; i++)
            {
                if (recFlag) return;
                await Task.Delay(100);
            }

            throw new TimeoutException("Waiting for next message timed out");
        }
        public async Task<bool> StartBridge(CancellationToken token)
        {
            client.UseDisconnectedHandler(async e =>
            {
                await Task.Delay(TimeSpan.FromSeconds(5));
                try
                {
                    await client.ConnectAsync(options, token);
                }
                catch (Exception ex)
                {
                    log.Warning("Failed to reconnect to broker: {msg}", ex.Message);
                }
            });
            client.UseConnectedHandler(async _ =>
            {
                log.Information("MQTT client connected");
                await client.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(config.MQTT.AssetTopic)
                    .WithTopicFilter(config.MQTT.TSTopic)
                    .WithTopicFilter(config.MQTT.DatapointTopic)
                    .WithTopicFilter(config.MQTT.EventTopic)
                    .Build());
                log.Information("Subscribed to topics");
            });
            client.UseApplicationMessageReceivedHandler(async msg =>
            {
                bool success;
                if (msg.ApplicationMessage.Topic == config.MQTT.DatapointTopic)
                {
                    log.Verbose("Datapoints message from: {src}", msg.ClientId);
                    try
                    {
                        success = await destination.PushDatapoints(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, "Unexpected failure while pushing datapoints to CDF: {msg}", ex.Message);
                        success = false;
                    }
                }
                else if (msg.ApplicationMessage.Topic == config.MQTT.EventTopic)
                {
                    log.Verbose("Events message from: {src}", msg.ClientId);
                    try
                    {
                        success = await destination.PushEvents(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, "Unexpected failure while pushing events to CDF: {msg}", ex.Message);
                        success = false;
                    }
                }
                else if (msg.ApplicationMessage.Topic == config.MQTT.AssetTopic)
                {
                    log.Verbose("Assets message from: {src}", msg.ClientId);
                    try
                    {
                        success = await destination.PushAssets(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, "Unexpected failure while pushing assets to CDF: {msg}", ex.Message);
                        success = false;
                    }
                }
                else if (msg.ApplicationMessage.Topic == config.MQTT.TSTopic)
                {
                    log.Verbose("Timeseries message from: {src}", msg.ClientId);
                    try
                    {
                        success = await destination.PushTimeseries(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, "Unexpected failure while pushing timeseries to CDF: {msg}", ex.Message);
                        success = false;
                    }
                }
                else
                {
                    log.Warning("Unknown topic: {tpc}, this message will be ignored", msg.ApplicationMessage.Topic);
                    success = true;
                }

                msg.ProcessingFailed = !success;
                recFlag = true;
            });
            if (!client.IsConnected)
            {
                try
                {
                    await client.ConnectAsync(options, token);
                }
                catch (Exception e)
                {
                    log.Error(e, "Failed to connect to broker: {msg}", e.Message);
                    return false;
                }
            }


            log.Information("Successfully started MQTT bridge");
            return true;
        }
        public void Dispose()
        {
            client.Dispose();
        }
    }
}

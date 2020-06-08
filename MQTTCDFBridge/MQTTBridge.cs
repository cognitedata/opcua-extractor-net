using System;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using MQTTnet.Client.Subscribing;
using Polly;
using Serilog;

namespace Cognite.Bridge
{
    public sealed class MQTTBridge : IDisposable
    {
        private readonly BridgeConfig config;
        private readonly IMqttClientOptions options;
        private readonly IMqttClient client;

        private readonly ILogger log = Log.Logger.ForContext(typeof(MQTTBridge));

        private readonly Destination destination;
        private bool recFlag;

        private bool disconnected;
        public MQTTBridge(Destination destination, BridgeConfig config)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.destination = destination;
            var builder = new MqttClientOptionsBuilder()
                .WithClientId(config.Mqtt.ClientId)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(10))
                .WithTcpServer(config.Mqtt.Host, config.Mqtt.Port)
                .WithCleanSession();

            if (config.Mqtt.UseTls)
            {
                builder = builder.WithTls();
            }
            if (!string.IsNullOrEmpty(config.Mqtt.Username) && !string.IsNullOrEmpty(config.Mqtt.Host))
            {
                builder = builder.WithCredentials(config.Mqtt.Username, config.Mqtt.Password);
            }

            options = builder.Build();
            client = new MqttFactory().CreateMqttClient();
        }
        /// <summary>
        /// Wait for up to timeout seconds for a message to arrive over MQTT. Throws an exception if waiting timed out.
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
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
        /// <summary>
        /// Start the bridge, adding handlers then connecting to the broker.
        /// </summary>
        /// <returns>True on success</returns>
        public async Task<bool> StartBridge(CancellationToken token)
        {
            client.UseDisconnectedHandler(async e =>
            {
                log.Warning("MQTT Client disconnected");
                log.Debug(e.Exception, "MQTT client disconnected");
                if (disconnected) return;
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
                    .WithTopicFilter(config.Mqtt.AssetTopic)
                    .WithTopicFilter(config.Mqtt.TSTopic)
                    .WithTopicFilter(config.Mqtt.DatapointTopic)
                    .WithTopicFilter(config.Mqtt.EventTopic)
                    .Build());
                log.Information("Subscribed to topics");
            });
            client.UseApplicationMessageReceivedHandler(async msg =>
            {
                bool success;
                if (msg.ApplicationMessage.Topic == config.Mqtt.DatapointTopic)
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
                else if (msg.ApplicationMessage.Topic == config.Mqtt.EventTopic)
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
                else if (msg.ApplicationMessage.Topic == config.Mqtt.AssetTopic)
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
                else if (msg.ApplicationMessage.Topic == config.Mqtt.TSTopic)
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

        public bool IsConnected()
        {
            return client.IsConnected;
        }

        public async Task Disconnect()
        {
            disconnected = true;
            await client.DisconnectAsync();
        }
        public void Dispose()
        {
            disconnected = true;
            client.DisconnectAsync().Wait();
            client.Dispose();
        }
    }
}

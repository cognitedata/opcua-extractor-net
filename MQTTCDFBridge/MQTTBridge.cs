using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Bridge
{
    public sealed class MQTTBridge : IDisposable
    {
        private readonly BridgeConfig config;
        private readonly MqttClientOptions options;
        private readonly IMqttClient client;

        private readonly ILogger log;

        private readonly Destination destination;
        private TaskCompletionSource? waitSource;
        private string? waitTopic;

        private bool disconnected;
        public MQTTBridge(Destination destination, BridgeConfig config, ILogger log)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.destination = destination;
            this.log = log;
            var builder = new MqttClientOptionsBuilder()
                .WithClientId(config.Mqtt.ClientId)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(10))
                .WithTcpServer(config.Mqtt.Host, config.Mqtt.Port)
                .WithCleanSession()
                .WithTlsOptions(new MqttClientTlsOptions { UseTls = config.Mqtt.UseTls });

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
        public async Task WaitForNextMessage(int timeout = 50, string? topic = null)
        {
            waitSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            waitTopic = topic;
            var res = await Task.WhenAny(waitSource.Task, Task.Delay(timeout * 100));
            if (res != waitSource.Task) throw new TimeoutException("Waiting for next message timed out");
            waitSource = null;
            waitTopic = null;
        }
        /// <summary>
        /// Start the bridge, adding handlers then connecting to the broker.
        /// </summary>
        /// <returns>True on success</returns>
        public async Task<bool> StartBridge(CancellationToken token)
        {
            client.DisconnectedAsync += async e =>
            {
                if (disconnected || token.IsCancellationRequested) return;
                log.LogWarning("MQTT Client disconnected");
                log.LogDebug(e.Exception, "MQTT client disconnected");
                await Task.Delay(1000);
                try
                {
                    await client.ConnectAsync(options, token);
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to reconnect to broker: {Message}", ex.Message);
                }
            };
            client.ConnectedAsync += async _ =>
            {
                if (token.IsCancellationRequested) return;

                log.LogInformation("MQTT client connected");
                await client.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(config.Mqtt.AssetTopic, MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithTopicFilter(config.Mqtt.TsTopic, MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithTopicFilter(config.Mqtt.DatapointTopic, MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithTopicFilter(config.Mqtt.EventTopic, MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithTopicFilter(config.Mqtt.RawTopic, MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithTopicFilter(config.Mqtt.RelationshipTopic, MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(), token);
                log.LogInformation("Subscribed to topics");
            };
            client.ApplicationMessageReceivedAsync += async msg =>
            {
                if (token.IsCancellationRequested) return;

                bool success;

                if (msg.ApplicationMessage.Topic == config.Mqtt.DatapointTopic)
                {
                    log.LogTrace("Datapoints message from: {Source}", msg.ClientId);
                    try
                    {
                        success = await destination.PushDatapoints(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Unexpected failure while pushing datapoints to CDF: {Message}", ex.Message);
                        success = false;
                    }
                }
                else if (msg.ApplicationMessage.Topic == config.Mqtt.EventTopic)
                {
                    log.LogTrace("Events message from: {Source}", msg.ClientId);
                    try
                    {
                        success = await destination.PushEvents(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Unexpected failure while pushing events to CDF: {Message}", ex.Message);
                        success = false;
                    }
                }
                else if (msg.ApplicationMessage.Topic == config.Mqtt.AssetTopic)
                {
                    log.LogTrace("Assets message from: {Source}", msg.ClientId);
                    try
                    {
                        success = await destination.PushAssets(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Unexpected failure while pushing assets to CDF: {Message}", ex.Message);
                        success = false;
                    }
                }
                else if (msg.ApplicationMessage.Topic == config.Mqtt.TsTopic)
                {
                    log.LogTrace("Timeseries message from: {Source}", msg.ClientId);
                    try
                    {
                        success = await destination.PushTimeseries(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Unexpected failure while pushing timeseries to CDF: {Message}", ex.Message);
                        success = false;
                    }
                }
                else if (msg.ApplicationMessage.Topic == config.Mqtt.RawTopic)
                {
                    log.LogTrace("Raw message from: {Source}", msg.ClientId);
                    try
                    {
                        success = await destination.PushRaw(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Unexpected failure while pushing raw to CDF: {Message}", ex.Message);
                        success = false;
                    }
                }
                else if (msg.ApplicationMessage.Topic == config.Mqtt.RelationshipTopic)
                {
                    log.LogTrace("Relationships message from: {Source}", msg.ClientId);
                    try
                    {
                        success = await destination.PushRelationships(msg.ApplicationMessage, token);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Unexpected failure while pushing relationships to CDF: {Message}", ex.Message);
                        success = false;
                    }

                }
                else
                {
                    log.LogWarning("Unknown topic: {Topic}, this message will be ignored", msg.ApplicationMessage.Topic);
                    success = true;
                }

                msg.ProcessingFailed = !success;
                if (waitSource != null)
                {
                    if (waitTopic == null || waitTopic == msg.ApplicationMessage.Topic) waitSource?.SetResult();
                }
            };
            if (!client.IsConnected)
            {
                try
                {
                    await client.ConnectAsync(options, token);
                }
                catch (Exception e)
                {
                    log.LogError(e, "Failed to connect to broker: {Message}", e.Message);
                    return false;
                }
            }
            log.LogInformation("Successfully started MQTT bridge");
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
            client.DisconnectAsync().Wait(TimeSpan.FromSeconds(10));
            client.Dispose();
        }
    }
}

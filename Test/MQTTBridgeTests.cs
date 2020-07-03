﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Bridge;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    /// <summary>
    /// Tests for the MQTT bridge as a standalone tool.
    /// </summary>
    [Collection("Extractor tests")]
    public class MQTTBridgeTests : MakeConsoleWork
    {
        public MQTTBridgeTests(ITestOutputHelper output) : base(output) { }

        private sealed class BridgeTester : IDisposable
        {
            private readonly BridgeConfig config;
            private MQTTBridge bridge;
            public CDFMockHandler Handler { get; }
            private readonly IMqttClient client;
            private readonly MqttApplicationMessageBuilder baseBuilder;
            private readonly IServiceProvider provider;
            public BridgeTester(CDFMockHandler.MockMode mode)
            {
                var services = new ServiceCollection();
                config = services.AddConfig<BridgeConfig>("config.bridge.yml");
                Handler = new CDFMockHandler("project", mode)
                {
                    StoreDatapoints = true
                };
                config.Logger.Console.Level = "debug";
                CommonTestUtils.AddDummyProvider(Handler, services);
                services.AddLogger();
                services.AddCogniteClient("MQTT-CDF Bridge", true, true, false);
                provider = services.BuildServiceProvider();

                bridge = new MQTTBridge(new Destination(config.Cognite, provider), config);
                bridge.StartBridge(CancellationToken.None).Wait();
                var options = new MqttClientOptionsBuilder()
                    .WithClientId("test-mqtt-publisher")
                    .WithTcpServer(config.Mqtt.Host, config.Mqtt.Port)
                    .WithCleanSession()
                    .Build();
                client = new MqttFactory().CreateMqttClient();
                baseBuilder = new MqttApplicationMessageBuilder()
                    .WithAtLeastOnceQoS();
                client.ConnectAsync(options).Wait();
            }

            public async Task RecreateBridge()
            {
                bridge.Dispose();
                bridge = new MQTTBridge(new Destination(config.Cognite, provider), config);
                bool success = await bridge.StartBridge(CancellationToken.None);
                if (!success) throw new Exception("Unable to start bridge");
            }

            public async Task PublishAssets(IEnumerable<AssetCreate> assets)
            {
                if (!client.IsConnected) throw new InvalidOperationException("Client is not connected");
                var data = JsonSerializer.SerializeToUtf8Bytes(assets, null);

                var msg = baseBuilder
                    .WithPayload(data)
                    .WithTopic(config.Mqtt.AssetTopic)
                    .Build();

                var waitTask = bridge.WaitForNextMessage();
                await client.PublishAsync(msg);
                await waitTask;
            }

            public async Task PublishTimeseries(IEnumerable<StatelessTimeSeriesCreate> timeseries)
            {
                if (!client.IsConnected) throw new InvalidOperationException("Client is not connected");
                var data = JsonSerializer.SerializeToUtf8Bytes(timeseries, null);

                var msg = baseBuilder
                    .WithPayload(data)
                    .WithTopic(config.Mqtt.TsTopic)
                    .Build();

                var waitTask = bridge.WaitForNextMessage();
                await client.PublishAsync(msg);
                await waitTask;
            }

            public async Task PublishEvents(IEnumerable<StatelessEventCreate> events)
            {
                if (!client.IsConnected) throw new InvalidOperationException("Client is not connected");
                var data = JsonSerializer.SerializeToUtf8Bytes(events, null);

                var msg = baseBuilder
                    .WithPayload(data)
                    .WithTopic(config.Mqtt.EventTopic)
                    .Build();

                var waitTask = bridge.WaitForNextMessage();
                await client.PublishAsync(msg);
                await waitTask;
            }

            public async Task PublishDatapoints(DataPointInsertionRequest dps)
            {
                if (!client.IsConnected) throw new InvalidOperationException("Client is not connected");
                var data = dps.ToByteArray();

                var msg = baseBuilder
                    .WithPayload(data)
                    .WithTopic(config.Mqtt.DatapointTopic)
                    .Build();

                var waitTask = bridge.WaitForNextMessage();
                await client.PublishAsync(msg);
                await waitTask;
            }

            public async Task PublishRawAssets(IEnumerable<AssetCreate> assets)
            {
                if (!client.IsConnected) throw new InvalidOperationException("Client is not connected");
                // Using assets because that is already dealt with in the handler,
                // the bridge should support anything
                var wrapper = new RawRequestWrapper<AssetCreate>
                {
                    Database = "metadata",
                    Table = "assets",
                    Rows = assets.Select(asset => new RawRowCreateDto<AssetCreate> { Key = asset.ExternalId, Columns = asset })
                };
                var data = JsonSerializer.SerializeToUtf8Bytes(wrapper, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

                var msg = baseBuilder
                    .WithPayload(data)
                    .WithTopic(config.Mqtt.RawTopic)
                    .Build();

                var waitTask = bridge.WaitForNextMessage();
                await client.PublishAsync(msg);
                await waitTask;
            }


            public void Dispose()
            {
                bridge?.Dispose();
                client?.Dispose();
            }
        }

        [Fact]
        [Trait("Server", "none")]
        [Trait("Target", "MQTTBridge")]
        [Trait("Test", "mqttcreateassets")]
        public async Task TestCreateAssets()
        {
            using var tester = new BridgeTester(CDFMockHandler.MockMode.None);
            var roundOne = new List<AssetCreate>
            {
                new AssetCreate
                {
                    ExternalId = "test-asset-1",
                    Name = "test-asset-1",
                },
                new AssetCreate
                {
                    ExternalId = "test-asset-2",
                    Name = "test-asset-2",
                    ParentExternalId = "test-asset-1"
                }
            };
            // One existing, one new with old parent
            var roundTwo = new List<AssetCreate>
            {
                new AssetCreate
                {
                    ExternalId = "test-asset-2",
                    Name = "test-asset-2",
                    ParentExternalId = "test-asset-1"
                },
                new AssetCreate
                {
                    ExternalId = "test-asset-3",
                    Name = "test-asset-3",
                    ParentExternalId = "test-asset-1"
                }
            };
            var roundThree = new List<AssetCreate>
            {
                new AssetCreate
                {
                    ExternalId = "test-asset-2",
                    Name = "test-asset-2",
                    ParentExternalId = "test-asset-1"
                },
                new AssetCreate
                {
                    ExternalId = "test-asset-4",
                    Name = "test-asset-4",
                    ParentExternalId = "test-asset-1"
                }
            };
            await tester.PublishAssets(roundOne);
            Assert.Equal(2, tester.Handler.Assets.Count);
            Assert.True(tester.Handler.Assets.ContainsKey("test-asset-1"));
            Assert.True(tester.Handler.Assets.ContainsKey("test-asset-2"));
            await tester.PublishAssets(roundOne);
            Assert.Equal(2, tester.Handler.Assets.Count);
            await tester.PublishAssets(roundTwo);
            Assert.Equal(3, tester.Handler.Assets.Count);
            Assert.True(tester.Handler.Assets.ContainsKey("test-asset-1"));
            Assert.True(tester.Handler.Assets.ContainsKey("test-asset-2"));
            Assert.True(tester.Handler.Assets.ContainsKey("test-asset-3"));
            var asset3 = tester.Handler.Assets["test-asset-3"];
            Assert.Equal("test-asset-1", asset3.parentExternalId);
            await tester.RecreateBridge();
            await tester.PublishAssets(roundThree);
            Assert.Equal(4, tester.Handler.Assets.Count);
            Assert.True(tester.Handler.Assets.ContainsKey("test-asset-4"));
        }
        [Fact]
        [Trait("Server", "none")]
        [Trait("Target", "MQTTBridge")]
        [Trait("Test", "mqttcreatetimeseries")]
        public async Task TestCreateTimeseries()
        {
            using var tester = new BridgeTester(CDFMockHandler.MockMode.None);
            var assets = new List<AssetCreate>
            {
                new AssetCreate
                {
                    ExternalId = "test-asset-1",
                    Name = "test-asset-1",
                },
                new AssetCreate
                {
                    ExternalId = "test-asset-2",
                    Name = "test-asset-2",
                    ParentExternalId = "test-asset-1"
                }
            };
            var roundOne = new List<StatelessTimeSeriesCreate>
            {
                new StatelessTimeSeriesCreate
                {
                    AssetExternalId = "test-asset-1",
                    ExternalId = "test-ts-1",
                    Name = "test-ts-1"
                },
                new StatelessTimeSeriesCreate
                {
                    AssetExternalId = "test-asset-2",
                    ExternalId = "test-ts-2",
                    Name = "test-ts-2"
                }
            };
            var roundTwo = new List<StatelessTimeSeriesCreate>
            {
                new StatelessTimeSeriesCreate
                {
                    AssetExternalId = "test-asset-3",
                    ExternalId = "test-ts-3",
                    Name = "test-ts-3"
                },
                new StatelessTimeSeriesCreate
                {
                    AssetExternalId = "test-asset-2",
                    Name = "test-ts-1",
                    ExternalId = "test-ts-1"
                },
                new StatelessTimeSeriesCreate
                {
                    AssetExternalId = "test-asset-2",
                    Name = "test-ts-4",
                    ExternalId = "test-ts-4"
                }
            };
            var roundThree = new List<StatelessTimeSeriesCreate>
            {
                new StatelessTimeSeriesCreate
                {
                    AssetExternalId = "test-asset-1",
                    ExternalId = "test-ts-5",
                    Name = "test-ts-5"
                },
                new StatelessTimeSeriesCreate
                {
                    AssetExternalId = "test-asset-3",
                    ExternalId = "test-ts-6",
                    Name = "test-ts-6"
                },
                new StatelessTimeSeriesCreate
                {
                    AssetExternalId = "test-asset-1",
                    ExternalId = "test-ts-1",
                    Name = "test-ts-1"
                }
            };
            await tester.PublishAssets(assets);
            Assert.Equal(2, tester.Handler.Assets.Count);
            Assert.Contains(tester.Handler.Assets.Values, asset => asset.name == "test-asset-1");
            Assert.Contains(tester.Handler.Assets.Values, asset => asset.name == "test-asset-2");
            await tester.PublishTimeseries(roundOne);
            Assert.Equal(2, tester.Handler.Timeseries.Count);
            Assert.Contains(tester.Handler.Timeseries.Values, ts => ts.name == "test-ts-1");
            Assert.Contains(tester.Handler.Timeseries.Values, ts => ts.name == "test-ts-2");
            await tester.PublishTimeseries(roundOne);
            Assert.Equal(2, tester.Handler.Timeseries.Count);
            await tester.PublishTimeseries(roundTwo);
            Assert.Equal(4, tester.Handler.Timeseries.Count);
            Assert.Contains(tester.Handler.Timeseries.Values, ts => ts.name == "test-ts-3");
            Assert.Contains(tester.Handler.Timeseries.Values, ts => ts.name == "test-ts-4");
            await tester.RecreateBridge();
            await tester.PublishTimeseries(roundThree);
            Assert.Equal(6, tester.Handler.Timeseries.Count);
            Assert.Contains(tester.Handler.Timeseries.Values, ts => ts.name == "test-ts-5");
            Assert.Contains(tester.Handler.Timeseries.Values, ts => ts.name == "test-ts-6");
        }
        [Fact]
        [Trait("Server", "none")]
        [Trait("Target", "MQTTBridge")]
        [Trait("Test", "mqttcreatedata")]
        public async Task TestCreateDatapoints()
        {
            using var tester = new BridgeTester(CDFMockHandler.MockMode.None);
            var timeseries = new List<StatelessTimeSeriesCreate>
            {
                new StatelessTimeSeriesCreate
                {
                    ExternalId = "test-ts-1",
                    Name = "test-ts-1"
                },
                new StatelessTimeSeriesCreate
                {
                    ExternalId = "test-ts-2",
                    Name = "test-ts-2",
                    IsString = true
                }
            };
            long now = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
            var roundOne = new DataPointInsertionRequest();
            var item1 = new DataPointInsertionItem
            {
                ExternalId = "test-ts-1", NumericDatapoints = new NumericDatapoints()
            };
            item1.NumericDatapoints.Datapoints.Add(new NumericDatapoint
            {
                Timestamp = now,
                Value = 1.5
            });
            item1.NumericDatapoints.Datapoints.Add(new NumericDatapoint
            {
                Timestamp = now + 10,
                Value = 2.5
            });
            var item2 = new DataPointInsertionItem
            {
                ExternalId = "test-ts-2", StringDatapoints = new StringDatapoints()
            };
            item2.StringDatapoints.Datapoints.Add(new StringDatapoint
            {
                Timestamp = now,
                Value = "test1"
            });
            item2.StringDatapoints.Datapoints.Add(new StringDatapoint
            {
                Timestamp = now + 10,
                Value = "test2"
            });
            roundOne.Items.Add(item1);
            roundOne.Items.Add(item2);

            var roundTwo = new DataPointInsertionRequest();
            var item3 = new DataPointInsertionItem
            {
                ExternalId = "test-ts-1", NumericDatapoints = new NumericDatapoints()
            };
            item3.NumericDatapoints.Datapoints.Add(new NumericDatapoint
            {
                Timestamp = now + 20,
                Value = 3.5
            });
            var item4 = new DataPointInsertionItem
            {
                ExternalId = "test-ts-2", NumericDatapoints = new NumericDatapoints()
            };
            item4.NumericDatapoints.Datapoints.Add(new NumericDatapoint
            {
                Timestamp = now,
                Value = 0
            });
            var item5 = new DataPointInsertionItem
            {
                ExternalId = "test-ts-5", NumericDatapoints = new NumericDatapoints()
            };
            item5.NumericDatapoints.Datapoints.Add(new NumericDatapoint
            {
                Timestamp = now,
                Value = 0
            });
            roundTwo.Items.Add(item3);
            roundTwo.Items.Add(item4);
            roundTwo.Items.Add(item5);

            var roundThree = new DataPointInsertionRequest();
            var item6 = new DataPointInsertionItem
            {
                ExternalId = "test-ts-1", NumericDatapoints = new NumericDatapoints()
            };
            item6.NumericDatapoints.Datapoints.Add(new NumericDatapoint
            {
                Timestamp = now + 40,
                Value = 3.5
            });
            var item7 = new DataPointInsertionItem
            {
                ExternalId = "test-ts-2", NumericDatapoints = new NumericDatapoints()
            };
            item7.NumericDatapoints.Datapoints.Add(new NumericDatapoint
            {
                Timestamp = now + 20,
                Value = 0
            });
            var item8 = new DataPointInsertionItem
            {
                ExternalId = "test-ts-5", NumericDatapoints = new NumericDatapoints()
            };
            item8.NumericDatapoints.Datapoints.Add(new NumericDatapoint
            {
                Timestamp = now + 20,
                Value = 0
            });
            roundThree.Items.Add(item6);
            roundThree.Items.Add(item7);
            roundThree.Items.Add(item8);


            await tester.PublishTimeseries(timeseries);
            Assert.Equal(2, tester.Handler.Timeseries.Count);
            await tester.PublishDatapoints(roundOne);
            Assert.True(tester.Handler.Datapoints.ContainsKey("test-ts-1"));
            Assert.True(tester.Handler.Datapoints.ContainsKey("test-ts-2"));
            Assert.Equal(2, tester.Handler.Datapoints.Count);
            Assert.Equal(2, tester.Handler.Datapoints["test-ts-1"].NumericDatapoints.Count);
            Assert.Equal(2, tester.Handler.Datapoints["test-ts-2"].StringDatapoints.Count);
            await tester.PublishDatapoints(roundTwo);
            Assert.False(tester.Handler.Datapoints.ContainsKey("test-ts-5"));
            Assert.Equal(2, tester.Handler.Datapoints.Count);
            Assert.Equal(3, tester.Handler.Datapoints["test-ts-1"].NumericDatapoints.Count);
            Assert.Equal(2, tester.Handler.Datapoints["test-ts-2"].StringDatapoints.Count);
            Assert.Empty(tester.Handler.Datapoints["test-ts-2"].NumericDatapoints);
            await tester.RecreateBridge();
            await tester.PublishDatapoints(roundThree);
            Assert.Equal(2, tester.Handler.Datapoints.Count);
            Assert.Equal(4, tester.Handler.Datapoints["test-ts-1"].NumericDatapoints.Count);
            Assert.Equal(2, tester.Handler.Datapoints["test-ts-2"].StringDatapoints.Count);
            Assert.Empty(tester.Handler.Datapoints["test-ts-2"].NumericDatapoints);
        }

        [Fact]
        [Trait("Server", "none")]
        [Trait("Target", "MQTTBridge")]
        [Trait("Test", "mqttcreateevents")]
        public async Task TestCreateEvents()
        {
            using var tester = new BridgeTester(CDFMockHandler.MockMode.None);
            var assets = new List<AssetCreate>
            {
                new AssetCreate
                {
                    ExternalId = "test-asset-1",
                    Name = "test-asset-1",
                },
                new AssetCreate
                {
                    ExternalId = "test-asset-2",
                    Name = "test-asset-2",
                    ParentExternalId = "test-asset-1"
                }
            };
            var roundOne = new List<StatelessEventCreate>
            {
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-1", "test-asset-2"},
                    ExternalId = "test-event-1"
                },
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-2"},
                    ExternalId = "test-event-2"
                },
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-1"},
                    ExternalId = "test-event-3"
                }
            };
            var roundTwo = new List<StatelessEventCreate>
            {
                new StatelessEventCreate
                {
                    AssetExternalIds = Array.Empty<string>(),
                    ExternalId = "test-event-4"
                },
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-3"},
                    ExternalId = "test-event-5"
                },
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-1"},
                    ExternalId = "test-event-3"
                },
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-1", "test-asset-3"},
                    ExternalId = "test-event-6"
                }
            };
            var roundThree = new List<StatelessEventCreate>
            {
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-1", "test-asset-2"},
                    ExternalId = "test-event-7"
                },
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-3", "test-asset-2"},
                    ExternalId = "test-event-8"
                },
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-3", "test-asset-2"},
                    ExternalId = "test-event-1"
                },
                new StatelessEventCreate
                {
                    AssetExternalIds = new[] {"test-asset-3"},
                    ExternalId = "test-event-9"
                }
            };
            await tester.PublishAssets(assets);
            Assert.Equal(2, tester.Handler.Assets.Count);
            await tester.PublishEvents(roundOne);
            Assert.Equal(3, tester.Handler.Events.Count);
            Assert.True(tester.Handler.Events.ContainsKey("test-event-1"));
            Assert.True(tester.Handler.Events.ContainsKey("test-event-2"));
            Assert.True(tester.Handler.Events.ContainsKey("test-event-3"));
            await tester.PublishEvents(roundOne);
            Assert.Equal(3, tester.Handler.Events.Count);
            await tester.PublishEvents(roundTwo);
            Assert.Equal(5, tester.Handler.Events.Count);
            Assert.True(tester.Handler.Events.ContainsKey("test-event-1"));
            Assert.True(tester.Handler.Events.ContainsKey("test-event-2"));
            Assert.True(tester.Handler.Events.ContainsKey("test-event-3"));
            Assert.True(tester.Handler.Events.ContainsKey("test-event-4"));
            Assert.False(tester.Handler.Events.ContainsKey("test-event-5"));
            Assert.True(tester.Handler.Events.ContainsKey("test-event-6"));
            Assert.Empty(tester.Handler.Events["test-event-4"].assetIds);
            Assert.Equal(2, tester.Handler.Events["test-event-1"].assetIds.Count());
            Assert.Single(tester.Handler.Events["test-event-2"].assetIds);
            Assert.Single(tester.Handler.Events["test-event-6"].assetIds);
            await tester.RecreateBridge();
            await tester.PublishEvents(roundThree);
            Assert.Equal(7, tester.Handler.Events.Count);
            Assert.True(tester.Handler.Events.ContainsKey("test-event-7"));
            Assert.True(tester.Handler.Events.ContainsKey("test-event-8"));
            Assert.False(tester.Handler.Events.ContainsKey("test-event-9"));
            Assert.Equal(2, tester.Handler.Events["test-event-7"].assetIds.Count());
            Assert.Single(tester.Handler.Events["test-event-8"].assetIds);
        }
        [Fact]
        [Trait("Server", "none")]
        [Trait("Target", "MQTTBridge")]
        [Trait("Test", "mqttraw")]
        public async Task TestMqttRaw()
        {
            using var tester = new BridgeTester(CDFMockHandler.MockMode.None);

            var roundOne = new AssetCreate[]
            {
                new AssetCreate
                {
                    ExternalId = "test-asset-1",
                    Name = "test-asset-1"
                },
                new AssetCreate
                {
                    ExternalId = "test-asset-2",
                    Name = "test-asset-2"
                },
                new AssetCreate
                {
                    ExternalId = "test-asset-1",
                    Name = "test-asset-3"
                }
            };

            var roundTwo = new AssetCreate[]
            {
                new AssetCreate
                {
                    ExternalId = "test-asset-1",
                    Name = "test-asset-1",
                    Metadata = new Dictionary<string, string>
                    {
                        { "test-prop", "test-value" }
                    },
                },
                new AssetCreate
                {
                    ExternalId = "test-asset-3",
                    Name = "test-asset-3"
                }
            };

            await tester.PublishRawAssets(roundOne);
            Assert.Equal(2, tester.Handler.AssetRaw.Count);
            Assert.True(tester.Handler.AssetRaw.ContainsKey("test-asset-1"));
            Assert.True(tester.Handler.AssetRaw.ContainsKey("test-asset-2"));
            await tester.PublishRawAssets(roundTwo);
            Assert.Equal(3, tester.Handler.AssetRaw.Count);
            Assert.Contains(tester.Handler.AssetRaw, kvp => kvp.Value.name == "test-asset-3");
            Assert.True(tester.Handler.AssetRaw.ContainsKey("test-asset-1"));
            var asset1 = tester.Handler.AssetRaw["test-asset-1"];
            Assert.Equal("test-value", asset1.metadata["test-prop"]);
        }
        class StatelessEventCreate : EventCreate
        {
            public IEnumerable<string> AssetExternalIds { get; set; }
        }

        class StatelessTimeSeriesCreate : TimeSeriesCreate
        {
            public string AssetExternalId { get; set; }
        }

        class RawRequestWrapper<T>
        {
            public string Database { get; set; }
            public string Table { get; set; }
            public IEnumerable<RawRowCreateDto<T>> Rows { get; set; }
        }

        class RawRowCreateDto<T>
        {
            public string Key { get; set; }
            public T Columns { get; set; }
        }
    }
}

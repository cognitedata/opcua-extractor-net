using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Bridge;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    /// <summary>
    /// Tests for the MQTT bridge as a standalone tool.
    /// </summary>
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
            public BridgeTester(CDFMockHandler.MockMode mode)
            {
                Handler = new CDFMockHandler("project", mode)
                {
                    StoreDatapoints = true
                };
                config = Config.GetConfig("config.bridge.yml");
                config.Logging.ConsoleLevel = "verbose";
                Logger.Configure(config.Logging);
                bridge = new MQTTBridge(new Destination(config.CDF, CommonTestUtils.GetDummyProvider(Handler)), config);
                bridge.StartBridge(CancellationToken.None).Wait();
                var options = new MqttClientOptionsBuilder()
                    .WithClientId("test-mqtt-publisher")
                    .WithTcpServer(config.MQTT.Host, config.MQTT.Port)
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
                bridge = new MQTTBridge(new Destination(config.CDF, CommonTestUtils.GetDummyProvider(Handler)), config);
                bool success = await bridge.StartBridge(CancellationToken.None);
                if (!success) throw new Exception("Unable to start bridge");
            }

            public async Task PublishAssets(IEnumerable<AssetCreate> assets)
            {
                if (!client.IsConnected) throw new InvalidOperationException("Client is not connected");
                var data = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(assets, null);

                var msg = baseBuilder
                    .WithPayload(data)
                    .WithTopic(config.MQTT.AssetTopic)
                    .Build();

                var waitTask = bridge.WaitForNextMessage();
                await client.PublishAsync(msg);
                await waitTask;
            }

            public async Task PublishTimeseries(IEnumerable<StatelessTimeSeriesCreate> timeseries)
            {
                if (!client.IsConnected) throw new InvalidOperationException("Client is not connected");
                var data = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(timeseries, null);

                var msg = baseBuilder
                    .WithPayload(data)
                    .WithTopic(config.MQTT.TSTopic)
                    .Build();

                var waitTask = bridge.WaitForNextMessage();
                await client.PublishAsync(msg);
                await waitTask;
            }

            public async Task PublishEvents(IEnumerable<StatelessEventCreate> events)
            {
                if (!client.IsConnected) throw new InvalidOperationException("Client is not connected");
                var data = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(events, null);

                var msg = baseBuilder
                    .WithPayload(data)
                    .WithTopic(config.MQTT.EventTopic)
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
                    .WithTopic(config.MQTT.DatapointTopic)
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
            Assert.Equal(2, tester.Handler.assets.Count);
            Assert.True(tester.Handler.assets.ContainsKey("test-asset-1"));
            Assert.True(tester.Handler.assets.ContainsKey("test-asset-2"));
            await tester.PublishAssets(roundOne);
            Assert.Equal(2, tester.Handler.assets.Count);
            await tester.PublishAssets(roundTwo);
            Assert.Equal(3, tester.Handler.assets.Count);
            Assert.True(tester.Handler.assets.ContainsKey("test-asset-1"));
            Assert.True(tester.Handler.assets.ContainsKey("test-asset-2"));
            Assert.True(tester.Handler.assets.ContainsKey("test-asset-3"));
            var asset3 = tester.Handler.assets["test-asset-3"];
            Assert.Equal("test-asset-1", asset3.parentExternalId);
            await tester.RecreateBridge();
            await tester.PublishAssets(roundThree);
            Assert.Equal(4, tester.Handler.assets.Count);
            Assert.True(tester.Handler.assets.ContainsKey("test-asset-4"));
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
            Assert.Equal(2, tester.Handler.assets.Count);
            Assert.Contains(tester.Handler.assets.Values, asset => asset.name == "test-asset-1");
            Assert.Contains(tester.Handler.assets.Values, asset => asset.name == "test-asset-2");
            await tester.PublishTimeseries(roundOne);
            Assert.Equal(2, tester.Handler.timeseries.Count);
            Assert.Contains(tester.Handler.timeseries.Values, ts => ts.name == "test-ts-1");
            Assert.Contains(tester.Handler.timeseries.Values, ts => ts.name == "test-ts-2");
            await tester.PublishTimeseries(roundOne);
            Assert.Equal(2, tester.Handler.timeseries.Count);
            await tester.PublishTimeseries(roundTwo);
            Assert.Equal(3, tester.Handler.timeseries.Count);
            Assert.DoesNotContain(tester.Handler.timeseries.Values, ts => ts.name == "test-ts-3");
            Assert.Contains(tester.Handler.timeseries.Values, ts => ts.name == "test-ts-4");
            await tester.RecreateBridge();
            await tester.PublishTimeseries(roundThree);
            Assert.Equal(4, tester.Handler.timeseries.Count);
            Assert.Contains(tester.Handler.timeseries.Values, ts => ts.name == "test-ts-5");
            Assert.DoesNotContain(tester.Handler.timeseries.Values, ts => ts.name == "test-ts-6");
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
            Assert.Equal(2, tester.Handler.timeseries.Count);
            await tester.PublishDatapoints(roundOne);
            Assert.True(tester.Handler.datapoints.ContainsKey("test-ts-1"));
            Assert.True(tester.Handler.datapoints.ContainsKey("test-ts-2"));
            Assert.Equal(2, tester.Handler.datapoints.Count);
            Assert.Equal(2, tester.Handler.datapoints["test-ts-1"].Item1.Count);
            Assert.Equal(2, tester.Handler.datapoints["test-ts-2"].Item2.Count);
            await tester.PublishDatapoints(roundTwo);
            Assert.False(tester.Handler.datapoints.ContainsKey("test-ts-5"));
            Assert.Equal(2, tester.Handler.datapoints.Count);
            Assert.Equal(3, tester.Handler.datapoints["test-ts-1"].Item1.Count);
            Assert.Equal(2, tester.Handler.datapoints["test-ts-2"].Item2.Count);
            Assert.Empty(tester.Handler.datapoints["test-ts-2"].Item1);
            await tester.RecreateBridge();
            await tester.PublishDatapoints(roundThree);
            Assert.Equal(2, tester.Handler.datapoints.Count);
            Assert.Equal(4, tester.Handler.datapoints["test-ts-1"].Item1.Count);
            Assert.Equal(2, tester.Handler.datapoints["test-ts-2"].Item2.Count);
            Assert.Empty(tester.Handler.datapoints["test-ts-2"].Item1);

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
            Assert.Equal(2, tester.Handler.assets.Count);
            await tester.PublishEvents(roundOne);
            Assert.Equal(3, tester.Handler.events.Count);
            Assert.True(tester.Handler.events.ContainsKey("test-event-1"));
            Assert.True(tester.Handler.events.ContainsKey("test-event-2"));
            Assert.True(tester.Handler.events.ContainsKey("test-event-3"));
            await tester.PublishEvents(roundOne);
            Assert.Equal(3, tester.Handler.events.Count);
            await tester.PublishEvents(roundTwo);
            Assert.Equal(5, tester.Handler.events.Count);
            Assert.True(tester.Handler.events.ContainsKey("test-event-1"));
            Assert.True(tester.Handler.events.ContainsKey("test-event-2"));
            Assert.True(tester.Handler.events.ContainsKey("test-event-3"));
            Assert.True(tester.Handler.events.ContainsKey("test-event-4"));
            Assert.False(tester.Handler.events.ContainsKey("test-event-5"));
            Assert.True(tester.Handler.events.ContainsKey("test-event-6"));
            Assert.Empty(tester.Handler.events["test-event-4"].assetIds);
            Assert.Equal(2, tester.Handler.events["test-event-1"].assetIds.Count());
            Assert.Single(tester.Handler.events["test-event-2"].assetIds);
            Assert.Single(tester.Handler.events["test-event-6"].assetIds);
            await tester.RecreateBridge();
            await tester.PublishEvents(roundThree);
            Assert.Equal(7, tester.Handler.events.Count);
            Assert.True(tester.Handler.events.ContainsKey("test-event-7"));
            Assert.True(tester.Handler.events.ContainsKey("test-event-8"));
            Assert.False(tester.Handler.events.ContainsKey("test-event-9"));
            Assert.Equal(2, tester.Handler.events["test-event-7"].assetIds.Count());
            Assert.Single(tester.Handler.events["test-event-8"].assetIds);
        }
        class StatelessEventCreate : EventCreate
        {
            public IEnumerable<string> AssetExternalIds { get; set; }
        }

        class StatelessTimeSeriesCreate : TimeSeriesCreate
        {
            public string AssetExternalId { get; set; }
        }
    }
}

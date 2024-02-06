using AdysTech.InfluxDB.Client.Net;
using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    [Collection("Shared server tests")]
    public sealed class InfluxPusherTest : IDisposable
    {
        private readonly StaticServerTestFixture tester;
        private readonly InfluxDBClient client;
        private readonly InfluxPusher pusher;
        private static int ifIndex;
        public InfluxPusherTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);

            var ifSetup = tester.GetInfluxPusher($"testdb-pusher{ifIndex++}");
            client = ifSetup.client;
            pusher = ifSetup.pusher;
            tester.Client.TypeManager.Reset();
        }
        public void Dispose()
        {
            client?.Dispose();
            pusher?.Dispose();
        }

        [Fact]
        public async Task TestTestConnection()
        {
            // Test with against non-existing server
            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();

            // Debug true
            tester.Config.DryRun = true;
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            tester.Config.DryRun = false;

            // Fail due to bad host
            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));

            // Db does not exist
            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();
            await client.DropDatabaseAsync(new InfluxDatabase(tester.Config.Influx.Database));
            var dbs = await client.GetInfluxDBNamesAsync();
            Assert.DoesNotContain(dbs, db => db == tester.Config.Influx.Database);
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            dbs = await client.GetInfluxDBNamesAsync();
            Assert.Contains(dbs, db => db == tester.Config.Influx.Database);

            // Normal operation
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
        }
        private async Task<IEnumerable<UADataPoint>> GetAllDataPoints(InfluxPusher pusher, UAExtractor extractor, string id, bool isString = false)
        {
            var dummy = new InfluxBufferState(extractor.State.GetNodeState(id));
            dummy.SetComplete();
            dummy.Type = isString ? InfluxBufferType.StringType : InfluxBufferType.DoubleType;
            return await pusher.ReadDataPoints(
                new Dictionary<string, InfluxBufferState> { { id, dummy } },
                tester.Source.Token);
        }
        private async Task<IEnumerable<UAEvent>> GetAllEvents(InfluxPusher pusher, UAExtractor extractor, NodeId id)
        {
            var dummy = new InfluxBufferState(extractor.State.GetEmitterState(id));
            dummy.SetComplete();
            dummy.Type = InfluxBufferType.EventType;
            return await pusher.ReadEvents(
                new Dictionary<string, InfluxBufferState> { { extractor.GetUniqueId(id), dummy } },
                tester.Source.Token);
        }
        [Fact]
        public async Task TestPushReadDataPoints()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);

            CommonTestUtils.ResetMetricValues("opcua_datapoint_push_failures_influx",
                "opcua_datapoints_pushed_influx", "opcua_datapoint_pushes_influx",
                "opcua_skipped_datapoints_influx");

            var state1 = new VariableExtractionState(tester.Client,
                CommonTestUtils.GetSimpleVariable("test-ts-double", new UADataType(DataTypeIds.Double)),
                true, true, true);
            var state2 = new VariableExtractionState(tester.Client,
                CommonTestUtils.GetSimpleVariable("test-ts-string", new UADataType(DataTypeIds.String)),
                true, true, true);


            extractor.State.SetNodeState(state1, "test-ts-double");
            extractor.State.SetNodeState(state2, "test-ts-string");

            // Null input
            Assert.Null(await pusher.PushDataPoints(null, tester.Source.Token));

            // Test filtering out dps
            var invalidDps = new[]
            {
                new UADataPoint(DateTime.MinValue, "test-ts-double", 123, StatusCodes.Good),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.NaN, StatusCodes.Good),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.NegativeInfinity, StatusCodes.Good),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.PositiveInfinity, StatusCodes.Good),
            };
            Assert.Null(await pusher.PushDataPoints(invalidDps, tester.Source.Token));

            tester.Config.DryRun = true;

            var time = DateTime.UtcNow;

            var dps = new[]
            {
                new UADataPoint(time, "test-ts-double", 123, StatusCodes.Good),
                new UADataPoint(time.AddSeconds(1), "test-ts-double", 321, StatusCodes.Good),
                new UADataPoint(time, "test-ts-string", "string", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(1), "test-ts-string", "string2", StatusCodes.Good)
            };

            // Debug true
            Assert.Null(await pusher.PushDataPoints(dps, tester.Source.Token));

            Assert.Empty(await GetAllDataPoints(pusher, extractor, "test-ts-double"));
            Assert.Empty(await GetAllDataPoints(pusher, extractor, "test-ts-string", true));

            tester.Config.DryRun = false;

            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();

            // Thrown error
            Assert.False(await pusher.PushDataPoints(dps, tester.Source.Token));

            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_push_failures_influx", 1));

            // Successful insertion
            Assert.True(await pusher.PushDataPoints(dps, tester.Source.Token));
            var doubleDps = await GetAllDataPoints(pusher, extractor, "test-ts-double");
            Assert.Equal(2, doubleDps.Count());
            Assert.Contains(doubleDps, dp => dp.DoubleValue.Value == 123);
            Assert.Contains(doubleDps, dp => dp.DoubleValue.Value == 321);
            Assert.Contains(doubleDps, dp => dp.Timestamp == time);
            Assert.Contains(doubleDps, dp => dp.Timestamp == time.AddSeconds(1));
            Assert.All(doubleDps, dp => Assert.Equal("test-ts-double", dp.Id));
            var stringDps = await GetAllDataPoints(pusher, extractor, "test-ts-string", true);
            Assert.Equal(2, stringDps.Count());
            Assert.Contains(stringDps, dp => dp.StringValue == "string");
            Assert.Contains(stringDps, dp => dp.StringValue == "string2");
            Assert.Contains(stringDps, dp => dp.Timestamp == time);
            Assert.Contains(stringDps, dp => dp.Timestamp == time.AddSeconds(1));
            Assert.All(stringDps, dp => Assert.Equal("test-ts-string", dp.Id));

            // Insert with mismatched types
            var dps2 = new[]
            {
                new UADataPoint(time.AddSeconds(2), "test-ts-double", "123", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(3), "test-ts-double", "321", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(2), "test-ts-string", 1, StatusCodes.Good),
                new UADataPoint(time.AddSeconds(3), "test-ts-string", 2, StatusCodes.Good),
                new UADataPoint(time, "test-ts-double-2", 123, StatusCodes.Good),
                new UADataPoint(time.AddSeconds(1), "test-ts-double-2", 321, StatusCodes.Good),
            };
            // Flip the two states.. only good way to do this
            extractor.State.SetNodeState(state1, "test-ts-string");
            extractor.State.SetNodeState(state2, "test-ts-double");

            var state3 = new VariableExtractionState(tester.Client,
                CommonTestUtils.GetSimpleVariable("test-ts-double-2", new UADataType(DataTypeIds.Double)),
                true, true, true);
            extractor.State.SetNodeState(state3, "test-ts-double-2");

            // The error response is not easy to parse, and the most reasonable thing is to just ignore it.
            Assert.True(await pusher.PushDataPoints(dps2, tester.Source.Token));
            // Influxdb write on error is generally partial
            Assert.Equal(2, (await GetAllDataPoints(pusher, extractor, "test-ts-double")).Count());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_skipped_datapoints_influx", 8));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_push_failures_influx", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_pushes_influx", 2));
        }
        [Fact]
        public async Task TestPushReadEvents()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);

            CommonTestUtils.ResetMetricValues("opcua_event_push_failures_influx",
                "opcua_events_pushed_influx", "opcua_event_pushes_influx",
                "opcua_skipped_events_influx");

            var state = new EventExtractionState(tester.Client, new NodeId("emitter", 0), true, true, true);
            extractor.State.SetEmitterState(state);
            extractor.State.RegisterNode(new NodeId("source", 0), extractor.GetUniqueId(new NodeId("source", 0)));
            extractor.State.RegisterNode(new NodeId("emitter", 0), extractor.GetUniqueId(new NodeId("emitter", 0)));
            extractor.State.RegisterNode(new NodeId("type", 0), extractor.GetUniqueId(new NodeId("type", 0)));

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
            Assert.True(CommonTestUtils.TestMetricValue("opcua_skipped_events_influx", 2));

            var eventType = new UAObjectType(new NodeId("type", 0));
            extractor.State.ActiveEvents[eventType.Id] = eventType;

            var time = DateTime.UtcNow;

            var events = new[]
            {
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter", 0),
                    SourceNode = new NodeId("source", 0),
                    EventType = eventType,
                    EventId = "someid",
                    MetaData = new Dictionary<string, string>
                    {
                        { "Key1", "object1" },
                        { "Key2", "123" }
                    }
                },
                new UAEvent
                {
                    Time = time.AddSeconds(1),
                    EmittingNode = new NodeId("emitter", 0),
                    SourceNode = new NodeId("source", 0),
                    EventType = eventType,
                    EventId = "someid2",
                    MetaData = new Dictionary<string, string>
                    {
                        { "Key1", "object1" },
                        { "Key2", "123" }
                    }
                }
            };

            tester.Config.DryRun = true;
            Assert.Null(await pusher.PushEvents(events, tester.Source.Token));
            tester.Config.DryRun = false;


            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();
            Assert.False(await pusher.PushEvents(events, tester.Source.Token));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_event_push_failures_influx", 1));
            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();

            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            var ifEvents = await GetAllEvents(pusher, extractor, new NodeId("emitter", 0));
            Assert.Equal(2, ifEvents.Count());
            var eventsById = events.ToDictionary(evt => evt.EventId);

            foreach (var evt in ifEvents)
            {
                var rawEvt = eventsById[evt.EventId];
                Assert.Equal(rawEvt.Time, evt.Time);
                Assert.Equal(rawEvt.EmittingNode, evt.EmittingNode);
                Assert.Equal(rawEvt.SourceNode, evt.SourceNode);
                Assert.Equal(rawEvt.EventId, evt.EventId);
                Assert.Equal(rawEvt.EventType, evt.EventType);
                Assert.Equal(rawEvt.MetaData.Count, evt.MetaData.Count);
                foreach (var kvp in evt.MetaData)
                {
                    Assert.Equal(extractor.StringConverter.ConvertToString(rawEvt.MetaData[kvp.Key]), kvp.Value);
                }
            }

            events = events.Append(new UAEvent
            {
                Time = time,
                EmittingNode = new NodeId("emitter", 0),
                SourceNode = new NodeId("source", 0),
                EventType = new UAObjectType(new NodeId("type", 0)),
                EventId = "someid3"
            }).ToArray();

            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            ifEvents = await GetAllEvents(pusher, extractor, new NodeId("emitter", 0));
            Assert.Equal(3, ifEvents.Count());
            Assert.True(CommonTestUtils.TestMetricValue("opcua_event_pushes_influx", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_events_pushed_influx", 5));
        }
        [Fact]
        public async Task TestInitExtractedRanges()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);

            tester.Config.Influx.ReadExtractedRanges = true;
            VariableExtractionState[] GetStates()
            {
                var state1 = new VariableExtractionState(tester.Client,
                    CommonTestUtils.GetSimpleVariable("double", new UADataType(DataTypeIds.Double)), true, true, true);
                var state2 = new VariableExtractionState(tester.Client,
                    CommonTestUtils.GetSimpleVariable("string", new UADataType(DataTypeIds.String)), true, true, true);
                var state3 = new VariableExtractionState(tester.Client,
                    CommonTestUtils.GetSimpleVariable("array", new UADataType(DataTypeIds.Double), 3), true, true, true);
                extractor.State.SetNodeState(state1, state1.Id);
                extractor.State.SetNodeState(state2, state2.Id);
                extractor.State.SetNodeState(state3, state3.Id);
                for (int i = 0; i < state3.ArrayDimensions[0]; i++)
                {
                    extractor.State.SetNodeState(state3, $"{state3.Id}[{i}]");
                }
                return new[] { state1, state2, state3 };
            }

            var states = GetStates();

            // Nothing in Influx
            // Failure
            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();
            Assert.False(await pusher.InitExtractedRanges(states, true, tester.Source.Token));

            // Init missing
            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();
            Assert.True(await pusher.InitExtractedRanges(states, true, tester.Source.Token));
            foreach (var state in states)
            {
                Assert.Equal(state.DestinationExtractedRange.First, state.DestinationExtractedRange.Last);
            }

            DateTime GetTs(int ms)
            {
                return CogniteTime.FromUnixTimeMilliseconds(ms);
            }


            // Stuff in influx
            states = GetStates();
            await pusher.PushDataPoints(new[]
            {
                new UADataPoint(GetTs(1000), states[0].Id, 123, StatusCodes.Good),
                new UADataPoint(GetTs(2000), states[0].Id, 123, StatusCodes.Good),
                new UADataPoint(GetTs(3000), states[0].Id, 123, StatusCodes.Good),
                new UADataPoint(GetTs(1000), states[1].Id, "s1", StatusCodes.Good),
                new UADataPoint(GetTs(2000), states[1].Id, "s2", StatusCodes.Good),
                new UADataPoint(GetTs(3000), states[1].Id, "s3", StatusCodes.Good),
                new UADataPoint(GetTs(1000), $"{states[2].Id}[0]", 123, StatusCodes.Good),
                new UADataPoint(GetTs(2000), $"{states[2].Id}[0]", 123, StatusCodes.Good),
                new UADataPoint(GetTs(2000), $"{states[2].Id}[1]", 123, StatusCodes.Good),
                new UADataPoint(GetTs(3000), $"{states[2].Id}[1]", 123, StatusCodes.Good)
            }, tester.Source.Token);

            // Failure
            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();
            Assert.False(await pusher.InitExtractedRanges(states, true, tester.Source.Token));

            // Normal init
            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();
            Assert.True(await pusher.InitExtractedRanges(states, true, tester.Source.Token));
            var range = new TimeRange(GetTs(1000), GetTs(3000));
            Assert.Equal(range, states[0].DestinationExtractedRange);
            Assert.Equal(range, states[1].DestinationExtractedRange);
            Assert.Equal(states[2].DestinationExtractedRange.First, states[2].DestinationExtractedRange.Last);

            // Init array

            await pusher.PushDataPoints(new[]
            {
                new UADataPoint(GetTs(1000), $"{states[2].Id}[2]", 123, StatusCodes.Good),
                new UADataPoint(GetTs(2000), $"{states[2].Id}[2]", 123, StatusCodes.Good),
                new UADataPoint(GetTs(3000), $"{states[2].Id}[2]", 123, StatusCodes.Good),
            }, tester.Source.Token);

            states = GetStates();
            Assert.True(await pusher.InitExtractedRanges(states, true, tester.Source.Token));
            Assert.Equal(range, states[0].DestinationExtractedRange);
            Assert.Equal(range, states[1].DestinationExtractedRange);
            Assert.Equal(GetTs(2000), states[2].DestinationExtractedRange.First);
            Assert.Equal(GetTs(2000), states[2].DestinationExtractedRange.Last);
        }
        [Fact]
        public async Task TestInitExtractedEventRanges()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);

            EventExtractionState[] GetStates()
            {
                var state = new EventExtractionState(tester.Client, new NodeId("emitter", 0), true, true, true);
                var state2 = new EventExtractionState(tester.Client, new NodeId("emitter2", 0), true, true, true);
                extractor.State.SetEmitterState(state);
                extractor.State.SetEmitterState(state2);
                return new[] { state, state2 };
            }

            extractor.State.RegisterNode(new NodeId("source", 0), extractor.GetUniqueId(new NodeId("source", 0)));
            extractor.State.RegisterNode(new NodeId("emitter", 0), extractor.GetUniqueId(new NodeId("emitter", 0)));
            extractor.State.RegisterNode(new NodeId("emitter2", 0), extractor.GetUniqueId(new NodeId("emitter2", 0)));
            extractor.State.RegisterNode(new NodeId("type", 0), extractor.GetUniqueId(new NodeId("type", 0)));
            extractor.State.RegisterNode(new NodeId("type2", 0), extractor.GetUniqueId(new NodeId("type2", 0)));

            var states = GetStates();

            // Nothing in Influx
            // Failure
            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();
            Assert.False(await pusher.InitExtractedEventRanges(states, true, tester.Source.Token));

            // Init missing
            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();
            Assert.True(await pusher.InitExtractedEventRanges(states, true, tester.Source.Token));
            foreach (var state in states)
            {
                Assert.Equal(state.DestinationExtractedRange.First, state.DestinationExtractedRange.Last);
            }

            // Stuff in influx
            DateTime GetTs(int ms)
            {
                return CogniteTime.FromUnixTimeMilliseconds(ms);
            }

            var events = new[]
            {
                new UAEvent
                {
                    Time = GetTs(1000),
                    EmittingNode = new NodeId("emitter", 0),
                    EventType = new UAObjectType (new NodeId("type", 0)),
                    EventId = "someid"
                },
                new UAEvent
                {
                    Time = GetTs(3000),
                    EmittingNode = new NodeId("emitter", 0),
                    EventType = new UAObjectType(new NodeId("type2", 0)),
                    EventId = "someid2"
                },
                new UAEvent
                {
                    Time = GetTs(1000),
                    EmittingNode = new NodeId("emitter2", 0),
                    EventType = new UAObjectType (new NodeId("type", 0)),
                    EventId = "someid3"
                },
                new UAEvent
                {
                    Time = GetTs(2000),
                    EmittingNode = new NodeId("emitter2", 0),
                    EventType = new UAObjectType (new NodeId("type", 0)),
                    EventId = "someid4"
                }
            };

            states = GetStates();

            await pusher.PushEvents(events, tester.Source.Token);

            // Failure
            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();
            Assert.False(await pusher.InitExtractedEventRanges(states, true, tester.Source.Token));

            // Normal init
            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();
            Assert.True(await pusher.InitExtractedEventRanges(states, true, tester.Source.Token));
            Assert.Equal(new TimeRange(GetTs(1000), GetTs(3000)), states[0].DestinationExtractedRange);
            Assert.Equal(new TimeRange(GetTs(1000), GetTs(2000)), states[1].DestinationExtractedRange);
        }
    }
}

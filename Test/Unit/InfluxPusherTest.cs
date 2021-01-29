using AdysTech.InfluxDB.Client.Net;
using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class InfluxPusherTestFixture : BaseExtractorTestFixture
    {
        public InfluxPusherTestFixture() : base(63000)
        {
        }
        public (InfluxDBClient, InfluxPusher) GetPusher(bool clear = true)
        {
            if (Config.Influx == null)
            {
                Config.Influx = new InfluxPusherConfig();
            }
            Config.Influx.Database ??= "testdb-pusher";
            Config.Influx.Host ??= "http://localhost:8086";

            var client = new InfluxDBClient(Config.Influx.Host, Config.Influx.Username, Config.Influx.Password);
            if (clear)
            {
                ClearDB(client).Wait();
            }
            var pusher = Config.Influx.ToPusher(null) as InfluxPusher;
            return (client, pusher);
        }
        public async Task ClearDB(InfluxDBClient client)
        {
            if (client == null) return;
            await client.DropDatabaseAsync(new InfluxDatabase(Config.Influx.Database));
            await client.CreateDatabaseAsync(Config.Influx.Database);
        }
    }
    public class InfluxPusherTest : MakeConsoleWork, IClassFixture<InfluxPusherTestFixture>
    {
        private readonly InfluxPusherTestFixture tester;
        private InfluxDBClient client;
        private InfluxPusher pusher;
        public InfluxPusherTest(ITestOutputHelper output, InfluxPusherTestFixture tester) : base(output)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
            this.tester = tester;
            (client, pusher) = tester.GetPusher();
        }
        [Fact]
        public async Task TestTestConnection()
        {
            // Test with against non-existing server
            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();

            // Debug true
            tester.Config.Influx.Debug = true;
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            tester.Config.Influx.Debug = false;

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
                new UAVariable(new NodeId("test-ts-double"), "test-ts-double", NodeId.Null) { DataType = new UADataType(DataTypeIds.Double) },
                true, true);
            var state2 = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("test-ts-string"), "test-ts-string", NodeId.Null) { DataType = new UADataType(DataTypeIds.String) },
                true, true);


            extractor.State.SetNodeState(state1, "test-ts-double");
            extractor.State.SetNodeState(state2, "test-ts-string");

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

            tester.Config.Influx.Debug = true;

            var time = DateTime.UtcNow;

            var dps = new[]
            {
                new UADataPoint(time, "test-ts-double", 123),
                new UADataPoint(time.AddSeconds(1), "test-ts-double", 321),
                new UADataPoint(time, "test-ts-string", "string"),
                new UADataPoint(time.AddSeconds(1), "test-ts-string", "string2")
            };

            // Debug true
            Assert.Null(await pusher.PushDataPoints(dps, tester.Source.Token));

            Assert.Empty(await GetAllDataPoints(pusher, extractor, "test-ts-double"));
            Assert.Empty(await GetAllDataPoints(pusher, extractor, "test-ts-string", true));

            tester.Config.Influx.Debug = false;

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
                new UADataPoint(time.AddSeconds(2), "test-ts-double", "123"),
                new UADataPoint(time.AddSeconds(3), "test-ts-double", "321"),
                new UADataPoint(time.AddSeconds(2), "test-ts-string", 1),
                new UADataPoint(time.AddSeconds(3), "test-ts-string", 2),
                new UADataPoint(time, "test-ts-double-2", 123),
                new UADataPoint(time.AddSeconds(1), "test-ts-double-2", 321),
            };
            // Flip the two states.. only good way to do this
            extractor.State.SetNodeState(state1, "test-ts-string");
            extractor.State.SetNodeState(state2, "test-ts-double");

            var state3 = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("test-ts-double-2"), "test-ts-double-2", NodeId.Null) { DataType = new UADataType(DataTypeIds.Double) },
                true, true);
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

            var state = new EventExtractionState(tester.Client, new NodeId("emitter"), true, true);
            extractor.State.SetEmitterState(state);
            extractor.State.RegisterNode(new NodeId("source"), extractor.GetUniqueId(new NodeId("source")));
            extractor.State.RegisterNode(new NodeId("emitter"), extractor.GetUniqueId(new NodeId("emitter")));
            extractor.State.RegisterNode(new NodeId("type"), extractor.GetUniqueId(new NodeId("type")));

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

            var time = DateTime.UtcNow;

            var events = new[]
            {
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter"),
                    SourceNode = new NodeId("source"),
                    EventType = new NodeId("type"),
                    EventId = "someid",
                    MetaData = new Dictionary<string, object>
                    {
                        { "Key1", "object1" },
                        { "Key2", 123 }
                    }
                },
                new UAEvent
                {
                    Time = time.AddSeconds(1),
                    EmittingNode = new NodeId("emitter"),
                    SourceNode = new NodeId("source"),
                    EventType = new NodeId("type"),
                    EventId = "someid2",
                    MetaData = new Dictionary<string, object>
                    {
                        { "Key1", "object1" },
                        { "Key2", 123 }
                    }
                }
            };

            tester.Config.Influx.Debug = true;
            Assert.Null(await pusher.PushEvents(events, tester.Source.Token));
            tester.Config.Influx.Debug = false;


            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();
            Assert.False(await pusher.PushEvents(events, tester.Source.Token));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_event_push_failures_influx", 1));
            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();

            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            var ifEvents = await GetAllEvents(pusher, extractor, new NodeId("emitter"));
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
                    Assert.Equal(extractor.ConvertToString(rawEvt.MetaData[kvp.Key]), kvp.Value);
                }
            }

            events = events.Append(new UAEvent
            {
                Time = time,
                EmittingNode = new NodeId("emitter"),
                SourceNode = new NodeId("source"),
                EventType = new NodeId("type"),
                EventId = "someid3"
            }).ToArray();

            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            ifEvents = await GetAllEvents(pusher, extractor, new NodeId("emitter"));
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
                new UAVariable(new NodeId("double"), "double", NodeId.Null) { DataType = new UADataType(DataTypeIds.Double) }, true, true);
                var state2 = new VariableExtractionState(tester.Client,
                    new UAVariable(new NodeId("string"), "string", NodeId.Null) { DataType = new UADataType(DataTypeIds.String) }, true, true);
                var state3 = new VariableExtractionState(tester.Client,
                    new UAVariable(new NodeId("array"), "array", NodeId.Null)
                    {
                        DataType = new UADataType(DataTypeIds.Double),
                        ArrayDimensions = new Collection<int> { 3 }
                    }, true, true);
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
                Assert.Equal(TimeRange.Empty, state.DestinationExtractedRange);
            }

            DateTime GetTs(int ms)
            {
                return CogniteTime.FromUnixTimeMilliseconds(ms);
            }


            // Stuff in influx
            states = GetStates();
            await pusher.PushDataPoints(new[]
            {
                new UADataPoint(GetTs(1000), states[0].Id, 123),
                new UADataPoint(GetTs(2000), states[0].Id, 123),
                new UADataPoint(GetTs(3000), states[0].Id, 123),
                new UADataPoint(GetTs(1000), states[1].Id, "s1"),
                new UADataPoint(GetTs(2000), states[1].Id, "s2"),
                new UADataPoint(GetTs(3000), states[1].Id, "s3"),
                new UADataPoint(GetTs(1000), $"{states[2].Id}[0]", 123),
                new UADataPoint(GetTs(2000), $"{states[2].Id}[0]", 123),
                new UADataPoint(GetTs(2000), $"{states[2].Id}[1]", 123),
                new UADataPoint(GetTs(3000), $"{states[2].Id}[1]", 123)
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
            Assert.Equal(TimeRange.Empty, states[2].DestinationExtractedRange);

            // Init array

            await pusher.PushDataPoints(new[]
            {
                new UADataPoint(GetTs(1000), $"{states[2].Id}[2]", 123),
                new UADataPoint(GetTs(2000), $"{states[2].Id}[2]", 123),
                new UADataPoint(GetTs(3000), $"{states[2].Id}[2]", 123),
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
                var state = new EventExtractionState(tester.Client, new NodeId("emitter"), true, true);
                var state2 = new EventExtractionState(tester.Client, new NodeId("emitter2"), true, true);
                extractor.State.SetEmitterState(state);
                extractor.State.SetEmitterState(state2);
                return new[] { state, state2 };
            }
            
            extractor.State.RegisterNode(new NodeId("source"), extractor.GetUniqueId(new NodeId("source")));
            extractor.State.RegisterNode(new NodeId("emitter"), extractor.GetUniqueId(new NodeId("emitter")));
            extractor.State.RegisterNode(new NodeId("emitter2"), extractor.GetUniqueId(new NodeId("emitter2")));
            extractor.State.RegisterNode(new NodeId("type"), extractor.GetUniqueId(new NodeId("type")));
            extractor.State.RegisterNode(new NodeId("type2"), extractor.GetUniqueId(new NodeId("type2")));

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
                Assert.Equal(TimeRange.Empty, state.DestinationExtractedRange);
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
                    EmittingNode = new NodeId("emitter"),
                    EventType = new NodeId("type"),
                    EventId = "someid"
                },
                new UAEvent
                {
                    Time = GetTs(3000),
                    EmittingNode = new NodeId("emitter"),
                    EventType = new NodeId("type2"),
                    EventId = "someid2"
                },
                new UAEvent
                {
                    Time = GetTs(1000),
                    EmittingNode = new NodeId("emitter2"),
                    EventType = new NodeId("type"),
                    EventId = "someid3"
                },
                new UAEvent
                {
                    Time = GetTs(2000),
                    EmittingNode = new NodeId("emitter2"),
                    EventType = new NodeId("type"),
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

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                client.Dispose();
                pusher.Dispose();
            }
        }
    }
}

using AdysTech.InfluxDB.Client.Net;
using Cognite.OpcUa;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
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
        public InfluxPusherTest(ITestOutputHelper output, InfluxPusherTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        [Fact]
        public async Task TestTestConnection()
        {
            var (client, pusher) = tester.GetPusher();

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
        public async Task TestPushDataPoints()
        {
            var (client, pusher) = tester.GetPusher();
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
            Assert.Equal(2, (await GetAllDataPoints(pusher, extractor, "test-ts-double")).Count());
            Assert.Equal(2, (await GetAllDataPoints(pusher, extractor, "test-ts-string", true)).Count());

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
        public async Task TestPushEvents()
        {
            var (client, pusher) = tester.GetPusher();
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
    }
}

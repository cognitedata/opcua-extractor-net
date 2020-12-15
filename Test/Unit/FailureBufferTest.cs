using AdysTech.InfluxDB.Client.Net;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa;
using Cognite.OpcUa.TypeCollectors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class FailureBufferTestFixture : BaseExtractorTestFixture
    {
        public FailureBufferTestFixture() : base(62400)
        {
            try
            {
                var files = Directory.GetFiles(".");
                foreach (var file in files)
                {
                    if (file.Contains("fb-", StringComparison.InvariantCulture)
                        && (file.EndsWith(".bin", StringComparison.InvariantCulture)
                        || file.EndsWith(".db", StringComparison.InvariantCulture)))
                    {
                        File.Delete(file);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to clear files: {ex.Message}");
            }
        }
    }
    public class FailureBufferTest : MakeConsoleWork, IClassFixture<FailureBufferTestFixture>
    {
        private static int idx;
        private readonly FailureBufferTestFixture tester;
        public FailureBufferTest(ITestOutputHelper output, FailureBufferTestFixture tester) : base(output)
        {
            this.tester = tester;
        }

        private FullConfig BuildConfig()
        {
            idx++;
            return new FullConfig
            {
                FailureBuffer = new FailureBufferConfig
                {
                    DatapointPath = $"fb-buffer-{idx}.bin",
                    EventPath = $"fb-buffer-{idx}.bin",
                    Enabled = true
                },
                StateStorage = new StateStorageConfig
                {
                    Database = StateStoreConfig.StorageType.LiteDb,
                    EventStore = "events",
                    InfluxEventStore = "influx_events",
                    InfluxVariableStore = "influx_variables",
                    Interval = -1,
                    Location = $"fb-store-{idx}.db",
                    VariableStore = "variables"
                },
                Influx = new InfluxPusherConfig
                {
                    Host = tester.Config.Influx.Host,
                    Database = $"testdb{idx}"
                }
            };
        }

        private static async Task<InfluxDBClient> SetupInfluxdb(InfluxPusherConfig config)
        {
            var client = new InfluxDBClient(config.Host);

            try
            {
                await client.DropDatabaseAsync(new InfluxDatabase(config.Database));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to drop influxdb database: {ex.Message}");
            }

            await client.CreateDatabaseAsync(config.Database);

            return client;
        }

        [Fact]
        public void TestBuildFailureBuffer()
        {
            using var stateStore = new DummyStateStore();
            using var extractor = tester.BuildExtractor(true, stateStore);
            var cfg = BuildConfig();

            Assert.Throws<ArgumentNullException>(() => new FailureBuffer(null, extractor, null));
            Assert.Throws<ArgumentNullException>(() => new FailureBuffer(cfg, null, null));

            Assert.False(File.Exists(cfg.FailureBuffer.DatapointPath));
            Assert.False(File.Exists(cfg.FailureBuffer.EventPath));

            var fb1 = new FailureBuffer(cfg, extractor, null);
            Assert.True(File.Exists(cfg.FailureBuffer.DatapointPath));
            Assert.True(File.Exists(cfg.FailureBuffer.EventPath));

            Assert.False(fb1.AnyPoints);
            Assert.False(fb1.AnyEvents);

            File.WriteAllText(cfg.FailureBuffer.DatapointPath, "testtest");
            File.WriteAllText(cfg.FailureBuffer.EventPath, "testtest");

            var fb2 = new FailureBuffer(cfg, extractor, null);

            Assert.True(fb2.AnyPoints);
            Assert.True(fb2.AnyEvents);

            cfg.FailureBuffer.Influx = true;
            using var pusher = new InfluxPusher(tester.Config.Influx);
            Assert.Throws<ConfigurationException>(() => new FailureBuffer(cfg, extractor, null));

            var fb3 = new FailureBuffer(cfg, extractor, pusher);
        }
        [Fact]
        public async Task TestReadBufferState()
        {
            var cfg = BuildConfig();
            using var stateStore = new LiteDBStateStore(cfg.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());
            using var extractor = tester.BuildExtractor(true, stateStore);

            cfg.FailureBuffer.Influx = true;
            cfg.FailureBuffer.InfluxStateStore = true;
            using var pusher = new InfluxPusher(tester.Config.Influx);
            var fb1 = new FailureBuffer(cfg, extractor, pusher);

            Assert.False(fb1.AnyPoints);
            Assert.False(fb1.AnyEvents);

            // Test restore nothing
            await fb1.InitializeBufferStates(Enumerable.Empty<NodeExtractionState>(), Enumerable.Empty<EventExtractionState>(), tester.Source.Token);

            Assert.False(fb1.AnyPoints);
            Assert.False(fb1.AnyEvents);

            // Test restore nonexisting states
            var dt = new BufferedDataType(DataTypeIds.Double);

            var state1 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state1"), "state1", NodeId.Null) { DataType = dt }, false, false, false);
            var state2 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state2"), "state2", NodeId.Null) { DataType = dt }, false, false, false);
            var state3 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state3"), "state3", NodeId.Null) { DataType = dt }, true, true, false);
            var nodeStates = new[] { state1, state2, state3 };

            var nodeInfluxStates = nodeStates.Select(state => new InfluxBufferState(state)).ToList();

            var estate1 = new EventExtractionState(extractor, new NodeId("emitter1"), false, false, false);
            var estate2 = new EventExtractionState(extractor, new NodeId("emitter2"), false, false, false);
            var estate3 = new EventExtractionState(extractor, new NodeId("emitter3"), true, true, false);
            var evtStates = new[] { estate1, estate2, estate3 };

            var evtInfluxStates = evtStates.Select(state => new InfluxBufferState(state)).ToList();

            await fb1.InitializeBufferStates(nodeStates, evtStates, tester.Source.Token);

            Assert.False(fb1.AnyPoints);
            Assert.False(fb1.AnyEvents);

            var nodeBufferStates = (Dictionary<string, InfluxBufferState>)fb1
                .GetType()
                .GetField("nodeBufferStates", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(fb1);

            var eventBufferStates = (Dictionary<string, InfluxBufferState>)fb1
                .GetType()
                .GetField("eventBufferStates", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(fb1);

            Assert.Empty(nodeBufferStates);
            Assert.Empty(eventBufferStates);

            // Write some states, then check if they are read back
            var range = new TimeRange(DateTime.UtcNow.AddSeconds(-100), DateTime.UtcNow.AddSeconds(100));
            nodeInfluxStates[0].UpdateDestinationRange(range.First, range.Last);
            nodeInfluxStates[2].UpdateDestinationRange(range.First, range.Last);

            evtInfluxStates[0].UpdateDestinationRange(range.First, range.Last);
            evtInfluxStates[2].UpdateDestinationRange(range.First, range.Last);

            await stateStore.StoreExtractionState(nodeInfluxStates, cfg.StateStorage.InfluxVariableStore, tester.Source.Token);
            await stateStore.StoreExtractionState(evtInfluxStates, cfg.StateStorage.InfluxEventStore, tester.Source.Token);

            await fb1.InitializeBufferStates(nodeStates, evtStates, tester.Source.Token);

            Assert.Single(nodeBufferStates);
            Assert.Single(eventBufferStates);
            Assert.Equal(range, nodeBufferStates.First().Value.DestinationExtractedRange);
            Assert.Equal(range, eventBufferStates.First().Value.DestinationExtractedRange);
            Assert.True(fb1.AnyPoints);
            Assert.True(fb1.AnyEvents);
        }

        private static IEnumerable<BufferedDataPoint> GetDps(DateTime start, string id, int count)
        {
            return Enumerable.Range(0, count).Select(idx => new BufferedDataPoint(start.AddSeconds(idx), id, idx));
        }

        [Fact]
        public async Task TestWriteDatapoints()
        {
            var cfg = BuildConfig();
            using var stateStore = new LiteDBStateStore(cfg.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());
            using var extractor = tester.BuildExtractor(true, stateStore);

            cfg.FailureBuffer.Influx = true;
            cfg.FailureBuffer.InfluxStateStore = true;
            using var pusher = new InfluxPusher(tester.Config.Influx);
            var fb1 = new FailureBuffer(cfg, extractor, pusher);

            var dt = new BufferedDataType(DataTypeIds.Double);

            var state1 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state1"), "state1", NodeId.Null) { DataType = dt }, false, false, false);
            var state2 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state2"), "state2", NodeId.Null) { DataType = dt }, false, false, false);
            var state3 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state3"), "state3", NodeId.Null) { DataType = dt }, true, true, false);
            extractor.State.SetNodeState(state1, "state1");
            extractor.State.SetNodeState(state2, "state2");
            extractor.State.SetNodeState(state3, "state3");

            var start = DateTime.UtcNow;

            var dps = GetDps(start, "state1", 100).Concat(GetDps(start, "state3", 100)).ToList();

            var ranges = new Dictionary<string, TimeRange>
            {
                { "state1", new TimeRange(start, start.AddSeconds(99)) },
                { "state3", new TimeRange(start, start.AddSeconds(99)) }
            };

            var nodeBufferStates = (Dictionary<string, InfluxBufferState>)fb1
                .GetType()
                .GetField("nodeBufferStates", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(fb1);

            // Missing ranges
            await fb1.WriteDatapoints(dps, new Dictionary<string, TimeRange>(), tester.Source.Token);

            Assert.Empty(nodeBufferStates);
            Assert.False(fb1.AnyPoints);

            // Failing influxpusher
            pusher.DataFailing = true;
            await fb1.WriteDatapoints(dps, ranges, tester.Source.Token);

            Assert.True(new FileInfo(cfg.FailureBuffer.DatapointPath).Length > 0);
            File.Delete(cfg.FailureBuffer.DatapointPath);
            Assert.Empty(nodeBufferStates);

            var fileAnyPointsField = fb1
                .GetType()
                .GetField("fileAnyPoints", BindingFlags.Instance | BindingFlags.NonPublic);

            Assert.True((bool)fileAnyPointsField.GetValue(fb1));
            fileAnyPointsField.SetValue(fb1, false);
            Assert.False(fb1.AnyPoints);

            // Successful write
            pusher.DataFailing = false;
            await fb1.WriteDatapoints(dps, ranges, tester.Source.Token);
            Assert.Single(nodeBufferStates);
            Assert.True(fb1.AnyPoints);

            var db = stateStore.GetDatabase();
            Assert.Equal(1, db.GetCollection(cfg.StateStorage.InfluxVariableStore).Count());

            bool anyPoints = (bool)fb1
                .GetType()
                .GetField("anyPoints", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(fb1);

            Assert.True((bool)fileAnyPointsField.GetValue(fb1));
            Assert.True(anyPoints);
            Assert.True(new FileInfo(cfg.FailureBuffer.DatapointPath).Length > 0);
        }

        private static IEnumerable<BufferedEvent> GetEvents(DateTime start, NodeId id, int cnt)
        {
            return Enumerable.Range(0, cnt).Select(idx => new BufferedEvent { EmittingNode = id, Time = start.AddSeconds(idx) });
        }

        [Fact]
        public async Task TestWriteEvents()
        {
            var cfg = BuildConfig();
            using var stateStore = new LiteDBStateStore(cfg.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());
            using var extractor = tester.BuildExtractor(true, stateStore);

            cfg.FailureBuffer.Influx = true;
            cfg.FailureBuffer.InfluxStateStore = true;
            using var pusher = new InfluxPusher(tester.Config.Influx);
            var fb1 = new FailureBuffer(cfg, extractor, pusher);

            var estate1 = new EventExtractionState(extractor, new NodeId("emitter1"), false, false, false);
            var estate2 = new EventExtractionState(extractor, new NodeId("emitter2"), false, false, false);
            var estate3 = new EventExtractionState(extractor, new NodeId("emitter3"), true, true, false);

            extractor.State.SetEmitterState(estate1);
            extractor.State.SetEmitterState(estate2);
            extractor.State.SetEmitterState(estate3);

            var start = DateTime.UtcNow;

            var evts = GetEvents(start, estate1.SourceId, 100).Concat(GetEvents(start, estate3.SourceId, 100)).ToList();

            var eventBufferStates = (Dictionary<string, InfluxBufferState>)fb1
                .GetType()
                .GetField("eventBufferStates", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(fb1);

            // Missing events
            await fb1.WriteEvents(Enumerable.Empty<BufferedEvent>(), tester.Source.Token);

            Assert.Empty(eventBufferStates);
            Assert.False(fb1.AnyEvents);

            // Failing influx pusher
            pusher.EventsFailing = true;
            await fb1.WriteEvents(evts, tester.Source.Token);

            Assert.True(new FileInfo(cfg.FailureBuffer.EventPath).Length > 0);
            File.Delete(cfg.FailureBuffer.EventPath);
            Assert.Empty(eventBufferStates);

            var fileAnyEventsField = fb1
                .GetType()
                .GetField("fileAnyEvents", BindingFlags.Instance | BindingFlags.NonPublic);

            Assert.True((bool)fileAnyEventsField.GetValue(fb1));
            fileAnyEventsField.SetValue(fb1, false);
            Assert.False(fb1.AnyPoints);

            // Successful write
            pusher.EventsFailing = false;
            await fb1.WriteEvents(evts, tester.Source.Token);
            Assert.Single(eventBufferStates);
            Assert.True(fb1.AnyEvents);

            var db = stateStore.GetDatabase();
            Assert.Equal(1, db.GetCollection(cfg.StateStorage.InfluxEventStore).Count());

            bool anyEvents = (bool)fb1
                .GetType()
                .GetField("anyEvents", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(fb1);

            Assert.True((bool)fileAnyEventsField.GetValue(fb1));
            Assert.True(anyEvents);
            Assert.True(new FileInfo(cfg.FailureBuffer.EventPath).Length > 0);
        }
        [Fact]
        public async Task TestReadDatapoints()
        {
            // Just testing the actual ReadDatapoints method, which covers influx. We can test reading from file properly separately.
            var cfg = BuildConfig();
            using var stateStore = new LiteDBStateStore(cfg.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());
            using var extractor = tester.BuildExtractor(true, stateStore);

            cfg.FailureBuffer.Influx = true;
            cfg.FailureBuffer.InfluxStateStore = true;
            using var pusher = new InfluxPusher(cfg.Influx);
            using var dPusher = new DummyPusher(new DummyPusherConfig());
            pusher.Extractor = extractor;
            var pushers = new IPusher[] { pusher, dPusher };
            var fb1 = new FailureBuffer(cfg, extractor, pusher);

            var dt = new BufferedDataType(DataTypeIds.Double);

            var state1 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state1"), "state1", NodeId.Null) { DataType = dt }, false, false, false);
            var state2 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state2"), "state2", NodeId.Null) { DataType = dt }, false, false, false);
            var state3 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state3"), "state3", NodeId.Null) { DataType = dt }, true, true, false);
            extractor.State.SetNodeState(state1, "state1");
            extractor.State.SetNodeState(state2, "state2");
            extractor.State.SetNodeState(state3, "state3");
            foreach (var state in new[] { state1, state2, state3 })
            {
                state.InitToEmpty();
                state.FinalizeRangeInit();
                dPusher.UniqueToNodeId[state.DisplayName] = (state.SourceId, -1);
            }

            state3.UpdateFromBackfill(DateTime.MaxValue, true);
            state3.UpdateFromFrontfill(DateTime.MinValue, true);

            var dps1 = dPusher.DataPoints[(new NodeId("state1"), -1)] = new List<BufferedDataPoint>();
            var dps2 = dPusher.DataPoints[(new NodeId("state2"), -1)] = new List<BufferedDataPoint>();
            var dps3 = dPusher.DataPoints[(new NodeId("state3"), -1)] = new List<BufferedDataPoint>();

            using var client = await SetupInfluxdb(cfg.Influx);

            // Just read, this happens on startup. We'd expect nothing to really happen here.
            Assert.False(fb1.AnyPoints);
            Assert.True(await fb1.ReadDatapoints(pushers, tester.Source.Token));

            Assert.Empty(dps1);
            Assert.Empty(dps2);
            Assert.Empty(dps3);

            // Read, but with everything set to null
            string oldPath = cfg.FailureBuffer.DatapointPath;
            cfg.FailureBuffer.DatapointPath = null;
            cfg.FailureBuffer.Influx = false;

            Assert.True(await fb1.ReadDatapoints(pushers, tester.Source.Token));
            cfg.FailureBuffer.Influx = true;

            // With datapoints disabled, write to influx then ensure that it is all read back.

            var nodeBufferStates = (Dictionary<string, InfluxBufferState>)fb1
                .GetType()
                .GetField("nodeBufferStates", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(fb1);

            var start = DateTime.UtcNow;
            var dps = GetDps(start, "state1", 100).Concat(GetDps(start, "state3", 100)).ToList();
            var ranges = new Dictionary<string, TimeRange>
            {
                { "state1", new TimeRange(start, start.AddSeconds(99)) },
                { "state3", new TimeRange(start, start.AddSeconds(99)) }
            };

            await pusher.PushDataPoints(dps, tester.Source.Token);
            await fb1.WriteDatapoints(dps, ranges, tester.Source.Token);

            Assert.Single(nodeBufferStates);
            Assert.Equal(nodeBufferStates.First().Value.DestinationExtractedRange, ranges["state1"]);

            Assert.True(await fb1.ReadDatapoints(pushers, tester.Source.Token));

            Assert.Equal(100, dps1.Count);
            Assert.Empty(dps2);
            Assert.Empty(dps3);

            Assert.Empty(nodeBufferStates);

            var db = stateStore.GetDatabase();
            Assert.Equal(0, db.GetCollection(cfg.StateStorage.InfluxVariableStore).Count());

            // Re-enable datapoints, we'd expect to get points twice.
            cfg.FailureBuffer.DatapointPath = oldPath;

            // No need to push to influxdb again, since the points are already there
            // This just registers the points in the failurebuffer again, and writes them to file
            await fb1.WriteDatapoints(dps, ranges, tester.Source.Token);

            Assert.Single(nodeBufferStates);
            Assert.True(new FileInfo(cfg.FailureBuffer.EventPath).Length > 0);

            Assert.True(await fb1.ReadDatapoints(pushers, tester.Source.Token));

            Assert.Equal(300, dps1.Count);

            Assert.Equal(0, new FileInfo(cfg.FailureBuffer.EventPath).Length);
        }
        [Fact]
        public async Task TestReadEvents()
        {
            var cfg = BuildConfig();
            using var stateStore = new LiteDBStateStore(cfg.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());
            using var extractor = tester.BuildExtractor(true, stateStore);

            cfg.FailureBuffer.Influx = true;
            cfg.FailureBuffer.InfluxStateStore = true;
            using var pusher = new InfluxPusher(cfg.Influx);
            using var dPusher = new DummyPusher(new DummyPusherConfig());
            pusher.Extractor = extractor;
            var pushers = new IPusher[] { pusher, dPusher };
            var fb1 = new FailureBuffer(cfg, extractor, pusher);

            var estate1 = new EventExtractionState(extractor, new NodeId("emitter1"), false, false, false);
            var estate2 = new EventExtractionState(extractor, new NodeId("emitter2"), false, false, false);
            var estate3 = new EventExtractionState(extractor, new NodeId("emitter3"), true, true, false);

            extractor.State.SetEmitterState(estate1);
            extractor.State.SetEmitterState(estate2);
            extractor.State.SetEmitterState(estate3);

            var start = DateTime.UtcNow;

            var evts = GetEvents(start, estate1.SourceId, 100).Concat(GetEvents(start, estate3.SourceId, 100)).ToList();

            var evts1 = dPusher.Events[new NodeId("emitter1")] = new List<BufferedEvent>();
            var evts2 = dPusher.Events[new NodeId("emitter2")] = new List<BufferedEvent>();
            var evts3 = dPusher.Events[new NodeId("emitter3")] = new List<BufferedEvent>();

            using var client = await SetupInfluxdb(cfg.Influx);

            // Just read, this happens on startup. We'd expect nothing to really happen here.
            Assert.False(fb1.AnyPoints);
            Assert.True(await fb1.ReadEvents(pushers, tester.Source.Token));

            Assert.Empty(evts1);
            Assert.Empty(evts2);
            Assert.Empty(evts3);

            // Read, but with everything set to null
            string oldPath = cfg.FailureBuffer.EventPath;
            cfg.FailureBuffer.EventPath = null;
            cfg.FailureBuffer.Influx = false;

            Assert.True(await fb1.ReadEvents(pushers, tester.Source.Token));
            cfg.FailureBuffer.Influx = true;

            // With datapoints disabled, write to influx then ensure that it is all read back.

            var eventBufferStates = (Dictionary<string, InfluxBufferState>)fb1
                .GetType()
                .GetField("eventBufferStates", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(fb1);

            await pusher.PushEvents(evts, tester.Source.Token);
            await fb1.WriteEvents(evts, tester.Source.Token);

            Assert.Single(eventBufferStates);
            Assert.Equal(eventBufferStates.First().Value.DestinationExtractedRange, new TimeRange(start, start.AddSeconds(99)));

            Assert.True(await fb1.ReadEvents(pushers, tester.Source.Token));

            Assert.Equal(100, evts1.Count);
            Assert.Empty(evts2);
            Assert.Empty(evts3);

            Assert.Empty(eventBufferStates);

            var db = stateStore.GetDatabase();
            Assert.Equal(0, db.GetCollection(cfg.StateStorage.InfluxEventStore).Count());

            // Re-enable events, we'd expect to get points twice.
            cfg.FailureBuffer.EventPath = oldPath;

            // No need to push to influxdb again, since the events are already there
            // This just registers the events in the failurebuffer again, and writes them to file
            await fb1.WriteEvents(evts, tester.Source.Token);

            Assert.Single(eventBufferStates);
            Assert.True(new FileInfo(cfg.FailureBuffer.EventPath).Length > 0);

            Assert.True(await fb1.ReadEvents(pushers, tester.Source.Token));

            Assert.Equal(300, evts1.Count);

            Assert.Equal(0, new FileInfo(cfg.FailureBuffer.EventPath).Length);
        }
        [Fact]
        public async Task TestWriteDatapointsToFile()
        {
            var cfg = BuildConfig();
            using var extractor = tester.BuildExtractor();

            using var dPusher = new DummyPusher(new DummyPusherConfig());
            var pushers = new IPusher[] { dPusher };
            var fb1 = new FailureBuffer(cfg, extractor, null);

            var dt = new BufferedDataType(DataTypeIds.Double);
            var dt2 = new BufferedDataType(DataTypeIds.String);

            var state1 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state1"), "state1", NodeId.Null) { DataType = dt }, false, false, false);
            var state2 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state2"), "state2", NodeId.Null) { DataType = dt2 }, false, false, false);
            var state3 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state3"), "state3", NodeId.Null) { DataType = dt }, false, false, false);
            extractor.State.SetNodeState(state1, "state1");
            extractor.State.SetNodeState(state2, "state2");
            extractor.State.SetNodeState(state3, "state3");

            var dps1 = dPusher.DataPoints[(new NodeId("state1"), -1)] = new List<BufferedDataPoint>();
            var dps2 = dPusher.DataPoints[(new NodeId("state2"), -1)] = new List<BufferedDataPoint>();
            var dps3 = dPusher.DataPoints[(new NodeId("state3"), -1)] = new List<BufferedDataPoint>();

            foreach (var state in new[] { state1, state2, state3 })
            {
                state.InitToEmpty();
                state.FinalizeRangeInit();
                dPusher.UniqueToNodeId[state.DisplayName] = (state.SourceId, -1);
            }

            var start = DateTime.UtcNow;

            var dps = GetDps(start, "state1", 100)
                .Concat(Enumerable.Range(0, 100).Select(idx => new BufferedDataPoint(start.AddSeconds(idx), "state2", "value" + idx)))
                .Concat(GetDps(start, "state4", 100))
                .Concat(GetDps(start, "state3", 100)).ToList();

            Assert.Equal(0, new FileInfo(cfg.FailureBuffer.DatapointPath).Length);


            // Write and read the points
            var writeDpsMethod = fb1.GetType().GetMethod("WriteDatapointsToFile", BindingFlags.Instance | BindingFlags.NonPublic);
            writeDpsMethod.Invoke(fb1, new object[] { dps, tester.Source.Token });

            Assert.True(new FileInfo(cfg.FailureBuffer.DatapointPath).Length > 0);

            var readDpsMethod = fb1.GetType().GetMethod("ReadDatapointsFromFile", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.True(await (Task<bool>)readDpsMethod.Invoke(fb1, new object[] { pushers, tester.Source.Token }));

            Assert.Equal(100, dps1.Count);
            Assert.Equal(100, dps2.Count);
            Assert.Equal(100, dps3.Count);

            var originalDp = dps.First();
            var readDp = dps1.First();
            Assert.Equal(originalDp.DoubleValue, readDp.DoubleValue);
            Assert.Equal(originalDp.Id, readDp.Id);
            Assert.Equal(originalDp.Timestamp, readDp.Timestamp);

            var originalStrDp = dps.ElementAt(100);
            var readStrDp = dps2.First();
            Assert.Equal(originalStrDp.StringValue, readStrDp.StringValue);
            Assert.Equal(originalStrDp.Id, readStrDp.Id);
            Assert.Equal(originalStrDp.Timestamp, readStrDp.Timestamp);

            Assert.Equal(0, new FileInfo(cfg.FailureBuffer.DatapointPath).Length);
        }
        [Fact]
        public async Task TestWriteEventsToFile()
        {
            var cfg = BuildConfig();
            using var extractor = tester.BuildExtractor();

            using var dPusher = new DummyPusher(new DummyPusherConfig());
            var pushers = new IPusher[] { dPusher };
            var fb1 = new FailureBuffer(cfg, extractor, null);

            var estate1 = new EventExtractionState(extractor, new NodeId("emitter1"), false, false, false);
            var estate2 = new EventExtractionState(extractor, new NodeId("emitter2"), false, false, false);
            var estate3 = new EventExtractionState(extractor, new NodeId("emitter3"), true, true, false);

            extractor.State.SetEmitterState(estate1);
            extractor.State.SetEmitterState(estate2);
            extractor.State.SetEmitterState(estate3);

            var start = DateTime.UtcNow;

            var evts = GetEvents(start, estate1.SourceId, 100)
                .Concat(GetEvents(start, estate2.SourceId, 100))
                .Concat(GetEvents(start, new NodeId("somemissingemitter"), 100))
                .Concat(GetEvents(start, estate3.SourceId, 100)).ToList();

            var evts1 = dPusher.Events[new NodeId("emitter1")] = new List<BufferedEvent>();
            var evts2 = dPusher.Events[new NodeId("emitter2")] = new List<BufferedEvent>();
            var evts3 = dPusher.Events[new NodeId("emitter3")] = new List<BufferedEvent>();
            var evts4 = dPusher.Events[new NodeId("somemissingemitter")] = new List<BufferedEvent>();

            Assert.Equal(0, new FileInfo(cfg.FailureBuffer.EventPath).Length);

            // Write and read the events
            var writeEvtsMethod = fb1.GetType().GetMethod("WriteEventsToFile", BindingFlags.Instance | BindingFlags.NonPublic);
            writeEvtsMethod.Invoke(fb1, new object[] { evts, tester.Source.Token });

            Assert.True(new FileInfo(cfg.FailureBuffer.EventPath).Length > 0);

            var readEvtsMethod = fb1.GetType().GetMethod("ReadEventsFromFile", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.True(await (Task<bool>)readEvtsMethod.Invoke(fb1, new object[] { pushers, tester.Source.Token }));

            Assert.Equal(100, evts1.Count);
            Assert.Equal(100, evts2.Count);
            Assert.Equal(100, evts3.Count);
            Assert.Empty(evts4);

            var originalEvt = evts.First();
            var readEvt = evts1.First();
            Assert.Equal(originalEvt.EmittingNode, readEvt.EmittingNode);
            Assert.Equal(originalEvt.Time, readEvt.Time);

            Assert.Equal(0, new FileInfo(cfg.FailureBuffer.EventPath).Length);

        }
    }
}

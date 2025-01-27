using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    [Collection("Shared server tests")]
    public class FailureBufferTest
    {
        private static int idx;
        private readonly StaticServerTestFixture tester;
        public FailureBufferTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
            tester.Client.TypeManager.Reset();
        }

        private static FullConfig BuildConfig()
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
                    Interval = "-1",
                    Location = $"fb-store-{idx}.db",
                    VariableStore = "variables"
                }
            };
        }

        [Fact]
        public void TestBuildFailureBuffer()
        {
            using var stateStore = new DummyStateStore();
            using var extractor = tester.BuildExtractor(null, true, stateStore);
            var cfg = BuildConfig();

            Assert.False(File.Exists(cfg.FailureBuffer.DatapointPath));
            Assert.False(File.Exists(cfg.FailureBuffer.EventPath));

            var log = tester.Provider.GetRequiredService<ILogger<FailureBuffer>>();

            var fb1 = new FailureBuffer(log, cfg, extractor);
            Assert.True(File.Exists(cfg.FailureBuffer.DatapointPath));
            Assert.True(File.Exists(cfg.FailureBuffer.EventPath));

            Assert.False(fb1.AnyPoints);
            Assert.False(fb1.AnyEvents);

            File.WriteAllText(cfg.FailureBuffer.DatapointPath, "testtest");
            File.WriteAllText(cfg.FailureBuffer.EventPath, "testtest");

            var fb2 = new FailureBuffer(log, cfg, extractor);

            Assert.True(fb2.AnyPoints);
            Assert.True(fb2.AnyEvents);
        }

        private static IEnumerable<UADataPoint> GetDps(DateTime start, string id, int count)
        {
            return Enumerable.Range(0, count).Select(idx => new UADataPoint(start.AddSeconds(idx), id, idx, StatusCodes.Good));
        }

        private static IEnumerable<UAEvent> GetEvents(DateTime start, NodeId id, int cnt)
        {
            return Enumerable.Range(0, cnt).Select(idx => new UAEvent { EmittingNode = id, Time = start.AddSeconds(idx) });
        }

        [Fact]
        public async Task TestWriteDatapointsToFile()
        {
            var cfg = BuildConfig();
            using var extractor = tester.BuildExtractor();

            using var pusher = new DummyPusher(new DummyPusherConfig());

            var log = tester.Provider.GetRequiredService<ILogger<FailureBuffer>>();
            var fb1 = new FailureBuffer(log, cfg, extractor);

            var dt = new UADataType(DataTypeIds.Double);
            var dt2 = new UADataType(DataTypeIds.String);

            var state1 = new VariableExtractionState(extractor, CommonTestUtils.GetSimpleVariable("state1", dt), false, false, true);
            var state2 = new VariableExtractionState(extractor, CommonTestUtils.GetSimpleVariable("state2", dt2), false, false, true);
            var state3 = new VariableExtractionState(extractor, CommonTestUtils.GetSimpleVariable("state3", dt), false, false, true);
            var nodeStates = new[] { state1, state2, state3 };

            extractor.State.SetNodeState(state1, "state1");
            extractor.State.SetNodeState(state2, "state2");
            extractor.State.SetNodeState(state3, "state3");

            var dps1 = pusher.DataPoints[(new NodeId("state1", 0), -1)] = new List<UADataPoint>();
            var dps2 = pusher.DataPoints[(new NodeId("state2", 0), -1)] = new List<UADataPoint>();
            var dps3 = pusher.DataPoints[(new NodeId("state3", 0), -1)] = new List<UADataPoint>();

            foreach (var state in new[] { state1, state2, state3 })
            {
                state.InitToEmpty();
                state.FinalizeRangeInit();
                pusher.UniqueToNodeId[state.DisplayName] = (state.SourceId, -1);
            }

            var start = DateTime.UtcNow;

            var dps = GetDps(start, "state1", 100)
                .Concat(Enumerable.Range(0, 100).Select(idx => new UADataPoint(start.AddSeconds(idx), "state2", "value" + idx, StatusCodes.Good)))
                .Concat(GetDps(start, "state4", 100))
                .Concat(GetDps(start, "state3", 100)).ToList();

            Assert.Equal(0, new FileInfo(cfg.FailureBuffer.DatapointPath).Length);


            // Write and read the points
            var writeDpsMethod = fb1.GetType().GetMethod("WriteDatapointsToFile", BindingFlags.Instance | BindingFlags.NonPublic);
            writeDpsMethod.Invoke(fb1, new object[] { dps, tester.Source.Token });

            Assert.True(new FileInfo(cfg.FailureBuffer.DatapointPath).Length > 0);

            var readDpsMethod = fb1.GetType().GetMethod("ReadDatapointsFromFile", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.True(await (Task<bool>)readDpsMethod.Invoke(fb1, new object[] { pusher, tester.Source.Token }));

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
        public void TestWriteDatapointsCap()
        {
            var log = tester.Provider.GetRequiredService<ILogger<FailureBuffer>>();
            var cfg = BuildConfig();
            cfg.FailureBuffer.MaxBufferSize = 2000;
            using var extractor = tester.BuildExtractor();
            var fb = new FailureBuffer(log, cfg, extractor);

            var writeDps = fb.GetType().GetMethod("WriteDatapointsToFile", BindingFlags.Instance | BindingFlags.NonPublic);

            var dps = Enumerable.Range(0, 200).Select(idx => new UADataPoint(DateTime.UtcNow, "id", idx, StatusCodes.Good));

            writeDps.Invoke(fb, new object[] { dps.Take(10), tester.Source.Token });

            var size = new FileInfo(cfg.FailureBuffer.DatapointPath).Length;
            Assert.True(size > 0 && size < 2000);

            writeDps.Invoke(fb, new object[] { dps.Skip(10), tester.Source.Token });
            size = new FileInfo(cfg.FailureBuffer.DatapointPath).Length;
            Assert.True(size <= 2000 && size > 1900, $"Expected size between 1900 and 2000 bytes, got {size}");
        }

        [Fact]
        public async Task TestWriteEventsToFile()
        {
            var cfg = BuildConfig();
            using var extractor = tester.BuildExtractor();

            using var pusher = new DummyPusher(new DummyPusherConfig());

            var log = tester.Provider.GetRequiredService<ILogger<FailureBuffer>>();
            var fb1 = new FailureBuffer(log, cfg, extractor);

            var estate1 = new EventExtractionState(extractor, new NodeId("emitter1", 0), false, false, true);
            var estate2 = new EventExtractionState(extractor, new NodeId("emitter2", 0), false, false, true);
            var estate3 = new EventExtractionState(extractor, new NodeId("emitter3", 0), true, true, true);

            extractor.State.SetEmitterState(estate1);
            extractor.State.RegisterNode(estate1.SourceId, estate1.Id);
            extractor.State.SetEmitterState(estate2);
            extractor.State.RegisterNode(estate2.SourceId, estate2.Id);
            extractor.State.SetEmitterState(estate3);
            extractor.State.RegisterNode(estate3.SourceId, estate3.Id);

            var start = DateTime.UtcNow;

            var evts = GetEvents(start, estate1.SourceId, 100)
                .Concat(GetEvents(start, estate2.SourceId, 100))
                .Concat(GetEvents(start, new NodeId("somemissingemitter", 0), 100))
                .Concat(GetEvents(start, estate3.SourceId, 100)).ToList();

            var evts1 = pusher.Events[new NodeId("emitter1", 0)] = new List<UAEvent>();
            var evts2 = pusher.Events[new NodeId("emitter2", 0)] = new List<UAEvent>();
            var evts3 = pusher.Events[new NodeId("emitter3", 0)] = new List<UAEvent>();
            var evts4 = pusher.Events[new NodeId("somemissingemitter", 0)] = new List<UAEvent>();

            Assert.Equal(0, new FileInfo(cfg.FailureBuffer.EventPath).Length);

            // Write and read the events
            var writeEvtsMethod = fb1.GetType().GetMethod("WriteEventsToFile", BindingFlags.Instance | BindingFlags.NonPublic);
            writeEvtsMethod.Invoke(fb1, new object[] { evts, tester.Source.Token });

            Assert.True(new FileInfo(cfg.FailureBuffer.EventPath).Length > 0);

            var readEvtsMethod = fb1.GetType().GetMethod("ReadEventsFromFile", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.True(await (Task<bool>)readEvtsMethod.Invoke(fb1, new object[] { pusher, tester.Source.Token }));

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

        [Fact]
        public void TestWriteEventsCap()
        {
            var log = tester.Provider.GetRequiredService<ILogger<FailureBuffer>>();
            var cfg = BuildConfig();
            cfg.FailureBuffer.MaxBufferSize = 2000;
            using var extractor = tester.BuildExtractor();
            var fb = new FailureBuffer(log, cfg, extractor);

            var writeEvents = fb.GetType().GetMethod("WriteEventsToFile", BindingFlags.Instance | BindingFlags.NonPublic);

            var dps = Enumerable.Range(0, 200).Select(idx => new UAEvent
            {
                EventId = $"id-{idx}",
                Message = $"message-{idx}",
                SourceNode = ObjectIds.Server,
                EmittingNode = ObjectIds.Server
            });

            writeEvents.Invoke(fb, new object[] { dps.Take(10), tester.Source.Token });

            var size = new FileInfo(cfg.FailureBuffer.EventPath).Length;
            Assert.True(size > 0 && size < 2000);

            writeEvents.Invoke(fb, new object[] { dps.Skip(10), tester.Source.Token });
            size = new FileInfo(cfg.FailureBuffer.EventPath).Length;
            Assert.True(size <= 2000 && size > 1900, $"Expected size between 1900 and 2000 bytes, got {size}");
        }
    }
}

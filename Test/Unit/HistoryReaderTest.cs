using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class HistoryReaderTestFixture : BaseExtractorTestFixture
    {
        public DateTime HistoryStart { get; }
        public HistoryReaderTestFixture() : base()
        {
            HistoryStart = DateTime.UtcNow.AddSeconds(-20);
            Server.PopulateCustomHistory(HistoryStart);
            Server.PopulateBaseHistory(HistoryStart);
            Server.PopulateEvents(HistoryStart);
        }
    }
    public class HistoryReaderTest : MakeConsoleWork, IClassFixture<HistoryReaderTestFixture>
    {
        private readonly HistoryReaderTestFixture tester;
        public HistoryReaderTest(ITestOutputHelper output, HistoryReaderTestFixture tester) : base(output)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
            this.tester = tester;
            tester.ResetConfig();
        }

        [Fact(Timeout = 10000)]
        public void TestHistoryDataHandler()
        {
            using var extractor = tester.BuildExtractor();
            var cfg = new HistoryConfig
            {
                Backfill = true,
                Data = true
            };

            using var throttler = new TaskThrottler(2, false);
            var cps = new BlockingResourceCounter(1000);

            var dummyState = new UAHistoryExtractionState(tester.Client, new NodeId("test"), true, true);

            using var reader = new HistoryScheduler(tester.Client, extractor, cfg, HistoryReadType.FrontfillData,
                throttler, cps, new [] { dummyState }, tester.Source.Token);
            using var backfillReader = new HistoryScheduler(tester.Client, extractor, cfg, HistoryReadType.BackfillData,
                throttler, cps, new[] { dummyState }, tester.Source.Token);

            var dt = new UADataType(DataTypeIds.Double);

            var var1 = new UAVariable(new NodeId("state1"), "state1", NodeId.Null);
            var1.VariableAttributes.DataType = dt;
            var state1 = new VariableExtractionState(extractor, var1, true, true);
            extractor.State.SetNodeState(state1, "state1");

            state1.FinalizeRangeInit();

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var historyDataHandler = reader.GetType().GetMethod("HistoryDataHandler", BindingFlags.NonPublic | BindingFlags.Instance);

            // Test null historyData
            var node = new HistoryReadNode(HistoryReadType.FrontfillData, new NodeId("state1")) { Completed = true };
            historyDataHandler.Invoke(reader, new object[] { node });
            Assert.Equal(0, node.TotalRead);

            // Test null datavalues
            var historyData = new HistoryData();

            var start = DateTime.UtcNow;

            // Test bad nodeId
            var frontfillDataValues = new DataValueCollection(Enumerable.Range(0, 100)
                .Select(idx => new DataValue(idx, StatusCodes.Good, start.AddSeconds(idx))));

            historyData.DataValues = frontfillDataValues;

            node = new HistoryReadNode(HistoryReadType.FrontfillData, new NodeId("badstate")) { Completed = true };
            node.LastResult = historyData;
            historyDataHandler.Invoke(reader, new object[] { node });
            Assert.Equal(0, node.TotalRead);
            Assert.False(state1.IsFrontfilling);
            state1.RestartHistory();

            // Test frontfill OK
            node = new HistoryReadNode(HistoryReadType.FrontfillData, new NodeId("state1")) { Completed = true };
            node.LastResult = historyData;
            historyDataHandler.Invoke(reader, new object[] { node });
            Assert.Equal(100, node.TotalRead);
            Assert.Equal(start.AddSeconds(99), state1.SourceExtractedRange.Last);
            Assert.False(state1.IsFrontfilling);
            Assert.Equal(100, queue.Count);

            // Test backfill OK
            var backfillDataValues = new DataValueCollection(Enumerable.Range(0, 100)
                .Select(idx => new DataValue(idx, StatusCodes.Good, start.AddSeconds(-idx))));
            historyData.DataValues = backfillDataValues;
            Assert.True(state1.IsBackfilling);
            node = new HistoryReadNode(HistoryReadType.BackfillData, new NodeId("state1")) { Completed = true };
            node.LastResult = historyData;
            historyDataHandler.Invoke(backfillReader, new object[] { node });
            Assert.Equal(100, node.TotalRead);
            Assert.Equal(new TimeRange(start.AddSeconds(-99), start.AddSeconds(99)), state1.SourceExtractedRange);
            Assert.False(state1.IsBackfilling);
            Assert.Equal(200, queue.Count);

            // Test bad datapoints
            CommonTestUtils.ResetMetricValue("opcua_bad_datapoints");
            var badDps = new DataValueCollection(Enumerable.Range(0, 100)
                .Select(idx => new DataValue(idx, StatusCodes.Bad, start.AddSeconds(100 + idx))).Concat(frontfillDataValues));
            historyData.DataValues = badDps;
            state1.RestartHistory();
            queue.Clear();
            node = new HistoryReadNode(HistoryReadType.FrontfillData, new NodeId("state1")) { Completed = false };
            node.LastResult = historyData;
            historyDataHandler.Invoke(reader, new object[] { node });
            Assert.Equal(100, node.TotalRead);
            Assert.Equal(start.AddSeconds(99), state1.SourceExtractedRange.Last);
            Assert.True(state1.IsFrontfilling);
            Assert.Equal(100, queue.Count);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_bad_datapoints", 100));

            // Test flush buffer
            historyData.DataValues = frontfillDataValues;
            state1.RestartHistory();
            queue.Clear();
            // Get a datapoint from stream that happened after the last history point was read from the server, but arrived
            // at the extractor before the history data was parsed. This is an edge-case, but a potential lost datapoint 
            state1.UpdateFromStream(new[] { new UADataPoint(start.AddSeconds(100), "state1", 1.0) });
            node = new HistoryReadNode(HistoryReadType.FrontfillData, new NodeId("state1")) { Completed = true };
            node.LastResult = historyData;
            historyDataHandler.Invoke(reader, new object[] { node });
            Assert.Equal(100, node.TotalRead);
            Assert.False(state1.IsFrontfilling);
            Assert.Equal(101, queue.Count);
            Assert.Equal(start.AddSeconds(100), state1.SourceExtractedRange.Last);

            // Test termination without cp
            historyData.DataValues = frontfillDataValues;
            state1.RestartHistory();
            queue.Clear();
            cfg.IgnoreContinuationPoints = true;
            node = new HistoryReadNode(HistoryReadType.FrontfillData, new NodeId("state1")) { Completed = true };
            node.LastResult = historyData;
            historyDataHandler.Invoke(reader, new object[] { node });
            Assert.Equal(100, node.TotalRead);
            Assert.True(state1.IsFrontfilling);
            Assert.Equal(100, queue.Count);
            Assert.Equal(start.AddSeconds(99), state1.SourceExtractedRange.Last);

            historyData.DataValues = null;
            node = new HistoryReadNode(HistoryReadType.FrontfillData, new NodeId("state1")) { Completed = false };
            node.LastResult = historyData;
            historyDataHandler.Invoke(reader, new object[] { node });
            Assert.Equal(0, node.TotalRead);
            Assert.False(state1.IsFrontfilling);
        }
        [Fact(Timeout = 10000)]
        public void TestHistoryEventHandler()
        {
            using var extractor = tester.BuildExtractor();
            var cfg = new HistoryConfig
            {
                Backfill = true
            };

            using var throttler = new TaskThrottler(2, false);
            var cps = new BlockingResourceCounter(1000);

            var dummyState = new UAHistoryExtractionState(tester.Client, new NodeId("test"), true, true);

            using var reader = new HistoryScheduler(tester.Client, extractor, cfg, HistoryReadType.FrontfillEvents,
                throttler, cps, new[] { dummyState }, tester.Source.Token);
            using var backfillReader = new HistoryScheduler(tester.Client, extractor, cfg, HistoryReadType.BackfillEvents,
                throttler, cps, new[] { dummyState }, tester.Source.Token);

            var state = EventUtils.PopulateEventData(extractor, tester, false);

            state.FinalizeRangeInit();

            var queue = (Queue<UAEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var filter = new EventFilter { SelectClauses = EventUtils.GetSelectClause(tester) };
            var details = new ReadEventDetails();

            var historyEventHandler = reader.GetType().GetMethod("HistoryEventHandler", BindingFlags.NonPublic | BindingFlags.Instance);

            // Test null historydata
            details.Filter = filter;
            var node = new HistoryReadNode(HistoryReadType.FrontfillEvents, new NodeId("emitter")) { Completed = true };
            historyEventHandler.Invoke(reader, new object[] { node, details });
            Assert.Equal(0, node.TotalRead);
            Assert.False(state.IsFrontfilling);
            state.RestartHistory();

            // Test null details
            var start = DateTime.UtcNow;
            var frontfillEvents = new HistoryEventFieldListCollection(Enumerable.Range(0, 100)
                .Select(idx => EventUtils.GetEventValues(start.AddSeconds(idx)))
                .Select(values => new HistoryEventFieldList { EventFields = values }));
            var historyEvents = new HistoryEvent { Events = frontfillEvents };
            node = new HistoryReadNode(HistoryReadType.FrontfillEvents, new NodeId("emitter")) { Completed = true };
            node.LastResult = historyEvents;
            historyEventHandler.Invoke(reader, new object[] { node, null });
            Assert.Equal(0, node.TotalRead);

            // Test bad emitter
            historyEvents.Events = frontfillEvents;
            node = new HistoryReadNode(HistoryReadType.FrontfillEvents, new NodeId("bademitter")) { Completed = true };
            node.LastResult = historyEvents;
            historyEventHandler.Invoke(reader, new object[] { node, details });
            Assert.Equal(0, node.TotalRead);

            // Test frontfill OK
            node = new HistoryReadNode(HistoryReadType.FrontfillEvents, new NodeId("emitter")) { Completed = true };
            node.LastResult = historyEvents;
            historyEventHandler.Invoke(reader, new object[] { node, details });
            Assert.Equal(100, node.TotalRead);
            Assert.Equal(start.AddSeconds(99), state.SourceExtractedRange.Last);
            Assert.False(state.IsFrontfilling);
            Assert.Equal(100, queue.Count);

            // Test backfill OK
            var backfillEvents = new HistoryEventFieldListCollection(Enumerable.Range(0, 100)
                .Select(idx => EventUtils.GetEventValues(start.AddSeconds(-idx)))
                .Select(values => new HistoryEventFieldList { EventFields = values }));
            historyEvents.Events = backfillEvents;
            Assert.True(state.IsBackfilling);
            node = new HistoryReadNode(HistoryReadType.BackfillEvents, new NodeId("emitter")) { Completed = true };
            node.LastResult = historyEvents;
            historyEventHandler.Invoke(backfillReader, new object[] { node, details });
            Assert.Equal(100, node.TotalRead);
            Assert.Equal(new TimeRange(start.AddSeconds(-99), start.AddSeconds(99)), state.SourceExtractedRange);
            Assert.False(state.IsBackfilling);
            Assert.Equal(200, queue.Count);

            // Test bad events
            CommonTestUtils.ResetMetricValue("opcua_bad_events");
            var badEvts = new HistoryEventFieldListCollection(Enumerable.Range(0, 100)
                .Select(idx =>
                {
                    var values = EventUtils.GetEventValues(start.AddSeconds(idx));
                    values[0] = Variant.Null;
                    return values;
                })
                .Select(values => new HistoryEventFieldList { EventFields = values }));

            historyEvents.Events = badEvts;
            state.RestartHistory();
            queue.Clear();
            node = new HistoryReadNode(HistoryReadType.FrontfillEvents, new NodeId("emitter")) { Completed = false };
            node.LastResult = historyEvents;
            historyEventHandler.Invoke(reader, new object[] { node, details });
            Assert.Equal(0, node.TotalRead);
            Assert.Equal(start.AddSeconds(99), state.SourceExtractedRange.Last);
            Assert.True(state.IsFrontfilling);
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_bad_events", 100));

            // Test flush buffer
            historyEvents.Events = frontfillEvents;
            state.RestartHistory();
            queue.Clear();
            state.UpdateFromStream(new UAEvent { Time = start.AddSeconds(100) });
            node = new HistoryReadNode(HistoryReadType.FrontfillEvents, new NodeId("emitter")) { Completed = true };
            node.LastResult = historyEvents;
            historyEventHandler.Invoke(reader, new object[] { node, details });
            Assert.Equal(100, node.TotalRead);
            Assert.False(state.IsFrontfilling);
            Assert.Equal(101, queue.Count);
            Assert.Equal(start.AddSeconds(100), state.SourceExtractedRange.Last);

            // Test termination without cp
            historyEvents.Events = frontfillEvents;
            state.RestartHistory();
            queue.Clear();
            cfg.IgnoreContinuationPoints = true;
            node = new HistoryReadNode(HistoryReadType.FrontfillEvents, new NodeId("emitter")) { Completed = true };
            node.LastResult = historyEvents;
            historyEventHandler.Invoke(reader, new object[] { node, details });
            Assert.Equal(100, node.TotalRead);
            Assert.True(state.IsFrontfilling);
            Assert.Equal(100, queue.Count);
            Assert.Equal(start.AddSeconds(99), state.SourceExtractedRange.Last);

            historyEvents.Events = null;
            node = new HistoryReadNode(HistoryReadType.FrontfillEvents, new NodeId("emitter")) { Completed = false };
            node.LastResult = historyEvents;
            historyEventHandler.Invoke(reader, new object[] { node, details });
            Assert.Equal(0, node.TotalRead);
            Assert.False(state.IsFrontfilling);
        }
        [Fact(Timeout = 10000)]
        public async Task TestFrontfillData()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true,
                Data = true
            };

            using var reader = new HistoryReader(tester.Client, extractor, cfg, tester.Source.Token);

            var dt = new UADataType(DataTypeIds.Double);
            var dt2 = new UADataType(DataTypeIds.String);

            var states = new[] { tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Base.DoubleVar1, tester.Server.Ids.Base.StringVar }
                .Select((id, idx) => new VariableExtractionState(
                    extractor,
                    CommonTestUtils.GetSimpleVariable("state",
                        idx == 3 ? dt2 : dt,
                        idx == 1 ? 4 : 0,
                        id),
                    true, true))
                .ToList();

            var start = tester.HistoryStart.AddSeconds(5);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetNodeState(state);
            }

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");

            // Test no states
            await reader.FrontfillData(Enumerable.Empty<VariableExtractionState>());
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 0));

            // Test read half
            await reader.FrontfillData(states);
            // 4 nodes, one is array of 4, half of history = 3*500 + 4*500
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));

            // Test read half, with node chunking
            cfg.DataNodesChunk = 2;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");
            await reader.FrontfillData(states);
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));

            // Test read half, with result chunking
            cfg.DataChunk = 100;
            cfg.DataNodesChunk = 100;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");
            await reader.FrontfillData(states);
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 5));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));
            foreach (var state in states)
            {
                Assert.False(state.IsFrontfilling);
                // 1000*10ms, but the first timestamp is included, so subtract 10 ms
                Assert.Equal(tester.HistoryStart.AddMilliseconds(9990), state.SourceExtractedRange.Last);
            }

            // Test read without using continuation points
            // We expect this to give one duplicate on each read, and one extra read at the end.
            cfg.IgnoreContinuationPoints = true;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");
            await Task.WhenAny(reader.FrontfillData(states), Task.Delay(10000));
            Assert.Equal(3542, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 7));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3542));
            foreach (var state in states)
            {
                Assert.False(state.IsFrontfilling);
                Assert.Equal(tester.HistoryStart.AddMilliseconds(9990), state.SourceExtractedRange.Last);
            }
        }
        [Fact(Timeout = 10000)]
        public async Task TestBackfillData()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true
            };

            using var reader = new HistoryReader(tester.Client, extractor, cfg, tester.Source.Token);

            var dt = new UADataType(DataTypeIds.Double);
            var dt2 = new UADataType(DataTypeIds.String);

            var states = new[] { tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Base.DoubleVar1, tester.Server.Ids.Base.StringVar }
                .Select((id, idx) => new VariableExtractionState(
                    extractor,
                    CommonTestUtils.GetSimpleVariable("state",
                        idx == 3 ? dt2 : dt,
                        idx == 1 ? 4 : 0,
                        id),
                    true, true))
                .ToList();

            var start = tester.HistoryStart.AddSeconds(5).AddMilliseconds(-10);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetNodeState(state);
            }

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            CommonTestUtils.ResetMetricValues("opcua_backfill_data_count", "opcua_backfill_data_points");

            // Test no states
            await reader.BackfillData(Enumerable.Empty<VariableExtractionState>());
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 0));

            // Test read half
            await reader.BackfillData(states);
            // 4 nodes, one is array of 4, half of history = 3*500 + 4*500
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_points", 3500));

            // Test read half, with node chunking
            cfg.DataNodesChunk = 2;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_data_count", "opcua_backfill_data_points");
            await reader.BackfillData(states);
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_points", 3500));

            // Test read half, with result chunking
            cfg.DataChunk = 100;
            cfg.DataNodesChunk = 100;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_data_count", "opcua_backfill_data_points");
            await reader.BackfillData(states);
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 5));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_points", 3500));
            foreach (var state in states)
            {
                Assert.False(state.IsBackfilling);
                Assert.Equal(tester.HistoryStart, state.SourceExtractedRange.First);
            }

            // Test read without using continuation points
            // We expect this to give duplicates on each read and one extra read at the end.
            cfg.IgnoreContinuationPoints = true;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_data_count", "opcua_backfill_data_points");
            await Task.WhenAny(reader.BackfillData(states), Task.Delay(10000));
            Assert.Equal(3542, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 7));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_points", 3542));
            foreach (var state in states)
            {
                Assert.False(state.IsBackfilling);
                Assert.Equal(tester.HistoryStart, state.SourceExtractedRange.First);
            }
        }
        [Fact(Timeout = 10000)]
        public async Task TestFrontfillEvents()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true,
                Data = true
            };

            using var reader = new HistoryReader(tester.Client, extractor, cfg, tester.Source.Token);

            var states = new[]
            {
                new EventExtractionState(tester.Client, ObjectIds.Server, true, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj1, true, true, true)
            };

            var start = tester.HistoryStart.AddSeconds(5);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetEmitterState(state);
            }

            var queue = (Queue<UAEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var fields = await tester.Client.GetEventFields(null, tester.Source.Token);
            foreach (var pair in fields)
            {
                extractor.State.ActiveEvents[pair.Key] = pair.Value;
            }

            CommonTestUtils.ResetMetricValues("opcua_frontfill_events_count", "opcua_frontfill_events");

            // Test no states
            await reader.FrontfillEvents(Enumerable.Empty<EventExtractionState>());
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 0));

            // Test read half
            await reader.FrontfillEvents(states);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 500));

            // Test read half, with node chunking
            cfg.EventNodesChunk = 1;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_events_count", "opcua_frontfill_events");
            await reader.FrontfillEvents(states);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 500));

            // Test read half, with result chunking
            cfg.EventChunk = 100;
            cfg.EventNodesChunk = 100;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_events_count", "opcua_frontfill_events");
            await reader.FrontfillEvents(states);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            // 100 events from obj1, 400 from the server. They are read together, so first read 100 from both,
            // then read 100 from the server four times.
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 4));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 500));
            foreach (var state in states)
            {
                Assert.False(state.IsFrontfilling);
                // 100*100ms, but the first timestamp is included, so subtract 100 ms
                Assert.Equal(tester.HistoryStart.AddMilliseconds(9900), state.SourceExtractedRange.Last);
            }

            // Test read without using continuation points
            // We expect this to give the exact same results as normal chunking, except we get one extra read,
            // and we get duplicates between each read, since we cannot guarantee that all in a given time chunk have been retrieved.
            // Really, when using this config option, you should read a single node per request.
            cfg.IgnoreContinuationPoints = true;
            cfg.Granularity = 1;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_events_count", "opcua_frontfill_events");
            await Task.WhenAny(reader.FrontfillEvents(states), Task.Delay(10000));
            Assert.Equal(526, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 7));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 526));
            foreach (var state in states)
            {
                Assert.False(state.IsFrontfilling);
                Assert.Equal(tester.HistoryStart.AddMilliseconds(9900), state.SourceExtractedRange.Last);
            }
        }
        [Fact(Timeout = 10000)]
        public async Task TestBackfillEvents()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true,
            };

            using var reader = new HistoryReader(tester.Client, extractor, cfg, tester.Source.Token);

            var states = new[]
            {
                new EventExtractionState(tester.Client, ObjectIds.Server, true, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj1, true, true, true)
            };

            var start = tester.HistoryStart.AddSeconds(5).AddMilliseconds(-100);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetEmitterState(state);
            }

            var queue = (Queue<UAEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var fields = await tester.Client.GetEventFields(null, tester.Source.Token);
            foreach (var pair in fields)
            {
                extractor.State.ActiveEvents[pair.Key] = pair.Value;
            }

            CommonTestUtils.ResetMetricValues("opcua_backfill_events_count", "opcua_backfill_events");

            // Test no states
            await reader.BackfillEvents(Enumerable.Empty<EventExtractionState>());
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 0));

            // Test read half
            await reader.BackfillEvents(states);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 500));

            // Test read half, with node chunking
            cfg.EventNodesChunk = 1;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_events_count", "opcua_backfill_events");
            await reader.BackfillEvents(states);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 500));

            // Test read half, with result chunking
            cfg.EventChunk = 100;
            cfg.EventNodesChunk = 100;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_events_count", "opcua_backfill_events");
            await reader.BackfillEvents(states);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            // 100 events from obj1, 400 from the server. They are read together, so first read 100 from both,
            // then read 100 from the server four times.
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 4));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 500));
            foreach (var state in states)
            {
                Assert.False(state.IsBackfilling);
                Assert.Equal(tester.HistoryStart, state.SourceExtractedRange.First);
            }

            // Test read without using continuation points
            // We expect this to give the exact same results as normal chunking, except we get one extra read,
            // and we get duplicates between each read, since we cannot guarantee that all in a given time chunk have been retrieved.
            cfg.IgnoreContinuationPoints = true;
            cfg.Granularity = 1;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_events_count", "opcua_backfill_events");
            await Task.WhenAny(reader.BackfillEvents(states), Task.Delay(10000));
            Assert.Equal(526, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 7));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 526));
            foreach (var state in states)
            {
                Assert.False(state.IsBackfilling);
                Assert.Equal(tester.HistoryStart, state.SourceExtractedRange.First);
            }
        }
        [Theory(Timeout = 10000)]
        [InlineData(1, 900)]
        [InlineData(4, 0)]
        public async Task TestReadGranularity(int expectedReads, int granularity)
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true,
                Data = true,
                Granularity = granularity
            };

            using var reader = new HistoryReader(tester.Client, extractor, cfg, tester.Source.Token);

            var dt = new UADataType(DataTypeIds.Double);
            var dt2 = new UADataType(DataTypeIds.String);

            var states = new[] { tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Base.DoubleVar1, tester.Server.Ids.Base.StringVar }
                .Select((id, idx) => new VariableExtractionState(
                    extractor,
                    CommonTestUtils.GetSimpleVariable("state",
                        idx == 3 ? dt2 : dt,
                        idx == 1 ? 4 : 0,
                        id),
                    true, true))
                .ToList();

            var start = tester.HistoryStart.AddSeconds(5);

            int i = 0;
            foreach (var state in states)
            {
                state.InitExtractedRange(start, start.AddTicks(--i));
                state.FinalizeRangeInit();
                extractor.State.SetNodeState(state);
            }

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points", "opcua_history_reads");

            await reader.FrontfillData(states);
            // 4 nodes, one is array of 4, half of history = 3*500 + 4*500
            Assert.Equal(3500, queue.Count);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_history_reads", expectedReads));
        }

        [Fact(Timeout = 10000)]
        public async Task TestHistoryThrottling()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true,
                Data = true
            };


            var dt = new UADataType(DataTypeIds.Double);
            var dt2 = new UADataType(DataTypeIds.String);

            var states = new[] { tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Base.DoubleVar1, tester.Server.Ids.Base.StringVar }
                .Select((id, idx) => new VariableExtractionState(
                    extractor,
                    CommonTestUtils.GetSimpleVariable("state",
                        idx == 3 ? dt2 : dt,
                        idx == 1 ? 4 : 0,
                        id),
                    true, true))
                .ToList();

            var start = tester.HistoryStart.AddSeconds(5);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetNodeState(state);
            }

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");

            cfg.Throttling = new ContinuationPointThrottlingConfig
            {
                MaxNodeParallelism = 1,
            };
            using (var reader = new HistoryReader(tester.Client, extractor, cfg, tester.Source.Token))
            {
                await reader.FrontfillData(states);
                Assert.Equal(3500, queue.Count);
                // 4 since nodes may not be read in parallel
                Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 4));
                Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));

                // Similar for 2 parallelism
                cfg.Throttling.MaxNodeParallelism = 2;
            }


            using (var reader = new HistoryReader(tester.Client, extractor, cfg, tester.Source.Token))
            {
                queue.Clear();
                foreach (var state in states) state.RestartHistory();
                CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");
                await reader.FrontfillData(states);
                Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 2));
                Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));

                // Task throttling should have no effect on metrics, since this is about the number of actual parallel tasks in total
                cfg.Throttling.MaxParallelism = 2;
                queue.Clear();
                foreach (var state in states) state.RestartHistory();
                CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");
                await reader.FrontfillData(states);
                Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 2));
                Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));

                // Expect this to not cause issues
                cfg.Throttling.MaxPerMinute = 2;
                queue.Clear();
                foreach (var state in states) state.RestartHistory();
                CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");
                await reader.FrontfillData(states);
                Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 2));
                Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));
            }
        }
    }
}

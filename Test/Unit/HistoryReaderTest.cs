using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
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
        public HistoryReaderTestFixture() : base(62500)
        {
            HistoryStart = DateTime.UtcNow.AddSeconds(-20);
            Server.PopulateArrayHistory(HistoryStart);
            Server.PopulateBaseHistory(HistoryStart);
            Server.PopulateEvents(HistoryStart);
        }
    }
    public class HistoryReaderTest : MakeConsoleWork, IClassFixture<HistoryReaderTestFixture>
    {
        private readonly HistoryReaderTestFixture tester;
        public HistoryReaderTest(ITestOutputHelper output, HistoryReaderTestFixture tester) : base(output)
        {
            this.tester = tester;
        }

        [Fact]
        public void TestHistoryDataHandler()
        {
            using var extractor = tester.BuildExtractor();
            var cfg = new HistoryConfig
            {
                Backfill = true,
                Data = true
            };

            var reader = new HistoryReader(tester.Client, extractor, cfg);

            var dt = new BufferedDataType(DataTypeIds.Double);

            var state1 = new NodeExtractionState(extractor,
                new BufferedVariable(new NodeId("state1"), "state1", NodeId.Null) { DataType = dt }, true, true);
            extractor.State.SetNodeState(state1, "state1");

            state1.FinalizeRangeInit();

            var queue = (Queue<BufferedDataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var historyDataHandler = reader.GetType().GetMethod("HistoryDataHandler", BindingFlags.NonPublic | BindingFlags.Instance);

            // Test null historyData
            Assert.Equal(0, historyDataHandler.Invoke(reader, new object[] { null, true, true, new NodeId("state1"), null }));

            // Test null datavalues
            var historyData = new HistoryData();

            var start = DateTime.UtcNow;

            // Test bad nodeId
            var frontfillDataValues = new DataValueCollection(Enumerable.Range(0, 100)
                .Select(idx => new DataValue(idx, StatusCodes.Good, start.AddSeconds(idx))));

            historyData.DataValues = frontfillDataValues;

            Assert.Equal(0, historyDataHandler.Invoke(reader, new object[] { historyData, true, true, new NodeId("badstate"), null }));

            // Test frontfill OK
            Assert.True(state1.IsFrontfilling);
            Assert.Equal(100, historyDataHandler.Invoke(reader, new object[] { historyData, true, true, new NodeId("state1"), null }));
            Assert.Equal(start.AddSeconds(99), state1.SourceExtractedRange.Last);
            Assert.False(state1.IsFrontfilling);
            Assert.Equal(100, queue.Count);

            // Test backfill OK
            var backfillDataValues = new DataValueCollection(Enumerable.Range(0, 100)
                .Select(idx => new DataValue(idx, StatusCodes.Good, start.AddSeconds(-idx))));
            historyData.DataValues = backfillDataValues;
            Assert.True(state1.IsBackfilling);
            Assert.Equal(100, historyDataHandler.Invoke(reader, new object[] { historyData, true, false, new NodeId("state1"), null }));
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
            Assert.Equal(100, historyDataHandler.Invoke(reader, new object[] { historyData, false, true, new NodeId("state1"), null }));
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
            state1.UpdateFromStream(new[] { new BufferedDataPoint(start.AddSeconds(100), "state1", 1.0) });
            Assert.Equal(100, historyDataHandler.Invoke(reader, new object[] { historyData, true, true, new NodeId("state1"), null }));
            Assert.False(state1.IsFrontfilling);
            Assert.Equal(101, queue.Count);
            Assert.Equal(start.AddSeconds(100), state1.SourceExtractedRange.Last);
        }
        [Fact]
        public void TestHistoryEventHandler()
        {
            using var extractor = tester.BuildExtractor();
            var cfg = new HistoryConfig
            {
                Backfill = true
            };

            var reader = new HistoryReader(tester.Client, extractor, cfg);
            var state = EventUtils.PopulateEventData(extractor, tester, false);

            state.FinalizeRangeInit();

            var queue = (Queue<BufferedEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var filter = new EventFilter { SelectClauses = EventUtils.GetSelectClause(tester) };
            var details = new ReadEventDetails();

            var historyEventHandler = reader.GetType().GetMethod("HistoryEventHandler", BindingFlags.NonPublic | BindingFlags.Instance);

            // Test null historydata
            details.Filter = filter;
            Assert.Equal(0, historyEventHandler.Invoke(reader, new object[] { null, true, true, new NodeId("emitter"), details }));

            // Test null details
            var start = DateTime.UtcNow;
            var frontfillEvents = new HistoryEventFieldListCollection(Enumerable.Range(0, 100)
                .Select(idx => EventUtils.GetEventValues(start.AddSeconds(idx)))
                .Select(values => new HistoryEventFieldList { EventFields = values }));
            var historyEvents = new HistoryEvent { Events = frontfillEvents };
            Assert.Equal(0, historyEventHandler.Invoke(reader, new object[] { historyEvents, true, true, new NodeId("emitter"), null }));

            // Test no events
            historyEvents.Events = null;
            Assert.Equal(0, historyEventHandler.Invoke(reader, new object[] { historyEvents, true, true, new NodeId("emitter"), details }));

            // Test bad emitter
            historyEvents.Events = frontfillEvents;
            Assert.Equal(0, historyEventHandler.Invoke(reader, new object[] { historyEvents, true, true, new NodeId("bademitter"), details }));

            // Test frontfill OK
            Assert.True(state.IsFrontfilling);
            Assert.Equal(100, historyEventHandler.Invoke(reader, new object[] { historyEvents, true, true, new NodeId("emitter"), details }));
            Assert.Equal(start.AddSeconds(99), state.SourceExtractedRange.Last);
            Assert.False(state.IsFrontfilling);
            Assert.Equal(100, queue.Count);

            // Test backfill OK
            var backfillEvents = new HistoryEventFieldListCollection(Enumerable.Range(0, 100)
                .Select(idx => EventUtils.GetEventValues(start.AddSeconds(-idx)))
                .Select(values => new HistoryEventFieldList { EventFields = values }));
            historyEvents.Events = backfillEvents;
            Assert.True(state.IsBackfilling);
            Assert.Equal(100, historyEventHandler.Invoke(reader, new object[] { historyEvents, true, false, new NodeId("emitter"), details }));
            Assert.Equal(new TimeRange(start.AddSeconds(-99), start.AddSeconds(99)), state.SourceExtractedRange);
            Assert.False(state.IsBackfilling);
            Assert.Equal(200, queue.Count);

            // Test bad events
            CommonTestUtils.ResetMetricValue("opcua_bad_events");
            var badEvts = new HistoryEventFieldListCollection(Enumerable.Range(0, 100)
                .Select(idx => {
                    var values = EventUtils.GetEventValues(start.AddSeconds(-idx));
                    values[0] = Variant.Null;
                    return values;
                })
                .Select(values => new HistoryEventFieldList { EventFields = values })
                .Concat(frontfillEvents));

            historyEvents.Events = badEvts;
            state.RestartHistory();
            queue.Clear();
            Assert.Equal(100, historyEventHandler.Invoke(reader, new object[] { historyEvents, false, true, new NodeId("emitter"), details }));
            Assert.Equal(start.AddSeconds(99), state.SourceExtractedRange.Last);
            Assert.True(state.IsFrontfilling);
            Assert.Equal(100, queue.Count);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_bad_events", 100));

            // Test flush buffer
            historyEvents.Events = frontfillEvents;
            state.RestartHistory();
            queue.Clear();
            state.UpdateFromStream(new BufferedEvent { Time = start.AddSeconds(100) });
            Assert.Equal(100, historyEventHandler.Invoke(reader, new object[] { historyEvents, true, true, new NodeId("emitter"), details }));
            Assert.False(state.IsFrontfilling);
            Assert.Equal(101, queue.Count);
            Assert.Equal(start.AddSeconds(100), state.SourceExtractedRange.Last);
        }
        [Fact]
        public async Task TestFrontfillData()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true,
                Data = true
            };

            var reader = new HistoryReader(tester.Client, extractor, cfg);

            var dt = new BufferedDataType(DataTypeIds.Double);
            var dt2 = new BufferedDataType(DataTypeIds.String);

            var states = new[] { tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Base.DoubleVar1, tester.Server.Ids.Base.StringVar }
                .Select((id, idx) => new NodeExtractionState(
                    extractor, new BufferedVariable(id, "state", NodeId.Null) {
                        DataType = idx == 3 ? dt2 : dt, ArrayDimensions = idx == 1 ? new Collection<int>(new[] { 4 }) : null }, true, true))
                .ToList();

            var start = tester.HistoryStart.AddSeconds(5);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetNodeState(state);
            }

            var queue = (Queue<BufferedDataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");

            // Test no states
            await reader.FrontfillData(Enumerable.Empty<NodeExtractionState>(), tester.Source.Token);
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 0));

            // Test read half
            await reader.FrontfillData(states, tester.Source.Token);
            // 4 nodes, one is array of 4, half of history = 3*500 + 4*500
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));

            // Test read half, with node chunking
            cfg.DataNodesChunk = 2;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");
            await reader.FrontfillData(states, tester.Source.Token);
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_points", 3500));

            // Test read half, with result chunking
            cfg.DataChunk = 100;
            cfg.DataNodesChunk = 100;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_data_count", "opcua_frontfill_data_points");
            await reader.FrontfillData(states, tester.Source.Token);
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
        }
        [Fact]
        public async Task TestBackfillData()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true
            };

            var reader = new HistoryReader(tester.Client, extractor, cfg);

            var dt = new BufferedDataType(DataTypeIds.Double);
            var dt2 = new BufferedDataType(DataTypeIds.String);

            var states = new[] { tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Base.DoubleVar1, tester.Server.Ids.Base.StringVar }
                .Select((id, idx) => new NodeExtractionState(
                    extractor, new BufferedVariable(id, "state", NodeId.Null)
                    {
                        DataType = idx == 3 ? dt2 : dt,
                        ArrayDimensions = idx == 1 ? new Collection<int>(new[] { 4 }) : null
                    }, true, true))
                .ToList();

            var start = tester.HistoryStart.AddSeconds(5).AddMilliseconds(-10);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetNodeState(state);
            }

            var queue = (Queue<BufferedDataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            CommonTestUtils.ResetMetricValues("opcua_backfill_data_count", "opcua_backfill_data_points");

            // Test no states
            await reader.BackfillData(Enumerable.Empty<NodeExtractionState>(), tester.Source.Token);
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 0));

            // Test read half
            await reader.BackfillData(states, tester.Source.Token);
            // 4 nodes, one is array of 4, half of history = 3*500 + 4*500
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_points", 3500));

            // Test read half, with node chunking
            cfg.DataNodesChunk = 2;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_data_count", "opcua_backfill_data_points");
            await reader.BackfillData(states, tester.Source.Token);
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_points", 3500));

            // Test read half, with result chunking
            cfg.DataChunk = 100;
            cfg.DataNodesChunk = 100;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_data_count", "opcua_backfill_data_points");
            await reader.BackfillData(states, tester.Source.Token);
            Assert.Equal(3500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 5));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_points", 3500));
            foreach (var state in states)
            {
                Assert.False(state.IsBackfilling);
                Assert.Equal(tester.HistoryStart, state.SourceExtractedRange.First);
            }
        }
        [Fact]
        public async Task TestFrontfillEvents()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true,
                Data = true
            };

            var reader = new HistoryReader(tester.Client, extractor, cfg);

            var states = new[]
            {
                new EventExtractionState(tester.Client, ObjectIds.Server, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj1, true, true)
            };

            var start = tester.HistoryStart.AddSeconds(5);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetEmitterState(state);
            }

            var queue = (Queue<BufferedEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var fields = tester.Client.GetEventFields(tester.Source.Token);
            foreach (var pair in fields)
            {
                extractor.State.ActiveEvents[pair.Key] = pair.Value;
            }

            CommonTestUtils.ResetMetricValues("opcua_frontfill_events_count", "opcua_frontfill_events");

            // Test no states
            await reader.FrontfillEvents(Enumerable.Empty<EventExtractionState>(), tester.Source.Token);
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 0));

            // Test read half
            await reader.FrontfillEvents(states, tester.Source.Token);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 500));

            // Test read half, with node chunking
            cfg.EventNodesChunk = 1;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_events_count", "opcua_frontfill_events");
            await reader.FrontfillEvents(states, tester.Source.Token);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 500));

            // Test read half, with result chunking
            cfg.EventChunk = 100;
            cfg.EventNodesChunk = 100;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_frontfill_events_count", "opcua_frontfill_events");
            await reader.FrontfillEvents(states, tester.Source.Token);
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
        }
        [Fact]
        public async Task TestBackfillEvents()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true,
            };

            var reader = new HistoryReader(tester.Client, extractor, cfg);

            var states = new[]
            {
                new EventExtractionState(tester.Client, ObjectIds.Server, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj1, true, true)
            };

            var start = tester.HistoryStart.AddSeconds(5).AddMilliseconds(-100);

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetEmitterState(state);
            }

            var queue = (Queue<BufferedEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var fields = tester.Client.GetEventFields(tester.Source.Token);
            foreach (var pair in fields)
            {
                extractor.State.ActiveEvents[pair.Key] = pair.Value;
            }

            CommonTestUtils.ResetMetricValues("opcua_backfill_events_count", "opcua_backfill_events");

            // Test no states
            await reader.BackfillEvents(Enumerable.Empty<EventExtractionState>(), tester.Source.Token);
            Assert.Empty(queue);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 0));

            // Test read half
            await reader.BackfillEvents(states, tester.Source.Token);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 500));

            // Test read half, with node chunking
            cfg.EventNodesChunk = 1;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_events_count", "opcua_backfill_events");
            await reader.BackfillEvents(states, tester.Source.Token);
            Assert.Equal(500, queue.Count);
            queue.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 500));

            // Test read half, with result chunking
            cfg.EventChunk = 100;
            cfg.EventNodesChunk = 100;
            foreach (var state in states) state.RestartHistory();
            CommonTestUtils.ResetMetricValues("opcua_backfill_events_count", "opcua_backfill_events");
            await reader.BackfillEvents(states, tester.Source.Token);
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
        }
        [Fact]
        public async Task TestTerminate()
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Backfill = true
            };

            var reader = new HistoryReader(tester.Client, extractor, cfg);

            // There is no good way to test if it actually terminates, but we can do a superficial test of the terminate method itself.
            Assert.True(await reader.Terminate(tester.Source.Token, 0));

            // Set the running field
            reader.GetType()
                .GetField("running", BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(reader, 1);
            // Try to terminate with 0 timeout.
            Assert.False(await reader.Terminate(tester.Source.Token, 0));

            var waitTask = reader.Terminate(tester.Source.Token, 5);
            await Task.Delay(100);
            var aborting = (bool)reader.GetType()
                .GetField("aborting", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(reader);
            Assert.True(aborting);
            reader.GetType()
                .GetField("running", BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(reader, 0);

            Assert.True(await waitTask);
        }
    }
}

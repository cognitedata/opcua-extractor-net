﻿using Cognite.OpcUa;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
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
    public sealed class StreamerTestFixture : BaseExtractorTestFixture
    {
        public StreamerTestFixture() : base(62200) { }
    }
    public class StreamerTest : MakeConsoleWork, IClassFixture<StreamerTestFixture>
    {
        private StreamerTestFixture tester;
        public StreamerTest(ITestOutputHelper output, StreamerTestFixture tester) : base(output)
        {
            this.tester = tester;
        }

        [Fact]
        public async Task TestDpQueue()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());

            pusher.UniqueToNodeId["id"] = (new NodeId("id"), -1);
            var dps = new List<UADataPoint>();
            pusher.DataPoints[(new NodeId("id"), -1)] = dps;
            using var extractor = tester.BuildExtractor(pushers: pusher);

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var triggerPush = (ManualResetEvent)extractor.Looper.GetType()
                .GetField("triggerPush", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Looper);

            extractor.Streamer.AllowData = true;
            var start = DateTime.UtcNow;

            var state = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("id"), "test", NodeId.Null),
                false, false);
            state.InitToEmpty();
            state.FinalizeRangeInit();

            extractor.State.SetNodeState(state, "id");

            Assert.False(triggerPush.WaitOne(0));

            extractor.Streamer.Enqueue(new UADataPoint(start, "id", -1));
            Assert.Single(queue);
            await extractor.Streamer.PushDataPoints(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);

            Assert.Single(dps);
            Assert.Empty(queue);

            extractor.Streamer.Enqueue(Enumerable.Range(0, 2000000).Select(idx => new UADataPoint(start.AddMilliseconds(idx), "id", idx)));

            Assert.True(triggerPush.WaitOne(0));
            Assert.Equal(2000000, queue.Count);
            await extractor.Streamer.PushDataPoints(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            triggerPush.Reset();

            Assert.Equal(2000001, dps.Count);
            Assert.Empty(queue);

            extractor.Streamer.Enqueue(Enumerable.Range(2000000, 999999).Select(idx => new UADataPoint(start.AddMilliseconds(idx), "id", idx)));
            Assert.False(triggerPush.WaitOne(0));
            Assert.Equal(999999, queue.Count);

            extractor.Streamer.Enqueue(new UADataPoint(start.AddMilliseconds(3000000), "id", 300));
            Assert.True(triggerPush.WaitOne(0));
            Assert.Equal(1000000, queue.Count);

            await extractor.Streamer.PushDataPoints(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            Assert.Equal(start, state.DestinationExtractedRange.First);
            Assert.Equal(start.AddMilliseconds(3000000), state.DestinationExtractedRange.Last);
        }
        [Fact]
        public async Task TestEventQueue()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            var id = new NodeId("id");

            var queue = (Queue<UAEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var triggerPush = (ManualResetEvent)extractor.Looper.GetType()
                .GetField("triggerPush", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Looper);

            extractor.Streamer.AllowEvents = true;
            var start = DateTime.UtcNow;

            var state = new EventExtractionState(tester.Client, new NodeId("id"), false, false);
            state.InitToEmpty();
            state.FinalizeRangeInit();
            extractor.State.SetEmitterState(state);

            Assert.False(triggerPush.WaitOne(0));

            extractor.Streamer.Enqueue(new UAEvent { EmittingNode = id, Time = start });
            Assert.Single(queue);
            await extractor.Streamer.PushEvents(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);

            var evts = pusher.Events[id];

            Assert.Single(evts);
            Assert.Empty(queue);

            extractor.Streamer.Enqueue(Enumerable.Range(0, 200000).Select(idx =>
                new UAEvent { EmittingNode = id, Time = start.AddMilliseconds(idx) }));

            Assert.True(triggerPush.WaitOne(0));
            Assert.Equal(200000, queue.Count);
            await extractor.Streamer.PushEvents(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            triggerPush.Reset();

            Assert.Equal(200001, evts.Count);
            Assert.Empty(queue);

            extractor.Streamer.Enqueue(Enumerable.Range(200000, 99999).Select(idx =>
                new UAEvent { EmittingNode = id, Time = start.AddMilliseconds(idx) }));

            Assert.False(triggerPush.WaitOne(0));
            Assert.Equal(99999, queue.Count);

            extractor.Streamer.Enqueue(new UAEvent { EmittingNode = id, Time = start.AddMilliseconds(300000) });
            Assert.True(triggerPush.WaitOne(0));
            Assert.Equal(100000, queue.Count);

            await extractor.Streamer.PushEvents(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            Assert.Equal(start, state.DestinationExtractedRange.First);
            Assert.Equal(start.AddMilliseconds(300000), state.DestinationExtractedRange.Last);
        }

        [Fact]
        public async Task TestPushDataPoints()
        {
            // With two pushers: Push successfully - Push with one failing - Push with one reconnected
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var pusher2 = new DummyPusher(new DummyPusherConfig());

            pusher.UniqueToNodeId["id"] = (new NodeId("id"), -1);
            var dps = new List<UADataPoint>();
            pusher.DataPoints[(new NodeId("id"), -1)] = dps;

            pusher2.UniqueToNodeId["id"] = (new NodeId("id"), -1);
            var dps2 = new List<UADataPoint>();
            pusher2.DataPoints[(new NodeId("id"), -1)] = dps2;

            using var extractor = tester.BuildExtractor(true, null, pusher, pusher2);

            extractor.Streamer.AllowData = true;
            var start = DateTime.UtcNow;

            var state = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("id"), "test", NodeId.Null),
                true, true);
            state.InitToEmpty();
            state.FinalizeRangeInit();
            state.UpdateFromBackfill(DateTime.MaxValue, true);
            state.UpdateFromFrontfill(DateTime.MinValue, true);

            extractor.State.SetNodeState(state, "id");
            var toPush = Enumerable.Range(0, 1000).Select(idx => new UADataPoint(start.AddMilliseconds(idx), "id", idx)).ToList();
            extractor.Streamer.Enqueue(toPush);
            bool result = await extractor.Streamer.PushDataPoints(new[] { pusher, pusher2 }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            Assert.Equal(1000, dps.Count);
            Assert.Equal(1000, dps2.Count);
            Assert.False(result);

            Assert.Equal(dps, dps2);

            pusher.PushDataPointResult = false;
            extractor.Streamer.Enqueue(toPush);
            result = await extractor.Streamer.PushDataPoints(new[] { pusher, pusher2 }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            Assert.True(pusher.DataFailing);
            Assert.Equal(1000, dps.Count);
            Assert.Equal(2000, dps2.Count);
            Assert.False(result);

            extractor.Streamer.Enqueue(toPush);
            result = await extractor.Streamer.PushDataPoints(new[] { pusher2 }, new[] { pusher }, tester.Source.Token);
            Assert.True(pusher.DataFailing);
            Assert.Equal(1000, dps.Count);
            Assert.Equal(3000, dps2.Count);
            Assert.False(result);

            extractor.Streamer.Enqueue(toPush);
            pusher.PushDataPointResult = true;
            result = await extractor.Streamer.PushDataPoints(new[] { pusher2, pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            Assert.False(pusher.DataFailing);
            Assert.Equal(2000, dps.Count);
            Assert.Equal(4000, dps2.Count);
            Assert.True(result);
        }
        [Fact]
        public async Task TestPushEvents()
        {
            // With two pushers: Push successfully - Push with one failing - Push with one reconnected
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var pusher2 = new DummyPusher(new DummyPusherConfig());

            using var extractor = tester.BuildExtractor(true, null, pusher, pusher2);

            tester.Config.Events.History = true;
            extractor.Streamer.AllowEvents = true;
            var start = DateTime.UtcNow;

            var id = new NodeId("id");

            var state = new EventExtractionState(tester.Client, id, true, true);
            state.InitToEmpty();
            state.FinalizeRangeInit();
            state.UpdateFromBackfill(DateTime.MaxValue, true);
            state.UpdateFromFrontfill(DateTime.MinValue, true);

            extractor.State.SetEmitterState(state);
            var toPush = Enumerable.Range(0, 1000).Select(idx => new UAEvent { Time = start.AddMilliseconds(idx), EmittingNode = id }).ToList();
            extractor.Streamer.Enqueue(toPush);
            bool result = await extractor.Streamer.PushEvents(new[] { pusher, pusher2 }, Enumerable.Empty<IPusher>(), tester.Source.Token);

            var evts = pusher.Events[id];
            var evts2 = pusher2.Events[id];

            Assert.Equal(1000, evts.Count);
            Assert.Equal(1000, evts2.Count);
            Assert.False(result);

            Assert.Equal(evts, evts2);

            pusher.PushEventResult = false;
            extractor.Streamer.Enqueue(toPush);
            result = await extractor.Streamer.PushEvents(new[] { pusher, pusher2 }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            Assert.True(pusher.EventsFailing);
            Assert.Equal(1000, evts.Count);
            Assert.Equal(2000, evts2.Count);
            Assert.False(result);

            extractor.Streamer.Enqueue(toPush);
            result = await extractor.Streamer.PushEvents(new[] { pusher2 }, new[] { pusher }, tester.Source.Token);
            Assert.True(pusher.EventsFailing);
            Assert.Equal(1000, evts.Count);
            Assert.Equal(3000, evts2.Count);
            Assert.False(result);

            extractor.Streamer.Enqueue(toPush);
            pusher.PushEventResult = true;
            result = await extractor.Streamer.PushEvents(new[] { pusher2, pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
            Assert.False(pusher.EventsFailing);
            Assert.Equal(2000, evts.Count);
            Assert.Equal(4000, evts2.Count);
            Assert.True(result);

            tester.Config.Events.History = false;
        }
        [Fact]
        public void TestDataHandler()
        {
            using var extractor = tester.BuildExtractor();
            var node = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("id"), "node", NodeId.Null) { DataType = new UADataType(DataTypeIds.Double) },
                true, true);
            extractor.State.SetNodeState(node, "id");

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var now = DateTime.UtcNow;

            node.InitExtractedRange(now.Subtract(TimeSpan.FromSeconds(100)), now.Add(TimeSpan.FromSeconds(100)));
            node.FinalizeRangeInit();

            CommonTestUtils.ResetMetricValue("opcua_bad_datapoints");

            // Test against existing node
            var item = new MonitoredItem() { StartNodeId = new NodeId("id"), CacheQueueSize = 10 };
            var values = new[]
            {
                (DateTime.UtcNow, 100.0, StatusCodes.Good), // OK value
                (DateTime.UtcNow, 100.0, StatusCodes.Bad), // Bad value
                (DateTime.UtcNow.AddDays(1), -100.0, StatusCodes.Good), // Too late
                (DateTime.UtcNow.Subtract(TimeSpan.FromDays(1)), -100.0, StatusCodes.Good) // Too early
            };
            var notifications = values.Select(val => new MonitoredItemNotification
            {
                Value = new DataValue(val.Item2, val.Item3, val.Item1) { SourceTimestamp = val.Item1 },
            });
            foreach (var not in notifications) item.SaveValueInCache(not);
            extractor.Streamer.DataSubscriptionHandler(item, null);

            Assert.Single(queue);

            node.UpdateFromFrontfill(DateTime.MinValue, true);
            foreach (var not in notifications) item.SaveValueInCache(not);
            extractor.Streamer.DataSubscriptionHandler(item, null);

            Assert.Equal(3, queue.Count); // 2 more this time

            node.UpdateFromBackfill(DateTime.MaxValue, true);
            foreach (var not in notifications) item.SaveValueInCache(not);
            extractor.Streamer.DataSubscriptionHandler(item, null);

            Assert.Equal(6, queue.Count);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_bad_datapoints", 3));

            var item2 = new MonitoredItem() { StartNodeId = new NodeId("otherid"), CacheQueueSize = 10 };
            foreach (var not in notifications) item2.SaveValueInCache(not);
            extractor.Streamer.DataSubscriptionHandler(item2, null);

            Assert.Equal(6, queue.Count);
        }
        [Fact]
        public void TestToDataPoint()
        {
            CommonTestUtils.ResetMetricValue("opcua_array_points_missed");
            using var extractor = tester.BuildExtractor();
            var node1 = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("node1"), "node1", NodeId.Null) { DataType = new UADataType(DataTypeIds.Double) },
                true, true);

            var ts = DateTime.UtcNow;

            // Test normal update
            var dps1 = extractor.Streamer.ToDataPoint(new DataValue(-1, StatusCodes.Good, ts), node1);
            Assert.Single(dps1);
            Assert.Equal(-1.0, dps1.First().DoubleValue);
            Assert.False(dps1.First().Id.EndsWith(']'));

            // Test bad datapoint
            var dps2 = extractor.Streamer.ToDataPoint(new DataValue("test", StatusCodes.Good, ts), node1);
            Assert.Single(dps2);
            Assert.Equal(0, dps2.First().DoubleValue);
            Assert.False(dps2.First().Id.EndsWith(']'));


            // Test array data
            var dps3 = extractor.Streamer.ToDataPoint(new DataValue(new[] { 1.0, 2.0, 3.0 }, StatusCodes.Good, ts), node1);
            Assert.Single(dps3);
            Assert.Equal(1.0, dps3.First().DoubleValue);
            Assert.False(dps3.First().Id.EndsWith(']'));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_array_points_missed", 2));

            // array node
            var node2 = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("node2"), "node2", NodeId.Null)
                {
                    DataType = new UADataType(DataTypeIds.Double),
                    ArrayDimensions = new Collection<int>(new[] { 4 })
                },
                true, true);

            // scalar value on array
            var dps4 = extractor.Streamer.ToDataPoint(new DataValue(1.0, StatusCodes.Good, ts), node2);
            Assert.Single(dps4);
            Assert.Equal(1.0, dps4.First().DoubleValue);
            Assert.EndsWith("[0]", dps4.First().Id, StringComparison.InvariantCulture);

            // Too few values
            var dps5 = extractor.Streamer.ToDataPoint(new DataValue(new[] { 1.0, 2.0, 3.0 }, StatusCodes.Good, ts), node2);
            Assert.Equal(3, dps5.Count());
            Assert.Equal(1.0, dps5.First().DoubleValue);
            Assert.Equal(3.0, dps5.Last().DoubleValue);
            Assert.EndsWith("[0]", dps5.First().Id, StringComparison.InvariantCulture);
            Assert.EndsWith("[2]", dps5.Last().Id, StringComparison.InvariantCulture);

            // Just right
            var dps6 = extractor.Streamer.ToDataPoint(new DataValue(new[] { 1.0, 2.0, 3.0, 4.0 }, StatusCodes.Good, ts), node2);
            Assert.Equal(4, dps6.Count());
            Assert.Equal(1.0, dps6.First().DoubleValue);
            Assert.Equal(4.0, dps6.Last().DoubleValue);
            Assert.EndsWith("[0]", dps6.First().Id, StringComparison.InvariantCulture);
            Assert.EndsWith("[3]", dps6.Last().Id, StringComparison.InvariantCulture);

            // Too many values
            var dps7 = extractor.Streamer.ToDataPoint(new DataValue(new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 }, StatusCodes.Good, ts), node2);
            Assert.Equal(4, dps7.Count());
            Assert.Equal(1.0, dps7.First().DoubleValue);
            Assert.Equal(4.0, dps7.Last().DoubleValue);
            Assert.EndsWith("[0]", dps7.First().Id, StringComparison.InvariantCulture);
            Assert.EndsWith("[3]", dps7.Last().Id, StringComparison.InvariantCulture);

            // Very long array name
            var node3 = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId(new string('x', 300)), new string('x', 300), NodeId.Null)
                {
                    DataType = new UADataType(DataTypeIds.Double),
                    ArrayDimensions = new Collection<int>(new[] { 20 })
                },
                true, true);
            var dps8 = extractor.Streamer.ToDataPoint(new DataValue(Enumerable.Range(1, 20).Select(val => (double)val).ToArray(),
                StatusCodes.Good, ts), node3);
            Assert.Equal(20, dps8.Count());
            Assert.Equal(1.0, dps8.First().DoubleValue);
            Assert.Equal(20.0, dps8.Last().DoubleValue);
            Assert.EndsWith("[0]", dps8.First().Id, StringComparison.InvariantCulture);
            Assert.EndsWith("[19]", dps8.Last().Id, StringComparison.InvariantCulture);
            Assert.Equal(255, dps8.First().Id.Length);
            Assert.Equal(255, dps8.Last().Id.Length);


            Assert.True(CommonTestUtils.TestMetricValue("opcua_array_points_missed", 4));
        }


        [Fact]
        public void TestEventHandler()
        {
            using var extractor = tester.BuildExtractor();
            var state = EventUtils.PopulateEventData(extractor, tester, true);

            var queue = (Queue<UAEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            var item = new MonitoredItem() { StartNodeId = new NodeId("emitter"), NodeClass = NodeClass.Object };
            var item2 = new MonitoredItem() { StartNodeId = new NodeId("someotherid"), NodeClass = NodeClass.Object };

            var values = EventUtils.GetEventValues(DateTime.UtcNow);
            var rawEvt = new EventFieldList { EventFields = values };
            item.SaveValueInCache(rawEvt);
            Assert.Empty(queue);

            // Test no filter
            extractor.Streamer.EventSubscriptionHandler(item, null);
            Assert.Empty(queue);


            // Test no state
            var filter = new EventFilter { SelectClauses = EventUtils.GetSelectClause(tester) };
            item2.Filter = filter;
            extractor.Streamer.EventSubscriptionHandler(item2, null);
            Assert.Empty(queue);

            // Test null eventFields
            var rawEvt2 = new EventFieldList { EventFields = null };
            item2.StartNodeId = new NodeId("emitter");
            item2.SaveValueInCache(rawEvt2);
            extractor.Streamer.EventSubscriptionHandler(item2, null);
            Assert.Empty(queue);

            // Test bad event
            values[0] = Variant.Null;
            item.SaveValueInCache(rawEvt);
            item.Filter = filter;
            extractor.Streamer.EventSubscriptionHandler(item, null);
            Assert.Empty(queue);
            Assert.Empty(item.DequeueEvents());

            // Test OK event
            values[0] = new byte[] { 0, 0, 0, 0, 2 };
            item.SaveValueInCache(rawEvt);
            extractor.Streamer.EventSubscriptionHandler(item, null);
            Assert.Single(queue);
            var generated = queue.Dequeue();
            Assert.Single(generated.MetaData);

            // Test multiple events, one early, one late, one ok

            var val1 = EventUtils.GetEventValues(DateTime.UtcNow);
            var val2 = EventUtils.GetEventValues(DateTime.UtcNow);
            var val3 = EventUtils.GetEventValues(DateTime.UtcNow);
            val1[4] = DateTime.UtcNow;
            val2[4] = DateTime.UtcNow.AddDays(1);
            val3[4] = DateTime.UtcNow.Subtract(TimeSpan.FromDays(1));
            var evt1 = new EventFieldList { EventFields = val1 };
            var evt2 = new EventFieldList { EventFields = val2 };
            var evt3 = new EventFieldList { EventFields = val3 };
            item.SaveValueInCache(evt1);
            item.SaveValueInCache(evt2);
            item.SaveValueInCache(evt3);
            extractor.Streamer.EventSubscriptionHandler(item, null);
            Assert.Single(queue);

            // Again after frontfill
            state.UpdateFromFrontfill(DateTime.MinValue, true);
            item.SaveValueInCache(evt1);
            item.SaveValueInCache(evt2);
            item.SaveValueInCache(evt3);
            extractor.Streamer.EventSubscriptionHandler(item, null);
            Assert.Equal(3, queue.Count);

            // After backfill
            state.UpdateFromBackfill(DateTime.MaxValue, true);
            item.SaveValueInCache(evt1);
            item.SaveValueInCache(evt2);
            item.SaveValueInCache(evt3);
            extractor.Streamer.EventSubscriptionHandler(item, null);
            Assert.Equal(6, queue.Count);

        }
        [Fact]
        public void TestToEvent()
        {
            using var extractor = tester.BuildExtractor();
            var state = EventUtils.PopulateEventData(extractor, tester, true);

            var filter = new EventFilter { SelectClauses = EventUtils.GetSelectClause(tester) };
            var values = EventUtils.GetEventValues(DateTime.UtcNow);
            var emitter = new NodeId("emitter");

            UAEvent created = null;
            Assert.Throws<ArgumentNullException>(() => extractor.Streamer.ConstructEvent(null, values, emitter));
            Assert.Throws<ArgumentNullException>(() => extractor.Streamer.ConstructEvent(filter, null, emitter));

            // Check that default results in created event
            created = extractor.Streamer.ConstructEvent(filter, values, emitter);
            Assert.NotNull(created);
            Assert.Equal(emitter, created.EmittingNode);
            Assert.Equal("message", created.Message);
            Assert.Equal(new NodeId("source"), created.SourceNode);
            Assert.Equal(new NodeId("test"), created.EventType);
            Assert.Equal(DateTime.UtcNow, created.Time, TimeSpan.FromMinutes(10));
            Assert.Single(created.MetaData);
            Assert.True(created.MetaData.ContainsKey("EUProp"));

            // not selecting type
            var noTypeFilter = new EventFilter
            {
                SelectClauses =
                new SimpleAttributeOperandCollection(EventUtils.GetSelectClause(tester)
                    .Where(attr => attr.BrowsePath[0].Name != BrowseNames.EventType))
            };
            created = extractor.Streamer.ConstructEvent(noTypeFilter, values, emitter);
            Assert.Null(created);

            // Bad type
            var badTypeValues = EventUtils.GetEventValues(DateTime.UtcNow);
            badTypeValues[2] = new NodeId("SomeOtherType");
            created = extractor.Streamer.ConstructEvent(filter, badTypeValues, emitter);
            Assert.Null(created);

            badTypeValues[2] = Variant.Null;
            created = extractor.Streamer.ConstructEvent(filter, badTypeValues, emitter);
            Assert.Null(created);

            // Bad Id
            var badIdValues = EventUtils.GetEventValues(DateTime.UtcNow);
            badIdValues[0] = Variant.Null;
            created = extractor.Streamer.ConstructEvent(filter, badIdValues, emitter);
            Assert.Null(created);

            // Bad time
            var badTimeValues = EventUtils.GetEventValues(DateTime.UtcNow);
            badTimeValues[4] = Variant.Null;
            created = extractor.Streamer.ConstructEvent(filter, badTimeValues, emitter);
            Assert.Null(created);

            // Name change
            tester.Config.Events.DestinationNameMap["EUProp"] = "unit";
            created = extractor.Streamer.ConstructEvent(filter, values, emitter);
            tester.Config.Events.DestinationNameMap.Remove("EUProp");
            Assert.NotNull(created);
            Assert.Single(created.MetaData);
            Assert.True(created.MetaData.ContainsKey("unit"));

        }
    }
}
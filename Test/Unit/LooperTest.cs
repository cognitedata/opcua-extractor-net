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
    public sealed class LooperTestFixture : BaseExtractorTestFixture
    {
        public LooperTestFixture() : base() { }
    }
    public class LooperTest : MakeConsoleWork, IClassFixture<LooperTestFixture>
    {
        private LooperTestFixture tester;
        public LooperTest(ITestOutputHelper output, LooperTestFixture tester) : base(output)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
            this.tester = tester;
            tester.ResetConfig();
            tester.Config.Extraction.DataPushDelay = -1;
            tester.Config.Extraction.AutoRebrowsePeriod = 1000;
        }
        [Fact]
        public async Task TestScheduleTasks()
        {
            tester.Config.StateStorage = new StateStorageConfig
            {
                Interval = 1000000
            };
            using var stateStore = new DummyStateStore();
            using var extractor = tester.BuildExtractor(true, stateStore, new DummyPusher(new DummyPusherConfig()));
            bool synch1 = false;
            bool synch2 = false;
            using var source = CancellationTokenSource.CreateLinkedTokenSource(tester.Source.Token);

            var tasks = (List<Task>)extractor.Looper.GetType()
                .GetField("tasks", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Looper);

            var loopTask = extractor.Looper.InitTaskLoop(
                new[] { Task.Run(() => synch1 = true), Task.Run(() => synch2 = true) }, source.Token);

            await CommonTestUtils.WaitForCondition(() => synch1 && synch2, 5);
            await CommonTestUtils.WaitForCondition(() => tasks.Count == 6, 5);

            await CommonTestUtils.WaitForCondition(() => stateStore.NumStoreState == 2, 5);

            extractor.Looper.TriggerStoreState();

            await CommonTestUtils.WaitForCondition(() => stateStore.NumStoreState == 4, 5);

            Assert.Equal(6, tasks.Count);

            // Schedule some interruptable tasks
            using var evt = new ManualResetEvent(false);
            using var evt2 = new ManualResetEvent(false);
            extractor.Looper.ScheduleTasks(new[]
            {
                Task.Run(() => {
                    Console.WriteLine("Starting task 1");
                    evt.WaitOne();
                    Console.WriteLine("Finishing task 1");
                }),
                Task.Run(() => {
                    Console.WriteLine("Starting task 2");
                    evt.WaitOne();
                    Console.WriteLine("Triggered once on task 2");
                    evt2.WaitOne();
                    Console.WriteLine("Finish task 2");
                })
            });

            Assert.Equal(8, tasks.Count);
            evt.Set();
            await CommonTestUtils.WaitForCondition(() => tasks.Count == 7, 5);
            evt2.Set();
            await CommonTestUtils.WaitForCondition(() => tasks.Count == 6, 5);

            // Try to schedule a failing task
            extractor.Looper.ScheduleTasks(new[]
            {
                Task.Run(() => throw new ExtractorFailureException("SomeException"))
            });

            await CommonTestUtils.WaitForCondition(() => loopTask.IsFaulted, 5);

            Assert.Equal("SomeException", ExtractorUtils.GetRootExceptionOfType<ExtractorFailureException>(loopTask.Exception).Message);

            loopTask = extractor.Looper.InitTaskLoop(Enumerable.Empty<Task>(), source.Token);
            source.Cancel();
            await CommonTestUtils.WaitForCondition(() => loopTask.IsCanceled, 5);
        }

        private void InitPusherLoopTest(UAExtractor extractor, params DummyPusher[] pushers)
        {
            var evtState = new EventExtractionState(tester.Client, new NodeId("id"), false, false, true);
            evtState.InitToEmpty();
            evtState.FinalizeRangeInit();
            extractor.State.SetEmitterState(evtState);

            var dpState = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("id"), "test", NodeId.Null),
                false, false);
            dpState.InitToEmpty();
            dpState.FinalizeRangeInit();
            extractor.State.SetNodeState(dpState, "id");
            extractor.Streamer.AllowData = true;
            extractor.Streamer.AllowEvents = true;

            foreach (var pusher in pushers)
            {
                pusher.UniqueToNodeId["id"] = (new NodeId("id"), -1);
            }
        }

        [Fact]
        public async Task TestPusherLoop()
        {
            var pusher1 = new DummyPusher(new DummyPusherConfig());
            var pusher2 = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher1, pusher2);

            var start = DateTime.UtcNow;

            var dps = Enumerable.Range(0, 100).Select(idx => new UADataPoint(start.AddMilliseconds(idx), "id", idx));
            var evts = Enumerable.Range(0, 100).Select(idx =>
                new UAEvent { EmittingNode = new NodeId("id"), Time = start.AddMilliseconds(idx) });

            InitPusherLoopTest(extractor, pusher1, pusher2);

            pusher1.Initialized = true;
            pusher2.Initialized = true;

            // Test all OK
            var dps1 = pusher1.DataPoints[(new NodeId("id"), -1)] = new List<UADataPoint>();
            var dps2 = pusher2.DataPoints[(new NodeId("id"), -1)] = new List<UADataPoint>();
            var evts1 = pusher1.Events[new NodeId("id")] = new List<UAEvent>();
            var evts2 = pusher2.Events[new NodeId("id")] = new List<UAEvent>();

            var loopTask = extractor.Looper.InitTaskLoop(Enumerable.Empty<Task>(), tester.Source.Token);

            Assert.Empty(dps1);
            Assert.Empty(dps2);
            Assert.Empty(evts1);
            Assert.Empty(evts2);

            extractor.Streamer.Enqueue(dps);
            extractor.Streamer.Enqueue(evts);

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(100, dps1.Count);
            Assert.Equal(100, dps2.Count);
            Assert.Equal(100, evts1.Count);
            Assert.Equal(100, evts2.Count);

            // Fail one

            extractor.Streamer.Enqueue(dps);
            extractor.Streamer.Enqueue(evts);

            pusher1.PushDataPointResult = false;
            pusher1.PushEventResult = false;
            pusher1.TestConnectionResult = false;

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(100, dps1.Count);
            Assert.Equal(200, dps2.Count);
            Assert.Equal(100, evts1.Count);
            Assert.Equal(200, evts2.Count);

            Assert.True(pusher1.DataFailing);
            Assert.True(pusher1.EventsFailing);

            // Allow points and events, but continue to fail connection test

            extractor.Streamer.Enqueue(dps);
            extractor.Streamer.Enqueue(evts);

            pusher1.PushDataPointResult = true;
            pusher1.PushEventResult = true;

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(100, dps1.Count);
            Assert.Equal(300, dps2.Count);
            Assert.Equal(100, evts1.Count);
            Assert.Equal(300, evts2.Count);

            // Re-allow connection test, verify reconnect

            pusher1.TestConnectionResult = true;

            extractor.Streamer.Enqueue(dps);
            extractor.Streamer.Enqueue(evts);

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(200, dps1.Count);
            Assert.Equal(400, dps2.Count);
            Assert.Equal(200, evts1.Count);
            Assert.Equal(400, evts2.Count);
        }
        [Fact]
        public async Task TestLateInit()
        {
            var pusher1 = new DummyPusher(new DummyPusherConfig()) { ReadProperties = false };
            var pusher2 = new DummyPusher(new DummyPusherConfig()) { ReadProperties = false };
            var pusher3 = new DummyPusher(new DummyPusherConfig()) { ReadProperties = false };
            tester.Config.Extraction.Relationships.Enabled = true;
            using var extractor = tester.BuildExtractor(true, null, pusher1, pusher2, pusher3);
            tester.Config.Extraction.Relationships.Enabled = false;

            var start = DateTime.UtcNow;

            var dps = Enumerable.Range(0, 100).Select(idx => new UADataPoint(start.AddMilliseconds(idx), "id", idx));
            var evts = Enumerable.Range(0, 100).Select(idx =>
                new UAEvent { EmittingNode = new NodeId("id"), Time = start.AddMilliseconds(idx) });

            InitPusherLoopTest(extractor, pusher1, pusher2, pusher3);

            pusher1.Initialized = false;
            pusher2.Initialized = false;
            pusher1.TestConnectionResult = false;
            pusher2.TestConnectionResult = false;
            pusher3.Initialized = true;

            var dps1 = pusher1.DataPoints[(new NodeId("id"), -1)] = new List<UADataPoint>();
            var dps2 = pusher2.DataPoints[(new NodeId("id"), -1)] = new List<UADataPoint>();
            var dps3 = pusher3.DataPoints[(new NodeId("id"), -1)] = new List<UADataPoint>();
            var evts1 = pusher1.Events[new NodeId("id")] = new List<UAEvent>();
            var evts2 = pusher2.Events[new NodeId("id")] = new List<UAEvent>();
            var evts3 = pusher3.Events[new NodeId("id")] = new List<UAEvent>();

            Assert.Empty(dps1);
            Assert.Empty(dps2);
            Assert.Empty(dps3);
            Assert.Empty(evts1);
            Assert.Empty(evts2);
            Assert.Empty(evts3);

            extractor.Streamer.Enqueue(dps);
            extractor.Streamer.Enqueue(evts);

            var loopTask = extractor.Looper.InitTaskLoop(Enumerable.Empty<Task>(), tester.Source.Token);

            // Verify that the two un-initialized pushers are set to failing

            pusher1.TestConnectionResult = false;
            pusher2.TestConnectionResult = false;

            await extractor.Looper.WaitForNextPush(true);

            Assert.Empty(dps1);
            Assert.Empty(dps2);
            Assert.Equal(100, dps3.Count);
            Assert.Empty(evts1);
            Assert.Empty(evts2);
            Assert.Equal(100, evts3.Count);

            // Add some missing nodes to each of the pushers, and verify that they are pushed on recovery

            var nodes = new List<UANode>
            {
                new UANode(new NodeId("missing1"), "missing1", new NodeId("test"), NodeClass.Object),
                new UAVariable(new NodeId("missing2"), "missing2", new NodeId("test"))
            };

            pusher1.PendingNodes.AddRange(nodes);
            pusher2.PendingNodes.Add(nodes.First());

            var refManager = extractor.ReferenceTypeManager;

            var reference = new UAReference(
                ReferenceTypeIds.Organizes,
                true, new NodeId("object1"),
                new NodeId("var1"),
                false,
                true,
                refManager);

            pusher1.PendingReferences.Add(reference);
            pusher2.PendingReferences.Add(reference);

            extractor.Streamer.Enqueue(dps);
            extractor.Streamer.Enqueue(evts);

            pusher1.TestConnectionResult = true;
            pusher2.TestConnectionResult = true;

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(100, dps1.Count);
            Assert.Equal(100, dps2.Count);
            Assert.Equal(200, dps3.Count);
            Assert.Equal(100, evts1.Count);
            Assert.Equal(100, evts2.Count);
            Assert.Equal(200, evts3.Count);

            Assert.Single(pusher1.PushedReferences);
            Assert.Single(pusher2.PushedReferences);
            Assert.Single(pusher1.PushedVariables);
            Assert.Empty(pusher2.PushedVariables);
            Assert.Single(pusher1.PushedNodes);
            Assert.Single(pusher2.PushedNodes);

            Assert.True(pusher1.Initialized);
            Assert.True(pusher2.Initialized);

            Assert.Empty(pusher1.PendingNodes);
            Assert.Empty(pusher1.PendingReferences);
            Assert.Empty(pusher2.PendingNodes);
            Assert.Empty(pusher2.PendingReferences);
        }
    }
}

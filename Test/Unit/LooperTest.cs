using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using Cognite.OpcUa.Utils;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    [Collection("Shared server tests")]
    public class LooperTest
    {
        private readonly StaticServerTestFixture tester;
        public LooperTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
            tester.Config.Extraction.DataPushDelay = "-1";
            tester.Config.Extraction.AutoRebrowsePeriod = "1000";
            tester.Client.TypeManager.Reset();
        }
        [Fact]
        public async Task TestScheduleTasks()
        {
            tester.Config.StateStorage = new StateStorageConfig
            {
                Interval = "1000000"
            };
            using var stateStore = new DummyStateStore();
            using var extractor = tester.BuildExtractor(null, true, stateStore);
            bool synch1 = false;
            bool synch2 = false;
            using var source = CancellationTokenSource.CreateLinkedTokenSource(tester.Source.Token);

            extractor.Looper.Run();
            extractor.Looper.Scheduler.ScheduleTask(null, async token =>
            {
                synch1 = true;
                await Task.Delay(100, token);
            });
            extractor.Looper.Scheduler.ScheduleTask(null, token =>
            {
                synch2 = true;
                return Task.CompletedTask;
            });

            await TestUtils.WaitForCondition(() => synch1 && synch2, 5);

            await TestUtils.WaitForCondition(() => stateStore.NumStoreState == 2, 5);

            Assert.True(extractor.Looper.Scheduler.TryTriggerTask("StoreState"));

            await TestUtils.WaitForCondition(() => stateStore.NumStoreState == 4, 5);

            bool int1 = false, int2 = false;

            // Schedule some interruptable tasks
            using var evt = new ManualResetEvent(false);
            using var evt2 = new ManualResetEvent(false);
            extractor.Looper.Scheduler.ScheduleTask("Interupt1", token =>
            {
                evt.WaitOne();
                int1 = true;
            });
            extractor.Looper.Scheduler.ScheduleTask("Interupt2", token =>
            {
                evt.WaitOne();
                evt2.WaitOne();
                int2 = true;
            });

            evt.Set();

            await TestUtils.WaitForCondition(() => int1, 5);
            evt2.Set();
            await TestUtils.WaitForCondition(() => int2, 5);

            evt.Reset();
            // Try to schedule a failing task
            extractor.Looper.Scheduler.ScheduleTask("failing", async token =>
            {
                await Task.Delay(100, token);
                throw new ExtractorFailureException("SomeException");
            });

            var loopTask = extractor.Looper.Scheduler.WaitForAll();
            await TestUtils.WaitForCondition(() => loopTask.IsFaulted || loopTask.IsCompleted, 5);
            var ex = loopTask.Exception.Flatten();
            Assert.IsType<ExtractorFailureException>(ex.InnerException);
            Assert.Equal("SomeException", ex.InnerException.Message);
        }

        private void InitPusherLoopTest(UAExtractor extractor, DummyPusher pusher)
        {
            var evtState = new EventExtractionState(tester.Client, new NodeId("id", 0), false, false, true);
            evtState.InitToEmpty();
            evtState.FinalizeRangeInit();
            extractor.State.SetEmitterState(evtState);

            var dpState = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("id", 0), "test", null, null, NodeId.Null, null),
                false, false, true);
            dpState.InitToEmpty();
            dpState.FinalizeRangeInit();
            extractor.State.SetNodeState(dpState, "id");
            extractor.Streamer.AllowData = true;
            extractor.Streamer.AllowEvents = true;

            pusher.UniqueToNodeId["id"] = (new NodeId("id", 0), -1);
        }

        [Fact]
        public async Task TestPusherLoop()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pusher);

            var start = DateTime.UtcNow;

            var dps = Enumerable.Range(0, 100).Select(idx => new UADataPoint(start.AddMilliseconds(idx), "id", idx, StatusCodes.Good));
            var evts = Enumerable.Range(0, 100).Select(idx =>
                new UAEvent { EmittingNode = new NodeId("id", 0), Time = start.AddMilliseconds(idx) });

            InitPusherLoopTest(extractor, pusher);

            pusher.Initialized = true;

            // Test all OK
            var dpsw = pusher.DataPoints[(new NodeId("id", 0), -1)] = new List<UADataPoint>();
            var evtsw = pusher.Events[new NodeId("id", 0)] = new List<UAEvent>();

            tester.Config.Extraction.DataPushDelay = "100";
            extractor.Looper.Run();
            var loopTask = extractor.Looper.Scheduler.WaitForAll();

            await extractor.Looper.WaitForNextPush(false);

            Assert.Empty(dpsw);
            Assert.Empty(evtsw);

            await extractor.Streamer.EnqueueAsync(dps);
            await extractor.Streamer.EnqueueAsync(evts);

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(100, dpsw.Count);
            Assert.Equal(100, evtsw.Count);

            // Fail

            await extractor.Streamer.EnqueueAsync(dps);
            await extractor.Streamer.EnqueueAsync(evts);

            pusher.PushDataPointResult = false;
            pusher.PushEventResult = false;
            pusher.TestConnectionResult = false;

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(100, dpsw.Count);
            Assert.Equal(100, evtsw.Count);

            Assert.True(pusher.DataFailing);
            Assert.True(pusher.EventsFailing);

            // Reconnected

            await extractor.Streamer.EnqueueAsync(dps);
            await extractor.Streamer.EnqueueAsync(evts);

            pusher.PushDataPointResult = true;
            pusher.PushEventResult = true;
            pusher.TestConnectionResult = true;

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(200, dpsw.Count);
            Assert.Equal(200, evtsw.Count);
        }
        [Fact]
        public async Task TestLateInit()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.Relationships.Enabled = true;
            using var extractor = tester.BuildExtractor(pusher);
            tester.Config.Extraction.Relationships.Enabled = false;

            var start = DateTime.UtcNow;

            var dps = Enumerable.Range(0, 100).Select(idx => new UADataPoint(start.AddMilliseconds(idx), "id", idx, StatusCodes.Good));
            var evts = Enumerable.Range(0, 100).Select(idx =>
                new UAEvent { EmittingNode = new NodeId("id", 0), Time = start.AddMilliseconds(idx) });

            InitPusherLoopTest(extractor, pusher);

            pusher.Initialized = false;
            pusher.TestConnectionResult = false;

            var dpsw = pusher.DataPoints[(new NodeId("id", 0), -1)] = new List<UADataPoint>();
            var evtsw = pusher.Events[new NodeId("id", 0)] = new List<UAEvent>();

            Assert.Empty(dpsw);
            Assert.Empty(evtsw);

            await extractor.Streamer.EnqueueAsync(dps);
            await extractor.Streamer.EnqueueAsync(evts);

            extractor.Looper.Run();
            var loopTask = extractor.Looper.Scheduler.WaitForAll();

            // Verify that the pusher is failing

            pusher.TestConnectionResult = false;

            await extractor.Looper.WaitForNextPush(true);

            Assert.Empty(dpsw);
            Assert.Empty(evtsw);

            // Add some missing nodes to each of the pushers, and verify that they are pushed on recovery
            var refManager = extractor.TypeManager;

            var objects = new[] { new UAObject(new NodeId("missing1", 0), "missing1", null, null, new NodeId("test", 0), null) };
            var variables = new[] { new UAVariable(new NodeId("missing2", 0), "missing2", null, null, new NodeId("test", 0), null) };

            var reference = new UAReference(
                refManager.GetReferenceType(ReferenceTypeIds.Organizes),
                true,
                objects[0],
                variables[0]);


            var input = new PusherInput(
                objects,
                variables,
                new[] { reference }, null);

            var input2 = new PusherInput(
                objects,
                Enumerable.Empty<UAVariable>(),
                new[] { reference }, null);

            (pusher as IPusher).AddPendingNodes(input, new FullPushResult(), tester.Config);

            await extractor.Streamer.EnqueueAsync(dps);
            await extractor.Streamer.EnqueueAsync(evts);

            pusher.TestConnectionResult = true;

            await extractor.Looper.WaitForNextPush(true);

            Assert.Equal(100, dpsw.Count);
            Assert.Equal(100, evtsw.Count);

            Assert.Single(pusher.PushedReferences);
            Assert.Single(pusher.PushedVariables);
            Assert.Single(pusher.PushedNodes);

            Assert.True(pusher.Initialized);

            Assert.Null(pusher.PendingNodes);
        }
    }
}

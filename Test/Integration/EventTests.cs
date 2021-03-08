using Cognite.Extractor.StateStorage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    public class EventTestFixture : BaseExtractorTestFixture
    {
        public EventTestFixture() : base(63400)
        {
            Config.Source.PublishingInterval = 200;
            Config.Extraction.DataPushDelay = 200;
            Config.History.Enabled = false;
            Config.Events.Enabled = true;
            Config.Extraction.RootNode = Server.Ids.Event.Root.ToProtoNodeId(Client);
        }
        public void WipeEventHistory()
        {
            Server.WipeEventHistory(Server.Ids.Event.Obj1);
            Server.WipeEventHistory(ObjectIds.Server);
        }
    }
    public class EventTests : MakeConsoleWork, IClassFixture<EventTestFixture>
    {
        private readonly EventTestFixture tester;
        public EventTests(ITestOutputHelper output, EventTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        #region subscriptions
        [Fact]
        public async Task TestBasicSubscriptions()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var runTask = extractor.RunExtractor();

            var ids = tester.Server.Ids.Event;
            tester.Config.History.Enabled = false;

            await extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(0);

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 3 && pusher.Events[ObjectIds.Server].Count == 8, 5);

            Assert.Single(pusher.Events[ids.Obj2]);
            Assert.Equal(2, pusher.Events[ids.Obj1].Count);
            Assert.Equal(8, pusher.Events[ObjectIds.Server].Count);

            var evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "prop 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Obj1, evt.SourceNode);
            Assert.Equal(4, evt.MetaData.Count);
            Assert.Equal(new Variant("str 0"), evt.MetaData["PropertyString"]);
            Assert.Equal(new Variant(0f), evt.MetaData["PropertyNum"]);
            Assert.Equal(new Variant("sub-type"), evt.MetaData["SubType"]);
            Assert.Equal(new Variant((ushort)100), evt.MetaData["Severity"]);
            Assert.True(evt.Time > DateTime.UtcNow.AddSeconds(-5));
            Assert.Equal(ids.PropType, evt.EventType);
            Assert.StartsWith(tester.Config.Extraction.IdPrefix, evt.EventId, StringComparison.InvariantCulture);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

            tester.WipeEventHistory();
        }
        [Fact]
        public async Task TestSubscriptionFilters()
        {
            // Exlude events ending in "2"
            tester.Config.Events.ExcludeEventFilter = "2$";
            tester.Config.Events.ExcludeProperties = new[] { "PropertyNum" };
            tester.Config.Events.DestinationNameMap["TypeProp"] = "Type";

            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var runTask = extractor.RunExtractor();

            var ids = tester.Server.Ids.Event;

            await extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(0);

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 3, 5);

            Assert.Single(pusher.Events[ids.Obj2]);
            Assert.Equal(2, pusher.Events[ids.Obj1].Count);
            Assert.Equal(7, pusher.Events[ObjectIds.Server].Count);

            var evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "prop 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Obj1, evt.SourceNode);
            Assert.Equal(3, evt.MetaData.Count);
            Assert.False(evt.MetaData.ContainsKey("PropertyNum"));
            Assert.Equal(new Variant("str 0"), evt.MetaData["PropertyString"]);
            Assert.Equal(new Variant("sub-type"), evt.MetaData["SubType"]);
            Assert.Equal(new Variant((ushort)100), evt.MetaData["Severity"]);
            Assert.True(evt.Time > DateTime.UtcNow.AddSeconds(-5));
            Assert.Equal(ids.PropType, evt.EventType);
            Assert.StartsWith(tester.Config.Extraction.IdPrefix, evt.EventId, StringComparison.InvariantCulture);

            evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "mapped 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(2, evt.MetaData.Count);
            Assert.Equal(new Variant("CustomType"), evt.MetaData["Type"]);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

            tester.Config.Events.ExcludeEventFilter = null;
            tester.Config.Events.ExcludeProperties = new List<string>();
            tester.Config.Events.DestinationNameMap.Clear();
            tester.WipeEventHistory();
        }
        #endregion

        #region history
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestHistory(bool backfill)
        {
            tester.Config.History.Enabled = true;
            tester.Config.History.Backfill = backfill;
            tester.Config.Events.History = true;
            tester.Config.Events.ExcludeEventFilter = "2$";
            tester.Config.Events.ExcludeProperties = new[] { "PropertyNum" };
            tester.Config.Events.DestinationNameMap["TypeProp"] = "Type";
            tester.WipeEventHistory();

            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var start = DateTime.UtcNow.AddSeconds(-5);

            tester.Server.PopulateEvents(start);

            var runTask = extractor.RunExtractor();
            var ids = tester.Server.Ids.Event;

            await extractor.WaitForSubscriptions();

            await CommonTestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(state => !state.IsFrontfilling
                            && !state.IsBackfilling), 5);

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 2 && pusher.Events[ObjectIds.Server].Count == 700, 5,
                () => $"Expected to get 700 events but got {pusher.Events[ObjectIds.Server].Count}");

            var evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "prop 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Obj1, evt.SourceNode);
            Assert.Equal(3, evt.MetaData.Count);
            Assert.False(evt.MetaData.ContainsKey("PropertyNum"));
            Assert.Equal(new Variant("str 0"), evt.MetaData["PropertyString"]);
            Assert.Equal(new Variant("sub-type"), evt.MetaData["SubType"]);
            Assert.Equal(new Variant((ushort)100), evt.MetaData["Severity"]);
            Assert.Equal(ids.PropType, evt.EventType);
            Assert.StartsWith(tester.Config.Extraction.IdPrefix, evt.EventId, StringComparison.InvariantCulture);

            evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "mapped 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(2, evt.MetaData.Count);
            Assert.Equal(new Variant("CustomType"), evt.MetaData["Type"]);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

            tester.Config.History.Enabled = false;
            tester.Config.History.Backfill = false;
            tester.Config.Events.History = false;
            tester.Config.Events.ExcludeEventFilter = null;
            tester.Config.Events.ExcludeProperties = new List<string>();
            tester.Config.Events.DestinationNameMap.Clear();
            tester.WipeEventHistory();
        }
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestHistoryContinuation(bool backfill)
        {
            tester.Config.History.Enabled = true;
            tester.Config.History.Backfill = backfill;
            tester.Config.Events.History = true;
            tester.Config.Events.ExcludeEventFilter = "2$";
            tester.Config.Events.ExcludeProperties = new[] { "PropertyNum" };
            tester.Config.Events.DestinationNameMap["TypeProp"] = "Type";
            tester.WipeEventHistory();

            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var now = DateTime.UtcNow;

            tester.Server.PopulateEvents(now.AddSeconds(-5));

            var runTask = extractor.RunExtractor();
            var ids = tester.Server.Ids.Event;

            await extractor.WaitForSubscriptions();

            await CommonTestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(state => !state.IsFrontfilling
                && !state.IsBackfilling), 5);

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 2 && pusher.Events[ObjectIds.Server].Count == 700, 5,
                () => $"Expected to get 700 events but got {pusher.Events[ObjectIds.Server].Count}");

            Assert.Equal(700, pusher.Events[ObjectIds.Server].Count);
            Assert.Equal(200, pusher.Events[ids.Obj1].Count);

            tester.Server.PopulateEvents(now.AddSeconds(5));
            tester.Server.PopulateEvents(now.AddSeconds(-15));

            foreach (var state in extractor.State.EmitterStates)
            {
                state.RestartHistory();
            }

            await extractor.RestartHistory();

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 2 && pusher.Events[ObjectIds.Server].Count == 1407
                && pusher.Events[ids.Obj1].Count == 402, 5,
                () => $"Expected to get 1407 events but got {pusher.Events[ObjectIds.Server].Count}");
            // One overlap per event type
            Assert.Equal(1407, pusher.Events[ObjectIds.Server].Count);
            Assert.Equal(402, pusher.Events[ids.Obj1].Count);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

            tester.Config.History.Enabled = false;
            tester.Config.History.Backfill = false;
            tester.Config.Events.History = false;
            tester.Config.Events.ExcludeEventFilter = null;
            tester.Config.Events.ExcludeProperties = new List<string>();
            tester.Config.Events.DestinationNameMap.Clear();
            tester.WipeEventHistory();
        }
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestLiteDbStateRestart(bool backfill)
        {
            try
            {
                File.Delete("history-event-test-1.db");
            }
            catch { }
            using var stateStore = new LiteDBStateStore(new StateStoreConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = "history-event-test-1.db"
            }, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());

            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = false });
            var extractor = tester.BuildExtractor(true, stateStore, pusher);

            var ids = tester.Server.Ids.Event;

            tester.Config.History.Enabled = true;
            tester.Config.StateStorage.Interval = 1000000;
            tester.Config.History.Backfill = backfill;
            tester.Config.Events.History = true;
            tester.Config.Events.ExcludeEventFilter = "2$";
            tester.Config.Events.ExcludeProperties = new[] { "PropertyNum" };
            tester.Config.Events.DestinationNameMap["TypeProp"] = "Type";

            var now = DateTime.UtcNow;


            tester.WipeEventHistory();
            tester.Server.PopulateEvents(now.AddSeconds(-5));

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await CommonTestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await CommonTestUtils.WaitForCondition(() =>
                    pusher.Events.ContainsKey(ObjectIds.Server) && pusher.Events[ObjectIds.Server].Count == 700, 5);

                await extractor.Looper.StoreState(tester.Source.Token);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                Assert.Equal(700, pusher.Events[ObjectIds.Server].Count);
                Assert.Equal(200, pusher.Events[ids.Obj1].Count);
                pusher.Wipe();
            }
            finally
            {
                extractor.Dispose();
            }

            tester.Server.PopulateEvents(now.AddSeconds(-15));
            tester.Server.PopulateEvents(now.AddSeconds(5));

            extractor = tester.BuildExtractor(true, stateStore, pusher);

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await CommonTestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await CommonTestUtils.WaitForCondition(() => 
                    pusher.Events.ContainsKey(ObjectIds.Server) && pusher.Events[ObjectIds.Server].Count == 707, 5);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                Assert.Equal(707, pusher.Events[ObjectIds.Server].Count);
                Assert.Equal(202, pusher.Events[ids.Obj1].Count);
                pusher.Wipe();
            }
            finally
            {
                extractor.Dispose();
            }

            tester.Config.History.Enabled = false;
            tester.Config.History.Backfill = false;
            tester.Config.Events.History = false;
            tester.Config.StateStorage.Interval = 0;
            tester.Config.Events.ExcludeEventFilter = null;
            tester.Config.Events.ExcludeProperties = new List<string>();
            tester.Config.Events.DestinationNameMap.Clear();
            tester.WipeEventHistory();
        }
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestPusherStateRestart(bool backfill)
        {
            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = true });
            var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Event;

            tester.Config.History.Enabled = true;
            tester.Config.History.Backfill = backfill;
            tester.Config.Events.History = true;
            tester.Config.Events.ExcludeEventFilter = "2$";
            tester.Config.Events.ExcludeProperties = new[] { "PropertyNum" };
            tester.Config.Events.DestinationNameMap["TypeProp"] = "Type";

            var now = DateTime.UtcNow;

            tester.WipeEventHistory();
            tester.Server.PopulateEvents(now.AddSeconds(-5));

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await CommonTestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await CommonTestUtils.WaitForCondition(() =>
                    pusher.Events.ContainsKey(ObjectIds.Server) && pusher.Events[ObjectIds.Server].Count == 700, 5);

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                Assert.Equal(700, pusher.Events[ObjectIds.Server].Count);
                Assert.Equal(200, pusher.Events[ids.Obj1].Count);
            }
            finally
            {
                extractor.Dispose();
            }

            tester.Server.PopulateEvents(now.AddSeconds(-15));
            tester.Server.PopulateEvents(now.AddSeconds(5));

            extractor = tester.BuildExtractor(true, null, pusher);

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await CommonTestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await CommonTestUtils.WaitForCondition(() =>
                    pusher.Events.ContainsKey(ObjectIds.Server) && pusher.Events[ObjectIds.Server].Count >= 1400, 5);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                if (backfill)
                {
                    Assert.Equal(2114, pusher.Events[ObjectIds.Server].Count);
                    Assert.Equal(604, pusher.Events[ids.Obj1].Count);
                }
                else
                {
                    Assert.Equal(1407, pusher.Events[ObjectIds.Server].Count);
                    Assert.Equal(402, pusher.Events[ids.Obj1].Count);
                }
            }
            finally
            {
                extractor.Dispose();
            }

            tester.Config.History.Enabled = false;
            tester.Config.History.Backfill = false;
            tester.Config.Events.History = false;
            tester.Config.Events.ExcludeEventFilter = null;
            tester.Config.Events.ExcludeProperties = new List<string>();
            tester.Config.Events.DestinationNameMap.Clear();
            tester.WipeEventHistory();
        }
        #endregion
        [Fact]
        public async Task TestFileAutoBuffer()
        {
            try
            {
                File.Delete("event-buffer-test.bin");
            }
            catch { }

            tester.Config.FailureBuffer.EventPath = "event-buffer-test.bin";
            tester.Config.FailureBuffer.Enabled = true;

            tester.WipeEventHistory();

            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = true });
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Event;

            tester.Config.History.Enabled = true;
            tester.Config.Events.History = true;
            tester.Config.Events.ExcludeEventFilter = "2$";
            tester.Config.Events.ExcludeProperties = new[] { "PropertyNum" };
            tester.Config.Events.DestinationNameMap["TypeProp"] = "Type";

            CommonTestUtils.ResetMetricValues("opcua_buffer_num_events");

            var now = DateTime.UtcNow;

            tester.Server.PopulateEvents(now.AddSeconds(-20));

            pusher.PushEventResult = false;
            pusher.PushDataPointResult = false;

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            Assert.False(runTask.IsFaulted, $"Faulted! {runTask.Exception}");

            // expect no data to arrive in pusher
            try
            {
                await CommonTestUtils.WaitForCondition(
                    () => pusher.DataFailing
                    && extractor.State.EmitterStates.All(state => !state.IsFrontfilling), 5,
                    () => $"Pusher is dataFailing: {pusher.DataFailing}");
            }
            finally
            {
                foreach (var state in extractor.State.EmitterStates)
                {
                    Console.WriteLine($"{state.Id}: {state.IsFrontfilling}");
                }
            }


            Assert.True(pusher.DataPoints.All(dps => !dps.Value.Any()));

            tester.Server.TriggerEvents(100);

            await CommonTestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_buffer_num_events", 1), 5,
                () => $"Expected 1 event to arrive in buffer, but got {CommonTestUtils.GetMetricValue("opcua_buffer_num_events")}");

            tester.Server.TriggerEvents(101);

            await CommonTestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_buffer_num_events", 2), 5,
                () => $"Expected 2 events to arrive in buffer, but got {CommonTestUtils.GetMetricValue("opcua_buffer_num_events")}");

            pusher.PushEventResult = true;
            pusher.PushDataPointResult = true;

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 3 && pusher.Events[ObjectIds.Server].Count == 714, 10);

            Assert.Equal(204, pusher.Events[ids.Obj1].Count);
            Assert.Equal(2, pusher.Events[ids.Obj2].Count);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_buffer_num_events", 0));
            tester.WipeEventHistory();
        }
        [Fact]
        public async Task TestAuditEvents()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Audit;
            tester.Config.Extraction.RootNode = ids.Root.ToProtoNodeId(tester.Client);
            tester.Config.Extraction.EnableAuditDiscovery = true;

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            Assert.Equal(3, pusher.PushedNodes.Count);

            tester.Server.DirectGrowth();

            await CommonTestUtils.WaitForCondition(() => pusher.PushedNodes.Count == 4 && pusher.PushedVariables.Count == 1, 10);

            var directRoot = pusher.PushedNodes[ids.DirectAdd];

            var directObj = pusher.PushedNodes.Values.First(obj => obj.DisplayName == "AddObj 0");
            var directVar = pusher.PushedVariables.Values.First(variable => variable.DisplayName == "AddVar 0");

            Assert.Equal(directRoot.Id, directObj.ParentId);
            Assert.Equal(directRoot.Id, directVar.ParentId);
            Assert.NotNull(directVar.DataType);
            Assert.True(extractor.Streamer.AllowData);

            tester.Server.ReferenceGrowth(1);

            await CommonTestUtils.WaitForCondition(() => pusher.PushedNodes.Count == 5 && pusher.PushedVariables.Count == 2, 10);

            var refRoot = pusher.PushedNodes[ids.RefAdd];

            var refObj = pusher.PushedNodes.Values.First(obj => obj.DisplayName == "AddObj 1");
            var refVar = pusher.PushedVariables.Values.First(variable => variable.DisplayName == "AddVar 1");

            Assert.Equal(refRoot.Id, refObj.ParentId);
            Assert.Equal(refRoot.Id, refVar.ParentId);
            Assert.NotNull(refVar.DataType);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
            tester.Config.Extraction.RootNode = tester.Server.Ids.Event.Root.ToProtoNodeId(tester.Client);
            tester.Config.Extraction.EnableAuditDiscovery = false;
        }
    }
}

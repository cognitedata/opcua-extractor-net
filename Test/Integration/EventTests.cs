using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Subscriptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using Server;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    public class EventTestFixture : BaseExtractorTestFixture
    {
        public EventTestFixture() : base()
        {
            Config.Source.PublishingInterval = 200;
            Config.Extraction.DataPushDelay = "200";
            Config.History.Enabled = false;
            Config.Events.Enabled = true;
        }

        public override async Task InitializeAsync()
        {
            await base.InitializeAsync();
            Config.Extraction.RootNode = Server.Ids.Event.Root.ToProtoNodeId(Client);
        }
    }
    public class EventTests : IClassFixture<EventTestFixture>
    {
        private readonly EventTestFixture tester;
        public EventTests(ITestOutputHelper output, EventTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
            tester.Config.Source.PublishingInterval = 200;
            tester.Config.Extraction.DataPushDelay = "200";
            tester.Config.History.Enabled = false;
            tester.Config.Events.Enabled = true;
            tester.Config.Extraction.RootNode = tester.Ids.Event.Root.ToProtoNodeId(tester.Client);
            tester.WipeEventHistory();
            tester.Client.TypeManager.Reset();
        }
        #region subscriptions
        [Fact]
        public async Task TestBasicSubscriptions()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.History.Enabled = false;
            tester.Config.Events.History = false;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var runTask = extractor.RunExtractor();

            var ids = tester.Server.Ids.Event;

            await extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(0);

            await TestUtils.WaitForCondition(() => pusher.Events.Count == 3 && pusher.Events[ObjectIds.Server].Count == 8, 5);

            Assert.Single(pusher.Events[ids.Obj2]);
            Assert.Equal(2, pusher.Events[ids.Obj1].Count);
            Assert.Equal(8, pusher.Events[ObjectIds.Server].Count);

            var evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "prop 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Obj1, evt.SourceNode);
            Assert.Equal(5, evt.MetaData.Count);
            Assert.Equal("str 0", evt.MetaData["PropertyString"]);
            Assert.Equal("0", evt.MetaData["PropertyNum"]);
            Assert.Equal("sub-type", evt.MetaData["SubType"]);
            Assert.Equal("100", evt.MetaData["Severity"]);
            Assert.Equal("Object 1", evt.MetaData["SourceName"]);
            Assert.True(evt.Time > DateTime.UtcNow.AddSeconds(-5));
            Assert.Equal(ids.PropType, evt.EventType.Id);
            Assert.StartsWith(tester.Config.Extraction.IdPrefix, evt.EventId, StringComparison.InvariantCulture);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
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

            await TestUtils.WaitForCondition(() => pusher.Events.Count == 3, 5);

            Assert.Single(pusher.Events[ids.Obj2]);
            Assert.Equal(2, pusher.Events[ids.Obj1].Count);
            Assert.Equal(7, pusher.Events[ObjectIds.Server].Count);

            var evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "prop 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Obj1, evt.SourceNode);
            Assert.Equal(4, evt.MetaData.Count);
            Assert.False(evt.MetaData.ContainsKey("PropertyNum"));
            Assert.Equal("str 0", evt.MetaData["PropertyString"]);
            Assert.Equal("sub-type", evt.MetaData["SubType"]);
            Assert.Equal("100", evt.MetaData["Severity"]);
            Assert.Equal("Object 1", evt.MetaData["SourceName"]);
            Assert.True(evt.Time > DateTime.UtcNow.AddSeconds(-5));
            Assert.Equal(ids.PropType, evt.EventType.Id);
            Assert.StartsWith(tester.Config.Extraction.IdPrefix, evt.EventId, StringComparison.InvariantCulture);

            evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "mapped 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(3, evt.MetaData.Count);
            Assert.Equal("CustomType", evt.MetaData["Type"]);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }
        [Fact]
        public async Task TestDeepEvent()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Events.ExcludeEventFilter = null;
            tester.Config.Events.ExcludeProperties = new List<string>();
            using var extractor = tester.BuildExtractor(true, null, pusher);
            tester.Config.Events.History = false;

            var runTask = extractor.RunExtractor();

            var ids = tester.Server.Ids.Event;


            await extractor.WaitForSubscriptions();

            tester.Server.Server.TriggerEvent<DeepEvent>(ids.DeepType, ObjectIds.Server, ids.Root, "TestMessage",
                evt =>
                {
                    evt.PropertyNum.Value = 123.123f;
                    evt.PropertyString.Value = "string";
                    evt.SubType.Value = "subType";
                    evt.DeepProp.Value = "deepValue";
                });

            await TestUtils.WaitForCondition(() => pusher.Events.Count == 1, 5);

            Assert.Single(pusher.Events[ObjectIds.Server]);
            var evt = pusher.Events[ObjectIds.Server].First();
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Root, evt.SourceNode);
            Assert.Equal(6, evt.MetaData.Count);
            Assert.Equal("123.123", evt.MetaData["PropertyNum"]);
            Assert.Equal("string", evt.MetaData["PropertyString"]);
            Assert.Equal("subType", evt.MetaData["SubType"]);
            Assert.Equal("100", evt.MetaData["Severity"]);
            Assert.Equal("EventRoot", evt.MetaData["SourceName"]);
            Assert.Equal(@"{""DeepProp"":""deepValue""}", evt.MetaData["DeepObj"]);
            Assert.True(evt.Time > DateTime.UtcNow.AddSeconds(-5));
            Assert.Equal(ids.DeepType, evt.EventType.Id);
        }
        [Fact]
        public async Task TestDisableSubscriptions()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = true });
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Event;

            var now = DateTime.UtcNow;

            tester.Config.History.Enabled = true;
            tester.Config.History.Backfill = true;
            tester.Config.Events.History = true;
            tester.Config.Subscriptions.RecreateSubscriptionGracePeriod = "100ms";

            async Task Reset()
            {
                extractor.State.Clear();
                extractor.GetType().GetField("subscribed", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, 0);
                extractor.GetType().GetField("subscribeFlag", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, false);
                var reader = (HistoryReader)extractor.GetType().GetField("historyReader", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(extractor);
                reader.AddIssue(HistoryReader.StateIssue.NodeHierarchyRead);
                await tester.RemoveSubscription(SubscriptionName.Events);
            }

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            tester.WipeEventHistory();
            tester.Server.PopulateEvents();
            CommonTestUtils.ResetMetricValue("opcua_frontfill_events_count");

            var session = (Session)tester.Client.GetType().GetProperty("Session", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(tester.Client);

            // Test everything normal
            await extractor.RunExtractor(true);
            Assert.All(extractor.State.EmitterStates, state => { Assert.True(state.ShouldSubscribe); });
            await extractor.WaitForSubscriptions();
            Assert.Equal(3u, session.Subscriptions.First(sub => sub.DisplayName.StartsWith(SubscriptionName.Events.Name(), StringComparison.InvariantCulture)).MonitoredItemCount);
            await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(s => !s.IsFrontfilling), 10);

            // Test disable subscriptions
            await Reset();
            tester.Config.Subscriptions.Events = false;
            await extractor.RunExtractor(true);
            var state = extractor.State.GetEmitterState(ids.Obj1);
            Assert.False(state.ShouldSubscribe);
            state = extractor.State.GetEmitterState(ObjectIds.Server);
            Assert.False(state.ShouldSubscribe);
            await extractor.WaitForSubscriptions();
            Assert.DoesNotContain(session.Subscriptions, sub => sub.DisplayName.StartsWith(SubscriptionName.Events.Name(), StringComparison.InvariantCulture));
            await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(s => !s.IsFrontfilling), 10);

            // Test disable specific subscriptions
            await Reset();
            var oldTransforms = tester.Config.Extraction.Transformations;
            tester.Config.Extraction.Transformations = new List<RawNodeTransformation>
            {
                new RawNodeTransformation
                {
                    Filter = new NodeFilter
                    {
                        Id = new RegexFieldFilter($"i={ids.Obj1.Identifier}$")
                    },
                    Type = TransformationType.DropSubscriptions
                }
            };

            tester.Config.Subscriptions.Events = true;
            await extractor.RunExtractor(true);
            state = extractor.State.GetEmitterState(ids.Obj1);
            Assert.False(state.ShouldSubscribe);
            state = extractor.State.GetEmitterState(ObjectIds.Server);
            Assert.True(state.ShouldSubscribe);
            await extractor.WaitForSubscriptions();
            Assert.Equal(2u, session.Subscriptions.First(sub => sub.DisplayName.StartsWith(SubscriptionName.Events.Name(), StringComparison.InvariantCulture)).MonitoredItemCount);
            await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(s => !s.IsFrontfilling), 10);
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

            await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(state => !state.IsFrontfilling
                            && !state.IsBackfilling), 5);

            await TestUtils.WaitForCondition(() => pusher.Events.Count == 2 && pusher.Events[ObjectIds.Server].Count == 700, 5,
                () => $"Expected to get 700 events but got {pusher.Events[ObjectIds.Server].Count} for {pusher.Events.Count}");

            var evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "prop 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Obj1, evt.SourceNode);
            Assert.Equal(4, evt.MetaData.Count);
            Assert.False(evt.MetaData.ContainsKey("PropertyNum"));
            Assert.Equal("str 0", evt.MetaData["PropertyString"]);
            Assert.Equal("sub-type", evt.MetaData["SubType"]);
            Assert.Equal("100", evt.MetaData["Severity"]);
            Assert.Equal("Object 1", evt.MetaData["SourceName"]);
            Assert.Equal(ids.PropType, evt.EventType.Id);
            Assert.StartsWith(tester.Config.Extraction.IdPrefix, evt.EventId, StringComparison.InvariantCulture);

            evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "mapped 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(3, evt.MetaData.Count);
            Assert.Equal("CustomType", evt.MetaData["Type"]);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
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

            await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(state => !state.IsFrontfilling
                && !state.IsBackfilling), 5);

            await TestUtils.WaitForCondition(() => pusher.Events.Count == 2 && pusher.Events[ObjectIds.Server].Count == 700
                && pusher.Events[ids.Obj1].Count == 200, 5,
                () => $"Expected to get 700 events but got {pusher.Events[ObjectIds.Server].Count}");

            Assert.Equal(700, pusher.Events[ObjectIds.Server].Count);
            Assert.Equal(200, pusher.Events[ids.Obj1].Count);

            tester.Server.PopulateEvents(now.AddSeconds(5));
            tester.Server.PopulateEvents(now.AddSeconds(-15));

            await extractor.RestartHistoryWaitForStop();

            await TestUtils.WaitForCondition(() => pusher.Events.Count == 2 && pusher.Events[ObjectIds.Server].Count == 1407
                && pusher.Events[ids.Obj1].Count == 402, 5,
                () => $"Expected to get 1407 events but got {pusher.Events[ObjectIds.Server].Count}");
            // One overlap per event type
            Assert.Equal(1407, pusher.Events[ObjectIds.Server].Count);
            Assert.Equal(402, pusher.Events[ids.Obj1].Count);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
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
            tester.Config.StateStorage.Interval = "1000000";
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

                await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await TestUtils.WaitForCondition(() =>
                    pusher.Events.ContainsKey(ObjectIds.Server) && pusher.Events[ObjectIds.Server].Count == 700, 5);

                await extractor.Looper.StoreState(tester.Source.Token);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                Assert.Equal(700, pusher.Events[ObjectIds.Server].Count);
                Assert.Equal(200, pusher.Events[ids.Obj1].Count);
                pusher.Wipe();
            }
            finally
            {
                await extractor.DisposeAsync();
            }

            tester.Server.PopulateEvents(now.AddSeconds(-15));
            tester.Server.PopulateEvents(now.AddSeconds(5));

            extractor = tester.BuildExtractor(true, stateStore, pusher);

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await TestUtils.WaitForCondition(() =>
                    pusher.Events.ContainsKey(ObjectIds.Server) && pusher.Events[ObjectIds.Server].Count == 707, 5);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                Assert.Equal(707, pusher.Events[ObjectIds.Server].Count);
                Assert.Equal(202, pusher.Events[ids.Obj1].Count);
                pusher.Wipe();
            }
            finally
            {
                await extractor.DisposeAsync();
            }
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

                await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await TestUtils.WaitForCondition(() =>
                    pusher.Events.ContainsKey(ObjectIds.Server) && pusher.Events[ObjectIds.Server].Count == 700, 5);

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                Assert.Equal(700, pusher.Events[ObjectIds.Server].Count);
                Assert.Equal(200, pusher.Events[ids.Obj1].Count);
            }
            finally
            {
                await extractor.DisposeAsync();
            }

            tester.Server.PopulateEvents(now.AddSeconds(-15));
            tester.Server.PopulateEvents(now.AddSeconds(5));

            extractor = tester.BuildExtractor(true, null, pusher);

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await TestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await TestUtils.WaitForCondition(() =>
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
                await extractor.DisposeAsync();
            }
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
            pusher.TestConnectionResult = false;

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            Assert.False(runTask.IsFaulted, $"Faulted! {runTask.Exception}");

            await TestUtils.WaitForCondition(
                () => pusher.DataFailing
                && extractor.State.EmitterStates.All(state => !state.IsFrontfilling), 5,
                () => $"Pusher is dataFailing: {pusher.DataFailing}");

            Assert.True(pusher.DataPoints.All(dps => dps.Value.Count == 0));

            tester.Server.TriggerEvents(100);

            await TestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_buffer_num_events", 1), 5,
                () => $"Expected 1 event to arrive in buffer, but got {CommonTestUtils.GetMetricValue("opcua_buffer_num_events")}");

            tester.Server.TriggerEvents(101);

            await TestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_buffer_num_events", 2), 5,
                () => $"Expected 2 events to arrive in buffer, but got {CommonTestUtils.GetMetricValue("opcua_buffer_num_events")}");

            pusher.PushEventResult = true;
            pusher.PushDataPointResult = true;
            pusher.TestConnectionResult = true;

            await TestUtils.WaitForCondition(() => pusher.Events.Count == 3 && pusher.Events[ObjectIds.Server].Count == 714, 10);

            Assert.Equal(204, pusher.Events[ids.Obj1].Count);
            Assert.Equal(2, pusher.Events[ids.Obj2].Count);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_buffer_num_events", 0));
        }
        [Fact]
        public async Task TestAuditEvents()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            tester.Server.SetEventConfig(true, true, true);

            var ids = tester.Server.Ids.Audit;
            tester.Config.Extraction.RootNode = ids.Root.ToProtoNodeId(tester.Client);
            tester.Config.Extraction.EnableAuditDiscovery = true;

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            Assert.Equal(3, pusher.PushedNodes.Count);

            tester.Server.DirectGrowth();

            await TestUtils.WaitForCondition(() => pusher.PushedNodes.Count == 4 && pusher.PushedVariables.Count == 1, 10);

            var directRoot = pusher.PushedNodes[ids.DirectAdd];

            var directObj = pusher.PushedNodes.Values.First(obj => obj.Name == "AddObj 0");
            var directVar = pusher.PushedVariables.Values.First(variable => variable.Name == "AddVar 0");

            Assert.Equal(directRoot.Id, directObj.ParentId);
            Assert.Equal(directRoot.Id, directVar.ParentId);
            Assert.NotNull(directVar.FullAttributes.DataType);
            Assert.True(extractor.Streamer.AllowData);

            tester.Server.ReferenceGrowth(1);

            await TestUtils.WaitForCondition(() => pusher.PushedNodes.Count == 5 && pusher.PushedVariables.Count == 2, 10);

            var refRoot = pusher.PushedNodes[ids.RefAdd];

            var refObj = pusher.PushedNodes.Values.First(obj => obj.Name == "AddObj 1");
            var refVar = pusher.PushedVariables.Values.First(variable => variable.Name == "AddVar 1");

            Assert.Equal(refRoot.Id, refObj.ParentId);
            Assert.Equal(refRoot.Id, refVar.ParentId);
            Assert.NotNull(refVar.FullAttributes.DataType);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }
    }
}

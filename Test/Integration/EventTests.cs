using Cognite.OpcUa.Types;
using Opc.Ua;
using Server;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
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

            await extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(0);

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 3, 5);

            Assert.Single(pusher.Events[ids.Obj2]);
            Assert.Equal(2, pusher.Events[ids.Obj1].Count);
            Assert.Equal(8, pusher.Events[ObjectIds.Server].Count);

            var evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "prop 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Obj1, evt.SourceNode);
            Assert.Equal(4, evt.MetaData.Count);
            Assert.Equal("str 0", evt.MetaData["PropertyString"]);
            Assert.Equal(0f, evt.MetaData["PropertyNum"]);
            Assert.Equal("sub-type", evt.MetaData["SubType"]);
            Assert.Equal((ushort)100, evt.MetaData["Severity"]);
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
            Assert.Equal("str 0", evt.MetaData["PropertyString"]);
            Assert.Equal("sub-type", evt.MetaData["SubType"]);
            Assert.Equal((ushort)100, evt.MetaData["Severity"]);
            Assert.True(evt.Time > DateTime.UtcNow.AddSeconds(-5));
            Assert.Equal(ids.PropType, evt.EventType);
            Assert.StartsWith(tester.Config.Extraction.IdPrefix, evt.EventId, StringComparison.InvariantCulture);

            evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "mapped 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(2, evt.MetaData.Count);
            Assert.Equal("CustomType", evt.MetaData["Type"]);

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

            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var start = DateTime.UtcNow.AddSeconds(-5);

            tester.Server.PopulateEvents(start);

            var runTask = extractor.RunExtractor();
            var ids = tester.Server.Ids.Event;

            await extractor.WaitForSubscriptions();

            await CommonTestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(state => !state.IsFrontfilling), 5);

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 2 && pusher.Events[ObjectIds.Server].Count == 700, 5,
                () => $"Expected to get 700 events but got {pusher.Events[ObjectIds.Server].Count}");

            var evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "prop 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(ids.Obj1, evt.SourceNode);
            Assert.Equal(3, evt.MetaData.Count);
            Assert.False(evt.MetaData.ContainsKey("PropertyNum"));
            Assert.Equal("str 0", evt.MetaData["PropertyString"]);
            Assert.Equal("sub-type", evt.MetaData["SubType"]);
            Assert.Equal((ushort)100, evt.MetaData["Severity"]);
            Assert.Equal(ids.PropType, evt.EventType);
            Assert.StartsWith(tester.Config.Extraction.IdPrefix, evt.EventId, StringComparison.InvariantCulture);

            evt = pusher.Events[ObjectIds.Server].First(evt => evt.Message == "mapped 0");
            Assert.Equal(ObjectIds.Server, evt.EmittingNode);
            Assert.Equal(2, evt.MetaData.Count);
            Assert.Equal("CustomType", evt.MetaData["Type"]);

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

            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var now = DateTime.UtcNow;

            tester.Server.PopulateEvents(now.AddSeconds(-5));

            var runTask = extractor.RunExtractor();
            var ids = tester.Server.Ids.Event;

            await extractor.WaitForSubscriptions();

            await CommonTestUtils.WaitForCondition(() => extractor.State.EmitterStates.All(state => !state.IsFrontfilling), 5);

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

            await CommonTestUtils.WaitForCondition(() => pusher.Events.Count == 2 && pusher.Events[ObjectIds.Server].Count == 1407, 5,
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
        #endregion
    }
}

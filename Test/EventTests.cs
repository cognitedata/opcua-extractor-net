/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [CollectionDefinition("Event_tests", DisableParallelization = true)]
    public class EventTests : MakeConsoleWork
    {
        public EventTests(ITestOutputHelper output) : base(output) { }
        [Trait("Server", "events")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "events")]
        [Fact]
        public async Task TestEventServer()
        {
            using var tester = new ExtractorTester(new TestParameters
            {
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events
            });
            await tester.ClearPersistentData();

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Handler.events.Values.Any() &&
                    tester.Extractor.EventEmitterStates.All(state => state.Value.IsStreaming),
                40, "Expected history read to finish");


            var events = tester.Handler.events.Values.ToList();
            Assert.True(events.Any());
            Assert.Contains(events, ev => ev.description.StartsWith("prop "));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            await tester.WaitForCondition(() =>
            {
                events = tester.Handler.events.Values.ToList();
                return events.Any(ev => ev.description.StartsWith("propOther "))
                       && events.Any(ev => ev.description.StartsWith("basicPass "))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource "))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource2 "))
                       && events.Any(ev => ev.description.StartsWith("basicVarSource "))
                       && events.Any(ev => ev.description.StartsWith("mappedType "));
            }, 40, "Expected remaining event subscriptions to trigger");

            await tester.TerminateRunTask();

            events = tester.Handler.events.Values.ToList();

            foreach (var ev in events)
            {
                TestEvent(ev, tester.Handler);
            }
        }
        [Trait("Server", "events")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "eventsrestart")]
        [Fact]
        public async Task TestEventServerRestart()
        {
            using var tester = new ExtractorTester(new TestParameters
            {
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events
            });
            await tester.ClearPersistentData();

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Handler.events.Values.Any()
                    && tester.Extractor.EventEmitterStates.All(state => state.Value.IsStreaming),
                40, "Expected history read to finish");

            int lastCount = tester.Handler.events.Count;
            Assert.Equal(0, (int)Common.GetMetricValue("opcua_event_push_failures"));
            tester.Extractor.RestartExtractor(tester.Source.Token);
            await Task.Delay(500);

            await tester.WaitForCondition(() =>
                    tester.Handler.events.Values.Any()
                    && tester.Extractor.EventEmitterStates.All(state => state.Value.IsStreaming)
                    && tester.Handler.events.Count > lastCount,
                40, "Expected number of events to be increasing");

            Assert.True((int)Common.GetMetricValue("opcua_duplicated_events") > 0);
            var events = tester.Handler.events.Values.ToList();
            Assert.True(events.Any());
            Assert.Contains(events, ev => ev.description.StartsWith("prop "));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            await tester.WaitForCondition(() =>
            {
                events = tester.Handler.events.Values.ToList();
                return events.Any(ev => ev.description.StartsWith("propOther "))
                       && events.Any(ev => ev.description.StartsWith("basicPass "))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource "))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource2 "))
                       && events.Any(ev => ev.description.StartsWith("basicVarSource "))
                       && events.Any(ev => ev.description.StartsWith("mappedType "));
            }, 40, "Expected remaining event subscriptions to trigger");

            await tester.TerminateRunTask();

            events = tester.Handler.events.Values.ToList();

            foreach (var ev in events)
            {
                TestEvent(ev, tester.Handler);
            }
        }
        /// <summary>
        /// Test that the event contains the appropriate data for the event server test
        /// </summary>
        /// <param name="ev"></param>
        private static void TestEvent(EventDummy ev, CDFMockHandler factory)
        {
            Assert.False(ev.description.StartsWith("propOther2 "));
            Assert.False(ev.description.StartsWith("basicBlock "));
            Assert.False(ev.description.StartsWith("basicNoVarSource "));
            Assert.False(ev.description.StartsWith("basicExcludeSource "));
            if (ev.description.StartsWith("prop "))
            {
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !string.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
                Assert.Equal("TestSubType", ev.subtype);
                Assert.Equal("gp.efg:i=12", ev.type);
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            else if (ev.description.StartsWith("propOther "))
            {
                // This node is not historizing, so the first event should be lost
                Assert.NotEqual("propOther 0", ev.description);
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !string.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            else if (ev.description.StartsWith("basicPass "))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            // both source1 and 2
            else if (ev.description.StartsWith("basicPassSource"))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject2", false));
                if (ev.description.StartsWith("basicPassSource2 "))
                {
                    Assert.NotEqual("basicPassSource2 0", ev.description);
                }
            }
            else if (ev.description.StartsWith("basicVarSource "))
            {
                Log.Information("Test event with extid {externalId}, source {source}", ev.externalId, ev.source);
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
                Assert.True(EventSourceIs(ev, factory, "MyVariable", true));
            }
            else if (ev.description.StartsWith("mappedType "))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("TypeProp"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
                Assert.Equal("MySpecialType", ev.type);
            }
            else
            {
                throw new Exception("Unknown event found");
            }
        }
        private static bool EventSourceIs(EventDummy ev, CDFMockHandler handler, string name, bool rawSource)
        {
            var asset = handler.assets.Values.FirstOrDefault(ast => ast.name == name);
            var timeseries = handler.timeseries.Values.FirstOrDefault(ts => ts.name == name);
            if (asset == null && timeseries == null) return false;
            return rawSource
                ? asset != null && asset.externalId == ev.metadata["SourceNode"] || timeseries != null && timeseries.externalId == ev.metadata["SourceNode"]
                : asset != null && ev.assetIds.Contains(asset.id);
        }
        [Fact]
        [Trait("Server", "audit")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "audit")]
        public async Task TestAuditEvents()
        {
            using var tester = new ExtractorTester(new TestParameters
            {
                ServerName = ServerName.Audit
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.EnableAuditDiscovery = true;

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Handler.assets.Count > 0 && tester.Handler.timeseries.Count > 0,
                20, "Expected some assets and timeseries to be discovered");

            int lastAssetBefore = tester.Handler.assets.Values
                .Where(asset => asset.name.StartsWith("Add"))
                .Select(asset => int.Parse(asset.name.Split(' ')[1])).Max();

            int lastTimeseriesBefore = tester.Handler.timeseries.Values
                .Where(timeseries => timeseries.name.StartsWith("Add"))
                .Select(timeseries => int.Parse(timeseries.name.Split(' ')[1])).Max();

            int assetCount = tester.Handler.assets.Count;
            int tsCount = tester.Handler.timeseries.Count;

            await tester.WaitForCondition(() =>
                    tester.Handler.assets.Count > assetCount && tester.Handler.timeseries.Count > tsCount,
                20, "Expected timeseries and asset count to be increasing");

            await tester.TerminateRunTask();

            Assert.Contains(tester.Handler.assets.Values, asset => asset.name == "AddObject 0");
            Assert.Contains(tester.Handler.timeseries.Values, timeseries => timeseries.name == "AddVariable 0");
            Assert.Contains(tester.Handler.timeseries.Values, timeseries => timeseries.name == "AddExtraVariable 0");

            Assert.Contains(tester.Handler.assets.Values, asset => asset.name == "AddObject " + (lastAssetBefore + 1));
            Assert.Contains(tester.Handler.timeseries.Values, timeseries => timeseries.name == "AddVariable " + (lastTimeseriesBefore + 1));
            Assert.Contains(tester.Handler.timeseries.Values, timeseries => timeseries.name == "AddExtraVariable " + (lastTimeseriesBefore + 1));
        }
    }
}

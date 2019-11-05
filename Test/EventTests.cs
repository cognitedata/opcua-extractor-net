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
            var fullConfig = Common.BuildConfig("events", 8, "config.events.yml");
            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);

            bool historyReadDone = false;
            await Task.Delay(1000);
            for (int i = 0; i < 20; i++)
            {
                if (handler.events.Values.Any() && extractor.EventEmitterStates.All(state => state.Value.IsStreaming))
                {
                    historyReadDone = true;
                    break;
                }
                await Task.Delay(1000);
            }
            Assert.True(historyReadDone);
            var events = handler.events.Values.ToList();
            Assert.True(events.Any());
            Assert.Contains(events, ev => ev.description.StartsWith("prop "));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            for (int i = 0; i < 10; i++)
            {
                events = handler.events.Values.ToList();
                if (events.Any(ev => ev.description.StartsWith("propOther "))
                    && events.Any(ev => ev.description.StartsWith("basicPass "))
                    && events.Any(ev => ev.description.StartsWith("basicPassSource "))
                    && events.Any(ev => ev.description.StartsWith("basicPassSource2 ")) 
                    && events.Any(ev => ev.description.StartsWith("basicVarSource "))
                    && events.Any(ev => ev.description.StartsWith("mappedType "))) break;
                await Task.Delay(1000);
            }

            Assert.Contains(events, ev => ev.description.StartsWith("propOther "));
            Assert.Contains(events, ev => ev.description.StartsWith("basicPass "));
            Assert.Contains(events, ev => ev.description.StartsWith("basicPassSource "));
            Assert.Contains(events, ev => ev.description.StartsWith("basicPassSource2 "));
            Assert.Contains(events, ev => ev.description.StartsWith("basicVarSource "));
            Assert.Contains(events, ev => ev.description.StartsWith("mappedType "));

            foreach (var ev in events)
            {
                TestEvent(ev, handler);
            }
            source.Cancel();
            try
            {
                await runTask;
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }
            extractor.Close();
        }
        [Trait("Server", "events")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "eventsrestart")]
        [Fact]
        public async Task TestEventServerRestart()
        {
            var fullConfig = Common.BuildConfig("events", 8, "config.events.yml");
            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);

            bool historyReadDone = false;
            await Task.Delay(1000);
            for (int i = 0; i < 40; i++)
            {
                if (handler.events.Values.Any() && extractor.EventEmitterStates.All(state => state.Value.IsStreaming))
                {
                    historyReadDone = true;
                    break;
                }
                await Task.Delay(500);
            }

            Assert.True(historyReadDone);
            int lastCount = handler.events.Count;
            Assert.Equal(0, (int)Common.GetMetricValue("opcua_event_push_failures"));
            extractor.RestartExtractor(source.Token);
            historyReadDone = false;
            await Task.Delay(1000);
            for (int i = 0; i < 40; i++)
            {
                if (handler.events.Values.Any() && extractor.EventEmitterStates.All(state => state.Value.IsStreaming) && handler.events.Count > lastCount)
                {
                    historyReadDone = true;
                    break;
                }
                await Task.Delay(500);
            }
            Assert.True(historyReadDone);
            Assert.True((int)Common.GetMetricValue("opcua_duplicated_events") > 0);
            var events = handler.events.Values.ToList();
            Assert.True(events.Any());
            Assert.Contains(events, ev => ev.description.StartsWith("prop "));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            for (int i = 0; i < 10; i++)
            {
                events = handler.events.Values.ToList();
                if (events.Any(ev => ev.description.StartsWith("propOther "))
                    && events.Any(ev => ev.description.StartsWith("basicPass "))
                    && events.Any(ev => ev.description.StartsWith("basicPassSource "))
                    && events.Any(ev => ev.description.StartsWith("basicPassSource2 "))
                    && events.Any(ev => ev.description.StartsWith("basicVarSource "))
                    && events.Any(ev => ev.description.StartsWith("mappedType "))) break;
                await Task.Delay(1000);
            }

            Assert.Contains(events, ev => ev.description.StartsWith("propOther "));
            Assert.Contains(events, ev => ev.description.StartsWith("basicPass "));
            Assert.Contains(events, ev => ev.description.StartsWith("basicPassSource "));
            Assert.Contains(events, ev => ev.description.StartsWith("basicPassSource2 "));
            Assert.Contains(events, ev => ev.description.StartsWith("basicVarSource "));
            Assert.Contains(events, ev => ev.description.StartsWith("mappedType "));

            foreach (var ev in events)
            {
                TestEvent(ev, handler);
            }
            source.Cancel();
            try
            {
                await runTask;
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }
            extractor.Close();
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
            var fullConfig = Common.BuildConfig("audit", 10);
            fullConfig.Extraction.EnableAuditDiscovery = true;
            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);

            var tsCnt = 0;
            var assetCnt = 0;
            for (int i = 0; i < 20; i++)
            {
                assetCnt = handler.assets.Count;
                tsCnt = handler.timeseries.Count;
                if (assetCnt > 0 && tsCnt > 0) break;
                await Task.Delay(500);
            }

            var lastAssetBefore = handler.assets.Values
                .Where(asset => asset.name.StartsWith("Add"))
                .Select(asset => int.Parse(asset.name.Split(' ')[1])).Max();

            var lastTimeseriesBefore = handler.timeseries.Values
                .Where(timeseries => timeseries.name.StartsWith("Add"))
                .Select(timeseries => int.Parse(timeseries.name.Split(' ')[1])).Max();

            Assert.True(tsCnt > 0, "Expected some timeseries");
            Assert.True(assetCnt > 0, "Expected some assets");

            var newTsCnt = 0;
            var newAssetCnt = 0;
            for (int i = 0; i < 20; i++)
            {
                newAssetCnt = handler.assets.Count;
                newTsCnt = handler.timeseries.Count;
                if (newAssetCnt > assetCnt && newTsCnt > tsCnt) break;
                await Task.Delay(500);
            }
            Assert.True(newTsCnt > tsCnt, "Expected some new timeseries");
            Assert.True(newAssetCnt > assetCnt, "Expected some new assets");
            await Task.Delay(500);

            source.Cancel();
            try
            {
                await runTask;
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }

            Assert.Contains(handler.assets.Values, asset => asset.name == "AddObject 0");
            Assert.Contains(handler.timeseries.Values, timeseries => timeseries.name == "AddVariable 0");
            Assert.Contains(handler.timeseries.Values, timeseries => timeseries.name == "AddExtraVariable 0");

            Assert.Contains(handler.assets.Values, asset => asset.name == "AddObject " + (lastAssetBefore + 1));
            Assert.Contains(handler.timeseries.Values, timeseries => timeseries.name == "AddVariable " + (lastTimeseriesBefore + 1));
            Assert.Contains(handler.timeseries.Values, timeseries => timeseries.name == "AddExtraVariable " + (lastTimeseriesBefore + 1));
        }
    }
}

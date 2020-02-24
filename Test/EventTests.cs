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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Microsoft.FSharp.Core;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [CollectionDefinition("Event_tests", DisableParallelization = true)]
    public class EventTests : MakeConsoleWork
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(EventTests));

        public EventTests(ITestOutputHelper output) : base(output) { }
        [Trait("Server", "events")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "events")]
        [Fact]
        public async Task TestEventServer()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events
            });
            await tester.ClearPersistentData();

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Handler.events.Values.Count > 20 &&
                    tester.Extractor.EmitterStates.All(state => state.Value.IsStreaming),
                40, "Expected history read to finish");


            var events = tester.Handler.events.Values.ToList();
            Assert.True(events.Any());
            Assert.Contains(events, ev => ev.description.StartsWith("prop ", StringComparison.InvariantCulture));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            await tester.WaitForCondition(() =>
            {
                events = tester.Handler.events.Values.ToList();
                return events.Any(ev => ev.description.StartsWith("propOther ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPass ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource2 ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicVarSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("mappedType ", StringComparison.InvariantCulture));
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
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events
            });
            await tester.ClearPersistentData();

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Handler.events.Values.Any()
                    && tester.Extractor.EmitterStates.All(state => state.Value.IsStreaming),
                40, "Expected history read to finish");

            await tester.Extractor.WaitForNextPush();

            int lastCount = tester.Handler.events.Count;
            Assert.Equal(0, (int)CommonTestUtils.GetMetricValue("opcua_event_push_failures"));
            tester.Extractor.RestartExtractor();
            await Task.Delay(500);

            await tester.WaitForCondition(() =>
                    tester.Handler.events.Values.Any()
                    && tester.Extractor.EmitterStates.All(state => state.Value.IsStreaming)
                    && tester.Handler.events.Count > lastCount,
                40, "Expected number of events to be increasing");

            Assert.True((int)CommonTestUtils.GetMetricValue("opcua_duplicated_events") > 0);
            var events = tester.Handler.events.Values.ToList();
            Assert.True(events.Any());
            Assert.Contains(events, ev => ev.description.StartsWith("prop ", StringComparison.InvariantCulture));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            await tester.WaitForCondition(() =>
            {
                events = tester.Handler.events.Values.ToList();
                return events.Any(ev => ev.description.StartsWith("propOther ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPass ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource2 ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicVarSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("mappedType ", StringComparison.InvariantCulture));
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
            Assert.False(ev.description.StartsWith("propOther2 ", StringComparison.InvariantCulture));
            Assert.False(ev.description.StartsWith("basicBlock ", StringComparison.InvariantCulture));
            Assert.False(ev.description.StartsWith("basicNoVarSource ", StringComparison.InvariantCulture));
            Assert.False(ev.description.StartsWith("basicExcludeSource ", StringComparison.InvariantCulture));
            if (ev.description.StartsWith("prop ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !string.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
                Assert.Equal("TestSubType", ev.subtype);
                Assert.Equal("gp.efg:i=12", ev.type);
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            else if (ev.description.StartsWith("propOther ", StringComparison.InvariantCulture))
            {
                // This node is not historizing, so the first event should be lost
                Assert.NotEqual("propOther 0", ev.description);
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !string.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            else if (ev.description.StartsWith("basicPass ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            // both source1 and 2
            else if (ev.description.StartsWith("basicPassSource", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject2", false));
                if (ev.description.StartsWith("basicPassSource2 ", StringComparison.InvariantCulture))
                {
                    Assert.NotEqual("basicPassSource2 0", ev.description);
                }
            }
            else if (ev.description.StartsWith("basicVarSource ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
                Assert.True(EventSourceIs(ev, factory, "MyVariable", true));
            }
            else if (ev.description.StartsWith("mappedType ", StringComparison.InvariantCulture))
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
            using var tester = new ExtractorTester(new ExtractorTestParameters
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
                .Where(asset => asset.name.StartsWith("Add", StringComparison.InvariantCulture))
                .Select(asset => int.Parse(asset.name.Split(' ')[1], CultureInfo.InvariantCulture)).Max();

            int lastTimeseriesBefore = tester.Handler.timeseries.Values
                .Where(timeseries => timeseries.name.StartsWith("Add", StringComparison.InvariantCulture))
                .Select(timeseries => int.Parse(timeseries.name.Split(' ')[1], CultureInfo.InvariantCulture)).Max();

            int assetCount = tester.Handler.assets.Count;
            int tsCount = tester.Handler.timeseries.Count;

            await tester.WaitForCondition(() =>
                    tester.Handler.assets.Count > assetCount && tester.Handler.timeseries.Count > tsCount,
                20, "Expected timeseries and asset count to be increasing");

            await Task.Delay(1000);

            await tester.TerminateRunTask();

            Assert.Contains(tester.Handler.assets.Values, asset => asset.name == "AddObject 0");
            Assert.Contains(tester.Handler.timeseries.Values, timeseries => timeseries.name == "AddVariable 0");
            Assert.Contains(tester.Handler.timeseries.Values, timeseries => timeseries.name == "AddExtraVariable 0");

            Assert.Contains(tester.Handler.assets.Values, asset => asset.name == "AddObject " + (lastAssetBefore + 1));
            Assert.Contains(tester.Handler.timeseries.Values, timeseries => timeseries.name == "AddVariable " + (lastTimeseriesBefore + 1));
            Assert.Contains(tester.Handler.timeseries.Values, timeseries => timeseries.name == "AddExtraVariable " + (lastTimeseriesBefore + 1));
        }
        [Fact]
        [Trait("Server", "events")]
        [Trait("Target", "FailureBuffer")]
        [Trait("Test", "influxeventsbuffering")]
        public async Task TestEventsInfluxBuffering()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events,
                FailureInflux = ConfigName.Influx,
                FailureInfluxWrite = true,
                LogLevel = "debug"
            });
            await tester.ClearPersistentData();
            tester.Handler.AllowEvents = false;
            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyEvents,
                20, "Expected failurebuffer to contain some events");

            tester.Handler.AllowEvents = true;
            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.AnyEvents,
                20, "Expected FailureBuffer to be emptied");
            Assert.False(tester.Extractor.FailureBuffer.Any);

            await tester.WaitForCondition(() => tester.Handler.events.Count > 20, 20,
                "Expected to receive some events");

            await tester.WaitForCondition(() =>
            {
                var events = tester.Handler.events.Values.ToList();
                return events.Any(ev => ev.description.StartsWith("propOther ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPass ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource2 ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicVarSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("mappedType ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description == "prop 0")
                       && events.Any(ev => ev.description == "basicPass 0")
                       && events.Any(ev => ev.description == "basicPassSource 0")
                       && events.Any(ev => ev.description == "basicVarSource 0")
                       && events.Any(ev => ev.description == "mappedType 0");
            }, 20, "Expected to receive the remaining events");

            await tester.TerminateRunTask();

            var events = tester.Handler.events.Values.ToList();

            foreach (var ev in events)
            {
                TestEvent(ev, tester.Handler);
            }
        }

        [Fact]
        [Trait("Server", "events")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "cdfeventsbackfill")]
        public async Task TestCDFEventsBackfill()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events,
                LogLevel = "debug"
            });
            tester.Config.History.EventChunk = 100;
            tester.Config.History.Backfill = true;
            await tester.ClearPersistentData();

            tester.StartExtractor();
            await tester.WaitForCondition(() =>
                    tester.Handler.events.Any()
                    && tester.Extractor.EmitterStates.Values.All(state => state.BackfillDone),
                40, "Expected backfill to finish");

            await Task.Delay(1000); // Wait for final pushes to complete

            var events = tester.Handler.events.Values.ToList();
            Assert.True(events.Any());

            Assert.Contains(events, ev => ev.description.StartsWith("prop ", StringComparison.InvariantCulture));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            await tester.WaitForCondition(() =>
            {
                events = tester.Handler.events.Values.ToList();
                return events.Any(ev => ev.description.StartsWith("propOther ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPass ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource2 ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicVarSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("mappedType ", StringComparison.InvariantCulture));
            }, 40, "Expected remaining event subscriptions to trigger");

            await tester.TerminateRunTask();

            events = tester.Handler.events.Values.ToList();

            foreach (var ev in events)
            {
                TestEvent(ev, tester.Handler);
            }

            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_events_count") >= 1);
            Assert.True(CommonTestUtils.VerifySuccessMetrics());
        }
        [Fact]
        [Trait("Server", "events")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "multipushereventbackfill")]
        public async Task TestMultiPusherBackfillRestart()
        {
            var influxCfg = ExtractorUtils.GetConfig("config.influxtest.yml");
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events,
                LogLevel = "debug",
                InfluxOverride = (InfluxClientConfig)influxCfg.Pushers.First(),
                Builder = (cfg, pusher, client) =>
                {
                    var pushers = new List<IPusher>
                    {
                        pusher, new InfluxPusher((InfluxClientConfig) influxCfg.Pushers.First())
                    };

                    return new Extractor(cfg, pushers, client);
                }
            });

            tester.Config.History.Backfill = true;

            await tester.ClearPersistentData();

            tester.StartExtractor();
            await tester.WaitForCondition(() =>
                    tester.Handler.events.Values.Any()
                    && tester.Extractor.EmitterStates.Values.All(state => state.BackfillDone),
                40, "Expected backfill to finish");

            await tester.WaitForCondition(() =>
            {
                var events = tester.Handler.events.Values.ToList();
                return events.Any(ev => ev.description.StartsWith("propOther ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPass ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicPassSource2 ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("basicVarSource ", StringComparison.InvariantCulture))
                       && events.Any(ev => ev.description.StartsWith("mappedType ", StringComparison.InvariantCulture));
            }, 20, "Expected remaining event subscriptions to trigger");

            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_events_count") >= 1);
            Assert.True(CommonTestUtils.VerifySuccessMetrics());

            CommonTestUtils.ResetTestMetrics();
            tester.Extractor.RestartExtractor();

            await tester.WaitForCondition(() =>
                    CommonTestUtils.TestMetricValue("opcua_extractor_starting", 0), 20,
                "Expected restart to begin");

            await tester.WaitForCondition(() =>
                    tester.Handler.events.Values.Any()
                    && tester.Extractor.EmitterStates.Values.All(state => state.BackfillDone),
                20, "Expected backfill to finish");

            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 1));


        }
    }
}

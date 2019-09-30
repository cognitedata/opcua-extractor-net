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
        [Trait("Category", "eventserver")]
        [Fact]
        public async Task TestEventServer()
        {
            var fullConfig = Common.BuildConfig("events", 8, "config.events.yml");
            Logger.Configure(fullConfig.Logging);
            UAClient client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var factory = new DummyFactory(config.Project, DummyFactory.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(factory), config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);

                bool historyReadDone = false;
                await Task.Delay(1000);
                for (int i = 0; i < 20; i++)
                {
                    if (factory.events.Values.Any() && !extractor.EventsNotInSync.Any())
                    {
                        historyReadDone = true;
                        break;
                    }
                    await Task.Delay(1000);
                }
                Assert.True(historyReadDone);
                await Task.Delay(1000);
                Assert.True(factory.events.Values.Any());
                Assert.Contains(factory.events.Values, ev => ev.description.StartsWith("prop "));
                Assert.Contains(factory.events.Values, ev => ev.description == "prop 0");
                Assert.Contains(factory.events.Values, ev => ev.description == "basicPass 0");
                Assert.Contains(factory.events.Values, ev => ev.description == "basicPassSource 0");
                Assert.Contains(factory.events.Values, ev => ev.description == "basicVarSource 0");
                Assert.Contains(factory.events.Values, ev => ev.description == "mappedType 0");

                Assert.Contains(factory.events.Values, ev => ev.description.StartsWith("propOther "));
                Assert.Contains(factory.events.Values, ev => ev.description.StartsWith("basicPass "));
                Assert.Contains(factory.events.Values, ev => ev.description.StartsWith("basicPassSource "));
                Assert.Contains(factory.events.Values, ev => ev.description.StartsWith("basicPassSource2 "));
                Assert.Contains(factory.events.Values, ev => ev.description.StartsWith("basicVarSource "));
                Assert.Contains(factory.events.Values, ev => ev.description.StartsWith("mappedType "));

                foreach (var ev in factory.events.Values)
                {
                    TestEvent(ev, factory);
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
        }
        /// <summary>
        /// Test that the event contains the appropriate data for the event server test
        /// </summary>
        /// <param name="ev"></param>
        private static void TestEvent(EventDummy ev, DummyFactory factory)
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
        private static bool EventSourceIs(EventDummy ev, DummyFactory factory, string name, bool rawSource)
        {
            var asset = factory.assets.Values.FirstOrDefault(ast => ast.name == name);
            var timeseries = factory.timeseries.Values.FirstOrDefault(ts => ts.name == name);
            if (asset == null && timeseries == null) return false;
            return rawSource
                ? asset != null && asset.externalId == ev.source || timeseries != null && timeseries.externalId == ev.source
                : asset != null &&  ev.assetIds.Contains(asset.id);
        }

    }
}

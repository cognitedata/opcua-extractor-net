using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    public class MakeConsoleWork : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly TextWriter _originalOut;
        private readonly TextWriter _textWriter;

        public MakeConsoleWork(ITestOutputHelper output)
        {
            _output = output;
            _originalOut = Console.Out;
            _textWriter = new StringWriter();
            Console.SetOut(_textWriter);
        }

        public void Dispose()
        {
            _output.WriteLine(_textWriter.ToString());
            Console.SetOut(_originalOut);
        }
    }
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
                // Re-add this check once fix to source is accepted into the sdk
                // Assert.True(EventSourceIs(ev, factory, "MyVariable", true));
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

using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Microsoft.Extensions.DependencyInjection;
using Server;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Test.Utils;
using System.Runtime.InteropServices;
using System.Linq;
using System.Collections.Generic;
using Opc.Ua;
using Cognite.OpcUa.TypeCollectors;

namespace Test.Unit
{

    public sealed class ExtractorTestFixture : IDisposable
    {
        public UAClient Client { get; }
        public FullConfig Config { get; }
        public ServerController Server { get; }
        public CancellationTokenSource Source { get; }
        public IServiceProvider Provider { get; }
        public ExtractorTestFixture()
        {
            Server = new ServerController(new[] {
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, 62100);
            Server.Start().Wait();

            var services = new ServiceCollection();
            Config = services.AddConfig<FullConfig>("config.test.yml", 1);
            Config.Source.EndpointUrl = $"opc.tcp://localhost:62100";
            services.AddLogging();
            LoggingUtils.Configure(Config.Logger);
            Provider = services.BuildServiceProvider();

            Client = new UAClient(Config);
            Source = new CancellationTokenSource();
            Client.Run(Source.Token).Wait();
        }

        public void Dispose()
        {
            Source.Cancel();
            Source.Dispose();
        }

        public UAExtractor BuildExtractor(bool clear = true, IExtractionStateStore stateStore = null, params IPusher[] pushers)
        {
            if (clear)
            {
                Client.ClearNodeOverrides();
                Client.ClearEventFields();
                Client.ResetVisitedNodes();
            }
            return new UAExtractor(Config, pushers, Client, stateStore, Source.Token);
        }
    }
    public class UAExtractorTest : MakeConsoleWork, IClassFixture<ExtractorTestFixture>
    {
        private ExtractorTestFixture tester;
        public UAExtractorTest(ITestOutputHelper output, ExtractorTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        [Fact]
        public async Task TestClientStartFailure()
        {
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:60000";
            tester.Client.Close();

            try
            {
                using var extractor = tester.BuildExtractor();
                await Assert.ThrowsAsync<SilentServiceException>(() => extractor.RunExtractor(true));
            }
            finally
            {
                tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62100";
                await tester.Client.Run(tester.Source.Token);
            }
        }
        [Fact]
        public async Task TestMapping()
        {
            tester.Config.Extraction.RootNode = tester.Server.Ids.Full.Root.ToProtoNodeId(tester.Client);
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            try
            {
                await extractor.RunExtractor(true);

                Assert.Equal(153, pusher.PushedNodes.Count);
                Assert.Equal(2000, pusher.PushedVariables.Count);

                Assert.Contains(pusher.PushedNodes.Values, node => node.DisplayName == "DeepObject 4, 25");
                Assert.Contains(pusher.PushedVariables.Values, node => node.DisplayName == "SubVariable 1234");
            }
            finally
            {
                tester.Config.Extraction.RootNode = null;
            }

        }
        private static void TriggerEventExternally(string field, object parent)
        {
            var dg = parent.GetType()
                .GetField(field, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .GetValue(parent) as MulticastDelegate;
            foreach (var handler in dg.GetInvocationList())
            {
                handler.Method.Invoke(
                    handler.Target,
                    new object[] { parent, EventArgs.Empty });
            }
        }

        [Fact]
        public async Task TestForceRestart()
        {
            tester.Config.Source.ForceRestart = true;
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            try
            {
                var task = extractor.RunExtractor();
                await extractor.WaitForSubscriptions();

                Assert.False(task.IsCompleted);

                TriggerEventExternally("OnServerDisconnect", tester.Client);

                await Task.WhenAny(task, Task.Delay(10000));
                Assert.True(task.IsCompleted);
            }
            finally
            {
                tester.Config.Source.ForceRestart = false;
            }
        }
        [Fact]
        public async Task TestRestartOnReconnect()
        {
            tester.Config.Source.RestartOnReconnect = true;

            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            try
            {
                var task = extractor.RunExtractor();
                await extractor.WaitForSubscriptions();
                Assert.True(pusher.PushedNodes.Any());
                pusher.PushedNodes.Clear();
                TriggerEventExternally("OnServerReconnect", tester.Client);

                Assert.True(pusher.OnReset.WaitOne(10000));

                await CommonTestUtils.WaitForCondition(() => pusher.PushedNodes.Count > 0, 10);

                extractor.Close();
            }
            finally
            {
                tester.Config.Source.RestartOnReconnect = false;
            }
        }
        [Theory]
        [InlineData(0, 2, 2, 1, 0, 0)]
        [InlineData(1, 0, 0, 1, 4, 0)]
        [InlineData(2, 2, 2, 0, 1, 1)]
        [InlineData(3, 2, 2, 1, 1, 0)]
        [InlineData(4, 2, 2, 1, 0, 0)]
        [InlineData(5, 0, 0, 0, 4, 1)]
        public async Task TestPushNodes(int failAt, int pushedObjects, int pushedVariables, int pushedRefs, int failedNodes, int failedRefs)
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.Relationships.Enabled = true;
            using var extractor = tester.BuildExtractor(pushers: pusher);

            switch (failAt)
            {
                case 1:
                    pusher.PushNodesResult = false;
                    break;
                case 2:
                    pusher.PushReferenceResult = false;
                    break;
                case 3:
                    pusher.InitDpRangesResult = false;
                    break;
                case 4:
                    pusher.InitEventRangesResult = false;
                    break;
                case 5:
                    pusher.NoInit = true;
                    break;
            }

            var root = new NodeId(1);
            var nodes = new List<BufferedNode>
            {
                new BufferedNode(new NodeId("object1"), "object1", root),
                new BufferedNode(new NodeId("object2"), "object2", root)
            };
            var variables = new List<BufferedVariable>
            {
                new BufferedVariable(new NodeId("var1"), "var1", root),
                new BufferedVariable(new NodeId("var2"), "var2", root)
            };

            extractor.State.SetNodeState(new NodeExtractionState(tester.Client, variables[0], true, true, false));
            extractor.State.SetNodeState(new NodeExtractionState(tester.Client, variables[1], false, false, false));


            var refManager = (ReferenceTypeManager)extractor.GetType().GetField("referenceTypeManager",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).GetValue(extractor);

            var references = new List<BufferedReference>
            {
                new BufferedReference(new ReferenceDescription {
                    IsForward = true, NodeClass = NodeClass.Variable, ReferenceTypeId = ReferenceTypeIds.Organizes },
                    new BufferedNode(new NodeId("object1"), "object1", root), new NodeId("var1"), null, refManager)
            };

            try
            {
                await extractor.PushNodes(nodes, variables, references, pusher, true, true);

                Assert.Equal(pushedObjects, pusher.PushedNodes.Count);
                Assert.Equal(pushedVariables, pusher.PushedVariables.Count);
                Assert.Equal(pushedRefs, pusher.PushedReferences.Count);
                Assert.Equal(failedNodes, pusher.PendingNodes.Count);
                Assert.Equal(failedRefs, pusher.PendingReferences.Count);

                if (failAt == 0)
                {
                    Assert.True(pusher.Initialized);
                }
                else
                {
                    Assert.False(pusher.Initialized);
                }
            }
            finally
            {
                tester.Config.Extraction.Relationships.Enabled = false;
            }
            
        }

        [Fact]
        public async Task TestGetProperties()
        {
            // Create multiple partially overlapping tasks to read properties, then wait for the last one to complete.
            // This should result in all tasks being completed and all properties being read.
            using var extractor = tester.BuildExtractor();

            var custIds = tester.Server.Ids.Custom;
            var var1 = new BufferedVariable(custIds.MysteryVar, "MysteryVar", custIds.Root);
            var var2 = new BufferedVariable(custIds.Array, "Array", custIds.Root);
            var obj1 = new BufferedNode(custIds.Obj1, "Object1", custIds.Root);
            obj1.Properties = new List<BufferedVariable>
            {
                new BufferedVariable(custIds.StringArray, "StringArray", custIds.Obj1) { IsProperty = true, PropertiesRead = true },
                new BufferedVariable(tester.Server.Ids.Base.DoubleVar1, "VarProp1", custIds.Obj1) { IsProperty = true }
            };
            var obj2 = new BufferedNode(custIds.Obj2, "Object2", custIds.Root);
            obj2.Properties = new List<BufferedVariable>
            {
                new BufferedVariable(custIds.ObjProp, "ObjProp1", custIds.Obj2) { IsProperty = true, PropertiesRead = true },
                new BufferedVariable(custIds.ObjProp2, "ObjProp2", custIds.Obj2) { IsProperty = true, PropertiesRead = true }
            };

            var chunks = new List<List<BufferedNode>>
            {
                new List<BufferedNode> { var1, obj1 },
                new List<BufferedNode> { var2, obj2 },
                new List<BufferedNode> { var1, obj2, var2 }
            };

            var tasks = chunks.Select(chunk => extractor.ReadProperties(chunk)).ToList();

            await tasks[2];

            Assert.True(tasks[0].IsCompleted);
            Assert.True(tasks[1].IsCompleted);

            Assert.Equal(2, var1.Properties.Count);
            Assert.Equal(2, var2.Properties.Count);
            foreach (var node in chunks.SelectMany(chunk => chunk))
            {
                Assert.Equal(2, node.Properties.Count);
                foreach (var prop in node.Properties)
                {
                    Assert.NotNull(prop.Value);
                    Assert.False(string.IsNullOrEmpty(prop.Value.StringValue));
                }
            }
        }
    }
}

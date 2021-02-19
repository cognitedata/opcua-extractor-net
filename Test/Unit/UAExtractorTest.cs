using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
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
    public sealed class ExtractorTestFixture : BaseExtractorTestFixture
    {
        public ExtractorTestFixture() : base(62100) { }
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
            var nodes = new List<UANode>
            {
                new UANode(new NodeId("object1"), "object1", root),
                new UANode(new NodeId("object2"), "object2", root)
            };
            var variables = new List<UAVariable>
            {
                new UAVariable(new NodeId("var1"), "var1", root),
                new UAVariable(new NodeId("var2"), "var2", root)
            };

            extractor.State.SetNodeState(new VariableExtractionState(tester.Client, variables[0], true, true));
            extractor.State.SetNodeState(new VariableExtractionState(tester.Client, variables[1], false, false));


            var refManager = (ReferenceTypeManager)extractor.GetType().GetField("referenceTypeManager",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).GetValue(extractor);

            var references = new List<UAReference>
            {
                new UAReference(
                    ReferenceTypeIds.Organizes,
                    true,
                    new NodeId("object1"),
                    new NodeId("var1"),
                    false,
                    true,
                    refManager)
            };

            try
            {
                await extractor.PushNodes(nodes, variables, references, pusher, true);

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
            var var1 = new UAVariable(custIds.MysteryVar, "MysteryVar", custIds.Root);
            var var2 = new UAVariable(custIds.Array, "Array", custIds.Root);
            var obj1 = new UANode(custIds.Obj1, "Object1", custIds.Root);
            obj1.Properties = new List<UAVariable>
            {
                new UAVariable(custIds.StringArray, "StringArray", custIds.Obj1) { IsProperty = true, PropertiesRead = true },
                new UAVariable(tester.Server.Ids.Base.DoubleVar1, "VarProp1", custIds.Obj1) { IsProperty = true }
            };
            var obj2 = new UANode(custIds.Obj2, "Object2", custIds.Root);
            obj2.Properties = new List<UAVariable>
            {
                new UAVariable(custIds.ObjProp, "ObjProp1", custIds.Obj2) { IsProperty = true, PropertiesRead = true },
                new UAVariable(custIds.ObjProp2, "ObjProp2", custIds.Obj2) { IsProperty = true, PropertiesRead = true }
            };

            var chunks = new List<List<UANode>>
            {
                new List<UANode> { var1, obj1 },
                new List<UANode> { var2, obj2 },
                new List<UANode> { var1, obj2, var2 }
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
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestExtractorRuntime(bool failedStart)
        {
            // Set up for each of the three pushers
            var services = new ServiceCollection();
            var config = services.AddConfig<FullConfig>("config.test.yml", 1);
            config.Source.EndpointUrl = "opc.tcp://localhost:62100";
            var handler = new CDFMockHandler(config.Cognite.Project, CDFMockHandler.MockMode.None);

            handler.AllowConnectionTest = !failedStart;

            CommonTestUtils.AddDummyProvider(handler, services);
            services.AddCogniteClient("OPC-UA Extractor", true, true, false);
            var provider = services.BuildServiceProvider();

            var runtime = new ExtractorRuntime(config, provider);

            using (var source = new CancellationTokenSource())
            {
                var runTask = runtime.Run(source.Token);

                await Task.Delay(2000);
                Assert.False(runTask.IsFaulted);
                if (!failedStart)
                {
                    await CommonTestUtils.WaitForCondition(() => handler.Timeseries.Any(), 10);
                }
                else
                {
                    Assert.Empty(handler.Timeseries);
                }
                Assert.False(runTask.IsFaulted);
                source.Cancel();

                try
                {
                    await runTask;
                }
                catch (Exception ex)
                {
                    CommonTestUtils.TestRunResult(ex);
                }
            }
        }
        [Fact]
        public async Task TestEmptyRuntime()
        {
            var services = new ServiceCollection();
            var config = services.AddConfig<FullConfig>("config.test.yml", 1);
            config.Source.EndpointUrl = "opc.tcp://localhost:62100";
            config.Cognite = null;
            config.Influx = null;
            config.Mqtt = null;
            var provider = services.BuildServiceProvider();

            var runtime = new ExtractorRuntime(config, provider);

            using (var source = new CancellationTokenSource())
            {
                var runTask = runtime.Run(source.Token);

                await Task.Delay(2000);
                Assert.False(runTask.IsFaulted);
                source.Cancel();

                try
                {
                    await runTask;
                }
                catch (Exception ex)
                {
                    CommonTestUtils.TestRunResult(ex);
                }
            }
        }
        [Fact]
        public void TestGetExtraMetadata()
        {
            using var extractor = tester.BuildExtractor();

            Assert.Null(extractor.GetExtraMetadata(null));

            tester.Config.Extraction.DataTypes.DataTypeMetadata = true;
            var variable = new UAVariable(new NodeId("test"), "test", NodeId.Null);
            variable.DataType = new UADataType(DataTypeIds.Double);
            var fields = extractor.GetExtraMetadata(variable);
            Assert.Single(fields);
            Assert.Equal("Double", fields["dataType"]);

            tester.Config.Extraction.NodeTypes.Metadata = true;
            var node = new UANode(new NodeId("test"), "test", NodeId.Null);
            node.NodeType = new UANodeType(new NodeId("type"), false) { Name = "SomeType" };
            fields = extractor.GetExtraMetadata(node);
            Assert.Single(fields);
            Assert.Equal("SomeType", fields["TypeDefinition"]);

            tester.Config.Extraction.DataTypes.DataTypeMetadata = false;
            tester.Config.Extraction.NodeTypes.Metadata = false;
        }
        [Fact]
        public async Task TestNodeMapping()
        {
            tester.Config.Extraction.NodeMap = new Dictionary<string, ProtoNodeId>
            {
                { "Test1", new NodeId("test").ToProtoNodeId(tester.Client) },
                { "Test2", new NodeId("test2", 2).ToProtoNodeId(tester.Client) }
            };
            using var extractor = tester.BuildExtractor();

            // Run the configure extractor method...
            await extractor.RunExtractor(true);

            Assert.Equal("Test1", extractor.GetUniqueId(new NodeId("test")));
            Assert.Equal("Test2", tester.Client.GetUniqueId(new NodeId("test2", 2)));
            Assert.Equal("Test1[0]", extractor.GetUniqueId(new NodeId("test"), 0));

            tester.Config.Extraction.NodeMap = null;
        }
    }
}

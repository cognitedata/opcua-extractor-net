using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.History;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    [Collection("Shared server tests")]
    public class UAExtractorTest
    {
        private readonly StaticServerTestFixture tester;
        public UAExtractorTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
        }
        [Fact]
        public async Task TestClientStartFailure()
        {
            var oldEP = tester.Config.Source.EndpointUrl;
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:60000";
            await tester.Client.Close(tester.Source.Token);

            try
            {
                using var extractor = tester.BuildExtractor();
                await Assert.ThrowsAsync<SilentServiceException>(() => extractor.RunExtractor(true, 0));
            }
            finally
            {
                tester.Config.Source.EndpointUrl = oldEP;
                await tester.Client.Run(tester.Source.Token, 0);
            }
        }
        [Fact]
        public async Task TestMapping()
        {
            tester.Config.Extraction.RootNode = tester.Server.Ids.Full.Root.ToProtoNodeId(tester.Client);
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            await extractor.RunExtractor(true);

            Assert.Equal(153, pusher.PushedNodes.Count);
            Assert.Equal(2000, pusher.PushedVariables.Count);

            Assert.Contains(pusher.PushedNodes.Values, node => node.DisplayName == "DeepObject 4, 25");
            Assert.Contains(pusher.PushedVariables.Values, node => node.DisplayName == "SubVariable 1234");
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
        public async Task TestRestartOnReconnect()
        {
            tester.Config.Source.RestartOnReconnect = true;
            if (!tester.Client.Started) await tester.Client.Run(tester.Source.Token, 0);
            tester.Config.Extraction.RootNode = tester.Ids.Base.Root.ToProtoNodeId(tester.Client);

            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            var task = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();
            Assert.True(pusher.PushedNodes.Any());
            pusher.PushedNodes.Clear();
            TriggerEventExternally("OnServerReconnect", tester.Client);

            Assert.True(pusher.OnReset.WaitOne(10000));

            await TestUtils.WaitForCondition(() => pusher.PushedNodes.Count > 0, 10);

            await extractor.Close();
            await tester.Client.Run(tester.Source.Token, 0);
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
            var ids = tester.Server.Ids.Base;
            var nodes = new List<UANode>
            {
                new UANode(new NodeId("object1"), "object1", root, NodeClass.Object),
                new UANode(new NodeId("object2"), "object2", root, NodeClass.Object)
            };
            var variables = new List<UAVariable>
            {
                new UAVariable(new NodeId("var1"), "var1", root),
                new UAVariable(new NodeId("var2"), "var2", root)
            };

            variables[0].VariableAttributes.ReadHistory = true;
            variables[1].VariableAttributes.ReadHistory = false;

            var refManager = extractor.ReferenceTypeManager;

            var references = new List<UAReference>
            {
                new UAReference(
                    ReferenceTypeIds.Organizes,
                    true,
                    new NodeId("object1"),
                    new NodeId("var1"),
                    false,
                    true,
                    false,
                    refManager)
            };

            var input = new PusherInput(nodes, variables, references, null);

            await extractor.PushNodes(input, pusher, true);

            Assert.Equal(pushedObjects, pusher.PushedNodes.Count);
            Assert.Equal(pushedVariables, pusher.PushedVariables.Count);
            Assert.Equal(pushedRefs, pusher.PushedReferences.Count);
            if (failAt == 0)
            {
                Assert.Null(pusher.PendingNodes);
            }
            else
            {
                Assert.Equal(failedNodes, pusher.PendingNodes.Objects.Count() + pusher.PendingNodes.Variables.Count());
                Assert.Equal(failedRefs, pusher.PendingNodes.References.Count());
            }
            

            if (failAt == 0)
            {
                Assert.True(pusher.Initialized);
            }
            else
            {
                Assert.False(pusher.Initialized);
            }
        }

        [Fact]
        public void TestGetExtraMetadata()
        {
            using var extractor = tester.BuildExtractor();

            tester.Config.Extraction.DataTypes.DataTypeMetadata = true;
            var variable = new UAVariable(new NodeId("test"), "test", NodeId.Null);
            variable.VariableAttributes.DataType = new UADataType(DataTypeIds.Double);
            var fields = variable.GetExtraMetadata(tester.Config.Extraction, extractor.DataTypeManager, extractor.StringConverter);
            Assert.Single(fields);
            Assert.Equal("Double", fields["dataType"]);

            tester.Config.Extraction.NodeTypes.Metadata = true;
            var node = new UANode(new NodeId("test"), "test", NodeId.Null, NodeClass.Object);
            node.Attributes.NodeType = new UANodeType(new NodeId("type"), false) { Name = "SomeType" };
            fields = node.GetExtraMetadata(tester.Config.Extraction, extractor.DataTypeManager, extractor.StringConverter);
            Assert.Single(fields);
            Assert.Equal("SomeType", fields["TypeDefinition"]);

            tester.Config.Extraction.DataTypes.DataTypeMetadata = false;
            tester.Config.Extraction.NodeTypes.Metadata = false;

            tester.Config.Extraction.NodeTypes.AsNodes = true;
            var type = new UAVariable(new NodeId("test"), "test", NodeId.Null, NodeClass.VariableType);
            type.VariableAttributes.DataType = new UADataType(DataTypeIds.String);
            type.SetDataPoint(new Variant("value"));
            fields = type.GetExtraMetadata(tester.Config.Extraction, extractor.DataTypeManager, extractor.StringConverter);
            Assert.Single(fields);
            Assert.Equal("value", fields["Value"]);
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
        }

        [Fact]
        public async Task TestServerConfigLimit()
        {
            var log = tester.Provider.GetRequiredService<ILogger<ServerInfoHelper>>();
            var helper = new ServerInfoHelper(log, tester.Client);
            tester.Config.History.Throttling.MaxNodeParallelism = 100;
            tester.Config.Source.BrowseThrottling.MaxNodeParallelism = 10000;
            await helper.LimitConfigValues(tester.Config, tester.Source.Token);

            Assert.Equal(100, tester.Config.History.Throttling.MaxNodeParallelism);
            Assert.Equal(1000, tester.Config.Source.BrowseThrottling.MaxNodeParallelism);

            tester.Config.History.Throttling.MaxNodeParallelism = 0;
            tester.Config.Source.BrowseThrottling.MaxNodeParallelism = 0;
            tester.Config.Source.BrowseNodesChunk = 100;

            await helper.LimitConfigValues(tester.Config, tester.Source.Token);

            Assert.Equal(1000, tester.Config.History.Throttling.MaxNodeParallelism);
            Assert.Equal(1000, tester.Config.Source.BrowseThrottling.MaxNodeParallelism);
            Assert.Equal(100, tester.Config.Source.BrowseNodesChunk);
        }

    }
}

using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
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
            tester.Client.TypeManager.Reset();
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

            Assert.Contains(pusher.PushedNodes.Values, node => node.Name == "DeepObject 4, 25");
            Assert.Contains(pusher.PushedVariables.Values, node => node.Name == "SubVariable 1234");
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
            Assert.True(pusher.PushedNodes.Count != 0);
            pusher.PushedNodes.Clear();
            await extractor.OnServerReconnect(tester.Client);

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
            var nodes = new List<BaseUANode>
            {
                new UAObject(new NodeId("object1", 0), "object1", null, null, root, null),
                new UAObject(new NodeId("object2", 0), "object2", null, null, root, null)
            };
            var variables = new List<UAVariable>
            {
                new UAVariable(new NodeId("var1", 0), "var1", null, null, root, null),
                new UAVariable(new NodeId("var2", 0), "var2", null, null, root, null)
            };

            variables[0].FullAttributes.ShouldReadHistoryOverride = true;
            variables[1].FullAttributes.ShouldReadHistoryOverride = false;

            var references = new List<UAReference>
            {
                new UAReference(
                    extractor.TypeManager.GetReferenceType(ReferenceTypeIds.Organizes),
                    true,
                    nodes[0],
                    variables[0])
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
            var variable = new UAVariable(new NodeId("test", 0), "test", null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            var fields = variable.GetExtraMetadata(tester.Config, extractor.Context, extractor.StringConverter);
            Assert.Single(fields);
            Assert.Equal("Double", fields["dataType"]);

            tester.Config.Extraction.NodeTypes.Metadata = true;
            var node = new UAObject(new NodeId("test", 0), "test", null, null, NodeId.Null, new UAObjectType(new NodeId("type", 0)));
            node.FullAttributes.TypeDefinition.Attributes.DisplayName = "SomeType";
            fields = node.GetExtraMetadata(tester.Config, extractor.Context, extractor.StringConverter);
            Assert.Single(fields);
            Assert.Equal("SomeType", fields["TypeDefinition"]);

            tester.Config.Extraction.DataTypes.DataTypeMetadata = false;
            tester.Config.Extraction.NodeTypes.Metadata = false;

            tester.Config.Extraction.NodeTypes.AsNodes = true;
            var type = new UAVariableType(new NodeId("test", 0), "test", null, null, NodeId.Null);
            type.FullAttributes.DataType = new UADataType(DataTypeIds.String);
            type.FullAttributes.Value = new Variant("value");
            fields = type.GetExtraMetadata(tester.Config, extractor.Context, extractor.StringConverter);
            Assert.Single(fields);
            Assert.Equal("value", fields["Value"]);
        }
        [Fact]
        public void TestNodeMapping()
        {
            tester.Config.Extraction.NodeMap = new Dictionary<string, ProtoNodeId>
            {
                { "Test1", new NodeId("test", 0).ToProtoNodeId(tester.Client) },
                { "Test2", new NodeId("test2", 2).ToProtoNodeId(tester.Client) }
            };
            var ctx = new SessionContext(tester.Config, tester.Log);
            ctx.UpdateFromSession(tester.Client.SessionManager.Session);

            Assert.Equal("Test1", ctx.GetUniqueId(new NodeId("test", 0)));
            Assert.Equal("Test2", ctx.GetUniqueId(new NodeId("test2", 2)));
            Assert.Equal("Test1[0]", ctx.GetUniqueId(new NodeId("test", 0), 0));
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

        [Fact]
        public async Task TestNoServerExtractor()
        {
            tester.Config.Source.EndpointUrl = null;
            tester.Config.Source.NodeSetSource = new NodeSetSourceConfig
            {
                NodeSets = new[]
                {
                    new NodeSetConfig
                    {
                        Url = new Uri("https://files.opcfoundation.org/schemas/UA/1.04/Opc.Ua.NodeSet2.xml")
                    },
                    new NodeSetConfig
                    {
                        FileName = "TestServer.NodeSet2.xml"
                    }
                },
                Instance = true,
                Types = true
            };
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 10;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.DataTypes.EstimateArraySizes = true;
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var client = new UAClient(tester.Provider, tester.Config);
            using var extractor = new UAExtractor(tester.Config, tester.Provider, new[] { pusher }, client, null);

            extractor.InitExternal(tester.Source.Token);
            await extractor.RunExtractor(true);

            foreach (var node in pusher.PushedNodes)
            {
                tester.Log.LogDebug("{Name}", node.Value.Name);
            }

            Assert.Equal(15, pusher.PushedNodes.Count);
            Assert.Equal(38, pusher.PushedVariables.Count);
            Assert.Equal(10, pusher.PushedReferences.Count);
        }
    }
}

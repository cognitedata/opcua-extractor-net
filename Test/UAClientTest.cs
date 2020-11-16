using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Server;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    public sealed class UAClientTestFixture : IDisposable
    {
        public UAClient Client { get; }
        public ServerController Server { get; }
        public FullConfig Config { get; }
        public CancellationTokenSource Source { get; }
        public IServiceProvider Provider { get; }
        public UAClientTestFixture()
        {
            Server = new ServerController(new[] {
                PredefinedSetup.Full, PredefinedSetup.Auditing, PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, 62000);
            Server.Start().Wait();

            var services = new ServiceCollection();
            Config = services.AddConfig<FullConfig>("config.test.yml", 1);
            Config.Source.EndpointUrl = $"opc.tcp://localhost:62000";
            services.AddLogging();
            LoggingUtils.Configure(Config.Logger);
            Provider = services.BuildServiceProvider();

            Client = new UAClient(Config);
            Source = new CancellationTokenSource();
            Client.Run(Source.Token).Wait();
        }
        public static (Action<ReferenceDescription, NodeId>, IDictionary<NodeId, ReferenceDescriptionCollection>) GetCallback()
        {
            var toWrite = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            return ((desc, parentId) =>
            {
                if (parentId == null || parentId == NodeId.Null) parentId = ObjectIds.ObjectsFolder;
                if (!toWrite.TryGetValue(parentId, out var children))
                {
                    toWrite[parentId] = children = new ReferenceDescriptionCollection();
                }
                children.Add(desc);
            }, toWrite);
        }

        public void Dispose()
        {
            Source.Cancel();
            Client.Close();
            Source.Dispose();
            Server.Stop();
        }
    }
    public class UAClientTest : MakeConsoleWork, IClassFixture<UAClientTestFixture>
    {
        private UAClientTestFixture tester;
        public UAClientTest(ITestOutputHelper output, UAClientTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        #region session
        [Fact]
        public void TestClientConnected()
        {
            Assert.True(tester.Client.Started);
            // base, server uri, node manager namespace, diagnostics
            Assert.Equal(4, tester.Client.GetNamespaceTable().Count);
            Assert.Equal("opc.tcp://test.localhost", tester.Client.GetNamespaceTable().GetString(2));
        }
        [Fact]
        public async Task TestConnectionFailure()
        {
            tester.Client.Close();
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62009";
            try
            {
                var exc = await Assert.ThrowsAsync<SilentServiceException>(() => tester.Client.Run(tester.Source.Token));
                Assert.Equal(StatusCodes.BadNotConnected, exc.StatusCode);
                Assert.Equal(ExtractorUtils.SourceOp.SelectEndpoint, exc.Operation);
            }
            finally
            {
                tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62000";
                await tester.Client.Run(tester.Source.Token);
            }
        }
        [Fact]
        public async Task TestConfigFailure()
        {
            tester.Client.Close();
            tester.Config.Source.ConfigRoot = "wrong";
            try
            {
                var exc = await Assert.ThrowsAsync<ExtractorFailureException>(() => tester.Client.Run(tester.Source.Token));
            }
            finally
            {
                tester.Config.Source.ConfigRoot = "config";
                await tester.Client.Run(tester.Source.Token);
            }
        }
        [Fact]
        public async Task TestReconnect()
        {
            Assert.True(RuntimeInformation.IsOSPlatform(OSPlatform.Linux), "This test only runs on Linux");
            tester.Client.Close();
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62001";
            tester.Config.Source.KeepAliveInterval = 1000;

            try
            {
                using var process = CommonTestUtils.GetProxyProcess(62001, 62000);
                await Task.Delay(500);
                process.Start();
                await tester.Client.Run(tester.Source.Token);
                Assert.True(CommonTestUtils.TestMetricValue("opcua_connected", 1));
                CommonTestUtils.StopProxyProcess();
                await CommonTestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_connected", 0), 10,
                    "Expected client to disconnect");
                process.Start();
                await CommonTestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_connected", 1), 10,
                    "Expected client to reconnect");
            }
            finally
            {
                tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62000";
                tester.Client.Close();
                tester.Config.Source.KeepAliveInterval = 5000;
                await tester.Client.Run(tester.Source.Token);
                CommonTestUtils.StopProxyProcess();
            }
        }


        #endregion

        #region browse
        [Fact]
        public void TestGetServerNode()
        {
            var server = tester.Client.GetServerNode(tester.Source.Token);
            Assert.Equal(ObjectIds.Server, server.Id);
            Assert.Equal(NodeId.Null, server.ParentId);
            Assert.Equal("Server", server.DisplayName);
        }
        [Fact]
        public void TestGetRoots()
        {
            CommonTestUtils.ResetMetricValue("opcua_browse_operations");
            var childrenDict = tester.Client.GetNodeChildren(new[] { ObjectIds.ObjectsFolder }, ReferenceTypeIds.HierarchicalReferences,
                0, tester.Source.Token);
            var children = childrenDict[ObjectIds.ObjectsFolder];
            Assert.Equal(6, children.Count);

            var nodes = children.ToDictionary(child => child.DisplayName.Text);
            var fullRoot = nodes["FullRoot"];
            Assert.Equal(tester.Server.Ids.Full.Root, fullRoot.NodeId);
            Assert.Equal(ReferenceTypeIds.Organizes, fullRoot.ReferenceTypeId);

            Assert.True(nodes.ContainsKey("EventRoot"));
            Assert.True(nodes.ContainsKey("Server"));
            Assert.True(nodes.ContainsKey("CustomRoot"));
            Assert.True(nodes.ContainsKey("GrowingRoot"));
            Assert.True(nodes.ContainsKey("WrongRoot"));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 1));
        }
        [Fact]
        public void TestGetNodeChildrenChunking()
        {
            CommonTestUtils.ResetMetricValue("opcua_browse_operations");
            tester.Config.Source.BrowseChunk = 100;
            var nums = new int[2000];
            try
            {
                var childrenDict = tester.Client.GetNodeChildren(new[] { tester.Server.Ids.Full.WideRoot }, ReferenceTypeIds.HierarchicalReferences,
                    0, tester.Source.Token);
                var children = childrenDict[tester.Server.Ids.Full.WideRoot];
                Assert.Equal(2000, children.Count);
                var suffixNums = children.Select(child => int.Parse(Regex.Match(child.DisplayName.Text, @"\d+$").Value, CultureInfo.InvariantCulture));
                foreach (var num in suffixNums)
                {
                    nums[num]++;
                }
            }
            finally
            {
                tester.Config.Source.BrowseChunk = 1000;
            }
            Assert.All(nums, cnt => Assert.Equal(1, cnt));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 20));
        }
        [Fact]
        public async Task TestBrowseNode()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            await tester.Client.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token);
            Assert.Equal(147, nodes.Count);
            Assert.Equal(151, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 31));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 31));
        }
        [Fact]
        public async Task TestBrowseNodesChunk()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            tester.Config.Source.BrowseNodesChunk = 2;
            try
            {
                await tester.Client.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token);
            }
            finally
            {
                tester.Config.Source.BrowseNodesChunk = 100;
            }
            Assert.Equal(147, nodes.Count);
            Assert.Equal(151, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 91));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 31));
        }
        [Fact]
        public async Task TestBrowseIgnoreName()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Config.Extraction.IgnoreName = new[] { "WideRoot" };
            try
            {
                await tester.Client.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            }
            finally
            {
                tester.Config.Extraction.IgnoreName = null;
            }
            Assert.False(nodes.ContainsKey(tester.Server.Ids.Full.WideRoot));
            Assert.Equal(152, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 32));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 32));
        }
        [Fact]
        public async Task TestBrowseIgnorePrefix()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Config.Extraction.IgnoreNamePrefix = new[] { "Sub", "Deep" };
            try
            {
                await tester.Client.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            }
            finally
            {
                tester.Config.Extraction.IgnoreNamePrefix = null;
            }
            Assert.Equal(2, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 3));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 2));
        }
        [Fact]
        public async Task TestIgnoreVisited()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Config.Extraction.IgnoreName = new[] { "WideRoot" };
            try
            {
                await tester.Client.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            }
            finally
            {
                tester.Config.Extraction.IgnoreName = null;
            }
            Assert.False(nodes.ContainsKey(tester.Server.Ids.Full.WideRoot));
            Assert.Equal(152, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 32));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 32));

            nodes.Clear();
            await tester.Client.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            Assert.False(nodes.ContainsKey(tester.Server.Ids.Full.DeepRoot));
            Assert.True(nodes.ContainsKey(tester.Server.Ids.Full.WideRoot));
            Assert.Equal(2001, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 32 + 32 + 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 32));
        }
        #endregion


    }
}

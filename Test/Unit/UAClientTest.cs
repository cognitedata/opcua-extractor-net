using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using Server;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class UAClientTestFixture : IAsyncLifetime
    {
        public UAClient Client { get; private set; }
        public ServerController Server { get; private set; }
        public FullConfig Config { get; }
        public CancellationTokenSource Source { get; private set; }
        public ServiceProvider Provider { get; }
        public UAClientTestFixture()
        {
            var services = new ServiceCollection();
            Config = services.AddConfig<FullConfig>("config.test.yml", 1);
            Config.Source.EndpointUrl = $"opc.tcp://localhost:62000";
            services.AddLogger();
            LoggingUtils.Configure(Config.Logger);
            Provider = services.BuildServiceProvider();

            Server = new ServerController(new[] {
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, 62000)
            {
                ConfigRoot = "Server.Test.UaClient"
            };

            if (Directory.Exists("./uaclienttestcerts/pki/"))
            {
                Directory.Delete("./uaclienttestcerts/pki/", true);
            }
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

        public async Task InitializeAsync()
        {
            await Server.Start();
            Client = new UAClient(Provider, Config);
            Source = new CancellationTokenSource();
            await Client.Run(Source.Token);
        }

        public async Task DisposeAsync()
        {
            Source.Cancel();
            await Client.Close(CancellationToken.None);
            Source.Dispose();
            Server.Stop();
            await Provider.DisposeAsync();
        }
    }
    public class UAClientTest : MakeConsoleWork, IClassFixture<UAClientTestFixture>
    {
        private readonly UAClientTestFixture tester;
        public UAClientTest(ITestOutputHelper output, UAClientTestFixture tester) : base(output)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
        }
        #region session
        [Fact]
        public void TestClientConnected()
        {
            Assert.True(tester.Client.Started);
            // base, server uri, node manager namespace, diagnostics
            Assert.Equal(4, tester.Client.NamespaceTable.Count);
            Assert.Equal("opc.tcp://test.localhost", tester.Client.NamespaceTable.GetString(2));
        }
        [Fact]
        public async Task TestConnectionFailure()
        {
            string oldEP = tester.Config.Source.EndpointUrl;
            await tester.Client.Close(tester.Source.Token);
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
            await tester.Client.Close(tester.Source.Token);
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
            await tester.Client.Close(tester.Source.Token);
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62001";
            tester.Config.Source.KeepAliveInterval = 1000;

            bool connected = true;

            tester.Client.OnServerDisconnect += (client, args) =>
            {
                connected = false;
            };
            tester.Client.OnServerReconnect += (client, args) =>
            {
                connected = true;
            };

            try
            {
                using var process = CommonTestUtils.GetProxyProcess(62001, 62000);
                process.Start();
                await Task.Delay(500);
                await tester.Client.Run(tester.Source.Token);
                Assert.True(CommonTestUtils.TestMetricValue("opcua_connected", 1));
                CommonTestUtils.StopProxyProcess();
                await CommonTestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_connected", 0) && !connected, 20,
                    "Expected client to disconnect");
                Assert.False(connected);
                process.Start();
                await CommonTestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_connected", 1) && connected, 20,
                    "Expected client to reconnect");
                Assert.True(connected);
            }
            finally
            {
                tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62000";
                await tester.Client.Close(tester.Source.Token);
                tester.Config.Source.KeepAliveInterval = 10000;
                await tester.Client.Run(tester.Source.Token);
                CommonTestUtils.StopProxyProcess();
            }
        }
        [Fact]
        public async Task TestCertificatePath()
        {
            if (File.Exists("./certificates-test/"))
            {
                Directory.Delete("./certificates-test/", true);
            }
            await tester.Client.Close(tester.Source.Token);
            try
            {
                Environment.SetEnvironmentVariable("OPCUA_CERTIFICATE_DIR", "certificates-test");
                await tester.Client.Run(tester.Source.Token);
                var dir = new DirectoryInfo("./certificates-test/pki/trusted/certs/");
                Assert.Single(dir.GetFiles());
            }
            finally
            {
                await tester.Client.Close(tester.Source.Token);
                Environment.SetEnvironmentVariable("OPCUA_CERTIFICATE_DIR", null);
                Directory.Delete("./certificates-test/", true);
                await tester.Client.Run(tester.Source.Token);
            }
        }
        [Fact]
        public async Task TestCertificateValidation()
        {
            // Slightly hacky test. Use the server application certificate, and validate it using the built-in systems in
            // the SDK.

            await tester.Client.Close(tester.Source.Token);
            tester.Server.Server.AllowAnonymous = false;
            try
            {
                var certCfg = new X509CertConfig();

                var serverCertName = new DirectoryInfo("./uaclienttestcerts/pki/own/private/").GetFiles().First().FullName;
                certCfg.FileName = serverCertName;
                tester.Config.Source.X509Certificate = certCfg;

                tester.Server.Server.SetValidator(true);

                await Assert.ThrowsAsync<SilentServiceException>(async () => await tester.Client.Run(tester.Source.Token));

                tester.Server.Server.SetValidator(false);

                await tester.Client.Run(tester.Source.Token);
            }
            finally
            {
                tester.Server.Server.AllowAnonymous = true;
                tester.Config.Source.X509Certificate = null;
                tester.Server.Server.SetValidator(false);
                await tester.Client.Run(tester.Source.Token);
            }
        }

        [Fact]
        public async Task TestPasswordAuthentication()
        {
            await tester.Client.Close(tester.Source.Token);
            tester.Server.Server.AllowAnonymous = false;

            try
            {
                tester.Config.Source.Username = "testuser";
                tester.Config.Source.Password = "wrongpassword";

                await Assert.ThrowsAsync<SilentServiceException>(async () => await tester.Client.Run(tester.Source.Token));

                tester.Config.Source.Password = "testpassword";

                await tester.Client.Run(tester.Source.Token);
            }
            finally
            {
                tester.Server.Server.AllowAnonymous = true;
                tester.Config.Source.Username = null;
                tester.Config.Source.Password = null;
                await tester.Client.Run(tester.Source.Token);
            }
        }
        [Fact]
        public async Task TestReverseConnect()
        {
            await tester.Client.Close(tester.Source.Token);
            tester.Server.Server.AddReverseConnection(new Uri("opc.tcp://localhost:61000"));
            tester.Config.Source.ReverseConnectUrl = "opc.tcp://localhost:61000";
            try
            {
                await tester.Client.Run(tester.Source.Token);
                Assert.True(tester.Client.Started);
                // Just check that we are able to read, indicating an established connection
                await tester.Client.ReadRawValues(new[] { VariableIds.Server_ServerStatus }, tester.Source.Token);
            }
            finally
            {
                tester.Server.Server.RemoveReverseConnection(new Uri("opc.tcp://localhost:61000"));
                tester.Config.Source.ReverseConnectUrl = null;
                await tester.Client.Run(tester.Source.Token);
            }
        }

        #endregion

        #region browse
        [Fact]
        public async Task TestGetServerNode()
        {
            var server = await tester.Client.GetServerNode(tester.Source.Token);
            Assert.Equal(ObjectIds.Server, server.Id);
            Assert.Equal(NodeId.Null, server.ParentId);
            Assert.Equal("Server", server.DisplayName);
        }
        [Fact]
        public async Task TestGetRoots()
        {
            CommonTestUtils.ResetMetricValue("opcua_browse_operations");
            var node = new BrowseNode(ObjectIds.ObjectsFolder);
            await tester.Client.GetReferences(new BrowseParams
            {
                Nodes = new Dictionary<NodeId, BrowseNode> { { ObjectIds.ObjectsFolder, node } }
            }, true, tester.Source.Token);
            var children = node.Result.References;
            Assert.Equal(8, children.Count);

            var nodes = children.ToDictionary(child => child.DisplayName.Text);
            var fullRoot = nodes["FullRoot"];
            Assert.Equal(tester.Server.Ids.Full.Root, fullRoot.NodeId);
            Assert.Equal(ReferenceTypeIds.Organizes, fullRoot.ReferenceTypeId);

            Assert.True(nodes.ContainsKey("EventRoot"));
            Assert.True(nodes.ContainsKey("Server"));
            Assert.True(nodes.ContainsKey("CustomRoot"));
            Assert.True(nodes.ContainsKey("GrowingRoot"));
            Assert.True(nodes.ContainsKey("WrongRoot"));
            Assert.True(nodes.ContainsKey("BaseRoot"));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 1));
            tester.Client.Browser.ResetVisitedNodes();
        }
        [Fact]
        public async Task TestGetReferencesChunking()
        {
            CommonTestUtils.ResetMetricValue("opcua_browse_operations");
            var nums = new int[2000];

            var node = new BrowseNode(tester.Server.Ids.Full.WideRoot);
            await tester.Client.GetReferences(new BrowseParams
            {
                Nodes = new Dictionary<NodeId, BrowseNode> { { tester.Server.Ids.Full.WideRoot, node } },
                MaxPerNode = 100
            }, true, tester.Source.Token);

            var children = node.Result.References;
            Assert.Equal(2000, children.Count);
            var suffixNums = children.Select(child =>
                int.Parse(Regex.Match(child.DisplayName.Text, @"\d+$").Value, CultureInfo.InvariantCulture));
            foreach (var num in suffixNums)
            {
                nums[num]++;
            }

            Assert.All(nums, cnt => Assert.Equal(1, cnt));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 20));
        }
        [Fact]
        public async Task TestBrowseNode()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token);
            Assert.Equal(147, nodes.Count);
            Assert.Equal(151, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 31));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 31));
            tester.Client.Browser.ResetVisitedNodes();
        }
        [Fact]
        public async Task TestBrowseNodesChunk()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            tester.Config.Source.BrowseNodesChunk = 2;
            try
            {
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token);
            }
            finally
            {
                tester.Client.Browser.ResetVisitedNodes();
                tester.Config.Source.BrowseNodesChunk = 100;
            }
            Assert.Equal(147, nodes.Count);
            Assert.Equal(151, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            double reads = CommonTestUtils.GetMetricValue("opcua_browse_operations");

            // Best case, it takes 91 reads: 1 read at level 0, 3 reads for each of the 30 remaining.
            // Timing might cause nodes to be read in a sligthly different order, so we might read 2 more times.
            // In practice this slight variance is irrelevant.
            Assert.True(reads >= 90 && reads <= 93);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 31));
        }
        [Theory]
        [InlineData(-1, 150, 31)]
        [InlineData(0, 5, 1)]
        [InlineData(1, 10, 2)]
        [InlineData(15, 80, 16)]
        public async Task TestBrowseDepth(int depth, int numNodes, int numBrowse)
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            await tester.Client.Browser.BrowseDirectory(new[] { tester.Server.Ids.Full.DeepRoot }, callback, tester.Source.Token,
                null, 3, true, true, depth);

            Assert.Equal(numNodes, nodes.Sum(node => node.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", numBrowse));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", depth < 0 ? 31 : depth + 2));
        }
        [Fact]
        public async Task TestBrowseThrottling()
        {
            // When running alone limiting node parallelism should have the same effect as chunking.
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            tester.Config.Source.BrowseThrottling.MaxNodeParallelism = 2;
            tester.Config.Source.BrowseThrottling.MaxParallelism = 1;
            var log = tester.Provider.GetRequiredService<ILogger<Cognite.OpcUa.Browser>>();
            using var browser = new Cognite.OpcUa.Browser(log, tester.Client, tester.Config);
            try
            {
                await browser.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token);
            }
            finally
            {
                tester.Config.Source.BrowseThrottling.MaxParallelism = 0;
                tester.Config.Source.BrowseThrottling.MaxNodeParallelism = 0;
            }
            Assert.Equal(147, nodes.Count);
            Assert.Equal(151, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 76));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 31));
        }
        [Fact]
        public async Task TestBrowseIgnoreName()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Client.Browser.IgnoreFilters = new List<NodeFilter>
            {
                new NodeFilter(new RawNodeFilter
                {
                    Name = "WideRoot"
                })
            };
            try
            {
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            }
            finally
            {
                tester.Client.Browser.ResetVisitedNodes();
                tester.Client.Browser.IgnoreFilters = null;
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
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Client.Browser.IgnoreFilters = new List<NodeFilter>
            {
                new NodeFilter(new RawNodeFilter
                {
                    Name = "^Sub|^Deep"
                })
            };

            try
            {
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            }
            finally
            {
                tester.Client.Browser.ResetVisitedNodes();
                tester.Client.Browser.IgnoreFilters = null;
            }
            Assert.Equal(2, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 3));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 2));
        }
        [Fact]
        public async Task TestIgnoreVisited()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Client.Browser.IgnoreFilters = new List<NodeFilter>
            {
                new NodeFilter(new RawNodeFilter
                {
                    Name = "WideRoot"
                })
            };

            try
            {
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            }
            finally
            {
                tester.Client.Browser.IgnoreFilters = null;
            }
            Assert.False(nodes.ContainsKey(tester.Server.Ids.Full.WideRoot));
            Assert.Equal(152, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 32));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 32));

            nodes.Clear();
            await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            Assert.False(nodes.ContainsKey(tester.Server.Ids.Full.DeepRoot));
            Assert.True(nodes.ContainsKey(tester.Server.Ids.Full.WideRoot));
            Assert.Equal(2001, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 32 + 32 + 3));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 32));
            tester.Client.Browser.ResetVisitedNodes();
        }
        [Fact]
        public async Task TestBrowseTypes()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Config.Extraction.NodeTypes.AsNodes = true;

            try
            {
                await tester.Client.Browser.BrowseNodeHierarchy(ObjectIds.TypesFolder, callback, tester.Source.Token);
            }
            finally
            {
                tester.Config.Extraction.NodeTypes.AsNodes = false;
            }
            var distinctNodes = nodes.SelectMany(kvp => kvp.Value).GroupBy(rd => rd.NodeId);
            foreach (var node in distinctNodes.Where(group => group.Count() > 1))
            {
                Console.WriteLine("Duplicate node: " + node.Key);
            }

            Assert.Equal(distinctNodes.Count(), nodes.Sum(kvp => kvp.Value.Count));
            Assert.Equal(2377, nodes.Sum(kvp => kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 10));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 11));
        }
        [Fact]
        public async Task TestAbortBrowse()
        {
            var nodes = new[]
            {
                new BrowseNode(tester.Server.Ids.Full.WideRoot),
                new BrowseNode(tester.Server.Ids.Custom.Root),
            };
            var opt = new BrowseParams()
            {
                Nodes = nodes.ToDictionary(node => node.Id),
                BrowseDirection = BrowseDirection.Forward,
                NodeClassMask = (uint)(NodeClass.Object | NodeClass.Variable),
                IncludeSubTypes = true,
                ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                MaxPerNode = 50
            };
            await tester.Client.GetReferences(opt, false, tester.Source.Token);
            Assert.Null(nodes[1].ContinuationPoint);
            Assert.NotNull(nodes[0].ContinuationPoint);
            await tester.Client.AbortBrowse(new[] { nodes[1] });
            await tester.Client.AbortBrowse(nodes);
            Assert.Null(nodes[0].ContinuationPoint);
        }
        [Fact]
        public async Task TestFailBrowse()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            tester.Client.Browser.ResetVisitedNodes();
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Server.Issues.RemainingBrowseCount = 5;
            var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token)
            );

            var root = ex.InnerException;
            Assert.IsType<FatalException>(root);

            // Root node is browsed once
            Assert.Equal(21, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 5));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 5));
        }

        #endregion

        #region nodedata
        [Fact]
        public async Task TestReadNodeData()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            var nodes = new UANode[]
            {
                new UANode(tester.Server.Ids.Full.Root, "FullRoot", ObjectIds.ObjectsFolder, NodeClass.Object),
                new UANode(tester.Server.Ids.Event.Obj1, "Object 1", tester.Server.Ids.Event.Root, NodeClass.Object),
                new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", ObjectIds.ObjectsFolder, NodeClass.Object),
                new UAVariable(tester.Server.Ids.Custom.StringyVar, "StringyVar", tester.Server.Ids.Custom.Root),
                new UAVariable(tester.Server.Ids.Custom.Array, "Array", tester.Server.Ids.Custom.Root),
                new UAVariable(tester.Server.Ids.Custom.ObjProp, "ObjProp", tester.Server.Ids.Custom.Obj2)
            };
            nodes[5].Attributes.IsProperty = true;
            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = -1;
            tester.Config.Events.Enabled = true;
            try
            {
                await tester.Client.ReadNodeData(nodes, tester.Source.Token);
            }
            finally
            {
                tester.Config.History.Enabled = false;
                tester.Config.History.Data = false;
                tester.Config.Extraction.DataTypes.MaxArraySize = 0;
                tester.Config.Events.Enabled = false;
            }

            Assert.Equal("FullRoot Description", nodes[0].Description);
            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, nodes[1].EventNotifier);
            Assert.Equal(tester.Server.Ids.Custom.StringyType, (nodes[3] as UAVariable).DataType.Raw);
            Assert.Equal(4, (nodes[4] as UAVariable).ArrayDimensions[0]);
            Assert.Single((nodes[4] as UAVariable).ArrayDimensions);
            Assert.True((nodes[4] as UAVariable).ReadHistory);
            Assert.Null((nodes[5] as UAVariable).ArrayDimensions);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 1));
        }
        [Fact]
        public async Task TestReadNodeDataConfig()
        {
            UANode[] GetNodes()
            {
                var nds = new UANode[]
                {
                    new UANode(tester.Server.Ids.Full.Root, "FullRoot", ObjectIds.ObjectsFolder, NodeClass.Object),
                    new UANode(tester.Server.Ids.Event.Obj1, "Object 1", tester.Server.Ids.Event.Root, NodeClass.Object),
                    new UAVariable(tester.Server.Ids.Custom.StringyVar, "StringyVar", tester.Server.Ids.Custom.Root),
                    new UAVariable(tester.Server.Ids.Custom.Array, "Array", tester.Server.Ids.Custom.Root),
                    new UAVariable(tester.Server.Ids.Wrong.TooLargeProp, "TooLargeProp", tester.Server.Ids.Custom.Obj2),
                    new UAVariable(VariableTypeIds.BaseDataVariableType, "BaseDataVariableType", NodeId.Null, NodeClass.VariableType),
                    new UANode(ObjectTypeIds.BaseObjectType, "BaseObjectType", NodeId.Null, NodeClass.ObjectType)
                };
                nds[4].Attributes.IsProperty = true;
                return nds;
            }
            var nodes = GetNodes();

            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = -1;
            tester.Config.Events.Enabled = true;

            // Read everything
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal("FullRoot Description", nodes[0].Description);
            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, nodes[1].EventNotifier);
            Assert.True(nodes[1].ShouldSubscribeEvents);
            Assert.Equal(tester.Server.Ids.Custom.StringyType, (nodes[2] as UAVariable).DataType.Raw);
            Assert.Equal(4, (nodes[3] as UAVariable).ArrayDimensions[0]);
            Assert.Single((nodes[3] as UAVariable).ArrayDimensions);
            Assert.True((nodes[3] as UAVariable).ReadHistory);
            Assert.Equal(AccessLevels.CurrentRead | AccessLevels.HistoryRead, (nodes[3] as UAVariable).AccessLevel);
            Assert.True((nodes[3] as UAVariable).VariableAttributes.Historizing);
            Assert.NotNull((nodes[4] as UAVariable).ArrayDimensions);
            Assert.NotNull((nodes[3] as UAVariable).ArrayDimensions);
            Assert.Equal(DataTypeIds.BaseDataType, (nodes[5] as UAVariable).DataType.Raw);
            Assert.Equal(0, (nodes[5] as UAVariable).AccessLevel);

            // Disable history
            tester.Config.History.Enabled = false;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, nodes[1].EventNotifier);
            Assert.True(nodes[1].ShouldSubscribeEvents);
            Assert.False((nodes[3] as UAVariable).ReadHistory);
            Assert.False((nodes[3] as UAVariable).VariableAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).ShouldSubscribeData);

            // Disable event discovery
            tester.Config.History.Enabled = true;
            tester.Config.Events.DiscoverEmitters = false;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(0, nodes[1].EventNotifier);
            Assert.False(nodes[1].ShouldSubscribeEvents);
            Assert.True((nodes[3] as UAVariable).ReadHistory);
            Assert.True((nodes[3] as UAVariable).VariableAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).ShouldSubscribeData);

            // Disable just data history
            tester.Config.Events.DiscoverEmitters = true;
            tester.Config.History.Data = false;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, nodes[1].EventNotifier);
            Assert.True(nodes[1].ShouldSubscribeEvents);
            Assert.False((nodes[3] as UAVariable).ReadHistory);
            Assert.True((nodes[3] as UAVariable).VariableAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).ShouldSubscribeData);


            // Enable require historizing
            tester.Config.History.Data = true;
            tester.Config.History.RequireHistorizing = true;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, nodes[1].EventNotifier);
            Assert.True(nodes[1].ShouldSubscribeEvents);
            Assert.True((nodes[3] as UAVariable).ReadHistory);
            Assert.True((nodes[3] as UAVariable).VariableAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).ShouldSubscribeData);

            // Enable ignore access level
            tester.Config.History.RequireHistorizing = false;
            tester.Config.Subscriptions.IgnoreAccessLevel = true;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, nodes[1].EventNotifier);
            Assert.True(nodes[1].ShouldSubscribeEvents);
            Assert.True((nodes[3] as UAVariable).ReadHistory);
            Assert.True((nodes[3] as UAVariable).VariableAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).ShouldSubscribeData);
            Assert.Equal(0, (nodes[3] as UAVariable).AccessLevel);

            // Disable reading array dimensions
            tester.Config.Extraction.DataTypes.MaxArraySize = 0;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Null((nodes[3] as UAVariable).ArrayDimensions);
            Assert.NotNull((nodes[4] as UAVariable).ArrayDimensions);
        }


        [Fact]
        public async Task TestReadNodeDataChunk()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            int start = (int)(uint)tester.Server.Ids.Full.WideRoot.Identifier;
            var nodes = Enumerable.Range(start + 1, 2000)
                .Select(idf => new NodeId((uint)idf, 2))
                .Select(id => new UAVariable(id, "subnode", tester.Server.Ids.Full.WideRoot))
                .ToList();
            tester.Config.Source.AttributesChunk = 100;
            tester.Config.History.Enabled = true;
            try
            {
                await tester.Client.ReadNodeData(nodes, tester.Source.Token);
            }
            finally
            {
                tester.Config.Source.AttributesChunk = 1000;
                tester.Config.History.Enabled = false;
            }
            Assert.All(nodes, node => Assert.Equal(DataTypeIds.Double, node.DataType.Raw));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 100));
        }
        [Fact]
        public async Task TestReadRawValues()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            var ids = new[]
            {
                tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Custom.StringArray,
                tester.Server.Ids.Custom.EUProp,
                tester.Server.Ids.Custom.RangeProp
            };
            var values = await tester.Client.ReadRawValues(ids, tester.Source.Token);
            Assert.Equal(new[] { 0.0, 0.0, 0.0, 0.0 }, values[ids[0]].Value as double[]);
            Assert.Equal(new[] { "test1", "test2" }, values[ids[1]].Value as string[]);
            var ext1 = Assert.IsType<ExtensionObject>(values[ids[2]].Value);
            var inf = Assert.IsType<EUInformation>(ext1.Body);
            Assert.Equal("degree Celsius", inf.Description);
            Assert.Equal("°C", inf.DisplayName.Text);
            Assert.Equal(4408652, inf.UnitId);
            var ext2 = Assert.IsType<ExtensionObject>(values[ids[3]].Value);
            var range = Assert.IsType<Opc.Ua.Range>(ext2.Body);
            Assert.Equal(0, range.Low);
            Assert.Equal(100, range.High);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 1));
        }
        [Fact]
        public async Task TestReadRawValuesChunk()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            int start = (int)(uint)tester.Server.Ids.Full.WideRoot.Identifier;
            var nodes = Enumerable.Range(start + 1, 2000)
                .Select(idf => new NodeId((uint)idf, 2)).ToList();

            tester.Config.Source.AttributesChunk = 100;
            try
            {
                var values = await tester.Client.ReadRawValues(nodes, tester.Source.Token);
                Assert.All(nodes, node =>
                {
                    Assert.True(values.TryGetValue(node, out var dv));
                    Assert.Null(dv.Value);
                });
            }
            finally
            {
                tester.Config.Source.AttributesChunk = 1000;
            }

            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 20));
        }

        [Fact]
        public async Task TestReadNodeValues()
        {
            var nodes = new[]
            {
                new UAVariable(tester.Server.Ids.Base.DoubleVar1, "DoubleVar", tester.Server.Ids.Base.Root),
                new UAVariable(tester.Server.Ids.Custom.Array, "Array", tester.Server.Ids.Custom.Root),
                new UAVariable(tester.Server.Ids.Custom.StringArray, "StringArray", tester.Server.Ids.Custom.Root),
                new UAVariable(tester.Server.Ids.Custom.EUProp, "EUProp", tester.Server.Ids.Custom.Root),
                new UAVariable(tester.Server.Ids.Custom.RangeProp, "RangeProp", tester.Server.Ids.Custom.Root)
            };
            nodes[1].Attributes.IsProperty = true;
            nodes[2].Attributes.IsProperty = true;
            nodes[3].Attributes.IsProperty = true;
            nodes[4].Attributes.IsProperty = true;

            // Need to read attributes first for this, to get proper conversion we need the datatype.
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);
            await tester.Client.ReadNodeValues(nodes, tester.Source.Token);

            Assert.Equal(new Variant(0.0), nodes[0].Value);
            Assert.Equal(new Variant(new double[] { 0, 0, 0, 0 }), nodes[1].Value);
            Assert.Equal(new Variant(new[] { "test1", "test2" }), nodes[2].Value);
            Assert.Equal("°C: degree Celsius", tester.Client.StringConverter.ConvertToString(nodes[3].Value));
            Assert.Equal("(0, 100)", tester.Client.StringConverter.ConvertToString(nodes[4].Value));
        }

        [Fact]
        public async Task TestMissingNodes()
        {
            var nodes = new[]
            {
                new UAVariable(tester.Server.Ids.Base.DoubleVar1, "DoubleVar1", tester.Server.Ids.Base.Root),
                new UAVariable(new NodeId("missing-node"), "MissingNode", tester.Server.Ids.Base.Root),
                new UAVariable(new NodeId("missing-node2"), "MissingNode2", tester.Server.Ids.Base.Root),
            };

            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.NotNull(nodes[0].DataType);
            Assert.False(nodes[0].Ignore);
            Assert.True(nodes[1].DataType.Raw.IsNullNodeId);
            Assert.True(nodes[2].DataType.Raw.IsNullNodeId);
            Assert.True(nodes[1].Ignore);
            Assert.True(nodes[2].Ignore);
        }
        #endregion

        #region synchronization
        // This just tests the actual history-read method in UAClient, further tests should use the HistoryReader
        [Fact]
        public async Task TestHistoryReadData()
        {
            CommonTestUtils.ResetMetricValues("opcua_history_reads");

            var nodes = new[] { tester.Server.Ids.Custom.Array, tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Base.StringVar }
                .Select(id => new HistoryReadNode(HistoryReadType.FrontfillData, id)).ToList();

            var req = new HistoryReadParams(nodes,
                new ReadRawModifiedDetails
                {
                    IsReadModified = false,
                    StartTime = DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(200)),
                    EndTime = DateTime.UtcNow,
                    NumValuesPerNode = 600
                });

            var start = DateTime.UtcNow.AddSeconds(-20);

            tester.Server.PopulateCustomHistory(start);
            tester.Server.Server.PopulateHistory(tester.Server.Ids.Base.StringVar, 1000, start, "string");

            try
            {
                await tester.Client.DoHistoryRead(req, tester.Source.Token);

                Assert.True(CommonTestUtils.TestMetricValue("opcua_history_reads", 1));

                foreach (var node in nodes)
                {
                    var result = node.LastResult;
                    Console.WriteLine($"{node.Id} {result}");
                    var historyData = result as HistoryData;
                    Assert.Equal(600, historyData.DataValues.Count);
                    Assert.False(node.Completed);
                    Assert.NotNull(node.ContinuationPoint);
                }

                await tester.Client.DoHistoryRead(req, tester.Source.Token);

                foreach (var node in nodes)
                {
                    var result = node.LastResult;
                    var historyData = result as HistoryData;
                    Assert.Equal(400, historyData.DataValues.Count);
                    Assert.True(node.Completed);
                }
            }
            finally
            {
                tester.Server.WipeHistory(tester.Server.Ids.Custom.Array, new double[] { 0, 0, 0, 0 });
                tester.Server.WipeHistory(tester.Server.Ids.Custom.MysteryVar, null);
                tester.Server.WipeHistory(tester.Server.Ids.Base.StringVar, null);
            }
        }
        [Fact]
        public async Task TestDataSubscriptions()
        {
            CommonTestUtils.ResetMetricValues("opcua_subscriptions");
            int start = (int)(uint)tester.Server.Ids.Full.WideRoot.Identifier;
            var nodes = Enumerable.Range(start + 1, 2000)
                .Select(idf => new NodeId((uint)idf, 2))
                .Select(id => new VariableExtractionState(tester.Client, new UAVariable(id, "somvar", tester.Server.Ids.Full.WideRoot), true, true))
                .ToList();

            var lck = new object();

            var dps = new List<DataValue>();

            void handler(MonitoredItem item, MonitoredItemNotificationEventArgs _)
            {
                var values = item.DequeueValues();
                lock (lck)
                {
                    dps.AddRange(values);
                }
            }

            tester.Config.Source.SubscriptionChunk = 100;

            try
            {
                await tester.Client.SubscribeToNodes(nodes.Take(1000), handler, tester.Source.Token);
                await tester.Client.SubscribeToNodes(nodes.Skip(1000), handler, tester.Source.Token);

                await CommonTestUtils.WaitForCondition(() => dps.Count == 2000, 5,
                    () => $"Expected to get 2000 datapoints, but got {dps.Count}");

                foreach (var node in nodes)
                {
                    tester.Server.UpdateNode(node.SourceId, 1.0);
                }

                await CommonTestUtils.WaitForCondition(() => dps.Count == 4000, 5,
                    () => $"Expected to get 4000 datapoints, but got {dps.Count}");
            }
            finally
            {
                tester.Client.RemoveSubscription("DataChangeListener");
                foreach (var node in nodes)
                {
                    tester.Server.UpdateNode(node.SourceId, null);
                }
                tester.Config.Source.SubscriptionChunk = 1000;
            }
            Assert.True(CommonTestUtils.TestMetricValue("opcua_subscriptions", 2000));
        }

        [Fact]
        public async Task TestAbortHistory()
        {
            var nodes = new[] { tester.Server.Ids.Base.DoubleVar1, tester.Server.Ids.Base.IntVar, tester.Server.Ids.Base.StringVar }
                .Select(id => new HistoryReadNode(HistoryReadType.FrontfillData, id)).ToList();
            var req = new HistoryReadParams(nodes,
                new ReadRawModifiedDetails
                {
                    IsReadModified = false,
                    StartTime = DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(200)),
                    EndTime = DateTime.UtcNow,
                    NumValuesPerNode = 100
                });
            var start = DateTime.UtcNow.AddSeconds(-20);

            tester.Server.PopulateBaseHistory(start);

            try
            {
                await tester.Client.DoHistoryRead(req, tester.Source.Token);

                Assert.All(req.Nodes, node => Assert.NotNull(node.ContinuationPoint));

                await tester.Client.AbortHistoryRead(req, tester.Source.Token);
                Assert.All(req.Nodes, node => Assert.Null(node.ContinuationPoint));
            }
            finally
            {
                tester.Server.WipeHistory(tester.Server.Ids.Base.DoubleVar1, 0.0);
                tester.Server.WipeHistory(tester.Server.Ids.Base.IntVar, 0);
                tester.Server.WipeHistory(tester.Server.Ids.Base.StringVar, null);
            }
        }

        [Fact]
        public async Task TestRecreateSubscriptions()
        {
            // Silently kill subscriptions, then wait for them to be recreated.
            CommonTestUtils.ResetMetricValues("opcua_subscriptions");
            var ids = tester.Server.Ids.Custom;
            var nodes = new[] { tester.Server.Ids.Custom.Array, tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Base.StringVar }
                .Select(id =>
                    new VariableExtractionState(tester.Client,
                        new UAVariable(id, "somevar", NodeId.Null), false, false))
                .ToList();

            void update(int idx)
            {
                tester.Server.UpdateNode(tester.Server.Ids.Custom.Array, new[] { idx, idx + 1, idx + 2, idx + 3 });
                tester.Server.UpdateNode(tester.Server.Ids.Custom.MysteryVar, idx);
                tester.Server.UpdateNode(tester.Server.Ids.Base.StringVar, $"val{idx}");
            }


            var lck = new object();

            var dps = new List<DataValue>();

            void handler(MonitoredItem item, MonitoredItemNotificationEventArgs _)
            {
                var values = item.DequeueValues();
                lock (lck)
                {
                    dps.AddRange(values);
                }
            }

            tester.Config.Source.PublishingInterval = 200;

            try
            {
                await tester.Client.SubscribeToNodes(nodes, handler, tester.Source.Token);

                await CommonTestUtils.WaitForCondition(() => dps.Count == 3, 5,
                    () => $"Expected to get 3 datapoints, but got {dps.Count}");

                update(1);

                await CommonTestUtils.WaitForCondition(() => dps.Count == 6, 5,
                    () => $"Expected to get 6 datapoints, but got {dps.Count}");

                tester.Server.Server.DropSubscriptions();

                await CommonTestUtils.WaitForCondition(() => dps.Count == 9, 20,
                    () => $"Expected to get 9 datapoints, but got {dps.Count}");

                update(2);

                await CommonTestUtils.WaitForCondition(() => dps.Count == 12, 5,
                    () => $"Expected to get 12 datapoints, but got {dps.Count}");
            }
            finally
            {
                tester.Client.RemoveSubscription("DataChangeListener");
                tester.Server.WipeHistory(tester.Server.Ids.Custom.Array, new double[] { 0, 0, 0, 0 });
                tester.Server.WipeHistory(tester.Server.Ids.Custom.MysteryVar, null);
                tester.Server.WipeHistory(tester.Server.Ids.Base.StringVar, null);

                tester.Config.Source.PublishingInterval = 500;
            }
        }

        #endregion
        #region events
        // Just basic testing here, separate tests should be written for the event field collector itself.
        [Fact]
        public async Task TestGetEventFields()
        {
            tester.Config.Events.Enabled = true;

            try
            {
                var fields = await tester.Client.GetEventFields(null, tester.Source.Token);
                Assert.True(fields.ContainsKey(tester.Server.Ids.Event.CustomType));
                Assert.True(fields.ContainsKey(ObjectTypeIds.AuditActivateSessionEventType));
            }
            finally
            {
                tester.Client.Browser.ResetVisitedNodes();
                tester.Config.Events.Enabled = false;
                tester.Client.ClearEventFields();
            }
        }

        [Fact]
        public async Task TestEventSubscriptions()
        {
            tester.Config.Events.Enabled = true;
            tester.Config.Source.QueueLength = 100;

            var emitters = new[]
            {
                new EventExtractionState(tester.Client, ObjectIds.Server, true, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj1, true, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj2, true, true, true)
            };
            tester.Config.Source.SubscriptionChunk = 1;

            int count = 0;

            void handler(MonitoredItem _, MonitoredItemNotificationEventArgs __)
            {
                Console.WriteLine("Trigger event");
                count++;
            }

            try
            {
                await tester.Client.GetEventFields(null, tester.Source.Token);

                await tester.Client.SubscribeToEvents(emitters.Take(2), handler, tester.Source.Token);
                await tester.Client.SubscribeToEvents(emitters.Skip(2), handler, tester.Source.Token);

                tester.Server.TriggerEvents(0);

                await CommonTestUtils.WaitForCondition(() => count == 11, 10,
                    () => $"Expected to get 11 events, but got {count}");

                tester.Server.TriggerEvents(1);

                await CommonTestUtils.WaitForCondition(() => count == 22, 10,
                    () => $"Expected to get 22 events, but got {count}");
            }
            finally
            {
                tester.Client.Browser.ResetVisitedNodes();
                tester.Config.Source.SubscriptionChunk = 1000;
                tester.Config.Events.Enabled = false;
                tester.Client.ClearEventFields();
                tester.Client.RemoveSubscription("EventListener");
                tester.Server.WipeEventHistory();
            }
        }
        [Fact]
        public async Task TestEventSubscriptionsFiltered()
        {
            tester.Config.Events.Enabled = true;
            tester.Config.Events.EventIds = new[]
            {
                new ProtoNodeId { NamespaceUri = tester.Client.NamespaceTable.GetString(2), NodeId = $"i={tester.Server.Ids.Event.BasicType1.Identifier}" },
                new ProtoNodeId { NamespaceUri = "http://opcfoundation.org/UA/", NodeId = $"i={ObjectTypeIds.AuditChannelEventType.Identifier}" }
            };
            var emitters = new[]
            {
                new EventExtractionState(tester.Client, ObjectIds.Server, true, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj1, true, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj2, true, true, true)
            };
            int count = 0;

            void handler(MonitoredItem _, MonitoredItemNotificationEventArgs __)
            {
                count++;
            }
            try
            {
                await tester.Client.GetEventFields(null, tester.Source.Token);
                await tester.Client.SubscribeToEvents(emitters, handler, tester.Source.Token);

                tester.Server.TriggerEvents(0);

                await CommonTestUtils.WaitForCondition(() => count == 6, 10,
                    () => $"Expected to get 6 events, but got {count}");
            }
            finally
            {
                tester.Client.Browser.ResetVisitedNodes();
                tester.Config.Source.SubscriptionChunk = 1000;
                tester.Config.Events.Enabled = false;
                tester.Config.Events.EventIds = null;
                tester.Client.ClearEventFields();
                tester.Client.RemoveSubscription("EventListener");
                tester.Server.WipeEventHistory();
            }

        }


        [Fact]
        public async Task TestAuditSubscription()
        {
            int count = 0;

            void handler(MonitoredItem _, MonitoredItemNotificationEventArgs __)
            {
                count++;
            }

            try
            {
                await tester.Client.SubscribeToAuditEvents(handler, tester.Source.Token);

                tester.Server.DirectGrowth();

                await CommonTestUtils.WaitForCondition(() => count == 2, 10,
                    () => $"Expected to get 2 events, but got {count}");

                tester.Server.ReferenceGrowth();

                await CommonTestUtils.WaitForCondition(() => count == 6, 10,
                    () => $"Expected to get 6 events, but got {count}");
            }
            finally
            {
                tester.Client.RemoveSubscription("AuditListener");
            }
        }
        #endregion
        #region utils
        [Fact]
        public void TestExpandedNodeIdConversion()
        {
            var nodeId = new ExpandedNodeId("string-ns", tester.Client.NamespaceTable.GetString(2));
            Assert.Equal(new NodeId("string-ns", 2), tester.Client.ToNodeId(nodeId));
            nodeId = new ExpandedNodeId(new byte[] { 12, 12, 6 }, 1);
            Assert.Equal(new NodeId(new byte[] { 12, 12, 6 }, 1), tester.Client.ToNodeId(nodeId));
            nodeId = new ExpandedNodeId("other-server", "opc.tcp://some-other-server.test", 1);
            Assert.Null(tester.Client.ToNodeId(nodeId));
        }
        [Fact]
        public void TestNodeIdConversion()
        {
            var nodeId = tester.Client.ToNodeId("i=123", tester.Client.NamespaceTable.GetString(2));
            Assert.Equal(new NodeId(123u, 2), nodeId);
            nodeId = tester.Client.ToNodeId("s=abc", tester.Client.NamespaceTable.GetString(1));
            Assert.Equal(new NodeId("abc", 1), nodeId);
            nodeId = tester.Client.ToNodeId("s=abcd", "some-namespaces-that-doesnt-exist");
            Assert.Equal(NodeId.Null, nodeId);
            nodeId = tester.Client.ToNodeId("s=bcd", "tl:");
            Assert.Equal(new NodeId("bcd", 2), nodeId);
            Assert.Equal(NodeId.Null, tester.Client.ToNodeId("i=123", null));
        }
        [Fact]
        public static void TestConvertToDouble()
        {
            Assert.Equal(0, UAClient.ConvertToDouble(null));
            Assert.Equal(1, UAClient.ConvertToDouble(1.0));
            Assert.Equal(2, UAClient.ConvertToDouble(2f));
            Assert.Equal(3, UAClient.ConvertToDouble(3u));
            Assert.Equal(4, UAClient.ConvertToDouble(new[] { 4, 5 }));
            Assert.Equal(5, UAClient.ConvertToDouble(new[] { 4, 5, 6 }.Where(val => val >= 5)));
            Assert.Equal(0, UAClient.ConvertToDouble(new object()));
            Assert.Equal(7, UAClient.ConvertToDouble(new Variant(7)));
        }
        [Fact]
        public void TestGetUniqueId()
        {
            Assert.Equal("gp.tl:i=123", tester.Client.GetUniqueId(new NodeId(123u, 2)));
            Assert.Equal("gp.base:i=123", tester.Client.GetUniqueId(new NodeId(123u)));
            var id = tester.Client.GetUniqueId(new NodeId(new string('s', 400), 2));
            Assert.Equal($"gp.tl:s={new string('s', 247)}", id);
            Assert.Equal(255, id.Length);
            id = tester.Client.GetUniqueId(new NodeId(new string('s', 400), 2), 123);
            Assert.Equal($"gp.tl:s={new string('s', 242)}[123]", id);
            Assert.Equal(255, id.Length);
            Assert.Equal("gp.tl:s=", tester.Client.GetUniqueId(new NodeId(new string(' ', 400), 2)));
            Assert.Equal("gp.tl:s=[123]", tester.Client.GetUniqueId(new NodeId(new string(' ', 400), 2), 123));

            tester.Client.AddNodeOverride(new NodeId(1234, 2), "override");
            Assert.Equal("override", tester.Client.GetUniqueId(new NodeId(1234, 2)));
            Assert.Equal("override[123]", tester.Client.GetUniqueId(new NodeId(1234, 2), 123));
            tester.Client.ClearNodeOverrides();

        }

        #endregion

        #region metrics
        [Fact]
        public async Task TestServerMetrics()
        {
            // I wanted better tests here, but this is a nightmare to hack, so this will do. It verifies that
            // if diagnostics are enabled we do get some metric values.
            CommonTestUtils.ResetMetricValues("opcua_node_CurrentSessionCount");
            tester.Config.Metrics.Nodes = new NodeMetricsConfig
            {
                ServerMetrics = true
            };
            var mgr = new NodeMetricsManager(tester.Client, tester.Config.Source, tester.Config.Metrics.Nodes);
            await mgr.StartNodeMetrics(tester.Source.Token);

            tester.Server.SetDiagnosticsEnabled(true);

            await CommonTestUtils.WaitForCondition(() => CommonTestUtils.GetMetricValue("opcua_node_CurrentSessionCount") >= 1, 20);

            tester.Client.RemoveSubscription("NodeMetrics");
            tester.Server.SetDiagnosticsEnabled(false);
            tester.Config.Metrics.Nodes = null;
        }
        [Fact]
        public async Task TestCustomNodeMetrics()
        {
            CommonTestUtils.ResetMetricValues("opcua_node_DoubleVar1", "opcua_node_DoubleVar2");
            var ids = tester.Server.Ids.Base;
            tester.Config.Metrics.Nodes = new NodeMetricsConfig
            {
                OtherMetrics = new List<ProtoNodeId>
                {
                    ids.DoubleVar1.ToProtoNodeId(tester.Client),
                    ids.DoubleVar2.ToProtoNodeId(tester.Client)
                }
            };
            tester.Server.UpdateNode(ids.DoubleVar1, 0);
            tester.Server.UpdateNode(ids.DoubleVar2, 0);
            var mgr = new NodeMetricsManager(tester.Client, tester.Config.Source, tester.Config.Metrics.Nodes);
            await mgr.StartNodeMetrics(tester.Source.Token);

            tester.Server.UpdateNode(ids.DoubleVar1, 15);
            await CommonTestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_node_Variable_1", 15), 5);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_Variable_2", 0));

            tester.Server.UpdateNode(ids.DoubleVar2, 25);
            await CommonTestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_node_Variable_2", 25), 5);

            tester.Server.UpdateNode(ids.DoubleVar1, 0);
            tester.Server.UpdateNode(ids.DoubleVar2, 0);
            tester.Client.RemoveSubscription("NodeMetrics");
            tester.Config.Metrics.Nodes = null;
        }
        #endregion
    }
}

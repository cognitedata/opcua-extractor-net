using Cognite.Extractor.Common;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.NodeSources;
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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;
using Cognite.Extractor.Configuration;

namespace Test.Unit
{
    public sealed class UAClientTestFixture : LoggingTestFixture, IAsyncLifetime
    {
        public UAClient Client { get; private set; }
        public ServerController Server { get; private set; }
        public FullConfig Config { get; }
        public CancellationTokenSource Source { get; private set; }
        public ServiceProvider Provider { get; }
        public ILogger Logger { get; }
        public DummyClientCallbacks Callbacks { get; private set; }
        public UAClientTestFixture()
        {
            var services = new ServiceCollection();

            try
            {
                ConfigurationUtils.AddTypeConverter(new FieldFilterConverter());
            }
            catch { }

            Config = services.AddConfig<FullConfig>("config.test.yml", 1);
            Config.Source.EndpointUrl = $"opc.tcp://localhost:62000";
            Configure(services);
            Provider = services.BuildServiceProvider();
            Logger = Provider.GetRequiredService<ILogger<UAClientTestFixture>>();

            Server = new ServerController(new[] {
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, Provider, 62000)
            {
                ConfigRoot = "Server.Test.UaClient"
            };

            if (Directory.Exists("./uaclienttestcerts/pki/"))
            {
                Directory.Delete("./uaclienttestcerts/pki/", true);
            }
        }
        public static (Action<ReferenceDescription, NodeId, bool>, IDictionary<NodeId, ReferenceDescriptionCollection>) GetCallback()
        {
            var toWrite = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            return ((desc, parentId, visited) =>
            {
                if (visited) return;
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
            Callbacks = new DummyClientCallbacks(Source.Token);
            Client.Callbacks = Callbacks;
            await Client.Run(Source.Token, 0);
        }

        public async Task DisposeAsync()
        {
            if (Source != null)
            {
                await Source.CancelAsync();
                Source.Dispose();
                Source = null;
            }
            await Client.Close(CancellationToken.None);
            Server.Stop();
            await Provider.DisposeAsync();
        }

        public async Task RemoveSubscription(SubscriptionName name)
        {
            if (TryGetSubscription(name, out var subscription) && subscription!.Created)
            {
                try
                {
                    await Client.SessionManager.Session!.RemoveSubscriptionAsync(subscription);
                }
                catch
                {
                    // A failure to delete the subscription generally means it just doesn't exist.
                }
                finally
                {
                    subscription!.Dispose();
                }
            }
        }

        public bool TryGetSubscription(SubscriptionName name, out Subscription subscription)
        {
            subscription = Client.SessionManager.Session?.Subscriptions?.FirstOrDefault(sub =>
                sub.DisplayName.StartsWith(name.Name(), StringComparison.InvariantCulture));
            return subscription != null;
        }
    }
    public class UAClientTest : IClassFixture<UAClientTestFixture>
    {
        private readonly UAClientTestFixture tester;
        public UAClientTest(ITestOutputHelper output, UAClientTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.Init(output);
            tester.Client.TypeManager.Reset();
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
        [Fact(Timeout = 10000)]
        public async Task TestConnectionFailure()
        {
            string oldEP = tester.Config.Source.EndpointUrl;
            await tester.Client.Close(tester.Source.Token);
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62009";
            try
            {
                var exc = await Assert.ThrowsAsync<SilentServiceException>(() => tester.Client.Run(tester.Source.Token, 0));
                Assert.Equal(StatusCodes.BadNoCommunication, exc.StatusCode);
                Assert.Equal(ExtractorUtils.SourceOp.SelectEndpoint, exc.Operation);
            }
            finally
            {
                tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62000";
                await tester.Client.Run(tester.Source.Token, 0);
            }
        }
        [Fact]
        public async Task TestConfigFailure()
        {
            await tester.Client.Close(tester.Source.Token);
            tester.Config.Source.ConfigRoot = "wrong";
            try
            {
                var exc = await Assert.ThrowsAsync<ExtractorFailureException>(() => tester.Client.Run(tester.Source.Token, 0));
            }
            finally
            {
                tester.Config.Source.ConfigRoot = "config";
                await tester.Client.Run(tester.Source.Token, 0);
            }
        }
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestReconnect(bool forceRestart)
        {
            await tester.Client.Close(tester.Source.Token);
            tester.Config.Source.KeepAliveInterval = 1000;
            tester.Config.Source.ForceRestart = forceRestart;

            try
            {
                await tester.Client.Run(tester.Source.Token, 10);

                Assert.True(CommonTestUtils.TestMetricValue("opcua_connected", 1));
                tester.Server.Stop();
                await TestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_connected", 0) && !tester.Callbacks.Connected, 20,
                    "Expected client to disconnect");
                Assert.False(tester.Callbacks.Connected);
                await tester.Server.Start();
                await TestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_connected", 1) && tester.Callbacks.Connected, 20,
                    "Expected client to reconnect");
                Assert.True(tester.Callbacks.Connected);
            }
            finally
            {
                await tester.Server.Start();
                tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62000";
                await tester.Client.Close(tester.Source.Token);
                tester.Config.Source.KeepAliveInterval = 10000;
                tester.Config.Logger.UaSessionTracing = false;
                await tester.Client.Run(tester.Source.Token, 0);
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
                await tester.Client.Run(tester.Source.Token, 0);
                var dir = new DirectoryInfo("./certificates-test/pki/trusted/certs/");
                Assert.Single(dir.GetFiles());
            }
            finally
            {
                await tester.Client.Close(tester.Source.Token);
                Environment.SetEnvironmentVariable("OPCUA_CERTIFICATE_DIR", null);
                Directory.Delete("./certificates-test/", true);
                await tester.Client.Run(tester.Source.Token, 0);
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

                await Assert.ThrowsAsync<SilentServiceException>(() => tester.Client.Run(tester.Source.Token, 0));

                tester.Server.Server.SetValidator(false);

                await tester.Client.Run(tester.Source.Token, 0);
            }
            finally
            {
                tester.Server.Server.AllowAnonymous = true;
                tester.Config.Source.X509Certificate = null;
                tester.Server.Server.SetValidator(false);
                await tester.Client.Run(tester.Source.Token, 0);
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

                await Assert.ThrowsAsync<SilentServiceException>((Func<Task>)(async () => await tester.Client.Run(tester.Source.Token, 0)));

                tester.Config.Source.Password = "testpassword";

                await tester.Client.Run(tester.Source.Token, 0);
            }
            finally
            {
                tester.Server.Server.AllowAnonymous = true;
                tester.Config.Source.Username = null;
                tester.Config.Source.Password = null;
                await tester.Client.Run(tester.Source.Token, 0);
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
                await tester.Client.Run(tester.Source.Token, 0);
                Assert.True(tester.Client.Started);
                // Just check that we are able to read, indicating an established connection
                await tester.Client.ReadRawValues(new[] { VariableIds.Server_ServerStatus }, tester.Source.Token);
            }
            finally
            {
                tester.Server.Server.RemoveReverseConnection(new Uri("opc.tcp://localhost:61000"));
                tester.Config.Source.ReverseConnectUrl = null;
                await tester.Client.Run(tester.Source.Token, 0);
            }
        }

        [Fact]
        public async Task TestRedundancy()
        {
            await tester.Client.Close(tester.Source.Token);
            tester.Server.SetServerRedundancyStatus(230, RedundancySupport.Hot);
            tester.Config.Source.KeepAliveInterval = 1000;
            var altServer = new ServerController(new[] {
                PredefinedSetup.Base
            }, tester.Provider, 62300)
            {
                ConfigRoot = "Server.Test.UaClient"
            };
            await altServer.Start();
            altServer.SetServerRedundancyStatus(240, RedundancySupport.Hot);
            tester.Config.Source.AltEndpointUrls = new[]
            {
                "opc.tcp://localhost:62300"
            };
            tester.Config.Source.ForceRestart = true;

            try
            {
                await tester.Client.Run(tester.Source.Token, 0);
                var sm = tester.Client.SessionManager;
                Assert.Equal("opc.tcp://localhost:62300", sm.EndpointUrl);

                altServer.Stop();
                altServer.Dispose();
                await TestUtils.WaitForCondition(() => sm.EndpointUrl == tester.Config.Source.EndpointUrl, 20, "Expected session to reconnect to original server");
            }
            finally
            {
                tester.Config.Source.AltEndpointUrls = null;
                tester.Config.Source.ForceRestart = false;
                tester.Config.Source.KeepAliveInterval = 10000;
                tester.Server.SetServerRedundancyStatus(255, RedundancySupport.Hot);
                await tester.Client.Run(tester.Source.Token, 0);
            }
        }

        [Fact]
        public async Task TestServiceLevelSwitch()
        {
            await tester.Client.Close(tester.Source.Token);
            tester.Server.SetServerRedundancyStatus(230, RedundancySupport.Hot);
            tester.Config.Source.Redundancy.MonitorServiceLevel = true;
            tester.Callbacks.Reset();
            tester.Client.Callbacks = tester.Callbacks;
            var altServer = new ServerController(new[] {
                PredefinedSetup.Base
            }, tester.Provider, 62300)
            {
                ConfigRoot = "Server.Test.UaClient"
            };
            await altServer.Start();
            altServer.SetServerRedundancyStatus(240, RedundancySupport.Hot);
            tester.Config.Source.AltEndpointUrls = new[]
            {
                "opc.tcp://localhost:62300"
            };
            tester.Config.Source.ForceRestart = true;

            try
            {
                // Should connect to altServer
                await tester.Client.Run(tester.Source.Token, 0);
                var sm = tester.Client.SessionManager;
                Assert.Equal("opc.tcp://localhost:62300", sm.EndpointUrl);

                // Should trigger a reconnect attempt, which finds the main server with status 230
                altServer.SetServerRedundancyStatus(190, RedundancySupport.Hot);
                await TestUtils.WaitForCondition(() => sm.CurrentServiceLevel == 230, 10, "Expected session to reconnect to original server");

                Assert.Equal(230, sm.CurrentServiceLevel);
                Assert.Equal(1, tester.Callbacks.ServiceLevelCbCount);
                Assert.Equal(1, tester.Callbacks.LowServiceLevelCbCount);
                Assert.Equal(1, tester.Callbacks.ReconnectCbCount);
                Assert.Equal(sm.EndpointUrl, tester.Config.Source.EndpointUrl);


                // This method is called by the extractor, so we call it here to ensure the test passes.
                // There is technically a tiny race condition here. If we connect to a new server, then that server immediately changes
                // its service level, we _may_ not be able to pick up the change until it changes again.
                // Some servers also send value updates on subscription creation, in which case this will not happen.
                sm.EnsureServiceLevelSubscription();
                // Should trigger a reconnect attempt, but it will not find a better alternative.
                tester.Server.SetServerRedundancyStatus(190, RedundancySupport.Hot);
                await TestUtils.WaitForCondition(() => sm.CurrentServiceLevel == 190, 10, "Expected service level to drop");

                Assert.Equal(sm.EndpointUrl, tester.Config.Source.EndpointUrl);
                Assert.Equal(2, tester.Callbacks.LowServiceLevelCbCount);

                // Set the servicelevel back up, should trigger a callback, but no switch
                tester.Server.SetServerRedundancyStatus(255, RedundancySupport.Hot);
                await TestUtils.WaitForCondition(() => tester.Callbacks.ServiceLevelCbCount == 2, 10);
                Assert.Equal(sm.EndpointUrl, tester.Config.Source.EndpointUrl);
                Assert.Equal(255, sm.CurrentServiceLevel);
            }
            finally
            {
                tester.Config.Source.AltEndpointUrls = null;
                tester.Config.Source.ForceRestart = false;
                tester.Config.Source.KeepAliveInterval = 10000;
                altServer.Stop();
                altServer.Dispose();
                tester.Server.SetServerRedundancyStatus(255, RedundancySupport.Hot);
                await tester.Client.Run(tester.Source.Token, 0);
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
            Assert.Equal("Server", server.Name);
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
            Assert.Equal(9, children.Count);

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
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token);
            Assert.Equal(147, nodes.Count);
            Assert.Equal(151, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 32));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 31));
        }
        [Fact]
        public async Task TestBrowseNodesChunk()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            tester.Config.Source.BrowseNodesChunk = 2;
            try
            {
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token);
            }
            finally
            {
                tester.Config.Source.BrowseNodesChunk = 100;
            }
            Assert.Equal(147, nodes.Count);
            Assert.Equal(151, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            double reads = CommonTestUtils.GetMetricValue("opcua_browse_operations");

            // Best case, it takes 91 reads: 1 read at level 0, 3 reads for each of the 30 remaining.
            // Timing might cause nodes to be read in a sligthly different order, so we might read 2 more times.
            // In practice this slight variance is irrelevant.
            Assert.True(reads >= 88 && reads <= 93, $"Expected reads between 88 and 93, got {reads}");
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
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            await tester.Client.Browser.BrowseDirectory(new[] { tester.Server.Ids.Full.DeepRoot }, callback, tester.Source.Token,
                null, 3, true, depth);

            Assert.Equal(numNodes, nodes.Sum(node => node.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", numBrowse));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", depth < 0 ? 31 : depth + 2));
        }
        [Fact]
        public async Task TestBrowseThrottling()
        {
            // When running alone limiting node parallelism should have the same effect as chunking.
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            var (callback, nodes) = UAClientTestFixture.GetCallback();
            tester.Config.Source.BrowseThrottling.MaxNodeParallelism = 2;
            tester.Config.Source.BrowseThrottling.MaxParallelism = 1;
            var log = tester.Provider.GetRequiredService<ILogger<Cognite.OpcUa.Browse.Browser>>();
            using var browser = new Cognite.OpcUa.Browse.Browser(log, tester.Client, tester.Config);
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
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 77));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 31));
        }
        [Fact]
        public async Task TestBrowseIgnoreName()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Client.Browser.Transformations = new TransformationCollection(new List<NodeTransformation>
            {
                new NodeTransformation(new RawNodeTransformation {
                    Filter = new NodeFilter
                    {
                        Name = new RegexFieldFilter("WideRoot")
                    },
                    Type = TransformationType.Ignore
                }, 0)
            });
            try
            {
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            }
            finally
            {
                tester.Client.Browser.Transformations = null;
            }
            Assert.False(nodes.ContainsKey(tester.Server.Ids.Full.WideRoot));
            Assert.Equal(152, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 33));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 32));
        }
        [Fact]
        public async Task TestBrowseIgnorePrefix()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Client.Browser.Transformations = new TransformationCollection(new List<NodeTransformation>
            {
                new NodeTransformation(new RawNodeTransformation {
                    Filter = new NodeFilter
                    {
                        Name = new RegexFieldFilter("^Sub|^Deep")
                    },
                    Type = TransformationType.Ignore
                }, 0)
            });

            try
            {
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.Root, callback, tester.Source.Token);
            }
            finally
            {
                tester.Client.Browser.Transformations = null;
            }
            Assert.Equal(2, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 4));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 2));
        }
        [Fact]
        public async Task TestBrowseTypes()
        {
            CommonTestUtils.ResetMetricValues("opcua_browse_operations", "opcua_tree_depth");
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

            Assert.Equal(distinctNodes.Count(), nodes.Sum(kvp => kvp.Value.Count));
            Assert.Equal(3117, nodes.Sum(kvp => kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 11));
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
            var (callback, nodes) = UAClientTestFixture.GetCallback();

            tester.Config.Source.Retries.MaxTries = 1;
            tester.Server.Issues.RemainingBrowse = 6;
            var ex = await Assert.ThrowsAsync<AggregateException>((Func<Task>)(async () =>
                await tester.Client.Browser.BrowseNodeHierarchy(tester.Server.Ids.Full.DeepRoot, callback, tester.Source.Token)
            ));

            var root = ex.InnerException;
            Assert.IsType<SilentServiceException>(root);

            // Root node is browsed twice, once for type definition, once for the hierarchy
            Assert.Equal(21, nodes.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 5, tester.Logger));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tree_depth", 5, tester.Logger));
        }

        #endregion

        #region nodedata
        [Fact]
        public async Task TestReadNodeData()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            var nodes = new BaseUANode[]
            {
                new UAObject(tester.Server.Ids.Full.Root, "FullRoot", null, null, ObjectIds.ObjectsFolder, null),
                new UAObject(tester.Server.Ids.Event.Obj1, "Object 1", null, null, tester.Server.Ids.Event.Root, null),
                new UAObject(tester.Server.Ids.Custom.Root, "CustomRoot", null, null, ObjectIds.ObjectsFolder, null),
                new UAVariable(tester.Server.Ids.Custom.StringyVar, "StringyVar", null, null, tester.Server.Ids.Custom.Root, null),
                new UAVariable(tester.Server.Ids.Custom.Array, "Array", null, null, tester.Server.Ids.Custom.Root, null),
                new UAVariable(tester.Server.Ids.Custom.ObjProp, "ObjProp", null, null, tester.Server.Ids.Custom.Obj2, null)
            };
            nodes[5].IsRawProperty = true;
            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = -1;
            tester.Config.Events.Enabled = true;
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal("FullRoot Description", nodes[0].Attributes.Description);
            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, (nodes[1] as UAObject).FullAttributes.EventNotifier);
            Assert.Equal(tester.Server.Ids.Custom.StringyType, (nodes[3] as UAVariable).FullAttributes.DataType.Id);
            Assert.Equal(4, (nodes[4] as UAVariable).ArrayDimensions[0]);
            Assert.Single((nodes[4] as UAVariable).ArrayDimensions);
            Assert.True((nodes[4] as UAVariable).FullAttributes.ShouldReadHistory(tester.Config));
            Assert.Null((nodes[5] as UAVariable).ArrayDimensions);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 1));
        }
        [Fact]
        public async Task TestReadNodeDataConfig()
        {
            BaseUANode[] GetNodes()
            {
                var nds = new BaseUANode[]
                {
                    new UAObject(tester.Server.Ids.Full.Root, "FullRoot", null, null, ObjectIds.ObjectsFolder, null),
                    new UAObject(tester.Server.Ids.Event.Obj1, "Object 1", null, null, tester.Server.Ids.Event.Root, null),
                    new UAVariable(tester.Server.Ids.Custom.StringyVar, "StringyVar", null, null, tester.Server.Ids.Custom.Root, null),
                    new UAVariable(tester.Server.Ids.Custom.Array, "Array", null, null, tester.Server.Ids.Custom.Root, null),
                    new UAVariable(tester.Server.Ids.Wrong.TooLargeProp, "TooLargeProp", null, null, tester.Server.Ids.Custom.Obj2, null),
                    new UAVariableType(VariableTypeIds.BaseDataVariableType, "BaseDataVariableType", null, null, NodeId.Null),
                    new UAObjectType(ObjectTypeIds.BaseObjectType, "BaseObjectType", null, null, NodeId.Null)
                };
                nds[4].IsRawProperty = true;
                return nds;
            }
            var nodes = GetNodes();

            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = -1;
            tester.Config.Events.Enabled = true;

            // Read everything
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal("FullRoot Description", nodes[0].Attributes.Description);
            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, (nodes[1] as UAObject).FullAttributes.EventNotifier);
            Assert.True((nodes[1] as UAObject).FullAttributes.ShouldSubscribeToEvents(tester.Config));
            Assert.Equal(tester.Server.Ids.Custom.StringyType, (nodes[2] as UAVariable).FullAttributes.DataType.Id);
            Assert.Equal(4, (nodes[3] as UAVariable).ArrayDimensions[0]);
            Assert.Single((nodes[3] as UAVariable).ArrayDimensions);
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldReadHistory(tester.Config));
            Assert.Equal(AccessLevels.CurrentRead | AccessLevels.HistoryRead, (nodes[3] as UAVariable).FullAttributes.AccessLevel);
            Assert.True((nodes[3] as UAVariable).FullAttributes.Historizing);
            Assert.NotNull((nodes[4] as UAVariable).ArrayDimensions);
            Assert.NotNull((nodes[3] as UAVariable).ArrayDimensions);
            Assert.Equal(DataTypeIds.BaseDataType, (nodes[5] as UAVariableType).FullAttributes.DataType.Id);

            // Disable history
            tester.Config.History.Enabled = false;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, (nodes[1] as UAObject).FullAttributes.EventNotifier);
            Assert.True((nodes[1] as UAObject).FullAttributes.ShouldSubscribeToEvents(tester.Config));
            Assert.False((nodes[3] as UAVariable).FullAttributes.ShouldReadHistory(tester.Config));
            Assert.False((nodes[3] as UAVariable).FullAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldSubscribe(tester.Config));

            // Disable event discovery
            tester.Config.History.Enabled = true;
            tester.Config.Events.DiscoverEmitters = false;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(0, (nodes[1] as UAObject).FullAttributes.EventNotifier);
            Assert.False((nodes[1] as UAObject).FullAttributes.ShouldSubscribeToEvents(tester.Config));
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldReadHistory(tester.Config));
            Assert.True((nodes[3] as UAVariable).FullAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldSubscribe(tester.Config));

            // Disable just data history
            tester.Config.Events.DiscoverEmitters = true;
            tester.Config.History.Data = false;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, (nodes[1] as UAObject).FullAttributes.EventNotifier);
            Assert.True((nodes[1] as UAObject).FullAttributes.ShouldSubscribeToEvents(tester.Config));
            Assert.False((nodes[3] as UAVariable).FullAttributes.ShouldReadHistory(tester.Config));
            Assert.True((nodes[3] as UAVariable).FullAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldSubscribe(tester.Config));


            // Enable require historizing
            tester.Config.History.Data = true;
            tester.Config.History.RequireHistorizing = true;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, (nodes[1] as UAObject).FullAttributes.EventNotifier);
            Assert.True((nodes[1] as UAObject).FullAttributes.ShouldSubscribeToEvents(tester.Config));
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldReadHistory(tester.Config));
            Assert.True((nodes[3] as UAVariable).FullAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldSubscribe(tester.Config));

            // Enable ignore access level
            tester.Config.History.RequireHistorizing = false;
            tester.Config.Subscriptions.IgnoreAccessLevel = true;

            nodes = GetNodes();
            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.Equal(EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead, (nodes[1] as UAObject).FullAttributes.EventNotifier);
            Assert.True((nodes[1] as UAObject).FullAttributes.ShouldSubscribeToEvents(tester.Config));
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldReadHistory(tester.Config));
            Assert.True((nodes[3] as UAVariable).FullAttributes.Historizing);
            Assert.True((nodes[3] as UAVariable).FullAttributes.ShouldSubscribe(tester.Config));
            Assert.Equal(0, (nodes[3] as UAVariable).FullAttributes.AccessLevel);
        }


        [Fact]
        public async Task TestReadNodeDataChunk()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            int start = (int)(uint)tester.Server.Ids.Full.WideRoot.Identifier;
            var nodes = Enumerable.Range(start + 1, 2000)
                .Select(idf => new NodeId((uint)idf, 2))
                .Select(id => new UAVariable(id, "subnode", null, null, tester.Server.Ids.Full.WideRoot, null))
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
            Assert.All(nodes, node => Assert.Equal(DataTypeIds.Double, node.FullAttributes.DataType.Id));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 140));
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
                new UAVariable(tester.Server.Ids.Base.DoubleVar1, "DoubleVar", null, null, tester.Server.Ids.Base.Root, null),
                new UAVariable(tester.Server.Ids.Custom.Array, "Array", null, null, tester.Server.Ids.Custom.Root, null),
                new UAVariable(tester.Server.Ids.Custom.StringArray, "StringArray", null, null, tester.Server.Ids.Custom.Root, null),
                new UAVariable(tester.Server.Ids.Custom.EUProp, "EUProp", null, null, tester.Server.Ids.Custom.Root, null),
                new UAVariable(tester.Server.Ids.Custom.RangeProp, "RangeProp", null, null, tester.Server.Ids.Custom.Root, null)
            };
            nodes[1].IsRawProperty = true;
            nodes[2].IsRawProperty = true;
            nodes[3].IsRawProperty = true;
            nodes[4].IsRawProperty = true;

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
                new UAVariable(tester.Server.Ids.Base.DoubleVar1, "DoubleVar1", null, null, tester.Server.Ids.Base.Root, null),
                new UAVariable(new NodeId("missing-node", 0), "MissingNode", null, null, tester.Server.Ids.Base.Root, null),
                new UAVariable(new NodeId("missing-node2", 0), "MissingNode2", null, null, tester.Server.Ids.Base.Root, null),
            };

            await tester.Client.ReadNodeData(nodes, tester.Source.Token);

            Assert.NotNull(nodes[0].FullAttributes.DataType);
            Assert.False(nodes[0].Ignore);
            Assert.True(nodes[1].FullAttributes.DataType.Id.IsNullNodeId);
            Assert.True(nodes[2].FullAttributes.DataType.Id.IsNullNodeId);
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
                    var historyData = result as HistoryData;
                    Assert.Equal(600, historyData.DataValues.Count);
                    Assert.NotNull(node.ContinuationPoint);
                }

                await tester.Client.DoHistoryRead(req, tester.Source.Token);

                foreach (var node in nodes)
                {
                    var result = node.LastResult;
                    var historyData = result as HistoryData;
                    Assert.Equal(400, historyData.DataValues.Count);
                    Assert.Null(node.ContinuationPoint);
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
                .Select(id => new VariableExtractionState(tester.Client, new UAVariable(id, "somvar", null, null, tester.Server.Ids.Full.WideRoot, null), true, true, true))
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
                await new DataPointSubscriptionTask(handler, nodes.Take(1000), tester.Callbacks).Run(tester.Logger,
                    tester.Client.SessionManager, tester.Config, tester.Client.SubscriptionManager, tester.Source.Token);
                await new DataPointSubscriptionTask(handler, nodes.Skip(1000), tester.Callbacks).Run(tester.Logger,
                    tester.Client.SessionManager, tester.Config, tester.Client.SubscriptionManager, tester.Source.Token);

                await TestUtils.WaitForCondition(() => dps.Count == 2000, 5,
                    () => $"Expected to get 2000 datapoints, but got {dps.Count}");

                foreach (var node in nodes)
                {
                    tester.Server.UpdateNode(node.SourceId, 1.0);
                }

                await TestUtils.WaitForCondition(() => dps.Count == 4000, 5,
                    () => $"Expected to get 4000 datapoints, but got {dps.Count}");
            }
            finally
            {
                await tester.RemoveSubscription(SubscriptionName.DataPoints);
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
                        new UAVariable(id, "somevar", null, null, NodeId.Null, null), false, false, true))
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
                await new DataPointSubscriptionTask(handler, nodes, tester.Callbacks).Run(tester.Logger,
                    tester.Client.SessionManager, tester.Config, tester.Client.SubscriptionManager, tester.Source.Token);

                await TestUtils.WaitForCondition(() => dps.Count == 3, 5,
                    () => $"Expected to get 3 datapoints, but got {dps.Count}");

                update(1);

                await TestUtils.WaitForCondition(() => dps.Count == 6, 5,
                    () => $"Expected to get 6 datapoints, but got {dps.Count}");

                tester.Server.Server.DropSubscriptions();

                await TestUtils.WaitForCondition(() => dps.Count == 9, 20,
                    () => $"Expected to get 9 datapoints, but got {dps.Count}");

                update(2);

                await TestUtils.WaitForCondition(() => dps.Count == 12, 5,
                    () => $"Expected to get 12 datapoints, but got {dps.Count}");
            }
            finally
            {
                await tester.RemoveSubscription(SubscriptionName.DataPoints);
                tester.Server.WipeHistory(tester.Server.Ids.Custom.Array, new double[] { 0, 0, 0, 0 });
                tester.Server.WipeHistory(tester.Server.Ids.Custom.MysteryVar, null);
                tester.Server.WipeHistory(tester.Server.Ids.Base.StringVar, null);

                tester.Config.Source.PublishingInterval = 500;
            }
        }

        #endregion
        #region events
        [Fact]
        public async Task TestEventSubscriptions()
        {
            tester.Config.Events.Enabled = true;
            tester.Config.Subscriptions.QueueLength = 100;

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
                count++;
            }

            var uaNodeSource = new UANodeSource(tester.Logger, null!, tester.Client, tester.Client.TypeManager);

            try
            {
                await tester.Client.TypeManager.Initialize(uaNodeSource, tester.Source.Token);

                await new EventSubscriptionTask(handler, emitters.Take(2), tester.Client.BuildEventFilter(tester.Client.TypeManager.EventFields), tester.Callbacks)
                    .Run(tester.Logger, tester.Client.SessionManager, tester.Config, tester.Client.SubscriptionManager, tester.Source.Token);
                await new EventSubscriptionTask(handler, emitters.Skip(2), tester.Client.BuildEventFilter(tester.Client.TypeManager.EventFields), tester.Callbacks)
                    .Run(tester.Logger, tester.Client.SessionManager, tester.Config, tester.Client.SubscriptionManager, tester.Source.Token);

                tester.Server.TriggerEvents(0);

                await TestUtils.WaitForCondition(() => count == 11, 10,
                    () => $"Expected to get 11 events, but got {count}");

                tester.Server.TriggerEvents(1);

                await TestUtils.WaitForCondition(() => count == 22, 10,
                    () => $"Expected to get 22 events, but got {count}");
            }
            finally
            {
                tester.Config.Source.SubscriptionChunk = 1000;
                tester.Config.Events.Enabled = false;
                await tester.RemoveSubscription(SubscriptionName.Events);
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
            var uaNodeSource = new UANodeSource(tester.Logger, null!, tester.Client, tester.Client.TypeManager);
            try
            {
                await tester.Client.TypeManager.Initialize(uaNodeSource, tester.Source.Token);
                await new EventSubscriptionTask(handler, emitters, tester.Client.BuildEventFilter(tester.Client.TypeManager.EventFields), tester.Callbacks)
                    .Run(tester.Logger, tester.Client.SessionManager, tester.Config, tester.Client.SubscriptionManager, tester.Source.Token);

                tester.Server.TriggerEvents(0);

                await TestUtils.WaitForCondition(() => count == 6, 10,
                    () => $"Expected to get 6 events, but got {count}");
            }
            finally
            {
                tester.Config.Source.SubscriptionChunk = 1000;
                tester.Config.Events.Enabled = false;
                tester.Config.Events.EventIds = null;
                await tester.RemoveSubscription(SubscriptionName.Events);
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

            tester.Server.SetEventConfig(true, true, true);

            try
            {
                await new AuditSubscriptionTask(handler, tester.Callbacks)
                    .Run(tester.Logger, tester.Client.SessionManager, tester.Config, tester.Client.SubscriptionManager, tester.Source.Token);

                tester.Server.DirectGrowth();

                await TestUtils.WaitForCondition(() => count == 2, 10,
                    () => $"Expected to get 2 events, but got {count}");

                tester.Server.ReferenceGrowth();

                await TestUtils.WaitForCondition(() => count == 6, 10,
                    () => $"Expected to get 6 events, but got {count}");
            }
            finally
            {
                tester.Server.SetEventConfig(false, true, false);
                await tester.RemoveSubscription(SubscriptionName.Audit);
            }
        }
        #endregion
        #region utils
        [Fact]
        public void TestExpandedNodeIdConversion()
        {
            var nodeId = new ExpandedNodeId("string-ns", tester.Client.NamespaceTable.GetString(2));
            Assert.Equal(new NodeId("string-ns", 2), tester.Client.Context.ToNodeId(nodeId));
            nodeId = new ExpandedNodeId(new byte[] { 12, 12, 6 }, 1);
            Assert.Equal(new NodeId(new byte[] { 12, 12, 6 }, 1), tester.Client.Context.ToNodeId(nodeId));
            nodeId = new ExpandedNodeId("other-server", 0, "opc.tcp://some-other-server.test", 1);
            Assert.Null(tester.Client.Context.ToNodeId(nodeId));
        }
        [Fact]
        public void TestNodeIdConversion()
        {
            var nodeId = tester.Client.Context.ToNodeId("i=123", tester.Client.NamespaceTable.GetString(2));
            Assert.Equal(new NodeId(123u, 2), nodeId);
            nodeId = tester.Client.Context.ToNodeId("s=abc", tester.Client.NamespaceTable.GetString(1));
            Assert.Equal(new NodeId("abc", 1), nodeId);
            nodeId = tester.Client.Context.ToNodeId("s=abcd", "some-namespaces-that-doesnt-exist");
            Assert.Equal(NodeId.Null, nodeId);
            nodeId = tester.Client.Context.ToNodeId("s=bcd", "tl:");
            Assert.Equal(new NodeId("bcd", 2), nodeId);
            Assert.Equal(NodeId.Null, tester.Client.Context.ToNodeId("i=123", null));
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


            var overrides = tester.Client.Context.GetType().GetField("nodeOverrides",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .GetValue(tester.Client.Context) as Dictionary<NodeId, string>;
            overrides.Add(new NodeId(1234, 2), "override");
            Assert.Equal("override", tester.Client.GetUniqueId(new NodeId(1234, 2)));
            Assert.Equal("override[123]", tester.Client.GetUniqueId(new NodeId(1234, 2), 123));
            overrides.Clear();
        }

        [Fact]
        public void TestRetryConfig()
        {
            var retryConfig = new UARetryConfig();
            retryConfig.RetryStatusCodes = new[]
            {
                "BadNotConnected",
                "BadWouldBlock",
                0x80B60000.ToString(),
                "0x80890000"
            };

            Assert.Contains(StatusCodes.BadNotConnected, retryConfig.FinalRetryStatusCodes);
            Assert.Contains(StatusCodes.BadWouldBlock, retryConfig.FinalRetryStatusCodes);
            Assert.Contains(StatusCodes.BadSyntaxError, retryConfig.FinalRetryStatusCodes);

            Assert.Throws<ConfigurationException>(() =>
            {
                retryConfig.RetryStatusCodes = new[]
                {
                    "BadSomethingWeirdoHappened"
                };
            });
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
            var mgr = new NodeMetricsManager(tester.Client, tester.Config.Metrics.Nodes);
            await mgr.StartNodeMetrics(tester.Client.TypeManager, tester.Source.Token);

            await tester.Client.SubscriptionManager.WaitForAllCurrentlyPendingTasks(tester.Source.Token);

            tester.Server.SetDiagnosticsEnabled(true);

            await TestUtils.WaitForCondition(() => CommonTestUtils.GetMetricValue("opcua_node_CurrentSessionCount") >= 1, 20);

            await tester.RemoveSubscription(SubscriptionName.NodeMetrics);
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
            var mgr = new NodeMetricsManager(tester.Client, tester.Config.Metrics.Nodes);
            await mgr.StartNodeMetrics(tester.Client.TypeManager, tester.Source.Token);

            await tester.Client.SubscriptionManager.WaitForAllCurrentlyPendingTasks(tester.Source.Token);

            tester.Server.UpdateNode(ids.DoubleVar1, 15);
            await TestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_node_Variable_1", 15), 5);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_Variable_2", 0));

            tester.Server.UpdateNode(ids.DoubleVar2, 25);
            await TestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_node_Variable_2", 25), 5);

            tester.Server.UpdateNode(ids.DoubleVar1, 0);
            tester.Server.UpdateNode(ids.DoubleVar2, 0);
            await tester.RemoveSubscription(SubscriptionName.NodeMetrics);
            tester.Config.Metrics.Nodes = null;
        }
        #endregion
    }
}

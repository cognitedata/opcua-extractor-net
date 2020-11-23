using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Opc.Ua.Client;
using Server;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
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
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, 62000);
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
            Assert.Equal(4, tester.Client.NamespaceTable.Count);
            Assert.Equal("opc.tcp://test.localhost", tester.Client.NamespaceTable.GetString(2));
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
        [Fact]
        public async Task TestCertificatePath()
        {
            if (File.Exists("./Certificates-test/"))
            {
                Directory.Delete("./certificates-test/", true);
            }
            tester.Client.Close();
            try
            {
                Environment.SetEnvironmentVariable("OPCUA_CERTIFICATE_DIR", "certificates-test");
                await tester.Client.Run(tester.Source.Token);
                var dir = new DirectoryInfo("./certificates-test/pki/trusted/certs/");
                Assert.Single(dir.GetFiles());
            }
            finally
            {
                tester.Client.Close();
                Environment.SetEnvironmentVariable("OPCUA_CERTIFICATE_DIR", null);
                Directory.Delete("./certificates-test/", true);
                await tester.Client.Run(tester.Source.Token);
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
            Assert.Equal(7, children.Count);

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
        [Fact]
        #endregion

        #region nodedata
        public void TestReadNodeData()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            var nodes = new BufferedNode[]
            {
                new BufferedNode(tester.Server.Ids.Full.Root, "FullRoot", ObjectIds.ObjectsFolder),
                new BufferedNode(tester.Server.Ids.Event.Obj1, "Object 1", tester.Server.Ids.Event.Root),
                new BufferedNode(tester.Server.Ids.Custom.Root, "CustomRoot", ObjectIds.ObjectsFolder),
                new BufferedVariable(tester.Server.Ids.Custom.StringyVar, "StringyVar", tester.Server.Ids.Custom.Root),
                new BufferedVariable(tester.Server.Ids.Custom.Array, "Array", tester.Server.Ids.Custom.Root),
                new BufferedVariable(tester.Server.Ids.Custom.ObjProp, "ObjProp", tester.Server.Ids.Custom.Obj2) { IsProperty = true }
            };
            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = -1;
            tester.Config.Events.Enabled = true;
            try
            {
                tester.Client.ReadNodeData(nodes, tester.Source.Token);
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
            Assert.Equal(tester.Server.Ids.Custom.StringyType, (nodes[3] as BufferedVariable).DataType.Raw);
            Assert.Equal(4, (nodes[4] as BufferedVariable).ArrayDimensions[0]);
            Assert.Single((nodes[4] as BufferedVariable).ArrayDimensions);
            Assert.True((nodes[4] as BufferedVariable).Historizing);
            Assert.Null((nodes[5] as BufferedVariable).ArrayDimensions);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 1));
        }
        [Fact]
        public void TestReadNodeDataChunk()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            int start = (int)(uint)tester.Server.Ids.Full.WideRoot.Identifier;
            var nodes = Enumerable.Range(start + 1, 2000)
                .Select(idf => new NodeId((uint)idf, 2))
                .Select(id => new BufferedVariable(id, "subnode", tester.Server.Ids.Full.WideRoot))
                .ToList();
            tester.Config.Source.AttributesChunk = 100;
            tester.Config.History.Enabled = true;
            try
            {
                tester.Client.ReadNodeData(nodes, tester.Source.Token);
            }
            finally
            {
                tester.Config.Source.AttributesChunk = 1000;
                tester.Config.History.Enabled = false;
            }
            Assert.All(nodes, node => Assert.Equal(DataTypeIds.Double, node.DataType.Raw));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 80));
        }
        [Fact]
        public void TestReadRawValues()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            var ids = new[]
            {
                tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Custom.StringArray,
                tester.Server.Ids.Custom.EUProp,
                tester.Server.Ids.Custom.RangeProp
            };
            var values = tester.Client.ReadRawValues(ids, tester.Source.Token);
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
        public void TestReadRawValuesChunk()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests");
            int start = (int)(uint)tester.Server.Ids.Full.WideRoot.Identifier;
            var nodes = Enumerable.Range(start + 1, 2000)
                .Select(idf => new NodeId((uint)idf, 2)).ToList();

            tester.Config.Source.AttributesChunk = 100;
            try
            {
                var values = tester.Client.ReadRawValues(nodes, tester.Source.Token);
                Assert.All(nodes, node => {
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
        public void TestReadNodeValues()
        {
            var nodes = new[]
            {
                new BufferedVariable(tester.Server.Ids.Base.DoubleVar1, "DoubleVar", tester.Server.Ids.Base.Root),
                new BufferedVariable(tester.Server.Ids.Custom.Array, "Array", tester.Server.Ids.Custom.Root) { IsProperty = true },
                new BufferedVariable(tester.Server.Ids.Custom.StringArray, "StringArray", tester.Server.Ids.Custom.Root) { IsProperty = true },
                new BufferedVariable(tester.Server.Ids.Custom.EUProp, "EUProp", tester.Server.Ids.Custom.Root) { IsProperty = true },
                new BufferedVariable(tester.Server.Ids.Custom.RangeProp, "RangeProp", tester.Server.Ids.Custom.Root) { IsProperty = true }
            };

            // Need to read attributes first for this, to get proper conversion we need the datatype.
            tester.Client.ReadNodeData(nodes, tester.Source.Token);
            tester.Client.ReadNodeValues(nodes, tester.Source.Token);

            Assert.Equal(0.0, nodes[0].Value.DoubleValue);
            Assert.Equal("[0, 0, 0, 0]", nodes[1].Value.StringValue);
            Assert.Equal("[test1, test2]", nodes[2].Value.StringValue);
            Assert.Equal("°C: degree Celsius", nodes[3].Value.StringValue);
            Assert.Equal("(0, 100)", nodes[4].Value.StringValue);
        }

        [Fact]
        public void TestGetNodeProperties()
        {
            CommonTestUtils.ResetMetricValues("opcua_attribute_requests", "opcua_browse_operations");
            var arrayVar = new BufferedVariable(tester.Server.Ids.Custom.Array, "Array", tester.Server.Ids.Custom.Root)
            {
                ArrayDimensions = new Collection<int> { 4 }
            };
            var nodes = new[]
            {
                // Normal variable
                new BufferedVariable(tester.Server.Ids.Base.DoubleVar1, "DoubleVar", tester.Server.Ids.Base.Root),
                // Array root
                arrayVar,
                // Array element
                arrayVar.CreateArrayChildren().First(),
                // Variable with properties
                new BufferedVariable(tester.Server.Ids.Custom.MysteryVar, "NumberVar", tester.Server.Ids.Custom.Root),
                // object with no children
                new BufferedNode(tester.Server.Ids.Custom.Root, "CustomRoot", ObjectIds.ObjectsFolder),
                // object with properties
                new BufferedNode(tester.Server.Ids.Custom.Obj2, "Object", tester.Server.Ids.Custom.Root)
                {
                    Properties = new List<BufferedVariable>
                    {
                        new BufferedVariable(tester.Server.Ids.Custom.EUProp, "EUProp", tester.Server.Ids.Custom.Obj2) { IsProperty = true },
                        new BufferedVariable(tester.Server.Ids.Custom.RangeProp, "Range", tester.Server.Ids.Custom.Obj2) { IsProperty = true },
                    }
                }
            };

            tester.Client.GetNodeProperties(nodes, tester.Source.Token);
            Assert.Equal(2, nodes[0].Properties.Count);
            Assert.Equal(2, nodes[1].Properties.Count);
            Assert.Equal(2, nodes[2].Properties.Count);
            Assert.Equal(nodes[1].Properties, nodes[2].Properties);
            Assert.Equal(2, nodes[3].Properties.Count);
            Assert.Null(nodes[4].Properties);
            Assert.Equal(2, nodes[5].Properties.Count);
            Assert.NotNull(nodes[5].Properties.First().Value);
            Assert.NotNull(nodes[5].Properties.Last().Value);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_browse_operations", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_attribute_requests", 2));
        }
        #endregion

        #region synchronization
        // This just tests the actual history-read method in UAClient, further tests should use the HistoryReader
        [Fact]
        public void TestHistoryReadData()
        {
            CommonTestUtils.ResetMetricValues("opcua_history_reads");

            var req = new HistoryReadParams(new[] { tester.Server.Ids.Custom.Array, tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Base.StringVar },
                new ReadRawModifiedDetails
                {
                    IsReadModified = false,
                    StartTime = DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(200)),
                    EndTime = DateTime.UtcNow,
                    NumValuesPerNode = 100
                });
            tester.Server.PopulateArrayHistory();
            tester.Server.Server.PopulateHistory(tester.Server.Ids.Base.StringVar, 1000, "string");


            IEnumerable<(NodeId Id, IEncodeable RawData)> results;
            try
            {
                results = tester.Client.DoHistoryRead(req);
            }
            finally
            {
                tester.Server.WipeHistory(tester.Server.Ids.Custom.Array, new double[] { 0, 0, 0, 0 });
                tester.Server.WipeHistory(tester.Server.Ids.Custom.MysteryVar, null);
                tester.Server.WipeHistory(tester.Server.Ids.Base.StringVar, null);
            }
            Assert.Equal(3, results.Count());
            Assert.True(CommonTestUtils.TestMetricValue("opcua_history_reads", 1));

            foreach (var result in results)
            {
                var historyData = result.RawData as HistoryData;
                Assert.Equal(100, historyData.DataValues.Count);
                Assert.False(req.Completed[result.Id]);
                Assert.NotNull(req.ContinuationPoints[result.Id]);
            }

        }
        [Fact]
        public async Task TestDataSubscriptions()
        {
            CommonTestUtils.ResetMetricValues("opcua_subscriptions");
            int start = (int)(uint)tester.Server.Ids.Full.WideRoot.Identifier;
            var nodes = Enumerable.Range(start + 1, 2000)
                .Select(idf => new NodeId((uint)idf, 2))
                .Select(id => new NodeExtractionState(tester.Client, new BufferedVariable(id, "somvar", tester.Server.Ids.Full.WideRoot), true, true, false))
                .ToList();

            var dps = new List<DataValue>();

            void handler(MonitoredItem item, MonitoredItemNotificationEventArgs _)
            {
                var values = item.DequeueValues();
                dps.AddRange(values);
            }

            tester.Config.Source.SubscriptionChunk = 100;

            try
            {
                tester.Client.SubscribeToNodes(nodes.Take(1000), handler, tester.Source.Token);
                tester.Client.SubscribeToNodes(nodes.Skip(1000), handler, tester.Source.Token);

                await CommonTestUtils.WaitForCondition(() => dps.Count == 2000, 10, 
                    () => $"Expected to get 2000 datapoints, but got {dps.Count}");

                foreach (var node in nodes)
                {
                    tester.Server.UpdateNode(node.SourceId, 1.0);
                }

                await CommonTestUtils.WaitForCondition(() => dps.Count == 4000, 10,
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
        #endregion
        #region events
        // Just basic testing here, separate tests should be written for the event field collector itself.
        [Fact]
        public void TestGetEventFields()
        {
            tester.Config.Events.Enabled = true;

            try
            {
                var fields = tester.Client.GetEventFields(tester.Source.Token);
                Assert.True(fields.ContainsKey(tester.Server.Ids.Event.CustomType));
                Assert.True(fields.ContainsKey(ObjectTypeIds.AuditActivateSessionEventType));
            }
            finally
            {
                tester.Config.Events.Enabled = false;
                tester.Client.ClearEventFields();
            }
        }
        [Fact]
        public async Task TestEventSubscriptions()
        {
            tester.Config.Events.Enabled = true;

            var emitters = new[]
            {
                new EventExtractionState(tester.Client, ObjectIds.Server, true, true, false),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj1, true, true, false),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj2, true, true, false)
            };
            tester.Config.Source.SubscriptionChunk = 1;

            int count = 0;

            void handler(MonitoredItem _, MonitoredItemNotificationEventArgs __)
            {
                count++;
            }

            try
            {
                tester.Client.GetEventFields(tester.Source.Token);

                tester.Client.SubscribeToEvents(emitters.Take(2), handler, tester.Source.Token);
                tester.Client.SubscribeToEvents(emitters.Skip(2), handler, tester.Source.Token);

                tester.Server.TriggerEvents(0);

                await CommonTestUtils.WaitForCondition(() => count == 11, 10,
                    () => $"Expected to get 11 events, but got {count}");

                tester.Server.TriggerEvents(1);

                await CommonTestUtils.WaitForCondition(() => count == 22, 10,
                    () => $"Expected to get 22 events, but got {count}");
            }
            finally
            {
                tester.Config.Source.SubscriptionChunk = 1000;
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
                tester.Client.SubscribeToAuditEvents(handler);

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

    }
}

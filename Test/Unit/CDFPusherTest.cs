using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Pushers;
using Xunit;
using Cognite.OpcUa.Types;
using Cognite.Extractor.Common;
using Opc.Ua;
using System.Reflection;
using Cognite.OpcUa.HistoryStates;
using System.Collections.ObjectModel;
using Com.Cognite.V1.Timeseries.Proto;
using Cognite.OpcUa.TypeCollectors;

namespace Test.Unit
{
    public sealed class CDFPusherTestFixture : BaseExtractorTestFixture
    {
        public CDFPusherTestFixture() : base(62900)
        {
        }
        public (CDFMockHandler, CDFPusher) GetPusher()
        {
            var handler = new CDFMockHandler("test", CDFMockHandler.MockMode.None);
            handler.StoreDatapoints = true;
            CommonTestUtils.AddDummyProvider(handler, Services);
            Services.AddCogniteClient("appid", true, true, false);
            var provider = Services.BuildServiceProvider();
            var pusher = Config.Cognite.ToPusher(provider) as CDFPusher;
            return (handler, pusher);
        }
    }
    public class CDFPusherTest : MakeConsoleWork, IClassFixture<CDFPusherTestFixture>
    {
        private CDFPusherTestFixture tester;
        private CDFMockHandler handler;
        private CDFPusher pusher;
        public CDFPusherTest(ITestOutputHelper output, CDFPusherTestFixture tester) : base(output)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
            this.tester = tester;
            (handler, pusher) = tester.GetPusher();
        }
        [Fact]
        public async Task TestTestConnection()
        {
            handler.AllowConnectionTest = false;

            tester.Config.Cognite.Debug = true;
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            tester.Config.Cognite.Debug = false;

            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));

            handler.AllowConnectionTest = true;

            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            handler.FailedRoutes.Add("/timeseries/list");

            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));

            handler.FailedRoutes.Clear();
            handler.FailedRoutes.Add("/events/list");

            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            tester.Config.Events.Enabled = true;

            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));

            handler.FailedRoutes.Clear();
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            tester.Config.Events.Enabled = false;
        }
        [Fact]
        public async Task TestPushDatapoints()
        {
            CommonTestUtils.ResetMetricValues("opcua_datapoint_push_failures_cdf",
                "opcua_missing_timeseries", "opcua_mismatched_timeseries",
                "opcua_datapoints_pushed_cdf", "opcua_datapoint_pushes_cdf");

            handler.MockTimeseries("test-ts-double");
            var stringTs = handler.MockTimeseries("test-ts-string");
            stringTs.isString = true;

            // Null input
            Assert.Null(await pusher.PushDataPoints(null, tester.Source.Token));

            // Test filtering out dps
            var invalidDps = new[]
            {
                new UADataPoint(DateTime.MinValue, "test-ts-double", 123),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.NaN),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.NegativeInfinity),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", double.PositiveInfinity),
            };
            Assert.Null(await pusher.PushDataPoints(invalidDps, tester.Source.Token));

            tester.Config.Cognite.Debug = true;

            var time = DateTime.UtcNow;

            var dps = new[]
            {
                new UADataPoint(time, "test-ts-double", 123),
                new UADataPoint(time.AddSeconds(1), "test-ts-double", 321),
                new UADataPoint(time, "test-ts-string", "string"),
                new UADataPoint(time.AddSeconds(1), "test-ts-string", "string2"),
                new UADataPoint(time, "test-ts-missing", "value")
            };

            // Debug true
            Assert.Null(await pusher.PushDataPoints(dps, tester.Source.Token));

            tester.Config.Cognite.Debug = false;

            handler.FailedRoutes.Add("/timeseries/data");

            // Thrown error
            Assert.False(await pusher.PushDataPoints(dps, tester.Source.Token));

            handler.FailedRoutes.Clear();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_push_failures_cdf", 1));

            // Missing timeseries, but the others should succeed
            Assert.True(await pusher.PushDataPoints(dps, tester.Source.Token));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_missing_timeseries", 1));

            Assert.Equal(2, handler.Datapoints["test-ts-double"].NumericDatapoints.Count);
            Assert.Equal(2, handler.Datapoints["test-ts-string"].StringDatapoints.Count);

            Assert.Equal(time.ToUnixTimeMilliseconds(), handler.Datapoints["test-ts-double"].NumericDatapoints.First().Timestamp);
            Assert.Equal(123, handler.Datapoints["test-ts-double"].NumericDatapoints.First().Value);
            Assert.Equal(time.ToUnixTimeMilliseconds(), handler.Datapoints["test-ts-string"].StringDatapoints.First().Timestamp);
            Assert.Equal("string", handler.Datapoints["test-ts-string"].StringDatapoints.First().Value);

            Assert.Equal(2, handler.Datapoints.Count);

            // Mismatched timeseries
            dps = new[]
            {
                new UADataPoint(time.AddSeconds(2), "test-ts-double", "string"),
                new UADataPoint(time.AddSeconds(3), "test-ts-double", "string2"),
                new UADataPoint(time.AddSeconds(2), "test-ts-string", "string3"),
                new UADataPoint(time.AddSeconds(3), "test-ts-string", "string4"),
                new UADataPoint(time, "test-ts-missing", "value")
            };
            Assert.True(await pusher.PushDataPoints(dps, tester.Source.Token));

            Assert.True(CommonTestUtils.TestMetricValue("opcua_mismatched_timeseries", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_missing_timeseries", 1));

            Assert.Equal(2, handler.Datapoints["test-ts-double"].NumericDatapoints.Count);
            Assert.Equal(4, handler.Datapoints["test-ts-string"].StringDatapoints.Count);

            // Final batch, all should now be filtered off
            invalidDps = new[]
            {
                new UADataPoint(DateTime.UtcNow, "test-ts-double", 123),
                new UADataPoint(time, "test-ts-double", 123),                
                new UADataPoint(time, "test-ts-missing", "value")
            };
            Assert.Null(await pusher.PushDataPoints(invalidDps, tester.Source.Token));

            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoints_pushed_cdf", 6));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_pushes_cdf", 2));
        }
        [Fact]
        public async Task TestPushEvents()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);

            CommonTestUtils.ResetMetricValues("opcua_event_push_failures_cdf",
                "opcua_events_pushed_cdf", "opcua_event_pushes_cdf");

            Assert.Null(await pusher.PushEvents(null, tester.Source.Token));
            var invalidEvents = new[]
            {
                new UAEvent
                {
                    Time = DateTime.MinValue
                },
                new UAEvent
                {
                    Time = DateTime.MaxValue
                }
            };
            Assert.Null(await pusher.PushEvents(invalidEvents, tester.Source.Token));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_skipped_events_cdf", 2));

            var nodeToAssetIds = (Dictionary<NodeId, long>)pusher.GetType()
                .GetField("nodeToAssetIds", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);

            nodeToAssetIds[new NodeId("source")] = 123;

            var time = DateTime.UtcNow;

            var events = new[]
            {
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter"),
                    SourceNode = new NodeId("source"),
                    EventType = new NodeId("type"),
                    EventId = "someid"
                },
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter"),
                    SourceNode = new NodeId("missingsource"),
                    EventType = new NodeId("type"),
                    EventId = "someid2"
                }
            };

            tester.Config.Cognite.Debug = true;
            Assert.Null(await pusher.PushEvents(events, tester.Source.Token));
            tester.Config.Cognite.Debug = false;

            handler.FailedRoutes.Add("/events");
            Assert.False(await pusher.PushEvents(events, tester.Source.Token));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_event_push_failures_cdf", 1));
            handler.FailedRoutes.Clear();

            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            Assert.Equal(2, handler.Events.Count);
            Assert.Equal(123, handler.Events.First().Value.assetIds.First());
            Assert.Null(handler.Events.Last().Value.assetIds);

            events = events.Append(new UAEvent
            {
                Time = time,
                EmittingNode = new NodeId("emitter"),
                SourceNode = new NodeId("source"),
                EventType = new NodeId("type"),
                EventId = "someid3"
            }).ToArray();

            Assert.True(await pusher.PushEvents(events, tester.Source.Token));
            Assert.Equal(3, handler.Events.Count);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_event_pushes_cdf", 2));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_events_pushed_cdf", 3));
        }
        #region pushnodes
        [Fact]
        public async Task TestCreateUpdateAssets()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");
            tester.Config.Cognite.RawMetadata = null;

            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();
            Assert.True(await pusher.PushNodes(Enumerable.Empty<UANode>(), tss, update, tester.Source.Token));

            // Test debug mode
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null);
            tester.Config.Cognite.Debug = true;
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            tester.Config.Cognite.Debug = false;
            Assert.Empty(handler.Assets);

            // Fail to create assets
            node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null);
            handler.FailedRoutes.Add("/assets");
            Assert.False(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            handler.FailedRoutes.Clear();

            // Create the asset
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            Assert.Single(handler.Assets);

            // Do nothing here, due to no update configured.
            handler.FailedRoutes.Add("/assets/update");
            node.Description = "description";
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));

            // Do nothing again, due to no changes on the node
            update.Objects.Context = true;
            update.Objects.Description = true;
            update.Objects.Metadata = true;
            update.Objects.Name = true;
            node.Description = null;
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));

            // Fail due to failed update, but the other will still be created
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", NodeId.Null);
            node.Description = "description";
            Assert.False(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            Assert.Equal(2, handler.Assets.Count);
            Assert.Null(handler.Assets.First().Value.description);
            Assert.Null(handler.Assets.Last().Value.description);

            // Update both nodes
            handler.FailedRoutes.Clear();
            node2.Description = "description";
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            Assert.Equal(2, handler.Assets.Count);
            Assert.Equal("description", handler.Assets.First().Value.description);
            Assert.Equal("description", handler.Assets.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 2));
        }
        [Fact]
        public async Task TestCreateRawAssets()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                AssetsTable = "assets",
                Database = "metadata"
            };
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null);
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();
            // Fail to create
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/assets/rows");
            Assert.False(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            Assert.Empty(handler.AssetRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            Assert.Single(handler.AssetRaw);
            Assert.Equal("BaseRoot", handler.AssetRaw.First().Value.name);

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", NodeId.Null);
            node.Description = "description";
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            Assert.Equal(2, handler.AssetRaw.Count);
            Assert.Null(handler.AssetRaw.First().Value.description);
            Assert.Null(handler.AssetRaw.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
            tester.Config.Cognite.RawMetadata = null;
        }
        [Fact]
        public async Task TestUpdateRawAssets()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                AssetsTable = "assets",
                Database = "metadata"
            };
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null);
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();
            update.Objects.Context = true;
            update.Objects.Description = true;
            update.Objects.Metadata = true;
            update.Objects.Name = true;

            // Fail to upsert
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/assets/rows");
            Assert.False(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            Assert.Empty(handler.AssetRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            Assert.Single(handler.AssetRaw);
            Assert.Equal("BaseRoot", handler.AssetRaw.First().Value.name);

            // Create another, overwrite the existing one
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", NodeId.Null);
            node.Description = "description";
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            Assert.Equal(2, handler.AssetRaw.Count);
            Assert.Single(handler.AssetRaw, asset => asset.Value.description == "description");

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
            tester.Config.Cognite.RawMetadata = null;
        }
        [Fact]
        public async Task TestCreateUpdateTimeseries()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");
            tester.Config.Cognite.RawMetadata = null;

            var dt = new UADataType(DataTypeIds.Double);

            var nodeToAssetIds = (Dictionary<NodeId, long>)pusher.GetType()
                .GetField("nodeToAssetIds", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);
            nodeToAssetIds[new NodeId("parent")] = 123;

            var assets = Enumerable.Empty<UANode>();
            var update = new UpdateConfig();

            // Test debug mode
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent")) { DataType = dt };
            tester.Config.Cognite.Debug = true;
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Equal(2, node.Properties.Count);
            tester.Config.Cognite.Debug = false;
            Assert.Empty(handler.Timeseries);

            // Fail to create timeseries, should still result in properties being read.
            node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent")) { DataType = dt };
            handler.FailedRoutes.Add("/timeseries");
            Assert.False(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Equal(2, node.Properties.Count);
            handler.FailedRoutes.Clear();
            Assert.Empty(handler.Timeseries);

            // Create the timeseries
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Single(handler.Timeseries);
            Assert.Equal(123, handler.Timeseries.First().Value.assetId);

            // Do nothing due to no configured update
            handler.FailedRoutes.Add("/timeseries/update");
            node.Description = "description";
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));

            // Do nothing again due to no changes on the node
            update.Variables.Context = true;
            update.Variables.Description = true;
            update.Variables.Metadata = true;
            update.Variables.Name = true;
            node.Description = null;
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));

            // Create one, fail to update the other
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", new NodeId("parent")) { DataType = dt };
            node.Description = "description";
            Assert.False(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            Assert.Equal(2, handler.Timeseries.Count);
            Assert.Null(handler.Timeseries.First().Value.description);
            Assert.Null(handler.Timeseries.Last().Value.description);

            // Update both nodes
            handler.FailedRoutes.Clear();
            node2.Description = "description";
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            Assert.Equal(2, handler.Timeseries.Count);
            Assert.Equal("description", handler.Timeseries.First().Value.description);
            Assert.Equal("description", handler.Timeseries.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 2));
        }
        [Fact]
        public async Task TestCreateRawTimeseries()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                TimeseriesTable = "timeseries",
                Database = "metadata"
            };

            var dt = new UADataType(DataTypeIds.Double);

            var nodeToAssetIds = (Dictionary<NodeId, long>)pusher.GetType()
                .GetField("nodeToAssetIds", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);
            nodeToAssetIds[new NodeId("parent")] = 123;

            var assets = Enumerable.Empty<UANode>();
            var update = new UpdateConfig();
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent")) { DataType = dt };

            // Fail to create
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/timeseries/rows");
            Assert.False(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Empty(handler.TimeseriesRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Single(handler.TimeseriesRaw);
            Assert.Equal("Variable 1", handler.TimeseriesRaw.First().Value.name);

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", new NodeId("parent")) { DataType = dt };
            node.Description = "description";
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Null(handler.TimeseriesRaw.First().Value.description);
            Assert.Null(handler.TimeseriesRaw.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
            tester.Config.Cognite.RawMetadata = null;
        }
        [Fact]
        public async Task TestUpdateRawTimeseries()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                TimeseriesTable = "timeseries",
                Database = "metadata"
            };

            var dt = new UADataType(DataTypeIds.Double);

            var nodeToAssetIds = (Dictionary<NodeId, long>)pusher.GetType()
                .GetField("nodeToAssetIds", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);
            nodeToAssetIds[new NodeId("parent")] = 123;

            var assets = Enumerable.Empty<UANode>();
            var update = new UpdateConfig();
            update.Variables.Context = true;
            update.Variables.Description = true;
            update.Variables.Metadata = true;
            update.Variables.Name = true;
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent")) { DataType = dt };

            // Fail to upsert
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/timeseries/rows");
            Assert.False(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Empty(handler.TimeseriesRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Single(handler.TimeseriesRaw);
            Assert.Equal("Variable 1", handler.TimeseriesRaw.First().Value.name);

            // Create another, overwrite the existing due to update settings
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", new NodeId("parent")) { DataType = dt };
            node.Description = "description";
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Contains(handler.TimeseriesRaw, ts => ts.Value.description == "description");

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
            tester.Config.Cognite.RawMetadata = null;
        }
        #endregion
        [Fact]
        public async Task TestInitExtractedRanges()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            tester.Config.Cognite.ReadExtractedRanges = true;
            VariableExtractionState[] GetStates()
            {
                var state1 = new VariableExtractionState(tester.Client,
                new UAVariable(new NodeId("double"), "double", NodeId.Null) { DataType = new UADataType(DataTypeIds.Double) }, true, true);
                var state2 = new VariableExtractionState(tester.Client,
                    new UAVariable(new NodeId("string"), "string", NodeId.Null) { DataType = new UADataType(DataTypeIds.String) }, true, true);
                var state3 = new VariableExtractionState(tester.Client,
                    new UAVariable(new NodeId("array"), "array", NodeId.Null)
                    {
                        DataType = new UADataType(DataTypeIds.Double),
                        ArrayDimensions = new Collection<int> { 3 }
                    }, true, true);
                return new[] { state1, state2, state3 };
            }

            var states = GetStates();

            // Nothing in CDF
            // Failure
            handler.FailedRoutes.Add("/timeseries/data/latest");
            Assert.False(await pusher.InitExtractedRanges(states, true, tester.Source.Token));

            // Init missing
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.InitExtractedRanges(states, true, tester.Source.Token));
            foreach (var state in states)
            {
                Assert.Equal(TimeRange.Empty, state.DestinationExtractedRange);
            }

            handler.MockTimeseries("gp.base:s=double");
            var ts = handler.MockTimeseries("gp.base:s=string");
            ts.isString = true;
            handler.MockTimeseries("gp.base:s=array[0]");
            handler.MockTimeseries("gp.base:s=array[1]");
            handler.MockTimeseries("gp.base:s=array[2]");


            // Stuff in CDF
            states = GetStates();
            handler.Datapoints[states[0].Id] = (new List<NumericDatapoint>
            {
                new NumericDatapoint { Timestamp = 1000 },
                new NumericDatapoint { Timestamp = 2000 },
                new NumericDatapoint { Timestamp = 3000 }
            }, null);
            handler.Datapoints[states[1].Id] = (null, new List<StringDatapoint>
            {
                new StringDatapoint { Timestamp = 1000 },
                new StringDatapoint { Timestamp = 2000 },
                new StringDatapoint { Timestamp = 3000 }
            });
            handler.Datapoints[$"{states[2].Id}[0]"] = (new List<NumericDatapoint>
            {
                new NumericDatapoint { Timestamp = 1000 },
                new NumericDatapoint { Timestamp = 2000 }
            }, null);
            handler.Datapoints[$"{states[2].Id}[1]"] = (new List<NumericDatapoint>
            {
                new NumericDatapoint { Timestamp = 2000 },
                new NumericDatapoint { Timestamp = 3000 }
            }, null);

            // Failure
            handler.FailedRoutes.Add("/timeseries/data/latest");
            Assert.False(await pusher.InitExtractedRanges(states, true, tester.Source.Token));

            // Normal init
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.InitExtractedRanges(states, true, tester.Source.Token));
            var range = new TimeRange(CogniteTime.FromUnixTimeMilliseconds(1000), CogniteTime.FromUnixTimeMilliseconds(3000));
            Assert.Equal(range, states[0].DestinationExtractedRange);
            Assert.Equal(range, states[1].DestinationExtractedRange);
            Assert.Equal(TimeRange.Empty, states[2].DestinationExtractedRange);

            // Init array
            handler.Datapoints[$"{states[2].Id}[2]"] = (new List<NumericDatapoint>
            {
                new NumericDatapoint { Timestamp = 1000 },
                new NumericDatapoint { Timestamp = 2000 },
                new NumericDatapoint { Timestamp = 3000 }
            }, null);

            states = GetStates();
            Assert.True(await pusher.InitExtractedRanges(states, true, tester.Source.Token));
            Assert.Equal(range, states[0].DestinationExtractedRange);
            Assert.Equal(range, states[1].DestinationExtractedRange);
            Assert.Equal(CogniteTime.FromUnixTimeMilliseconds(2000), states[2].DestinationExtractedRange.First);
            Assert.Equal(CogniteTime.FromUnixTimeMilliseconds(2000), states[2].DestinationExtractedRange.Last);
        }
        [Fact]
        public async Task TestCreateRelationships()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            var mgr = new ReferenceTypeManager(tester.Client, extractor);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            // Push none
            Assert.True(await pusher.PushReferences(Enumerable.Empty<UAReference>(), tester.Source.Token));

            // Fail to push
            var references = new List<UAReference>
            {
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target2"), true, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target"), false, true, mgr),
            };
            await mgr.GetReferenceTypeDataAsync(tester.Source.Token);
            handler.FailedRoutes.Add("/relationships");
            Assert.False(await pusher.PushReferences(references, tester.Source.Token));
            Assert.Empty(handler.Relationships);

            // Push successful
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.PushReferences(references, tester.Source.Token));
            Assert.Equal(2, handler.Relationships.Count);

            // Push again, with duplicates
            var references2 = new List<UAReference>
            {
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), true, true, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target2"), false, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target2"), true, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target"), false, true, mgr)
            };
            Assert.True(await pusher.PushReferences(references2, tester.Source.Token));
            Assert.Equal(4, handler.Relationships.Count);
            var ids = new List<string>
            {
                "gp.Organizes;base:s=source;base:s=target",
                "gp.OrganizedBy;base:s=source2;base:s=target2",
                "gp.Organizes;base:s=source;base:s=target2",
                "gp.OrganizedBy;base:s=source2;base:s=target",
            };
            Assert.All(ids, id => Assert.Contains(handler.Relationships, rel => rel.Key == id));

            // Test pushing all duplicates
            Assert.True(await pusher.PushReferences(references, tester.Source.Token));

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
        }
        [Fact]
        public async Task TestCreateRawRelationships()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            var mgr = new ReferenceTypeManager(tester.Client, extractor);
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                RelationshipsTable = "relationships",
                Database = "metadata"
            };

            // Push none
            Assert.True(await pusher.PushReferences(Enumerable.Empty<UAReference>(), tester.Source.Token));

            // Fail to push
            var references = new List<UAReference>
            {
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target2"), true, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target"), false, true, mgr),
            };
            await mgr.GetReferenceTypeDataAsync(tester.Source.Token);
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/relationships/rows");
            Assert.False(await pusher.PushReferences(references, tester.Source.Token));
            Assert.Empty(handler.RelationshipsRaw);

            // Push successful
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.PushReferences(references, tester.Source.Token));
            Assert.Equal(2, handler.RelationshipsRaw.Count);

            // Push again, with duplicates
            var references2 = new List<UAReference>
            {
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), true, true, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target2"), false, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target2"), true, false, mgr),
                new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source2"), new NodeId("target"), false, true, mgr)
            };
            Assert.True(await pusher.PushReferences(references2, tester.Source.Token));
            Assert.Equal(4, handler.RelationshipsRaw.Count);
            var ids = new List<string>
            {
                "gp.Organizes;base:s=source;base:s=target",
                "gp.OrganizedBy;base:s=source2;base:s=target2",
                "gp.Organizes;base:s=source;base:s=target2",
                "gp.OrganizedBy;base:s=source2;base:s=target",
            };
            Assert.All(ids, id => Assert.Contains(handler.RelationshipsRaw, rel => rel.Key == id));

            // Test pushing all duplicates
            Assert.True(await pusher.PushReferences(references, tester.Source.Token));

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
        }
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                pusher.Dispose();
            }
        }
    }
}

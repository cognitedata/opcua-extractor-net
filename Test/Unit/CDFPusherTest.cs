using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Newtonsoft.Json;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class CDFPusherTestFixture : BaseExtractorTestFixture
    {
        public CDFPusherTestFixture() : base()
        {
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
            tester.ResetConfig();
            (handler, pusher) = tester.GetCDFPusher();
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

            Assert.Null(tester.Config.Cognite.DataSetId);
            handler.DataSets.Add("test-data-set", new DataSet
            {
                ExternalId = "test-data-set",
                Id = 123,
                CreatedTime = 1000,
                LastUpdatedTime = 1000
            });
            tester.Config.Cognite.DataSetExternalId = "test-data-set";

            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            Assert.Equal(123, tester.Config.Cognite.DataSetId);

            handler.FailedRoutes.Add("/datasets/byids");
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            tester.Config.Cognite.DataSetId = null;

            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));
            Assert.Null(tester.Config.Cognite.DataSetId);
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

            tester.Config.Cognite.Debug = true;

            var time = DateTime.UtcNow;

            var invalidDps = new[]
            {
                new UADataPoint(DateTime.MaxValue, "test-ts-double", 123),
                new UADataPoint(time.AddSeconds(1), "test-ts-double", double.NaN)
            };

            Assert.Null(await pusher.PushDataPoints(invalidDps, tester.Source.Token));

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
                "opcua_events_pushed_cdf", "opcua_event_pushes_cdf",
                "opcua_skipped_events_cdf");

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
                    EventType = new UAEventType(new NodeId("type"), "EventType"),
                    EventId = "someid"
                },
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter"),
                    SourceNode = new NodeId("missingsource"),
                    EventType = new UAEventType(new NodeId("type"), "EventType"),
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
                EventType = new UAEventType(new NodeId("type"), "EventType"),
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
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null, NodeClass.Object);
            tester.Config.Cognite.Debug = true;
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            tester.Config.Cognite.Debug = false;
            Assert.Empty(handler.Assets);

            // Fail to create assets
            node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null, NodeClass.Object);
            handler.FailedRoutes.Add("/assets");
            Assert.False(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            handler.FailedRoutes.Clear();

            // Create the asset
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));
            Assert.Single(handler.Assets);

            // Do nothing here, due to no update configured.
            handler.FailedRoutes.Add("/assets/update");
            node.Attributes.Description = "description";
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));

            // Do nothing again, due to no changes on the node
            update.Objects.Context = true;
            update.Objects.Description = true;
            update.Objects.Metadata = true;
            update.Objects.Name = true;
            node.Attributes.Description = null;
            Assert.True(await pusher.PushNodes(new[] { node }, tss, update, tester.Source.Token));

            // Fail due to failed update, but the other will still be created
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", NodeId.Null, NodeClass.Object);
            node.Attributes.Description = "description";
            Assert.False(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            Assert.Equal(2, handler.Assets.Count);
            Assert.Null(handler.Assets.First().Value.description);
            Assert.Null(handler.Assets.Last().Value.description);

            // Update both nodes
            handler.FailedRoutes.Clear();
            node2.Attributes.Description = "description";
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
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null, NodeClass.Object);
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
            Assert.Equal("BaseRoot", handler.AssetRaw.First().Value.GetProperty("name").GetString());

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", NodeId.Null, NodeClass.Object);
            node.Attributes.Description = "description";
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            Assert.Equal(2, handler.AssetRaw.Count);
            Assert.Null(handler.AssetRaw.First().Value.GetProperty("description").GetString());
            Assert.Null(handler.AssetRaw.Last().Value.GetProperty("description").GetString());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
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
            var node = new UANode(tester.Server.Ids.Base.Root, "BaseRoot", NodeId.Null, NodeClass.Object);
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
            Assert.Equal("BaseRoot", handler.AssetRaw.First().Value.GetProperty("name").GetString());

            // Create another, overwrite the existing one
            var node2 = new UANode(tester.Server.Ids.Custom.Root, "CustomRoot", NodeId.Null, NodeClass.Object);
            node.Attributes.Description = "description";
            Assert.True(await pusher.PushNodes(new[] { node, node2 }, tss, update, tester.Source.Token));
            Assert.Equal(2, handler.AssetRaw.Count);
            Assert.Single(handler.AssetRaw, asset => asset.Value.GetProperty("description").GetString() == "description");

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
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
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent"));
            node.VariableAttributes.DataType = dt;
            tester.Config.Cognite.Debug = true;
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Equal(2, node.Properties.Count());
            tester.Config.Cognite.Debug = false;
            Assert.Empty(handler.Timeseries);

            // Fail to create timeseries, should still result in properties being read.
            node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent"));
            node.VariableAttributes.DataType = dt;
            handler.FailedRoutes.Add("/timeseries");
            Assert.False(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Equal(2, node.Properties.Count());
            handler.FailedRoutes.Clear();
            Assert.Empty(handler.Timeseries);

            // Create the timeseries
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Single(handler.Timeseries);
            Assert.Equal(123, handler.Timeseries.First().Value.assetId);

            // Do nothing due to no configured update
            handler.FailedRoutes.Add("/timeseries/update");
            node.Attributes.Description = "description";
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));

            // Do nothing again due to no changes on the node
            update.Variables.Context = true;
            update.Variables.Description = true;
            update.Variables.Metadata = true;
            update.Variables.Name = true;
            node.Attributes.Description = null;
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));

            // Create one, fail to update the other
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", new NodeId("parent"));
            node2.VariableAttributes.DataType = dt;
            node.Attributes.Description = "description";
            Assert.False(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            Assert.Equal(2, handler.Timeseries.Count);
            Assert.Null(handler.Timeseries.First().Value.description);
            Assert.Null(handler.Timeseries.Last().Value.description);

            // Update both nodes
            handler.FailedRoutes.Clear();
            node2.Attributes.Description = "description";
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

            var assets = Enumerable.Empty<UANode>();
            var update = new UpdateConfig();
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent"));
            node.VariableAttributes.DataType = dt;

            // Fail to create
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/timeseries/rows");
            Assert.False(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Empty(handler.TimeseriesRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Single(handler.TimeseriesRaw);
            Assert.Equal("Variable 1", handler.TimeseriesRaw.First().Value.GetProperty("name").GetString());

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", new NodeId("parent"));
            node2.VariableAttributes.DataType = dt;
            node.Attributes.Description = "description";
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Null(handler.TimeseriesRaw.First().Value.GetProperty("description").GetString());
            Assert.Null(handler.TimeseriesRaw.Last().Value.GetProperty("description").GetString());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
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
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", new NodeId("parent"));
            node.VariableAttributes.DataType = dt;

            // Fail to upsert
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/timeseries/rows");
            Assert.False(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Empty(handler.TimeseriesRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True(await pusher.PushNodes(assets, new[] { node }, update, tester.Source.Token));
            Assert.Single(handler.TimeseriesRaw);
            Assert.Equal("Variable 1", handler.TimeseriesRaw.First().Value.GetProperty("name").GetString());

            // Create another, overwrite the existing due to update settings
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", new NodeId("parent"));
            node2.VariableAttributes.DataType = dt;
            node.Attributes.Description = "description";
            Assert.True(await pusher.PushNodes(assets, new[] { node, node2 }, update, tester.Source.Token));
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Contains(handler.TimeseriesRaw, ts => ts.Value.GetProperty("description").GetString() == "description");

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
        }
        #endregion

        #region other-methods
        [Fact]
        public async Task TestInitExtractedRanges()
        {
            using var extractor = tester.BuildExtractor(true, null, pusher);
            tester.Config.Cognite.ReadExtractedRanges = true;
            VariableExtractionState[] GetStates()
            {
                var state1 = new VariableExtractionState(tester.Client,
                    CommonTestUtils.GetSimpleVariable("double", new UADataType(DataTypeIds.Double)), true, true);
                var state2 = new VariableExtractionState(tester.Client,
                    CommonTestUtils.GetSimpleVariable("string", new UADataType(DataTypeIds.String)), true, true);
                var state3 = new VariableExtractionState(tester.Client,
                    CommonTestUtils.GetSimpleVariable("array", new UADataType(DataTypeIds.Double), 3), true, true);
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
                Assert.Equal(state.DestinationExtractedRange.First, state.DestinationExtractedRange.Last);
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
            var range = new Cognite.Extractor.Common.TimeRange(CogniteTime.FromUnixTimeMilliseconds(1000),
                CogniteTime.FromUnixTimeMilliseconds(3000));
            Assert.Equal(range, states[0].DestinationExtractedRange);
            Assert.Equal(range, states[1].DestinationExtractedRange);
            Assert.Equal(states[2].DestinationExtractedRange.First, states[2].DestinationExtractedRange.Last);

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
        #endregion

        #region node-source

        void NodeToRaw(UAExtractor extractor, UANode node, ConverterType type, bool ts)
        {
            var serializer = new Newtonsoft.Json.JsonSerializer();
            extractor.StringConverter.AddConverters(serializer, type);
            var id = extractor.GetUniqueId(node.Id, (node is UAVariable variable) ? variable.Index : -1);

            var sb = new StringBuilder();
            var sw = new StringWriter(sb);
            using var writer = new JsonTextWriter(sw);
            serializer.Serialize(writer, node);

            var val = System.Text.Json.JsonSerializer.Deserialize<JsonElement>(sb.ToString());
            if (ts)
            {
                handler.TimeseriesRaw[id] = val;
            }
            else
            {
                handler.AssetRaw[id] = val;
            }
        }

        [Fact]
        public async Task TestGetNodesFromCDF()
        {
            using var extractor = tester.BuildExtractor();

            tester.Config.Cognite.RawNodeBuffer = new CDFNodeSourceConfig
            {
                AssetsTable = "assets",
                TimeseriesTable = "timeseries",
                Database = "metadata",
                Enable = true
            };
            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;
            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 10;

            var source = new CDFNodeSource(tester.Config, extractor, tester.Client, pusher);

            // Nothing in CDF
            await source.ReadRawNodes(tester.Source.Token);
            var result = await source.ParseResults(tester.Source.Token);
            Assert.Null(result);

            // Datapoints
            // Not a variable
            var node = new UANode(new NodeId("test"), "test", NodeId.Null, NodeClass.Object);
            NodeToRaw(extractor, node, ConverterType.Node, false);
            // Normal double
            var variable = new UAVariable(new NodeId("test2"), "test2", NodeId.Null, NodeClass.Variable);
            variable.VariableAttributes.DataType = new UADataType(DataTypeIds.Double);
            variable.VariableAttributes.ValueRank = -1;
            NodeToRaw(extractor, variable, ConverterType.Variable, true);
            // Normal string
            variable = new UAVariable(new NodeId("test3"), "test3", NodeId.Null, NodeClass.Variable);
            variable.VariableAttributes.DataType = new UADataType(DataTypeIds.String);
            variable.VariableAttributes.ValueRank = -1;
            NodeToRaw(extractor, variable, ConverterType.Variable, true);
            // Array
            variable = new UAVariable(new NodeId("test4"), "test4", NodeId.Null, NodeClass.Variable);
            variable.VariableAttributes.DataType = new UADataType(DataTypeIds.Double);
            variable.VariableAttributes.ValueRank = 1;
            variable.VariableAttributes.ArrayDimensions = new[] { 4 };
            NodeToRaw(extractor, variable, ConverterType.Node, false);
            foreach (var child in variable.CreateArrayChildren())
            {
                NodeToRaw(extractor, child, ConverterType.Variable, true);
            }

            source = new CDFNodeSource(tester.Config, extractor, tester.Client, pusher);
            await source.ReadRawNodes(tester.Source.Token);
            result = await source.ParseResults(tester.Source.Token);
            Assert.Single(result.DestinationObjects);
            Assert.Equal("test4", result.DestinationObjects.First().DisplayName);
            Assert.Equal(-1, (result.DestinationObjects.First() as UAVariable).Index);
            Assert.Equal(6, result.DestinationVariables.Count());
            Assert.Empty(result.SourceObjects);
            Assert.Equal(3, result.SourceVariables.Count());

            Assert.Equal(3, extractor.State.NodeStates.Count());
            extractor.State.Clear();

            // Events
            // First, try disabling timeseries subscriptions and seeing that no results are returned
            tester.Config.Subscriptions.DataPoints = false;
            tester.Config.History.Enabled = false;
            source = new CDFNodeSource(tester.Config, extractor, tester.Client, pusher);
            await source.ReadRawNodes(tester.Source.Token);
            result = await source.ParseResults(tester.Source.Token);
            Assert.Null(result);
            Assert.Empty(extractor.State.NodeStates);

            // Enable events, but no states should be created
            tester.Config.Events.Enabled = true;
            source = new CDFNodeSource(tester.Config, extractor, tester.Client, pusher);
            await source.ReadRawNodes(tester.Source.Token);
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(2, result.DestinationObjects.Count());
            Assert.Single(result.SourceObjects);
            Assert.Equal(6, result.DestinationVariables.Count());
            Assert.Equal(3, result.SourceVariables.Count());
            Assert.Empty(extractor.State.NodeStates);
            Assert.Empty(extractor.State.EmitterStates);

            // Add a couple emitters
            node = new UANode(new NodeId("test5"), "test5", NodeId.Null, NodeClass.Object);
            node.Attributes.EventNotifier = EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
            NodeToRaw(extractor, node, ConverterType.Node, false);

            variable = new UAVariable(new NodeId("test6"), "test6", NodeId.Null, NodeClass.Variable);
            variable.VariableAttributes.DataType = new UADataType(DataTypeIds.String);
            variable.VariableAttributes.ValueRank = -1;
            variable.VariableAttributes.EventNotifier = EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
            NodeToRaw(extractor, variable, ConverterType.Variable, true);

            source = new CDFNodeSource(tester.Config, extractor, tester.Client, pusher);
            await source.ReadRawNodes(tester.Source.Token);
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(3, result.DestinationObjects.Count());
            Assert.Equal(2, result.SourceObjects.Count());
            Assert.Equal(7, result.DestinationVariables.Count());
            Assert.Equal(4, result.SourceVariables.Count());
            Assert.Empty(extractor.State.NodeStates);
            Assert.Equal(2, extractor.State.EmitterStates.Count());
        }

        [Fact]
        public async Task TestCDFAsSourceData()
        {
            tester.Config.Cognite.RawNodeBuffer = new CDFNodeSourceConfig
            {
                AssetsTable = "assets",
                TimeseriesTable = "timeseries",
                Database = "metadata",
                Enable = true
            };
            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                AssetsTable = "assets",
                TimeseriesTable = "timeseries",
                Database = "metadata"
            };
            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;
            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 10;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
            tester.Config.Extraction.RootNode = tester.Ids.Custom.Root.ToProtoNodeId(tester.Client);

            using var extractor = tester.BuildExtractor(true, null, pusher);

            // Nothing in CDF
            await Assert.ThrowsAsync<ExtractorFailureException>(async () => await extractor.RunExtractor(true));
            Assert.Empty(extractor.State.EmitterStates);
            Assert.Empty(extractor.State.NodeStates);
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = true;
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());
            Assert.True(handler.AssetRaw.Any());
            Assert.True(handler.TimeseriesRaw.Any());
            Assert.True(handler.Timeseries.Any());
            Assert.Empty(handler.Assets);

            await extractor.WaitForSubscriptions();
            tester.Client.Browser.ResetVisitedNodes();
            tester.Client.RemoveSubscription("DataChangeListener");

            extractor.State.Clear();

            // Now there is something in CDF, read it back
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = false;
            string oldAssets = System.Text.Json.JsonSerializer.Serialize(handler.AssetRaw);
            string oldTimeseries = System.Text.Json.JsonSerializer.Serialize(handler.TimeseriesRaw);
            handler.Timeseries.Clear();
            extractor.GetType().GetField("subscribed", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, 0);
            extractor.GetType().GetField("subscribeFlag", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, false);
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());

            string newAssets = System.Text.Json.JsonSerializer.Serialize(handler.AssetRaw);
            string newTimeseries = System.Text.Json.JsonSerializer.Serialize(handler.TimeseriesRaw);

            // Ensure data in raw is untouched.
            Assert.Equal(oldAssets, newAssets);
            Assert.Equal(oldTimeseries, newTimeseries);

            await extractor.WaitForSubscriptions();

            var id = tester.Client.GetUniqueId(tester.Server.Ids.Custom.MysteryVar);
            tester.Server.UpdateNode(tester.Server.Ids.Custom.MysteryVar, 1.0);

            await CommonTestUtils.WaitForCondition(async () =>
            {
                await extractor.Streamer.PushDataPoints(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
                return handler.Datapoints.ContainsKey(id) && handler.Datapoints[id].NumericDatapoints.Any();
            }, 10);

            tester.WipeCustomHistory();
        }
        [Fact]
        public async Task TestCDFAsSourceEvents()
        {
            tester.Config.Cognite.RawNodeBuffer = new CDFNodeSourceConfig
            {
                AssetsTable = "assets",
                TimeseriesTable = "timeseries",
                Database = "metadata",
                Enable = true
            };
            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                AssetsTable = "assets",
                TimeseriesTable = "timeseries",
                Database = "metadata"
            };
            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;
            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Events.Enabled = true;
            tester.Config.Events.ReadServer = false;
            tester.Config.Subscriptions.DataPoints = false;
            tester.Config.Extraction.RootNode = tester.Ids.Event.Root.ToProtoNodeId(tester.Client);

            using var extractor = tester.BuildExtractor(true, null, pusher);

            // Nothing in CDF
            await Assert.ThrowsAsync<ExtractorFailureException>(async () => await extractor.RunExtractor(true));
            Assert.Empty(extractor.State.EmitterStates);
            Assert.Empty(extractor.State.NodeStates);
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = true;
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());
            Assert.True(handler.AssetRaw.Any());
            Assert.True(handler.TimeseriesRaw.Any());
            Assert.True(handler.Timeseries.Any());
            Assert.Empty(handler.Assets);

            await extractor.WaitForSubscriptions();
            tester.Client.Browser.ResetVisitedNodes();
            tester.Client.RemoveSubscription("EventListener");

            extractor.State.Clear();

            // Now there is something in CDF, read it back
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = false;
            string oldAssets = System.Text.Json.JsonSerializer.Serialize(handler.AssetRaw);
            string oldTimeseries = System.Text.Json.JsonSerializer.Serialize(handler.TimeseriesRaw);
            handler.Timeseries.Clear();
            extractor.GetType().GetField("subscribed", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, 0);
            extractor.GetType().GetField("subscribeFlag", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, false);
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());

            string newAssets = System.Text.Json.JsonSerializer.Serialize(handler.AssetRaw);
            string newTimeseries = System.Text.Json.JsonSerializer.Serialize(handler.TimeseriesRaw);

            // Ensure data in raw is untouched.
            Assert.Equal(oldAssets, newAssets);
            Assert.Equal(oldTimeseries, newTimeseries);

            await extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(0);

            await CommonTestUtils.WaitForCondition(async () =>
            {
                await extractor.Streamer.PushEvents(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
                return handler.Events.Any();
            }, 10);

            tester.WipeEventHistory();
        }
        #endregion
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

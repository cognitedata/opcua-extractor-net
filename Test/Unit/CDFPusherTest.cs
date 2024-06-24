using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Pushers.Writers;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
    public sealed class CDFPusherTest : IClassFixture<CDFPusherTestFixture>, IDisposable
    {
        private readonly CDFPusherTestFixture tester;
        private readonly ITestOutputHelper _output;
        private CDFMockHandler handler;
        private CDFPusher pusher;
        public CDFPusherTest(ITestOutputHelper output, CDFPusherTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.Init(output);
            tester.ResetConfig();
            (handler, pusher) = tester.GetCDFPusher();
            tester.Client.TypeManager.Reset();
            _output = output;
        }

        public void Dispose()
        {
            pusher.Dispose();
        }


        [Fact]
        public async Task TestTestConnection()
        {
            tester.Config.DryRun = true;
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            tester.Config.DryRun = false;

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

            Assert.Null(tester.Config.Cognite.DataSet?.Id);
            handler.DataSets.Add("test-data-set", new DataSet
            {
                ExternalId = "test-data-set",
                Id = 123,
                CreatedTime = 1000,
                LastUpdatedTime = 1000
            });
            tester.Config.Cognite.DataSet = new DataSetConfig { ExternalId = "test-data-set" };

            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            Assert.Equal(123, tester.Config.Cognite.DataSet?.Id);

            handler.FailedRoutes.Add("/datasets/byids");
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));

            tester.Config.Cognite.DataSet.Id = null;

            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));
            Assert.Null(tester.Config.Cognite.DataSet.Id);
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

            tester.Config.DryRun = true;

            var time = DateTime.UtcNow;

            var invalidDps = new[]
            {
                new UADataPoint(DateTime.MaxValue, "test-ts-double", 123, StatusCodes.Good),
                new UADataPoint(time.AddSeconds(1), "test-ts-double", double.NaN, StatusCodes.Good)
            };

            Assert.Null(await pusher.PushDataPoints(invalidDps, tester.Source.Token));

            var dps = new[]
            {
                new UADataPoint(time, "test-ts-double", 123, StatusCodes.Good),
                new UADataPoint(time.AddSeconds(1), "test-ts-double", 321, StatusCodes.Good),
                new UADataPoint(time, "test-ts-string", "string", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(1), "test-ts-string", "string2", StatusCodes.Good),
                new UADataPoint(time, "test-ts-missing", "value", StatusCodes.Good)
            };

            // Debug true
            Assert.Null(await pusher.PushDataPoints(dps, tester.Source.Token));

            tester.Config.DryRun = false;

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
                new UADataPoint(time.AddSeconds(2), "test-ts-double", "string", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(3), "test-ts-double", "string2", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(2), "test-ts-string", "string3", StatusCodes.Good),
                new UADataPoint(time.AddSeconds(3), "test-ts-string", "string4", StatusCodes.Good),
                new UADataPoint(time, "test-ts-missing", "value", StatusCodes.Good)
            };
            Assert.True(await pusher.PushDataPoints(dps, tester.Source.Token));

            Assert.True(CommonTestUtils.TestMetricValue("opcua_mismatched_timeseries", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_missing_timeseries", 1));

            Assert.Equal(2, handler.Datapoints["test-ts-double"].NumericDatapoints.Count);
            Assert.Equal(4, handler.Datapoints["test-ts-string"].StringDatapoints.Count);

            // Final batch, all should now be filtered off
            invalidDps = new[]
            {
                new UADataPoint(DateTime.UtcNow, "test-ts-double", 123, StatusCodes.Good),
                new UADataPoint(time, "test-ts-double", 123, StatusCodes.Good),
                new UADataPoint(time, "test-ts-missing", "value", StatusCodes.Good)
            };
            Assert.Null(await pusher.PushDataPoints(invalidDps, tester.Source.Token));

            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoints_pushed_cdf", 6));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_pushes_cdf", 2));
        }
        [Fact]
        public async Task TestPushDataPointsWithStatus()
        {
            tester.Config.Extraction.StatusCodes.IngestStatusCodes = true;

            handler.MockTimeseries("test-ts-double");

            var dps = new[] {
                new UADataPoint(DateTime.UtcNow, "test-ts-double", 1, StatusCodes.Bad),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", 1, StatusCodes.Good),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", 1, StatusCodes.Uncertain),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", 1, StatusCodes.UncertainDataSubNormal),
                new UADataPoint(DateTime.UtcNow, "test-ts-double", 1, StatusCodes.GoodClamped)
            };

            Assert.True(await pusher.PushDataPoints(dps, tester.Source.Token));

            var idps = handler.Datapoints["test-ts-double"].NumericDatapoints;
            Assert.Equal(5, idps.Count);

            Assert.Equal(StatusCodes.Bad, idps[0].Status.Code);
            Assert.Equal(StatusCodes.Good, idps[1].Status.Code);
            Assert.Equal(StatusCodes.Uncertain, idps[2].Status.Code);
            Assert.Equal(StatusCodes.UncertainDataSubNormal, idps[3].Status.Code);
            Assert.Equal(StatusCodes.GoodClamped, idps[4].Status.Code);
        }

        [Fact]
        public async Task TestPushDataPointsNull()
        {
            tester.Config.Extraction.StatusCodes.IngestStatusCodes = true;

            handler.MockTimeseries("test-ts-double");

            var start = DateTime.UnixEpoch;

            var dps = new[] {
                new UADataPoint(start, "test-ts-double", false, StatusCodes.Bad),
                new UADataPoint(start + TimeSpan.FromSeconds(1), "test-ts-double", false, StatusCodes.Good),
                new UADataPoint(start + TimeSpan.FromSeconds(2), "test-ts-double", false, StatusCodes.Uncertain),
                new UADataPoint(start + TimeSpan.FromSeconds(3), "test-ts-double", false, StatusCodes.UncertainDataSubNormal),
                new UADataPoint(start + TimeSpan.FromSeconds(4), "test-ts-double", false, StatusCodes.GoodClamped)
            };

            Assert.True(await pusher.PushDataPoints(dps, tester.Source.Token));

            var idps = handler.Datapoints["test-ts-double"].NumericDatapoints;
            Assert.Equal(5, idps.Count);

            Assert.Equal(StatusCodes.Bad, idps[0].Status.Code);
            Assert.True(idps[0].NullValue);
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


            var writer = (CDFWriter)pusher.GetType()
                .GetField("cdfWriter", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);
            var nodeToAssetIds = writer.NodeToAssetIds;

            nodeToAssetIds[new NodeId("source", 0)] = 123;

            var time = DateTime.UtcNow;

            var events = new[]
            {
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter", 0),
                    SourceNode = new NodeId("source", 0),
                    EventType = new UAObjectType(new NodeId("type", 0)),
                    EventId = "someid"
                },
                new UAEvent
                {
                    Time = time,
                    EmittingNode = new NodeId("emitter", 0),
                    SourceNode = new NodeId("missingsource", 0),
                    EventType = new UAObjectType(new NodeId("type", 0)),
                    EventId = "someid2"
                }
            };

            tester.Config.DryRun = true;
            Assert.Null(await pusher.PushEvents(events, tester.Source.Token));
            tester.Config.DryRun = false;

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
                EmittingNode = new NodeId("emitter", 0),
                SourceNode = new NodeId("source", 0),
                EventType = new UAObjectType(new NodeId("type", 0)),
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
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Assets = true,
                    Timeseries = false,
                    Relationships = false,
                }
            };
            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var rels = Enumerable.Empty<UAReference>();
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();
            Assert.True((await pusher.PushNodes(Enumerable.Empty<BaseUANode>(), tss, rels, update, tester.Source.Token)).Objects);

            // Test debug mode
            var node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            tester.Config.DryRun = true;
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);
            tester.Config.DryRun = false;
            Assert.Empty(handler.Assets);

            // Fail to create assets
            node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            handler.FailedRoutes.Add("/assets");
            Assert.False((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);
            handler.FailedRoutes.Clear();

            // Create the asset
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);
            Assert.Single(handler.Assets);

            // Do nothing here, due to no update configured.
            handler.FailedRoutes.Add("/assets/update");
            node.Attributes.Description = "description";
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);

            // Do nothing again, due to no changes on the node
            update.Objects.Context = true;
            update.Objects.Description = true;
            update.Objects.Metadata = true;
            update.Objects.Name = true;
            node.Attributes.Description = null;
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).Objects);

            // Fail due to failed update, but the other will still be created
            var node2 = new UAObject(tester.Server.Ids.Custom.Root, "CustomRoot", null, null, NodeId.Null, null);
            node.Attributes.Description = "description";
            Assert.False((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).Objects);
            Assert.Equal(2, handler.Assets.Count);
            Assert.Null(handler.Assets.First().Value.description);
            Assert.Null(handler.Assets.Last().Value.description);

            // Update both nodes
            handler.FailedRoutes.Clear();
            node2.Attributes.Description = "description";
            Assert.True((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).Objects);
            Assert.Equal(2, handler.Assets.Count);
            Assert.Equal("description", handler.Assets.First().Value.description);
            Assert.Equal("description", handler.Assets.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 2));
        }
        [Fact]
        public async Task TestCreateRawAssets()
        {
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig(),
                Raw = new RawMetadataTargetConfig
                {
                    Database = "metadata",
                    AssetsTable = "assets"
                }
            };
            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            var rels = Enumerable.Empty<UAReference>();
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();
            // Fail to create
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/assets/rows");
            Assert.False((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).RawObjects);
            Assert.Empty(handler.AssetsRaw);
            handler.FailedRoutes.Clear();

            // Create one
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).RawObjects);
            Assert.Single(handler.AssetsRaw);
            Assert.Equal("BaseRoot", handler.AssetsRaw.First().Value.GetProperty("name").GetString());

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UAObject(tester.Server.Ids.Custom.Root, "CustomRoot", null, null, NodeId.Null, null);
            node.Attributes.Description = "description";
            Assert.True((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).RawObjects);
            Assert.Equal(2, handler.AssetsRaw.Count);
            Assert.Null(handler.AssetsRaw.First().Value.GetProperty("description").GetString());
            Assert.Null(handler.AssetsRaw.Last().Value.GetProperty("description").GetString());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
        }
        [Fact]
        public async Task TestUpdateRawAssets()
        {
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Relationships = true,
                    Assets = false,
                    Timeseries = true
                },
                Raw = new RawMetadataTargetConfig
                {
                    Database = "metadata",
                    AssetsTable = "assets",
                    TimeseriesTable = "timeseries"
                }
            };
            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);
            var node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            var rels = Enumerable.Empty<UAReference>();
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();
            update.Objects.Context = true;
            update.Objects.Description = true;
            update.Objects.Metadata = true;
            update.Objects.Name = true;

            // Fail to upsert
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/assets/rows");
            Assert.False((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).RawObjects);
            Assert.Empty(handler.AssetsRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True((await pusher.PushNodes(new[] { node }, tss, rels, update, tester.Source.Token)).RawObjects);
            Assert.Single(handler.AssetsRaw);
            Assert.Equal("BaseRoot", handler.AssetsRaw.First().Value.GetProperty("name").GetString());

            // Create another, overwrite the existing one
            var node2 = new UAObject(tester.Server.Ids.Custom.Root, "CustomRoot", null, null, NodeId.Null, null);
            node.Attributes.Description = "description";
            Assert.True((await pusher.PushNodes(new[] { node, node2 }, tss, rels, update, tester.Source.Token)).RawObjects);
            Assert.Equal(2, handler.AssetsRaw.Count);
            Assert.Single(handler.AssetsRaw, asset => asset.Value.GetProperty("description").GetString() == "description");

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
        }
        [Fact]
        public async Task TestCreateUpdateTimeseries()
        {
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Relationships = true,
                    Assets = false,
                    Timeseries = true
                },
            };
            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dt = new UADataType(DataTypeIds.Double);

            var writer = (CDFWriter)pusher.GetType()
                .GetField("cdfWriter", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);

            var nodeToAssetIds = writer.NodeToAssetIds;
            nodeToAssetIds[new NodeId("parent", 0)] = 123;

            var rels = Enumerable.Empty<UAReference>();
            var assets = Enumerable.Empty<BaseUANode>();
            var update = new UpdateConfig();

            // Test debug mode
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", null, null, new NodeId("parent", 0), null);
            node.FullAttributes.DataType = dt;
            tester.Config.DryRun = true;
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);
            tester.Config.DryRun = false;
            Assert.Empty(handler.Timeseries);

            // Fail to create timeseries
            node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", null, null, new NodeId("parent", 0), null);
            node.FullAttributes.DataType = dt;
            handler.FailedRoutes.Add("/timeseries");
            Assert.False((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);
            handler.FailedRoutes.Clear();
            Assert.Empty(handler.Timeseries);

            // Create the timeseries
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);
            Assert.Single(handler.Timeseries);
            Assert.Equal(123, handler.Timeseries.First().Value.assetId);

            // Do nothing due to no configured update
            handler.FailedRoutes.Add("/timeseries/update");
            node.Attributes.Description = "description";
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);

            // Do nothing again due to no changes on the node
            update.Variables.Context = true;
            update.Variables.Description = true;
            update.Variables.Metadata = true;
            update.Variables.Name = true;
            node.Attributes.Description = null;
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).Variables);

            // Create one, fail to update the other
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", null, null, new NodeId("parent", 0), null);
            node2.FullAttributes.DataType = dt;
            node.Attributes.Description = "description";
            Assert.False((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).Variables);
            Assert.Equal(2, handler.Timeseries.Count);
            Assert.Null(handler.Timeseries.First().Value.description);
            Assert.Null(handler.Timeseries.Last().Value.description);

            // Update both nodes
            handler.FailedRoutes.Clear();
            node2.Attributes.Description = "description";
            Assert.True((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).Variables);
            Assert.Equal(2, handler.Timeseries.Count);
            Assert.Equal("description", handler.Timeseries.First().Value.description);
            Assert.Equal("description", handler.Timeseries.Last().Value.description);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 2));
        }
        [Fact]
        public async Task TestCreateRawTimeseries()
        {
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Relationships = true,
                    Assets = false,
                    Timeseries = false
                },
                Raw = new RawMetadataTargetConfig
                {
                    Database = "metadata",
                    AssetsTable = "assets",
                    TimeseriesTable = "timeseries"
                }
            };
            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dt = new UADataType(DataTypeIds.Double);

            var rels = Enumerable.Empty<UAReference>();
            var assets = Enumerable.Empty<BaseUANode>();
            var update = new UpdateConfig();
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", null, null, new NodeId("parent", 0), null);
            node.FullAttributes.DataType = dt;

            // Fail to create
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/timeseries/rows");
            Assert.False((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).RawVariables);
            Assert.Empty(handler.TimeseriesRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).RawVariables);
            Assert.Single(handler.TimeseriesRaw);
            Assert.Equal("Variable 1", handler.TimeseriesRaw.First().Value.GetProperty("name").GetString());

            // Create another, do not overwrite the existing one, due to no update settings
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", null, null, new NodeId("parent", 0), null);
            node2.FullAttributes.DataType = dt;
            node.Attributes.Description = "description";
            Assert.True((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).RawVariables);
            Assert.Equal(2, handler.TimeseriesRaw.Count);
            Assert.Null(handler.TimeseriesRaw.First().Value.GetProperty("description").GetString());
            Assert.Null(handler.TimeseriesRaw.Last().Value.GetProperty("description").GetString());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
        }
        [Fact]
        public async Task TestUpdateRawTimeseries()
        {
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Relationships = true,
                    Assets = false,
                    Timeseries = false
                },
                Raw = new RawMetadataTargetConfig
                {
                    Database = "metadata",
                    AssetsTable = "assets",
                    TimeseriesTable = "timeseries"
                }
            };

            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);
            var dt = new UADataType(DataTypeIds.Double);

            var writer = (CDFWriter)pusher.GetType()
                .GetField("cdfWriter", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pusher);

            var nodeToAssetIds = writer.NodeToAssetIds;

            var rels = Enumerable.Empty<UAReference>();
            var assets = Enumerable.Empty<BaseUANode>();
            var update = new UpdateConfig();
            update.Variables.Context = true;
            update.Variables.Description = true;
            update.Variables.Metadata = true;
            update.Variables.Name = true;
            var node = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", null, null, new NodeId("parent", 0), null);
            node.FullAttributes.DataType = dt;

            // Fail to upsert
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/timeseries/rows");
            Assert.False((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).RawVariables);
            Assert.Empty(handler.TimeseriesRaw);

            // Create one
            handler.FailedRoutes.Clear();
            Assert.True((await pusher.PushNodes(assets, new[] { node }, rels, update, tester.Source.Token)).RawVariables);
            Assert.Single(handler.TimeseriesRaw);
            Assert.Equal("Variable 1", handler.TimeseriesRaw.First().Value.GetProperty("name").GetString());

            // Create another, overwrite the existing due to update settings
            var node2 = new UAVariable(tester.Server.Ids.Custom.MysteryVar, "MysteryVar", null, null, new NodeId("parent", 0), null);
            node2.FullAttributes.DataType = dt;
            node.Attributes.Description = "description";
            Assert.True((await pusher.PushNodes(assets, new[] { node, node2 }, rels, update, tester.Source.Token)).Variables);
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
                    CommonTestUtils.GetSimpleVariable("double", new UADataType(DataTypeIds.Double)), true, true, true);
                var state2 = new VariableExtractionState(tester.Client,
                    CommonTestUtils.GetSimpleVariable("string", new UADataType(DataTypeIds.String)), true, true, true);
                var state3 = new VariableExtractionState(tester.Client,
                    CommonTestUtils.GetSimpleVariable("array", new UADataType(DataTypeIds.Double), 3), true, true, true);
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
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Assets = false,
                    Timeseries = false,
                    Relationships = true,
                }
            };

            var organizes = tester.Client.TypeManager.GetReferenceType(ReferenceTypeIds.Organizes);

            var source = new UAObject(new NodeId("source", 0), "Source", "Source", null, NodeId.Null, null);
            var target = new UAObject(new NodeId("target", 0), "Target", "Target", null, NodeId.Null, null);
            var sourceVar = new UAVariable(new NodeId("source2", 0), "Source", "Source", null, NodeId.Null, null);
            var targetVar = new UAVariable(new NodeId("target2", 0), "Target", "Target", null, NodeId.Null, null);

            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var uaSource = new UANodeSource(tester.Log, extractor, tester.Client, tester.Client.TypeManager);

            var assets = Enumerable.Empty<BaseUANode>();
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();

            // Push none
            Assert.True((await pusher.PushNodes(assets, tss, Enumerable.Empty<UAReference>(), update, tester.Source.Token)).References);

            // Fail to push
            var references = new List<UAReference>
            {
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target),
            };
            await extractor.TypeManager.LoadTypeData(uaSource, tester.Source.Token);
            handler.FailedRoutes.Add("/relationships");
            Assert.False((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).References);
            Assert.Empty(handler.Relationships);
            handler.FailedRoutes.Clear();

            // Push successful
            Assert.True((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).References);
            Assert.Equal(2, handler.Relationships.Count);

            // Push again, with duplicates
            var references2 = new List<UAReference>
            {
                new UAReference(organizes, true, source, target),
                new UAReference(organizes, false, sourceVar, targetVar),
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target)
            };
            Assert.True((await pusher.PushNodes(assets, tss, references2, update, tester.Source.Token)).References);
            Assert.Equal(4, handler.Relationships.Count);
            var ids = new List<string>
            {
                "gp.Organizes;base:s=source;base:s=target",
                "gp.OrganizedBy;base:s=source2;base:s=target2",
                "gp.Organizes;base:s=source;base:s=target2",
                "gp.OrganizedBy;base:s=source2;base:s=target",
            };
            foreach (var rl in handler.Relationships)
            {
                tester.Log.LogInformation("{Key}", rl.Key);
            }
            Assert.All(ids, id => Assert.True(handler.Relationships.ContainsKey(id)));

            // Test pushing all duplicates
            Assert.True((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).References);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
        }
        [Fact]
        public async Task TestCreateRawRelationships()
        {
            CommonTestUtils.ResetMetricValue("opcua_node_ensure_failures_cdf");

            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Raw = new RawMetadataTargetConfig
                {
                    RelationshipsTable = "relationships",
                    Database = "metadata"
                }
            };

            var organizes = tester.Client.TypeManager.GetReferenceType(ReferenceTypeIds.Organizes);

            var source = new UAObject(new NodeId("source", 0), "Source", "Source", null, NodeId.Null, null);
            var target = new UAObject(new NodeId("target", 0), "Target", "Target", null, NodeId.Null, null);
            var sourceVar = new UAVariable(new NodeId("source2", 0), "Source", "Source", null, NodeId.Null, null);
            var targetVar = new UAVariable(new NodeId("target2", 0), "Target", "Target", null, NodeId.Null, null);

            tester.Config.Extraction.Relationships.Enabled = true;
            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var uaSource = new UANodeSource(tester.Log, extractor, tester.Client, tester.Client.TypeManager);

            var assets = Enumerable.Empty<BaseUANode>();
            var tss = Enumerable.Empty<UAVariable>();
            var update = new UpdateConfig();

            // Push none
            Assert.True((await pusher.PushNodes(assets, tss, Enumerable.Empty<UAReference>(), update, tester.Source.Token)).RawReferences);

            // Fail to push
            var references = new List<UAReference>
            {
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target),
            };
            await extractor.TypeManager.LoadTypeData(uaSource, tester.Source.Token);
            handler.FailedRoutes.Add("/raw/dbs/metadata/tables/relationships/rows");
            Assert.False((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).RawReferences);
            Assert.Empty(handler.RelationshipsRaw);
            handler.FailedRoutes.Clear();

            // Push successful
            Assert.True((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).RawReferences);
            Assert.Equal(2, handler.RelationshipsRaw.Count);

            // Push again, with duplicates
            var references2 = new List<UAReference>
            {
                new UAReference(organizes, true, source, target),
                new UAReference(organizes, false, sourceVar, targetVar),
                new UAReference(organizes, true, source, targetVar),
                new UAReference(organizes, false, sourceVar, target)
            };
            Assert.True((await pusher.PushNodes(assets, tss, references2, update, tester.Source.Token)).References);
            Assert.Equal(4, handler.RelationshipsRaw.Count);
            var ids = new List<string>
            {
                "gp.Organizes;base:s=source;base:s=target",
                "gp.OrganizedBy;base:s=source2;base:s=target2",
                "gp.Organizes;base:s=source;base:s=target2",
                "gp.OrganizedBy;base:s=source2;base:s=target",
            };
            Assert.All(ids, id => Assert.True(handler.RelationshipsRaw.ContainsKey(id)));

            // Test pushing all duplicates
            Assert.True((await pusher.PushNodes(assets, tss, references, update, tester.Source.Token)).References);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_node_ensure_failures_cdf", 1));
        }
        #endregion

        #region node-source

        private void NodeToRaw(UAExtractor extractor, BaseUANode node, ConverterType type, bool ts)
        {
            var options = new JsonSerializerOptions();
            var converter = tester.Client.StringConverter;
            converter.AddConverters(options, type);

            var id = node.GetUniqueId(extractor.Context);

            var json = JsonSerializer.Serialize(node, options);

            var val = JsonSerializer.Deserialize<JsonElement>(json, options);
            tester.Log.LogInformation("{Node}", val);
            if (ts)
            {
                handler.TimeseriesRaw[id] = val;
            }
            else
            {
                handler.AssetsRaw[id] = val;
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
            tester.Config.Subscriptions.IgnoreAccessLevel = true;

            var log = tester.Provider.GetRequiredService<ILogger<CDFNodeSource>>();
            var source = new CDFNodeSource(log, tester.Config, extractor, pusher, extractor.TypeManager);
            var uaSource = new UANodeSource(tester.Log, extractor, tester.Client, extractor.TypeManager);
            var builder = new NodeHierarchyBuilder(source, uaSource, tester.Config, Enumerable.Empty<NodeId>(),
                tester.Client, extractor, extractor.Transformations, log);

            // Nothing in CDF
            var result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Empty(result.DestinationObjects);
            Assert.Empty(result.DestinationVariables);

            // Datapoints
            // Not a variable
            var node = new UAObject(new NodeId("test", 0), "test", null, null, NodeId.Null, null);
            NodeToRaw(extractor, node, ConverterType.Node, false);
            // Normal double
            var variable = new UAVariable(new NodeId("test2", 0), "test2", null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            variable.FullAttributes.ValueRank = -1;
            NodeToRaw(extractor, variable, ConverterType.Variable, true);
            // Normal string
            variable = new UAVariable(new NodeId("test3", 0), "test3", null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.String);
            variable.FullAttributes.ValueRank = -1;
            NodeToRaw(extractor, variable, ConverterType.Variable, true);
            // Array
            variable = new UAVariable(new NodeId("test4", 0), "test4", null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            variable.FullAttributes.ValueRank = 1;
            variable.FullAttributes.ArrayDimensions = new[] { 4 };
            NodeToRaw(extractor, variable, ConverterType.Node, false);
            foreach (var child in variable.CreateTimeseries())
            {
                NodeToRaw(extractor, child, ConverterType.Variable, true);
            }

            builder = new NodeHierarchyBuilder(source, uaSource, tester.Config, Enumerable.Empty<NodeId>(),
                 tester.Client, extractor, extractor.Transformations, log);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Single(result.DestinationObjects);
            Assert.Equal("test4", result.DestinationObjects.First().Name);
            Assert.Equal(6, result.DestinationVariables.Count());
            Assert.Empty(result.SourceObjects);
            Assert.Equal(3, result.SourceVariables.Count());

            Assert.Equal(3, extractor.State.NodeStates.Count());
            extractor.State.Clear();

            // Events
            // First, try disabling timeseries subscriptions and seeing that no results are returned
            tester.Config.Subscriptions.DataPoints = false;
            tester.Config.History.Enabled = false;
            builder = new NodeHierarchyBuilder(source, uaSource, tester.Config, Enumerable.Empty<NodeId>(),
                 tester.Client, extractor, extractor.Transformations, log);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Empty(result.DestinationObjects);
            Assert.Empty(result.DestinationVariables);
            Assert.Empty(extractor.State.NodeStates);

            // Enable events, but no states should be created
            tester.Config.Events.Enabled = true;
            builder = new NodeHierarchyBuilder(source, uaSource, tester.Config, Enumerable.Empty<NodeId>(),
                 tester.Client, extractor, extractor.Transformations, log);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Equal(2, result.DestinationObjects.Count());
            Assert.Single(result.SourceObjects);
            Assert.Equal(6, result.DestinationVariables.Count());
            Assert.Equal(3, result.SourceVariables.Count());
            Assert.Empty(extractor.State.NodeStates);
            Assert.Empty(extractor.State.EmitterStates);

            // Add a couple emitters
            node = new UAObject(new NodeId("test5", 0), "test5", null, null, NodeId.Null, null);
            node.FullAttributes.EventNotifier = EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
            NodeToRaw(extractor, node, ConverterType.Node, false);

            variable = new UAVariable(new NodeId("test6", 0), "test6", null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.String);
            variable.FullAttributes.ValueRank = -1;
            NodeToRaw(extractor, variable, ConverterType.Variable, true);

            builder = new NodeHierarchyBuilder(source, uaSource, tester.Config, Enumerable.Empty<NodeId>(),
                 tester.Client, extractor, extractor.Transformations, log);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Equal(3, result.DestinationObjects.Count());
            Assert.Equal(2, result.SourceObjects.Count());
            Assert.Equal(7, result.DestinationVariables.Count());
            Assert.Equal(4, result.SourceVariables.Count());
            Assert.Empty(extractor.State.NodeStates);
            Assert.Single(extractor.State.EmitterStates);
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
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Raw = new RawMetadataTargetConfig
                {
                    AssetsTable = "assets",
                    TimeseriesTable = "timeseries",
                    Database = "metadata"
                }
            };
            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;
            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 10;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
            tester.Config.Extraction.RootNode = tester.Ids.Custom.Root.ToProtoNodeId(tester.Client);
            tester.Config.History.Enabled = true;

            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            // Nothing in CDF
            await extractor.RunExtractor(true);
            Assert.Empty(extractor.State.EmitterStates);
            Assert.Empty(extractor.State.NodeStates);
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = true;
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());
            Assert.True(handler.AssetsRaw.Count != 0);
            Assert.True(handler.TimeseriesRaw.Count != 0);
            Assert.True(handler.Timeseries.Count != 0);
            Assert.Empty(handler.Assets);

            await extractor.WaitForSubscriptions();
            await tester.RemoveSubscription(SubscriptionName.DataPoints);

            extractor.State.Clear();

            // Now there is something in CDF, read it back
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = false;
            string oldAssets = System.Text.Json.JsonSerializer.Serialize(handler.AssetsRaw);
            string oldTimeseries = System.Text.Json.JsonSerializer.Serialize(handler.TimeseriesRaw);
            handler.Timeseries.Clear();
            extractor.GetType().GetField("subscribed", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, 0);
            extractor.GetType().GetField("subscribeFlag", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, false);
            var reader = (HistoryReader)extractor.GetType().GetField("historyReader", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(extractor);
            reader.AddIssue(HistoryReader.StateIssue.NodeHierarchyRead);
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());

            string newAssets = System.Text.Json.JsonSerializer.Serialize(handler.AssetsRaw);
            string newTimeseries = System.Text.Json.JsonSerializer.Serialize(handler.TimeseriesRaw);

            // Ensure data in raw is untouched.
            Assert.Equal(oldAssets, newAssets);
            Assert.Equal(oldTimeseries, newTimeseries);

            await extractor.WaitForSubscriptions();

            try
            {
                await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(s => !s.IsFrontfilling), 5);
            }
            finally
            {
                foreach (var state in extractor.State.NodeStates)
                {
                    tester.Log.LogDebug("State is frontfilling: {Id} {State}", state.Id, state.IsFrontfilling);
                }
            }

            var id = tester.Client.GetUniqueId(tester.Server.Ids.Custom.MysteryVar);

            int idx = 0;
            await TestUtils.WaitForCondition(async () =>
            {
                tester.Server.UpdateNode(tester.Server.Ids.Custom.MysteryVar, idx++);
                await extractor.Streamer.PushDataPoints(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
                return handler.Datapoints.ContainsKey(id) && handler.Datapoints[id].NumericDatapoints.Count != 0;
            }, 10);

            Assert.True(extractor.State.NodeStates.Where(state => state.FrontfillEnabled).Any());

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
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Raw = new RawMetadataTargetConfig
                {
                    AssetsTable = "assets",
                    TimeseriesTable = "timeseries",
                    Database = "metadata"
                }
            };
            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;
            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Events.Enabled = true;
            tester.Config.Events.ReadServer = false;
            tester.Config.Subscriptions.DataPoints = true;
            tester.Config.Extraction.RootNode = tester.Ids.Event.Root.ToProtoNodeId(tester.Client);

            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            // Nothing in CDF
            await extractor.RunExtractor(true);
            Assert.Empty(extractor.State.EmitterStates);
            Assert.Empty(extractor.State.NodeStates);
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = true;
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());
            Assert.True(handler.AssetsRaw.Count != 0);
            Assert.True(handler.TimeseriesRaw.Count != 0);
            Assert.True(handler.Timeseries.Count != 0);
            Assert.Empty(handler.Assets);

            await extractor.WaitForSubscriptions();
            await tester.RemoveSubscription(SubscriptionName.Events);

            extractor.State.Clear();

            // Now there is something in CDF, read it back
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = false;
            string oldAssets = JsonSerializer.Serialize(handler.AssetsRaw);
            string oldTimeseries = JsonSerializer.Serialize(handler.TimeseriesRaw);
            handler.Timeseries.Clear();
            extractor.GetType().GetField("subscribed", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, 0);
            extractor.GetType().GetField("subscribeFlag", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, false);
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());

            string newAssets = JsonSerializer.Serialize(handler.AssetsRaw);
            string newTimeseries = JsonSerializer.Serialize(handler.TimeseriesRaw);

            // Ensure data in raw is untouched.
            Assert.Equal(oldAssets, newAssets);
            Assert.Equal(oldTimeseries, newTimeseries);

            await extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(0);

            await TestUtils.WaitForCondition(async () =>
            {
                await extractor.Streamer.PushEvents(new[] { pusher }, Enumerable.Empty<IPusher>(), tester.Source.Token);
                return handler.Events.Count != 0;
            }, 10);

            tester.WipeEventHistory();
        }

        [Fact]
        public async Task TestCDFNodeSetBackground()
        {
            tester.Config.Cognite.RawNodeBuffer = new CDFNodeSourceConfig
            {
                AssetsTable = "assets",
                TimeseriesTable = "timeseries",
                Database = "metadata",
                Enable = true
            };
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Raw = new RawMetadataTargetConfig
                {
                    Database = "metadata",
                    TimeseriesTable = "timeseries",
                    AssetsTable = "assets"
                }
            };
            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;
            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Events.Enabled = true;
            tester.Config.Events.ReadServer = false;
            tester.Config.Subscriptions.DataPoints = true;
            tester.Config.Extraction.RootNode = tester.Ids.Event.Root.ToProtoNodeId(tester.Client);
            tester.Config.Source.AltSourceBackgroundBrowse = true;
            (handler, pusher) = tester.GetCDFPusher();

            using var extractor = tester.BuildExtractor(true, null, pusher);

            // Populate data in Raw
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = true;
            await extractor.RunExtractor(true);
            Assert.True(extractor.State.NodeStates.Any());
            Assert.True(handler.AssetsRaw.Count != 0);
            Assert.True(handler.TimeseriesRaw.Count != 0);
            Assert.True(handler.Timeseries.Count != 0);
            Assert.Empty(handler.Assets);

            await extractor.WaitForSubscriptions();
            await tester.RemoveSubscription(SubscriptionName.Events);

            extractor.State.Clear();

            // Remove all timeseries
            handler.TimeseriesRaw.Clear();
            handler.Timeseries.Clear();

            // Now the extractor should throw instead of falling back to browse
            tester.Config.Cognite.RawNodeBuffer.BrowseOnEmpty = false;

            await extractor.RunExtractor(true);

            await TestUtils.WaitForCondition(() => handler.TimeseriesRaw.Count > 0, 10);
        }

        [Fact]
        public async Task TestAllDestinationsActive()
        {
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Assets = true,
                    Timeseries = true,
                    Relationships = true,
                },
                Raw = new RawMetadataTargetConfig
                {
                    Database = "metadata",
                    AssetsTable = "assets",
                    TimeseriesTable = "timeseries",
                    RelationshipsTable = "relationships"
                },
            };

            (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var update = new UpdateConfig();
            var dt = new UADataType(DataTypeIds.Double);
            var node = new UAObject(tester.Server.Ids.Base.Root, "BaseRoot", null, null, NodeId.Null, null);
            var variable = new UAVariable(tester.Server.Ids.Base.DoubleVar1, "Variable 1", null, null, new NodeId("parent", 0), null);
            variable.FullAttributes.DataType = dt;
            var rel = new UAReference(extractor.TypeManager.GetReferenceType(ReferenceTypeIds.Organizes), true, node, variable);

            var result = await pusher.PushNodes(new[] { node }, new[] { variable }, new[] { rel }, update, tester.Source.Token);

            Assert.True(result.Objects);
            Assert.True(result.RawObjects);

            Assert.True(result.Variables);
            Assert.True(result.RawVariables);

            Assert.True(result.References);
            Assert.True(result.RawReferences);

            Assert.Single(handler.Assets);
            Assert.Single(handler.AssetsRaw);

            Assert.Single(handler.Timeseries);
            Assert.Single(handler.TimeseriesRaw);

            Assert.Single(handler.Relationships);
            Assert.Single(handler.RelationshipsRaw);
        }
        #endregion
    }
}

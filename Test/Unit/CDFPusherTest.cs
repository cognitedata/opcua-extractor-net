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
        public CDFPusherTest(ITestOutputHelper output, CDFPusherTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        [Fact]
        public async Task TestTestConnection()
        {
            var (handler, pusher) = tester.GetPusher();

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
            var (handler, pusher) = tester.GetPusher();

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
            var (handler, pusher) = tester.GetPusher();
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

            Assert.True(false);
        }
    }
}

/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Cognite.OpcUa;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [Collection("Extractor tests")]
    public class InfluxPusherTests : MakeConsoleWork
    {
        // private static readonly ILogger log = Log.Logger.ForContext(typeof(InfluxPusherTests));

        public InfluxPusherTests(ITestOutputHelper output) : base(output) { }
        [Trait("Server", "basic")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxpusher")]
        [Fact]
        public async Task TestInfluxPusher()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters {
                Pusher = "influx"
            });
            tester.Config.History.Enabled = false;
            await tester.ClearPersistentData();
            Assert.True(await tester.Pusher.TestConnection(tester.Config, tester.Source.Token));
            await tester.StartServer();

            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();
            await tester.Extractor.Looper.WaitForNextPush();

            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar1, 1);

            await tester.WaitForCondition(async () =>
            {
                var dps = await tester.GetAllInfluxPoints(tester.Server.Ids.Base.DoubleVar1, false);
                return dps.Count() == 2;
            }, 20, "Expected to get some data");

            var dps = await tester.GetAllInfluxPoints(tester.Server.Ids.Base.DoubleVar1);
            Assert.Equal(2, dps.Count());

            await tester.TerminateRunTask();
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_push_failures_influx", 0));
        }
        [Trait("Server", "array")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxarraydata")]
        [Fact]
        public async Task TestArrayData()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                Pusher = "influx",
                ServerName = ServerName.Array
            });
            await tester.ClearPersistentData();
            tester.Config.Extraction.MaxArraySize = 4;
            tester.Config.Extraction.AllowStringVariables = true;
            tester.Config.History.Enabled = false;

            await tester.StartServer();
            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();
            await tester.Extractor.Looper.WaitForNextPush();

            tester.Server.UpdateNode(tester.Server.Ids.Custom.Array, new int[] { 1, 1, 1, 1 });

            await tester.WaitForCondition(async () =>
            {
                var dps = await tester.GetAllInfluxPoints(tester.Server.Ids.Custom.Array, false, 3);
                return dps.Count() == 2;
            }, 20, "Expected to get some data");

            var dps = await tester.GetAllInfluxPoints(tester.Server.Ids.Custom.Array, false, 3);
            Assert.Equal(2, dps.Count());

            await tester.TerminateRunTask();

            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_push_failures_influx", 0));
        }

        [Trait("Server", "basic")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "nonfiniteinflux")]
        [Fact]
        public async Task TestNonFiniteInflux()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                Pusher = "influx",
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();
            tester.Config.History.Enabled = false;

            await tester.StartServer();

            tester.StartExtractor();

            await tester.TerminateRunTask();

            var values = new List<double>
            {
                1E100,
                -1E100,
                1E105,
                -1E105,
                double.MaxValue,
                double.MinValue,
                double.PositiveInfinity,
                double.NegativeInfinity,
                double.NaN
            };

            var pusher = tester.Pusher;

            var badPoints = values.Select(value => new BufferedDataPoint(DateTime.Now, "gp.tl:i=2", value)).ToList();

            await pusher.PushDataPoints(badPoints, CancellationToken.None);

            var read = await tester.IfDbClient.QueryMultiSeriesAsync(tester.Config.Influx.Database, 
                "SELECT * FROM \"gp.tl:i=2\"");

            Assert.True(read.Count > 0);
            var readValues = read.First();

            foreach (var value in values)
            {
                if (!double.IsFinite(value)) continue;
                Assert.Contains(readValues.Entries, entry => Math.Abs(Convert.ToDouble(entry.Value) - value) < 1);
            }
            Assert.True(CommonTestUtils.TestMetricValue("opcua_datapoint_push_failures_influx", 0));
        }

        [Trait("Server", "events")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxdbevents")]
        [Fact]
        public async Task TestInfluxdbEvents()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                Pusher = "influx",
                ConfigName = ConfigName.Events
            });
            await tester.ClearPersistentData();
            tester.Config.History.Enabled = false;

            await tester.StartServer();

            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(0);

            await tester.WaitForCondition(async () =>
            {
                var evts = await tester.GetAllInfluxEvents(tester.Server.Ids.Event.Obj1);
                var evts2 = await tester.GetAllInfluxEvents(tester.Server.Ids.Event.Obj2);
                return evts.Count() == 4 && evts2.Count() == 2;
            }, 5, "Expected to get some events in influxdb");

            await tester.TerminateRunTask();
        }

        [Trait("Server", "basic")]
        [Trait("Target", "FailureBuffer")]
        [Trait("Test", "influxbuffer")]
        [Fact]
        public async Task TestInfluxBuffering()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Test,
                FailureInflux = true,
                StoreDatapoints = true,
            });
            await tester.ClearPersistentData();
            tester.Config.Extraction.AllowStringVariables = true;

            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);

            await tester.Extractor.Looper.WaitForNextPush();

            tester.Handler.AllowPush = false;
            tester.Handler.AllowConnectionTest = false;

            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 1000);
            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1000);

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.Any,
                20, "Failurebuffer must receive some data");

            await Task.Delay(500);
            tester.Handler.AllowPush = true;
            tester.Handler.AllowConnectionTest = true;

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.Any,
                20, "FailureBuffer should be emptied");

            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 1001);
            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1001);

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);
            
            await tester.WaitForCondition(() => tester.Handler.Datapoints.ContainsKey("gp.tl:i=10")
                && tester.Handler.Datapoints["gp.tl:i=10"].NumericDatapoints.DistinctBy(dp => dp.Timestamp).Count() == 1002, 20);
            await tester.TerminateRunTask();
            
            tester.TestContinuity("gp.tl:i=10");

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(1002, tester.Handler.Datapoints["gp.tl:i=10"].NumericDatapoints.DistinctBy(dp => dp.Timestamp).Count());
            Assert.Equal(3, tester.Handler.Datapoints["gp.tl:i=3"].NumericDatapoints.DistinctBy(dp => dp.Timestamp).Count());

            Assert.Equal(2, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(5, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_datapoint_push_failures_cdf"));
        }

        [Trait("Server", "basic")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxbackfill")]
        [Fact]
        public async Task TestInfluxBackfill()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                Pusher = "influx"
            });
            await tester.ClearPersistentData();

            tester.Config.History.Backfill = true;

            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.StartExtractor();

            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1000);

            await tester.WaitForCondition(() =>
                    tester.Extractor.State.GetNodeState("gp.tl:i=10") != null
                    && !tester.Extractor.State.GetNodeState("gp.tl:i=10").IsBackfilling
                    && !tester.Extractor.State.GetNodeState("gp.tl:i=10").IsFrontfilling,
                20, "Expected backfill to terminate");

            await tester.Extractor.Looper.WaitForNextPush();

            await tester.WaitForCondition(async () =>
            {
                var dps = await tester.GetAllInfluxPoints(tester.Server.Ids.Base.IntVar);
                return dps.Count() == 1001;
            }, 20, "Expected points to arrive in influxdb");

            await tester.TerminateRunTask();
            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_data_count") >= 1);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
        }
        [Trait("Server", "events")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxbackfillrestart")]
        [Fact]
        public async Task TestInfluxBackfillRestart()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                Pusher = "influx"
            });
            await tester.ClearPersistentData();

            tester.Config.History.Backfill = true;

            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Extractor.State.GetNodeState("gp.tl:i=10") != null
                    && !tester.Extractor.State.GetNodeState("gp.tl:i=10").IsBackfilling
                    && !tester.Extractor.State.GetNodeState("gp.tl:i=10").IsFrontfilling,
                20, "Expected backfill to terminate");

            await tester.WaitForCondition(async () =>
            {
                var dps = await tester.GetAllInfluxPoints(tester.Server.Ids.Base.IntVar);
                return dps.Count() == 1000;
            }, 20, "Expected points to arrive in influxdb");

            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_data_count") >= 1);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));

            CommonTestUtils.ResetTestMetrics();
            tester.Extractor.RestartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            await tester.WaitForCondition(() =>
                    tester.Extractor.State.GetNodeState("gp.tl:i=10") != null
                    && !tester.Extractor.State.GetNodeState("gp.tl:i=10").IsBackfilling
                    && !tester.Extractor.State.GetNodeState("gp.tl:i=10").IsFrontfilling,
                20, "Expected backfill to terminate");

            var dps = await tester.GetAllInfluxPoints(tester.Server.Ids.Base.IntVar);

            Assert.Equal(1000, dps.Count());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));

            await tester.TerminateRunTask();
        }
        [Trait("Server", "events")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxbackfillevents")]
        [Fact]
        public async Task TestInfluxBackfillEvents()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                Pusher = "influx",
                ConfigName = ConfigName.Events,
            });
            await tester.ClearPersistentData();

            tester.Config.History.Backfill = true;
            tester.Config.History.Enabled = true;

            await tester.StartServer();
            tester.Server.PopulateEvents();

            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state =>
                    !state.FrontfillEnabled || !state.IsBackfilling && !state.IsFrontfilling),
                20, "Expected backfill of events to terminate");

            await tester.WaitForCondition(async () =>
            {
                var evts = await tester.GetAllInfluxEvents(tester.Server.Ids.Event.Obj1);
                return evts.Count() == 300;
            }, 5, "Expected to get some events in influxdb");

            await tester.TerminateRunTask();

            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_events_count") >= 1);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 0));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 500));
        }
        [Trait("Server", "events")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxbackfilleventsrestart")]
        [Fact]
        public async Task TestInfluxBackfillEventsRestart()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                Pusher = "influx",
                ConfigName = ConfigName.Events,
            });
            await tester.ClearPersistentData();

            tester.Config.History.Backfill = true;
            tester.Config.History.Enabled = true;

            await tester.StartServer();
            tester.Server.PopulateEvents();

            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state =>
                    !state.FrontfillEnabled || !state.IsBackfilling && !state.IsFrontfilling),
                20, "Expected backfill of events to terminate");

            await tester.WaitForCondition(async () =>
            {
                var evts = await tester.GetAllInfluxEvents(tester.Server.Ids.Event.Obj1);
                return evts.Count() == 300;
            }, 5, "Expected to get some events in influxdb");

            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_events_count") >= 1);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 0));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 500));

            CommonTestUtils.ResetTestMetrics();
            tester.Extractor.RestartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(100);

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state =>
                    !state.FrontfillEnabled || !state.IsBackfilling && !state.IsFrontfilling),
                20, "Expected backfill of events to terminate");

            await tester.WaitForCondition(async () =>
            {
                var evts = await tester.GetAllInfluxEvents(tester.Server.Ids.Event.Obj1);
                return evts.Count() == 304;
            }, 5, "Expected to get some events in influxdb");

            await tester.TerminateRunTask();

            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
        }
    }
}

/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.Types;
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
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;

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

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyPoints,
                10, "Failurebuffer must receive some data");

            await Task.Delay(500);
            tester.Handler.AllowPush = true;
            tester.Handler.AllowConnectionTest = true;

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.AnyPoints,
                10, "FailureBuffer should be emptied");

            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 1001);
            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1001);

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);
            
            await tester.WaitForCondition(() => tester.Handler.Datapoints.ContainsKey("gp.tl:i=10")
                && tester.Handler.Datapoints["gp.tl:i=10"].NumericDatapoints.DistinctBy(dp => dp.Timestamp).Count() == 1002, 20);
            await tester.TerminateRunTask(true);
            
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

            await tester.TerminateRunTask(false);
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

            await tester.TerminateRunTask(false);
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

            int cnt = 0;
            await tester.WaitForCondition(async () =>
            {
                var evts = await tester.GetAllInfluxEvents(ObjectIds.Server);
                cnt = evts.Count();
                return cnt == 700;
            }, 5, () => $"Expected to get 700 events in influxdb, but got {cnt}");

            await tester.TerminateRunTask(false);

            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_events_count") >= 1);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 0));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 900));
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

            int count = 0;
            await tester.WaitForCondition(async () =>
            {
                var evts = await tester.GetAllInfluxEvents(ObjectIds.Server);
                count = evts.Count();
                return count == 700;
            }, 10, () => $"Expected to get 700 events in influxdb, but got {count}");

            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_events_count") >= 1);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events", 0));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events", 900));

            CommonTestUtils.ResetTestMetrics();
            tester.Extractor.RestartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            tester.Server.TriggerEvents(100);

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state =>
                    !state.FrontfillEnabled || !state.IsBackfilling && !state.IsFrontfilling),
                20, "Expected backfill of events to terminate");

            await tester.WaitForCondition(async () =>
            {
                var evts = await tester.GetAllInfluxEvents(ObjectIds.Server);
                return evts.Count() == 707;
            }, 5, "Expected to get some events in influxdb");

            await tester.TerminateRunTask(false);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
        }
    }
}

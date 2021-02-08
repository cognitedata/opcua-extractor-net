﻿/* Cognite Extractor for OPC-UA
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [Collection("Extractor tests")]
    public class CDFPusherTests : MakeConsoleWork
    {
        public CDFPusherTests(ITestOutputHelper output) : base(output) { }
        
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "cdfarraydata")]
        [Fact]
        public async Task TestArrayData()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array,
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;

            await tester.StartServer();
            tester.Server.PopulateArrayHistory();

            tester.StartExtractor();

            var arrId = tester.UAClient.GetUniqueId(tester.Server.Ids.Custom.Array, 2);

            await tester.WaitForCondition(() =>
                tester.Handler.Assets.Count == 7
                && tester.Handler.Timeseries.Count == 16
                && tester.Handler.Datapoints.ContainsKey(arrId)
                && tester.Handler.Datapoints[arrId].NumericDatapoints.DistinctBy(pt => pt.Timestamp).Count() == 1000, 10,
                () => $"Expected to get 7 assets and got {tester.Handler.Assets.Count}, 16 timeseries and got {tester.Handler.Timeseries.Count}," +
                    $"1000 datapoints and got {tester.Handler.Datapoints[arrId].NumericDatapoints.DistinctBy(pt => pt.Timestamp).Count()}");

            int lastData = tester.Handler.Datapoints[arrId].NumericDatapoints.DistinctBy(pt => pt.Timestamp).Count();
            Assert.Equal(1000, lastData);

            tester.Server.UpdateNode(tester.Server.Ids.Custom.Array, new int[] { 1000, 1000, 1000, 1000 });

            await tester.WaitForCondition(() =>
                    tester.Handler.Datapoints[arrId].NumericDatapoints.Count > lastData, 20,
                "Expected data to increase");

            await tester.TerminateRunTask(true);
            
            tester.TestContinuity(arrId);

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(7, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(16, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
        }
        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "continuity")]
        public async Task TestDataContinuity()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();

            tester.Config.Source.SamplingInterval = 10;

            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.StartExtractor();

            string intVar = tester.UAClient.GetUniqueId(tester.Server.Ids.Base.IntVar);

            await tester.WaitForCondition(() =>
                    tester.Handler.Datapoints.ContainsKey(intVar)
                    && tester.Handler.Datapoints[intVar].NumericDatapoints.DistinctBy(pt => pt.Timestamp).Count() == 1000, 20,
                "Expected integer datapoint to get some values");

            await tester.Server.UpdateNodeMultiple(tester.Server.Ids.Base.IntVar, 10, i => i + 1000, 20);

            await tester.WaitForCondition(() =>
                tester.Handler.Datapoints[intVar].NumericDatapoints.DistinctBy(pt => pt.Timestamp).Count() == 1010, 5,
                "Expected to get the next 10 points");

            await tester.TerminateRunTask(true);

            tester.TestContinuity(intVar);

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(2, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(4, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
        }

        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "backfill")]
        public async Task TestBackfill()
        {
            long startTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                QuitAfterMap = false,
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();

            tester.Config.History.Backfill = true;

            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.StartExtractor();

            var intVar = "gp.tl:i=10";

            await tester.WaitForCondition(() =>
                    tester.Extractor.State.GetNodeState(intVar) != null
                    && !tester.Extractor.State.GetNodeState(intVar).IsBackfilling
                    && !tester.Extractor.State.GetNodeState(intVar).IsFrontfilling
                    && tester.Handler.Datapoints.ContainsKey(intVar)
                    && tester.Handler.Datapoints[intVar].NumericDatapoints.Any(pt => pt.Timestamp < startTime), 20,
                "Expected integer datapoint to finish backfill and frontfill");

            await tester.TerminateRunTask(true);

            tester.TestContinuity(intVar);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_data_count") >= 1);
            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Contains(tester.Handler.Datapoints[intVar].NumericDatapoints,pt => pt.Timestamp < startTime);
        }
        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "backfillrestart")]
        public async Task TestBackfillRestart()
        {
            long startTime = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                QuitAfterMap = false,
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();

            tester.Config.History.Backfill = true;

            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.StartExtractor();

            var intVar = "gp.tl:i=10";

            await tester.WaitForCondition(() =>
                    tester.Extractor.State.GetNodeState(intVar) != null
                    && !tester.Extractor.State.GetNodeState(intVar).IsBackfilling
                    && !tester.Extractor.State.GetNodeState(intVar).IsFrontfilling
                    && tester.Handler.Datapoints.ContainsKey(intVar)
                    && tester.Handler.Datapoints[intVar].NumericDatapoints.Any(pt => pt.Timestamp < startTime), 20,
                "Expected integer datapoint to finish backfill and frontfill");

            tester.TestContinuity(intVar);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_data_count") >= 1);
            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Contains(tester.Handler.Datapoints[intVar].NumericDatapoints, pt => pt.Timestamp < startTime);
            await tester.Extractor.Looper.WaitForNextPush();
            CommonTestUtils.ResetTestMetrics();
            tester.Extractor.RestartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            await tester.WaitForCondition(() =>
                    tester.Extractor.State.GetNodeState(intVar) != null
                    && !tester.Extractor.State.GetNodeState(intVar).IsBackfilling
                    && !tester.Extractor.State.GetNodeState(intVar).IsFrontfilling, 20,
                "Expected integer datapoint to finish backfill and frontfill");

            await tester.TerminateRunTask(false);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 1));
        }
    }
}

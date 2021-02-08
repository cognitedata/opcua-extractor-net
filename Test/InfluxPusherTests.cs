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
    }
}

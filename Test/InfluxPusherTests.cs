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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [CollectionDefinition("Influx_tests", DisableParallelization = true)]
    public class InfluxPusherTests : MakeConsoleWork
    {
        public InfluxPusherTests(ITestOutputHelper output) : base(output) { }
        [Trait("Server", "basic")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "pusher")]
        [Fact]
        public async Task TestInfluxPusher()
        {
            using var tester = new ExtractorTester(new TestParameters
            {
                ConfigName = ConfigName.Influx
            });
            await tester.ClearPersistentData();

            Assert.True(await tester.Pusher.TestConnection(tester.Source.Token));

            tester.StartExtractor();

            await tester.WaitForCondition(async () =>
            {
                var read = await tester.IfDbClient.QueryMultiSeriesAsync(tester.InfluxConfig.Database,
                    "SELECT * FROM \"gp.efg:i=2\"");
                return read.Count > 0 && read.First().HasEntries;
            }, 20, "Expected to find some data in influxdb");

            await tester.TerminateRunTask();
            Assert.False(((InfluxPusher) tester.Pusher).Failing);
        }
        [Trait("Server", "array")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "arraydata")]
        [Fact]
        public async Task TestArrayData()
        {
            using var tester = new ExtractorTester(new TestParameters
            {
                ConfigName = ConfigName.Influx,
                ServerName = ServerName.Array
            });
            await tester.ClearPersistentData();
            tester.Config.Extraction.MaxArraySize = 4;
            tester.Config.Extraction.AllowStringVariables = true;

            tester.StartExtractor();

            await tester.WaitForCondition(async () =>
            {
                var read = await tester.IfDbClient.QueryMultiSeriesAsync(tester.InfluxConfig.Database,
                    "SELECT * FROM \"gp.efg:i=2[3]\"");
                return read.Count > 0 && read.First().HasEntries;
            }, 20, "Expected to get some data");

            await tester.TerminateRunTask();

            Assert.False(((InfluxPusher)tester.Pusher).Failing);
        }

        [Trait("Server", "basic")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "nonfiniteinflux")]
        [Fact]
        public async Task TestNonFiniteInflux()
        {
            using var tester = new ExtractorTester(new TestParameters
            {
                ConfigName = ConfigName.Influx,
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();
            tester.Config.Source.History = false;

            tester.StartExtractor();

            await tester.TerminateRunTask();

            var values = new List<double>
            {
                1E100,
                -1E100,
                1E105,
                -1E105,
                double.MaxValue,
                double.MinValue
            };

            var pusher = tester.Pusher;

            foreach (var value in values)
            {
                pusher.BufferedDPQueue.Enqueue(new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", value));
            }
            pusher.BufferedDPQueue.Enqueue(new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.PositiveInfinity));
            pusher.BufferedDPQueue.Enqueue(new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.NegativeInfinity));
            pusher.BufferedDPQueue.Enqueue(new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.NaN));

            await pusher.PushDataPoints(CancellationToken.None);

            var read = await tester.IfDbClient.QueryMultiSeriesAsync(tester.InfluxConfig.Database, 
                "SELECT * FROM \"gp.efg:i=2\"");
            Assert.True(read.Count > 0);
            var readValues = read.First();

            foreach (var value in values)
            {
                Assert.Contains(readValues.Entries, entry => Math.Abs(Convert.ToDouble(entry.Value) - value) < 1);
            }
            Assert.False(((InfluxPusher)tester.Pusher).Failing);
        }

        [Trait("Server", "events")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxdbevents")]
        [Fact]
        public async Task TestInfluxdbEvents()
        {
            using var tester = new ExtractorTester(new TestParameters
            {
                ServerName = ServerName.Events,
                LogLevel = "debug",
                PusherConfig = ConfigName.Influx,
                ConfigName = ConfigName.Events
            });
            await tester.ClearPersistentData();
            tester.Config.Source.History = false;

            tester.StartExtractor();

            await tester.WaitForCondition(async () =>
            {
                var read = await tester.IfDbClient.QueryMultiSeriesAsync(tester.InfluxConfig.Database, 
                    "SELECT * FROM \"events.gp.efg:i=1\"");
                return read.Count > 0 && read.First().HasEntries &&
                       tester.Extractor.EventEmitterStates.All(state => state.Value.IsStreaming);
            }, 20, "Expected to get some events in influxdb");

            await tester.TerminateRunTask();
        }

        [Trait("Server", "basic")]
        [Trait("Target", "FailureBuffer")]
        [Trait("Test", "influxbuffer")]
        [Fact]
        public async Task TestInfluxBuffering()
        {
            var tester = new ExtractorTester(new TestParameters
            {
                ConfigName = ConfigName.Test,
                FailureInflux = ConfigName.Influx,
                StoreDatapoints = true,
                FailureInfluxWrite = true,
            });
            await tester.ClearPersistentData();
            tester.StartExtractor();

            tester.Handler.AllowPush = false;

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.Any,
                20, "Failurebuffer must receive some data");

            await Task.Delay(500);
            tester.Handler.AllowPush = true;

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.Any,
                20, "FailureBuffer should be emptied");

            await tester.TerminateRunTask();
            
            tester.TestContinuity("gp.efg:i=10");

            Assert.True(Common.VerifySuccessMetrics());
            Assert.Equal(2, (int)Common.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(4, (int)Common.GetMetricValue("opcua_tracked_timeseries"));
            Assert.NotEqual(0, (int)Common.GetMetricValue("opcua_datapoint_push_failures_cdf"));
        }

        [Trait("Server", "basic")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "influxautobuffer")]
        [Fact]
        public async Task TestInfluxAutoBuffer()
        {
            var tester = new ExtractorTester(new TestParameters
            {
                ConfigName = ConfigName.Influx,
                BufferDir = "./buffer/"
            });
            tester.Config.Source.History = false;
            await tester.ClearPersistentData();
            var bufferPath = Path.Join(tester.Config.FailureBuffer.FilePath, "buffer.bin");

            tester.StartExtractor();

            await tester.WaitForCondition(() => Common.GetMetricValue("opcua_datapoints_pushed_influx") > 0,
                20, "Expected InfluxPusher to start working");

            var oldHost = tester.InfluxConfig.Host;
            tester.InfluxConfig.Host = "testWrong";
            ((InfluxPusher)tester.Pusher).Reconfigure();

            await tester.WaitForCondition(() => new FileInfo(bufferPath).Length > 0, 20,
                "Expected some data to be written");

            tester.InfluxConfig.Host = oldHost;
            ((InfluxPusher)tester.Pusher).Reconfigure();

            await tester.WaitForCondition(() => new FileInfo(bufferPath).Length == 0, 20,
                () => $"Expected file to be emptied, but it contained {new FileInfo(bufferPath).Length} bytes of data");

            await tester.TerminateRunTask();

            var dps = await ((InfluxPusher)tester.Pusher)
                .ReadDataPoints(DateTime.UnixEpoch, new Dictionary<string, bool> {{"gp.efg:i=10", false}}, tester.Source.Token);

            var intdps = dps.GroupBy(dp => dp.Timestamp).Select(dp => (int)Math.Round(dp.First().DoubleValue)).ToList();

            tester.TestContinuity(intdps);

            Assert.True(Common.VerifySuccessMetrics());
            Assert.NotEqual(0, (int)Common.GetMetricValue("opcua_datapoint_push_failures_influx"));
        }
    }
}

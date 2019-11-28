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
    }
}

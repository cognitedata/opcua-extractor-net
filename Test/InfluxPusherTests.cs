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

using AdysTech.InfluxDB.Client.Net;
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
            
            var fullConfig = Common.BuildConfig("basic", 6, "config.influxtest.yml");
            if (fullConfig == null) throw new Exception("Bad config");
            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);
            var config = (InfluxClientConfig)fullConfig.Pushers.First();
            var ifDBclient = new InfluxDBClient(config.Host, config.Username, config.Password);
            await ifDBclient.DropMeasurementAsync(new InfluxDatabase(config.Database), new InfluxMeasurement("gp.efg:i=2"));
            var pusher = new InfluxPusher(config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();

            Assert.True(await pusher.TestConnection(source.Token));

            var runTask = extractor.RunExtractor(source.Token);
            bool gotData = false;
            for (int i = 0; i < 20; i++)
            {
                var read = await ifDBclient.QueryMultiSeriesAsync(config.Database, "SELECT * FROM \"gp.efg:i=2\"");
                if (read.Count > 0 && read.First().HasEntries)
                {
                    gotData = true;
                    break;
                }
                Thread.Sleep(1000);
            }
            Assert.True(gotData, "Expecting to find some data in influxdb");
            source.Cancel();
            try
            {
                await runTask;
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }
            extractor.Close();
            Assert.False(pusher.Failing);
        }
        [Trait("Server", "array")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "arraydata")]
        [Fact]
        public async Task TestArrayData()
        {
            var fullConfig = Common.BuildConfig("array", 7, "config.influxtest.yml");
            fullConfig.Extraction.MaxArraySize = 4;
            fullConfig.Extraction.AllowStringVariables = true;
            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);
            var config = (InfluxClientConfig)fullConfig.Pushers.First();
            var pusher = new InfluxPusher(config);
            var ifDBclient = new InfluxDBClient(config.Host, config.Username, config.Password);
            await ifDBclient.DropMeasurementAsync(new InfluxDatabase(config.Database), new InfluxMeasurement("gp.efg:i=2[3]"));


            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);
            bool gotData = false;
            for (int i = 0; i < 20; i++)
            {
                var read = await ifDBclient.QueryMultiSeriesAsync(config.Database, "SELECT * FROM \"gp.efg:i=2[3]\"");
                if (read.Count > 0 && read.First().HasEntries)
                {
                    gotData = true;
                    break;
                }
                Thread.Sleep(1000);
            }
            Assert.True(gotData);
            source.Cancel();
            try
            {
                await runTask;
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }
            extractor.Close();
            Assert.False(pusher.Failing);
        }

        [Trait("Server", "basic")]
        [Trait("Target", "InfluxPusher")]
        [Trait("Test", "nonfiniteinflux")]
        [Fact]
        public async Task TestNonFiniteInflux()
        {
            var fullConfig = Common.BuildConfig("basic", 21, "config.influxtest.yml");
            Logger.Configure(fullConfig.Logging);
            fullConfig.Source.History = false;
            var client = new UAClient(fullConfig);
            var config = (InfluxClientConfig)fullConfig.Pushers.First();
            var pusher = new InfluxPusher(config);
            var ifDBclient = new InfluxDBClient(config.Host, config.Username, config.Password);
            await ifDBclient.DropMeasurementAsync(new InfluxDatabase(config.Database), new InfluxMeasurement("gp.efg:i=2"));

            var extractor = new Extractor(fullConfig, pusher, client);
            try
            {
                await extractor.RunExtractor(CancellationToken.None, true);
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }
            extractor.Close();
            var values = new List<double>
            {
                1E100,
                -1E100,
                1E105,
                -1E105,
                double.MaxValue,
                double.MinValue
            };

            foreach (var value in values)
            {
                pusher.BufferedDPQueue.Enqueue(new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", value));
            }
            pusher.BufferedDPQueue.Enqueue(new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.PositiveInfinity));
            pusher.BufferedDPQueue.Enqueue(new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.NegativeInfinity));
            pusher.BufferedDPQueue.Enqueue(new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.NaN));


            await pusher.PushDataPoints(CancellationToken.None);


            var read = await ifDBclient.QueryMultiSeriesAsync(config.Database, "SELECT * FROM \"gp.efg:i=2\"");
            Assert.True(read.Count > 0);
            var readValues = read.First();
            foreach (var value in values)
            {
                Assert.Contains(readValues.Entries, entry => Math.Abs(Convert.ToDouble(entry.Value) - value) < 1);
            }
            Assert.False(pusher.Failing);
        }
    }
}

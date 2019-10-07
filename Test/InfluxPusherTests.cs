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
        [Trait("Category", "basicserver")]
        [Fact]
        public async Task TestInfluxPusher()
        {
            var fullConfig = Common.BuildConfig("basic", 6, "config.influxtest.yml");
            if (fullConfig == null) throw new Exception("Bad config");
            Logger.Configure(fullConfig.Logging);
            UAClient client = new UAClient(fullConfig);
            var config = (InfluxClientConfig)fullConfig.Pushers.First();
            var pusher = new InfluxPusher(config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);
                var ifDBclient = new InfluxDBClient(config.Host, config.Username, config.Password);
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
                Assert.False(pusher.failing);
            }
        }
        [Trait("Category", "ArrayServer")]
        [Fact]
        public async Task TestArrayData()
        {
            var fullConfig = Common.BuildConfig("array", 7, "config.influxtest.yml");
            fullConfig.Extraction.MaxArraySize = 4;
            fullConfig.Extraction.AllowStringVariables = true;
            Logger.Configure(fullConfig.Logging);
            UAClient client = new UAClient(fullConfig);
            var config = (InfluxClientConfig)fullConfig.Pushers.First();
            var pusher = new InfluxPusher(config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);
                var ifDBclient = new InfluxDBClient(config.Host, config.Username, config.Password);
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
                Assert.False(pusher.failing);
            }
        }
    }
}

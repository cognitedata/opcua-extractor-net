using AdysTech.InfluxDB.Client.Net;
using Cognite.OpcUa;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Test
{
    [CollectionDefinition("Influx_tests", DisableParallelization = true)]
    public class InfluxPusherTests
    {
        [Trait("Category", "basicserver")]
        [Fact]
        public async Task TestInfluxPusher()
        {
            var fullConfig = Common.BuildConfig("basic", 6, "config.influxtest.yml");
            if (fullConfig == null) throw new Exception("Bad config");
            Logger.Configure(fullConfig.LoggerConfig);
            UAClient client = new UAClient(fullConfig);
            var config = (InfluxClientConfig)fullConfig.Pushers.First();
            var pusher = new InfluxPusher(config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);
                var ifDBclient = new InfluxDBClient(config.Host, config.Username, config.Password);
                bool gotData = false;
                for (int i = 0; i < 10; i++)
                {
                    var read = await ifDBclient.QueryMultiSeriesAsync(config.Database, "SELECT * FROM \"gp.efg:i=2\"");
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
        [Trait("Category", "ArrayServer")]
        [Fact]
        public async Task TestArrayData()
        {
            var fullConfig = Common.BuildConfig("array", 6, "config.influxtest.yml");
            fullConfig.UAConfig.MaxArraySize = 4;
            fullConfig.UAConfig.AllowStringVariables = true;
            Logger.Configure(fullConfig.LoggerConfig);
            UAClient client = new UAClient(fullConfig);
            var config = (InfluxClientConfig)fullConfig.Pushers.First();
            var pusher = new InfluxPusher(config);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);
                var ifDBclient = new InfluxDBClient(config.Host, config.Username, config.Password);
                bool gotData = false;
                for (int i = 0; i < 10; i++)
                {
                    var read = await ifDBclient.QueryMultiSeriesAsync(config.Database, "SELECT * FROM \"gp.efg:i=2[3]\"");
                    var read2 = await ifDBclient.QueryMultiSeriesAsync(config.Database, "SELECT * FROM \"gp.efg:i=3[1]\"");
                    if (read.Count > 0 && read.First().HasEntries && read2.Count > 0 && read2.First().HasEntries)
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

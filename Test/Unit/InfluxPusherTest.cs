using AdysTech.InfluxDB.Client.Net;
using Cognite.OpcUa;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class InfluxPusherTestFixture : BaseExtractorTestFixture
    {
        public InfluxPusherTestFixture() : base(63000)
        {
        }
        public (InfluxDBClient, InfluxPusher) GetPusher(bool clear = true)
        {
            if (Config.Influx == null)
            {
                Config.Influx = new InfluxPusherConfig();
            }
            Config.Influx.Database ??= "testdb-pusher";
            Config.Influx.Host ??= "http://localhost:8086";

            var client = new InfluxDBClient(Config.Influx.Host, Config.Influx.Username, Config.Influx.Password);
            if (clear)
            {
                ClearDB(client).Wait();
            }
            var pusher = Config.Influx.ToPusher(null) as InfluxPusher;
            return (client, pusher);
        }
        public async Task ClearDB(InfluxDBClient client)
        {
            if (client == null) return;
            await client.DropDatabaseAsync(new InfluxDatabase(Config.Influx.Database));
            await client.CreateDatabaseAsync(Config.Influx.Database);
        }
    }
    public class InfluxPusherTest : MakeConsoleWork, IClassFixture<InfluxPusherTestFixture>
    {
        private readonly InfluxPusherTestFixture tester;
        public InfluxPusherTest(ITestOutputHelper output, InfluxPusherTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        [Fact]
        public async Task TestTestConnection()
        {
            var (client, pusher) = tester.GetPusher();

            // Test with against non-existing server
            tester.Config.Influx.Host = "http://localhost:8000";
            pusher.Reconfigure();

            // Debug true
            tester.Config.Influx.Debug = true;
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            tester.Config.Influx.Debug = false;

            // Fail due to bad host
            Assert.False(await pusher.TestConnection(tester.Config, tester.Source.Token));

            // Db does not exist
            tester.Config.Influx.Host = "http://localhost:8086";
            pusher.Reconfigure();
            await client.DropDatabaseAsync(new InfluxDatabase(tester.Config.Influx.Database));
            var dbs = await client.GetInfluxDBNamesAsync();
            Assert.DoesNotContain(dbs, db => db == tester.Config.Influx.Database);
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
            dbs = await client.GetInfluxDBNamesAsync();
            Assert.Contains(dbs, db => db == tester.Config.Influx.Database);

            // Normal operation
            Assert.True(await pusher.TestConnection(tester.Config, tester.Source.Token));
        }
    }
}

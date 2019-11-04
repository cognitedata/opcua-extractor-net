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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Prometheus.Client;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [CollectionDefinition("Pusher_tests", DisableParallelization = true)]
    public class CDFPusherTests : MakeConsoleWork
    {
        public CDFPusherTests(ITestOutputHelper output) : base(output) { }
        [Trait("Server", "basic")]
        [Trait("Server", "full")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "pusher")]
        [Theory]
        [InlineData(CDFMockHandler.MockMode.All, "basic")]
        [InlineData(CDFMockHandler.MockMode.Some, "basic")]
        [InlineData(CDFMockHandler.MockMode.None, "basic")]
        [InlineData(CDFMockHandler.MockMode.FailAsset, "basic")]
        [InlineData(CDFMockHandler.MockMode.All, "full")]
        [InlineData(CDFMockHandler.MockMode.Some, "full")]
        [InlineData(CDFMockHandler.MockMode.None, "full")]
        [InlineData(CDFMockHandler.MockMode.FailAsset, "full")]
        public async Task TestBasicPushing(CDFMockHandler.MockMode mode, string serverType)
        {
            Common.ResetTestMetrics();
            var fullConfig = Common.BuildConfig(serverType, 3);
            fullConfig.Extraction.AllowStringVariables = false;
            Logger.Configure(fullConfig.Logging);
            Log.Information("Starting OPC UA Extractor version {version}", Cognite.OpcUa.Version.GetVersion());
            Log.Information("Revision information: {status}", Cognite.OpcUa.Version.Status());
            Log.Information("Testing with MockMode {TestBasicPushingMockMode}", mode.ToString());
            var client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var handler = new CDFMockHandler(config.Project, mode);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);
            var extractor = new Extractor(fullConfig, pusher, client);
            try
            {
                await extractor.RunExtractor(CancellationToken.None, true);
            }
            catch (Exception e)
            {
                if (mode != CDFMockHandler.MockMode.FailAsset)
                {
                    if (!Common.TestRunResult(e)) throw;
                }
            }
            extractor.Close();
            Assert.True(Common.VerifySuccessMetrics());
            Assert.Equal(mode == CDFMockHandler.MockMode.FailAsset ? 1 : 0, (int)Common.GetMetricValue("opcua_node_ensure_failures"));

            if (mode == CDFMockHandler.MockMode.None)
            {
                Assert.DoesNotContain(handler.timeseries.Values, ts => ts.name == "MyString");
                Assert.Contains(handler.assets.Values, asset =>
                    asset.name == "MyObject" && asset.metadata != null
                    && asset.metadata["Asset prop 1"] == "test"
                    && asset.metadata["Asset prop 2"] == "123.21");
                Assert.Contains(handler.timeseries.Values, ts =>
                    ts.name == "MyVariable" && ts.metadata != null
                    && ts.metadata["TS property 1"] == "test"
                    && ts.metadata["TS property 2"] == "123.2");
            }

            if (mode != CDFMockHandler.MockMode.FailAsset)
            {
                Assert.Equal(serverType == "basic" ? 2 : 154, handler.assets.Count);
                Assert.Equal(serverType == "basic" ? 4 : 2002, handler.timeseries.Count);
            }
        }
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "autobuffer")]
        [Fact]
        public async Task TestAutoBuffering()
        {
            Common.ResetTestMetrics();
            var fullConfig = Common.BuildConfig("basic", 4);
            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            handler.StoreDatapoints = true;
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);

            File.Create(config.BufferFile).Close();
            handler.AllowPush = false;
            bool gotData = false;
            for (int i = 0; i < 40; i++)
            {
                if (new FileInfo(config.BufferFile).Length > 0)
                {
                    gotData = true;
                    break;
                }
                await Task.Delay(500);
            }

            await Task.Delay(1000);
            Assert.True(gotData, "Some data must be written");
            handler.AllowPush = true;
            gotData = false;
            for (int i = 0; i < 40; i++)
            {
                if (new FileInfo(config.BufferFile).Length == 0)
                {
                    gotData = true;
                    break;
                }
                await Task.Delay(500);
            }
            Assert.True(gotData, $"Expecting file to be emptied, but it contained {new FileInfo(config.BufferFile).Length} bytes of data");
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

            var dps = handler.datapoints["gp.efg:i=10"].Item1;
            var intdps = dps.GroupBy(dp => dp.Timestamp).Select(dp => (int)Math.Round(dp.First().Value)).ToList();
            var min = intdps.Min();
            var check = new int[intdps.Count];

            int last = 0;
            foreach (var dp in intdps)
            {
                if (last != dp - 1)
                {
                    Log.Information("Out of order points at {dp}, {last}", dp, last);
                }
                last = dp;
                check[dp - min]++;
            }
            Assert.True(Common.VerifySuccessMetrics());
            Assert.Equal(2, (int)Common.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(4, (int)Common.GetMetricValue("opcua_tracked_timeseries"));
            Assert.NotEqual(0, (int)Common.GetMetricValue("opcua_datapoint_push_failures"));
        }
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "debugmode")]
        [Fact]
        public async Task TestDebugMode()
        {
            Common.ResetTestMetrics();
            var fullConfig = Common.BuildConfig("basic", 5);
            if (fullConfig == null) throw new Exception("No config");
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            config.Debug = true;
            config.ApiKey = null;

            Logger.Configure(fullConfig.Logging);
            var client = new UAClient(fullConfig);
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);
            bool started = false;
            for (int i = 0; i < 20; i++)
            {
                if (extractor.Pushing)
                {
                    started = true;
                    break;
                }
                Thread.Sleep(500);
            }
            Assert.True(started);
            source.Cancel();
            try
            {
                await runTask;
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }
            Assert.Equal(0, handler.RequestCount);
            extractor.Close();
            Assert.True(Common.VerifySuccessMetrics());
        }
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "arraydata")]
        [Fact]
        public async Task TestArrayData()
        {
            Common.ResetTestMetrics();
            var fullConfig = Common.BuildConfig("array", 6);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            fullConfig.Extraction.AllowStringVariables = true;
            fullConfig.Extraction.MaxArraySize = 4;
            Logger.Configure(fullConfig.Logging);

            var client = new UAClient(fullConfig);
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None) {StoreDatapoints = true};
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            bool gotData = false;
            var runTask = extractor.RunExtractor(source.Token);
            for (int i = 0; i < 20; i++)
            {
                if (handler.assets.Count == 4 && handler.timeseries.Count == 7
                    && handler.datapoints.ContainsKey("gp.efg:i=2[2]"))
                {
                    gotData = true;
                    break;
                }
                await Task.Delay(500);
            }

            Assert.True(gotData, $"Expected to get 4 assets and got {handler.assets.Count}, 7 timeseries and got {handler.timeseries.Count}");

            // Expect data to be increasing through subscriptions
            int lastData = handler.datapoints["gp.efg:i=2[2]"].Item1.Count;
            for (int i = 0; i < 20; i++)
            {
                if (handler.datapoints["gp.efg:i=2[2]"].Item1.Count > lastData) break;
                await Task.Delay(500);
            }
            Assert.True(handler.datapoints["gp.efg:i=2[2]"].Item1.Count > lastData);

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

            var dps = handler.datapoints["gp.efg:i=2[2]"].Item1;
            var intdps = dps.GroupBy(dp => dp.Timestamp).Select(dp => (int)dp.First().Value).ToList();
            var min = intdps.Min();
            var check = new int[intdps.Count];

            int last = 0;
            foreach (var dp in intdps)
            {
                if (last != dp - 1)
                {
                    Log.Information("Out of order points at {dp}, {last}", dp, last);
                }
                last = dp;
                check[dp - min]++;
            }
            Assert.True(Common.VerifySuccessMetrics());
            Assert.Equal(4, (int)Common.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(7, (int)Common.GetMetricValue("opcua_tracked_timeseries"));
        }
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "restart")]
        [Fact]
        public async Task TestExtractorRestart()
        {
            Common.ResetTestMetrics();
            var fullConfig = Common.BuildConfig("basic", 9);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            Logger.Configure(fullConfig.Logging);

            var client = new UAClient(fullConfig);
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            bool started = false;
            var runTask = extractor.RunExtractor(source.Token);
            for (int i = 0; i < 20; i++)
            {
                if (extractor.Pushing)
                {
                    started = true;
                    break;
                }
                Thread.Sleep(500);
            }
            Assert.True(started);
            Assert.True(extractor.Started);
            extractor.RestartExtractor(source.Token);
            Thread.Sleep(500);
            started = false;
            for (int i = 0; i < 20; i++)
            {
                if (extractor.Started)
                {
                    started = true;
                    break;
                }
                Thread.Sleep(500);
            }
            Assert.True(started);
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
        }
        [InlineData(20000, 100, 20, 1000, 100000)]
        [InlineData(200, 10000, 20, 10, 100000)]
        [InlineData(20000, 5, 2, 10000, 50000)]
        [Trait("Server", "none")]
        [Trait("Target", "Utils")]
        [Trait("Test", "dictChunk")]
        [Theory]
        public void TestDictionaryChunking(int timeseries, int datapoints, int expChunks, int expTimeseriesMax, int expDatapointsMax)
        {
            var dict = new Dictionary<string, List<BufferedDataPoint>>();
            for (int i = 0; i < timeseries; i++)
            {
                var points = new List<BufferedDataPoint>();
                for (int j = 0; j < datapoints; j++)
                {
                    points.Add(new BufferedDataPoint(DateTime.MinValue, "id" + i, i*datapoints + j));
                }

                dict["id" + i] = points;
            }
            var results = Utils.ChunkDictOfLists(dict, 100000, 10000);
            var min = results.Min(dct => dct.Values.Min(val => val.Count()));
            Assert.True(min > 0);
            var max = results.Max(dct => dct.Values.Sum(val => val.Count()));
            var maxTs = results.Max(dct => dct.Values.Count);
            Assert.Equal(expDatapointsMax, max);
            Assert.Equal(expTimeseriesMax, maxTs);
            Assert.Equal(expChunks, results.Count());
            var total = results.Sum(dct => dct.Values.Sum(val => val.Count()));
            var totalTs = results.Sum(dct => dct.Values.Count);
            Assert.Equal(timeseries, totalTs);
            Assert.Equal(datapoints * timeseries, total);

            var exists = new bool[timeseries * datapoints];
            foreach (var dct in results)
            {
                foreach (var kvp in dct)
                {
                    foreach (var dp in kvp.Value)
                    {
                        exists[(int) dp.DoubleValue] = true;
                    }
                }
            }
            Assert.True(exists.All(val => val));

        }
        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "connectiontest")]
        public async Task TestConnectionTest()
        {
            var fullConfig = Common.BuildConfig("basic", 9);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            Logger.Configure(fullConfig.Logging);

            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);
            var res = await pusher.TestConnection(CancellationToken.None);
            Log.CloseAndFlush();
            Assert.True(res);
        }
        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "continuity")]
        public async Task TestDataContinuity()
        {
            Common.ResetTestMetrics();
            var fullConfig = Common.BuildConfig("basic", 15);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            Logger.Configure(fullConfig.Logging);

            var client = new UAClient(fullConfig);
            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher = new CDFPusher(Common.GetDummyProvider(handler), config);
            handler.StoreDatapoints = true;

            var extractor = new Extractor(fullConfig, pusher, client);
            using var source = new CancellationTokenSource();
            var runTask = extractor.RunExtractor(source.Token);

            for (int i = 0; i < 10; i++)
            {
                // Wait until we get some data on the integer datapoint
                if (handler.datapoints.ContainsKey("gp.efg:i=10") &&
                    handler.datapoints["gp.efg:i=10"].Item1.Count > 100) break;
                await Task.Delay(1000);
            }
            // We want some extra subscriptions as well
            await Task.Delay(1000);

            Log.Information("End loop: {count}", handler.datapoints["gp.efg:i=10"].Item1.Count);
            Assert.True(handler.datapoints["gp.efg:i=10"].Item1.Count > 100);

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

            var dps = handler.datapoints["gp.efg:i=10"].Item1;
            var intdps = dps.GroupBy(dp => dp.Timestamp).Select(dp => (int)dp.First().Value).ToList();
            var min = intdps.Min();
            var check = new int[intdps.Count];

            int last = 0;
            foreach (var dp in intdps)
            {
                if (last != dp - 1)
                {
                    Log.Information("Out of order points at {dp}, {last}", dp, last);
                }
                last = dp;
                check[dp-min]++;
            }

            Assert.True(check.All(count => count == 1));
            Assert.True(Common.VerifySuccessMetrics());
            Assert.Equal(2, (int)Common.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(4, (int)Common.GetMetricValue("opcua_tracked_timeseries"));
        }

        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "multiplecdf")]
        // Multiple pushers that fetch properties does some magic to avoid fetching data twice
        public async Task TestMultipleCDFPushers()
        {
            Common.ResetTestMetrics();
            var fullConfig = Common.BuildConfig("basic", 16);
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            Logger.Configure(fullConfig.Logging);

            var client = new UAClient(fullConfig);
            var handler1 = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var handler2 = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher1 = new CDFPusher(Common.GetDummyProvider(handler1), config);
            var pusher2 = new CDFPusher(Common.GetDummyProvider(handler2), config);

            var extractor = new Extractor(fullConfig, new List<IPusher> { pusher1, pusher2 }, client);
            try
            {
                await extractor.RunExtractor(CancellationToken.None, true);
            }
            catch (Exception e)
            {
                if (!Common.TestRunResult(e)) throw;
            }
            extractor.Close();
            Assert.DoesNotContain(handler1.timeseries.Values, ts => ts.name == "MyString");
            Assert.Contains(handler1.assets.Values, asset =>
                asset.name == "MyObject"
                && asset.metadata != null 
                && asset.metadata["Asset prop 1"] == "test" 
                && asset.metadata["Asset prop 2"] == "123.21");
            Assert.Contains(handler1.timeseries.Values, ts =>
                ts.name == "MyVariable" 
                && ts.metadata != null 
                && ts.metadata["TS property 1"] == "test" 
                && ts.metadata["TS property 2"] == "123.2");
            Assert.DoesNotContain(handler2.timeseries.Values, ts => ts.name == "MyString");
            Assert.Contains(handler2.assets.Values, asset =>
                asset.name == "MyObject"
                && asset.metadata != null
                && asset.metadata["Asset prop 1"] == "test"
                && asset.metadata["Asset prop 2"] == "123.21");
            Assert.Contains(handler2.timeseries.Values, ts =>
                ts.name == "MyVariable"
                && ts.metadata != null
                && ts.metadata["TS property 1"] == "test"
                && ts.metadata["TS property 2"] == "123.2");
            // Note that each pusher counts on the same metrics, so we would expect double values here.
            Assert.True(Common.VerifySuccessMetrics());
            Assert.Equal(4, (int)Common.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(8, (int)Common.GetMetricValue("opcua_tracked_timeseries"));
            // 1 for root, 1 for MyObject, 1 for asset/timeseries properties
            Assert.Equal(3, (int)Common.GetMetricValue("opcua_browse_operations"));
        }
    }
}

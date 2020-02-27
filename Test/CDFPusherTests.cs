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
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [CollectionDefinition("Pusher_tests", DisableParallelization = true)]
    public class CDFPusherTests : MakeConsoleWork
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(CDFPusherTests));

        public CDFPusherTests(ITestOutputHelper output) : base(output) { }
        [Trait("Server", "basic+full")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "pusher")]
        [Theory]
        [InlineData(CDFMockHandler.MockMode.All, ServerName.Basic)]
        [InlineData(CDFMockHandler.MockMode.Some, ServerName.Basic)]
        [InlineData(CDFMockHandler.MockMode.None, ServerName.Basic)]
        [InlineData(CDFMockHandler.MockMode.FailAsset, ServerName.Basic)]
        [InlineData(CDFMockHandler.MockMode.All, ServerName.Full)]
        [InlineData(CDFMockHandler.MockMode.Some, ServerName.Full)]
        [InlineData(CDFMockHandler.MockMode.None, ServerName.Full)]
        [InlineData(CDFMockHandler.MockMode.FailAsset, ServerName.Full)]
        public async Task TestBasicPushing(CDFMockHandler.MockMode mode, ServerName serverType)
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = serverType,
                QuitAfterMap = true,
                MockMode = mode
            });
            tester.Config.Extraction.AllowStringVariables = false;
            await tester.ClearPersistentData();

            log.Information("Testing with MockMode {TestBasicPushingMockMode}", mode.ToString());
            tester.StartExtractor();
            await tester.TerminateRunTask(ex => mode == CDFMockHandler.MockMode.FailAsset || CommonTestUtils.TestRunResult(ex));

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(mode == CDFMockHandler.MockMode.FailAsset ? 1 : 0, (int)CommonTestUtils.GetMetricValue("opcua_node_ensure_failures"));

            if (mode == CDFMockHandler.MockMode.None)
            {
                Assert.DoesNotContain(tester.Handler.timeseries.Values, ts => ts.name == "MyString");
                Assert.Contains(tester.Handler.assets.Values, asset =>
                    asset.name == "MyObject" && asset.metadata != null
                    && asset.metadata["Asset prop 1"] == "test"
                    && asset.metadata["Asset prop 2"] == "123.21");
                Assert.Contains(tester.Handler.timeseries.Values, ts =>
                    ts.name == "MyVariable" && ts.metadata != null
                    && ts.metadata["TS property 1"] == "test"
                    && ts.metadata["TS property 2"] == "123.2");
            }

            if (mode != CDFMockHandler.MockMode.FailAsset)
            {
                Assert.Equal(serverType == ServerName.Basic ? 2 : 154, tester.Handler.assets.Count);
                Assert.Equal(serverType == ServerName.Basic ? 4 : 2002, tester.Handler.timeseries.Count);
            }
        }
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "debugmode")]
        [Fact]
        public async Task TestDebugMode()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters());
            await tester.ClearPersistentData();

            tester.CogniteConfig.Debug = true;
            tester.CogniteConfig.ApiKey = null;

            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Extractor.Pushing, 10, "Expected extractor to start pushing");

            await tester.TerminateRunTask();

            Assert.Equal(0, tester.Handler.RequestCount);
            Assert.True(CommonTestUtils.VerifySuccessMetrics());
        }
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

            tester.Config.Extraction.AllowStringVariables = true;
            tester.Config.Extraction.MaxArraySize = 4;

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                tester.Handler.assets.Count == 5
                && tester.Handler.timeseries.Count == 10
                && tester.Handler.datapoints.ContainsKey("gp.efg:i=2[2]"), 20, 
                () => $"Expected to get 5 assets and got {tester.Handler.assets.Count}"
                      + $", 10 timeseries and got {tester.Handler.timeseries.Count}");

            int lastData = tester.Handler.datapoints["gp.efg:i=2[2]"].Item1.Count;

            await tester.WaitForCondition(() =>
                    tester.Handler.datapoints["gp.efg:i=2[2]"].Item1.Count > lastData, 20,
                "Expected data to be increasing");

            await tester.TerminateRunTask();
            
            tester.TestContinuity("gp.efg:i=2[2]");

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(5, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(10, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
        }
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "customdatatypes")]
        [Fact]
        public async Task TestCustomDataTypes()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array,
                StoreDatapoints = true,
                LogLevel = "debug"
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.AllowStringVariables = true;
            tester.Config.Extraction.MaxArraySize = 4;

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Handler.assets.Count == 5
                    && tester.Handler.timeseries.Count == 10
                    && tester.Handler.datapoints.ContainsKey("gp.efg:i=2[2]"), 20,
                () => $"Expected to get 5 assets and got {tester.Handler.assets.Count}"
                      + $", 10 timeseries and got {tester.Handler.timeseries.Count}");

            await tester.WaitForCondition(() =>
                    tester.Handler.datapoints.ContainsKey("gp.efg:i=16")
                    && tester.Handler.datapoints.ContainsKey("gp.efg:i=17")
                    && tester.Handler.datapoints.ContainsKey("gp.efg:i=14")
                    && tester.Handler.datapoints["gp.efg:i=16"].Item1.Any()
                    && tester.Handler.datapoints["gp.efg:i=17"].Item1.Any()
                    && tester.Handler.datapoints["gp.efg:i=14"].Item2.Any(), 20,
                "Expected to get some data");

            await tester.TerminateRunTask();

            var numericTypeVar = tester.Handler.timeseries.Values.First(ts => ts.name == "NumericTypeVar");
            Assert.False(numericTypeVar.isString);
            Assert.True(numericTypeVar.metadata.ContainsKey("EngineeringUnits"));
            Assert.Equal("°C: degree Celsius", numericTypeVar.metadata["EngineeringUnits"]);
            Assert.True(numericTypeVar.metadata.ContainsKey("EURange"));
            Assert.Equal("(0, 100)", numericTypeVar.metadata["EURange"]);

            var numericTypeVar2 = tester.Handler.timeseries.Values.First(ts => ts.name == "NumericTypeVar2");
            Assert.False(numericTypeVar2.isString);

            var stringyVar = tester.Handler.timeseries.Values.First(ts => ts.name == "StringyVar");
            Assert.True(stringyVar.isString);

            Assert.DoesNotContain(tester.Handler.timeseries.Values, ts => ts.name == "IgnoreTypeVar");

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(5, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(10, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
        }
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "restart")]
        [Fact]
        public async Task TestExtractorRestart()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters());
            await tester.ClearPersistentData();
            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Extractor.Pushing, 20,
                () => "Expected extractor to start pushing initially");

            tester.Extractor.RestartExtractor();

            await Task.Delay(500);

            await tester.WaitForCondition(() => tester.Extractor.Started, 20,
                "Expected extractor to start up after restart");

            await tester.TerminateRunTask();
        }
        [InlineData(20000, 100, 20, 1000, 100000)]
        [InlineData(200, 10000, 20, 10, 100000)]
        [InlineData(20000, 5, 2, 10000, 50000)]
        [InlineData(20, 150000, 30, 2, 100000)]
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
            var results = ExtractorUtils.ChunkDictOfLists(dict, 100000, 10000);
            var min = results.Min(dct => dct.Values.Min(val => val.Count()));
            Assert.True(min > 0);
            var max = results.Max(dct => dct.Values.Sum(val => val.Count()));
            var maxTs = results.Max(dct => dct.Values.Count);
            Assert.Equal(expDatapointsMax, max);
            Assert.Equal(expTimeseriesMax, maxTs);
            Assert.Equal(expChunks, results.Count());
            var total = results.Sum(dct => dct.Values.Sum(val => val.Count()));
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
            var fullConfig = CommonTestUtils.BuildConfig("basic", "config.events.yml");
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            Logger.Configure(fullConfig.Logging);

            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            using var pusher = new CDFPusher(CommonTestUtils.GetDummyProvider(handler), config);
            var res = await pusher.TestConnection(fullConfig, CancellationToken.None);
            Assert.True(res);
        }
        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "continuity")]
        public async Task TestDataContinuity()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true,
                LogLevel = "debug"
            });
            await tester.ClearPersistentData();

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Handler.datapoints.ContainsKey("gp.efg:i=10")
                    && tester.Handler.datapoints["gp.efg:i=10"].Item1.Count > 100, 20,
                "Expected integer datapoint to get some values");

            // We want some extra subscriptions as well
            await Task.Delay(1000);

            await tester.TerminateRunTask();

            tester.TestContinuity("gp.efg:i=10");

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(2, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(4, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
        }

        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "multiplecdf")]
        // Multiple pushers that fetch properties does some magic to avoid fetching data twice
        public async Task TestMultipleCDFPushers()
        {
            CommonTestUtils.ResetTestMetrics();
            var fullConfig = CommonTestUtils.BuildConfig("basic");
            var config = (CogniteClientConfig)fullConfig.Pushers.First();
            fullConfig.Logging.ConsoleLevel = "debug";
            Logger.Configure(fullConfig.Logging);

            using var client = new UAClient(fullConfig);
            var handler1 = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var handler2 = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            var pusher1 = new CDFPusher(CommonTestUtils.GetDummyProvider(handler1), config);
            var pusher2 = new CDFPusher(CommonTestUtils.GetDummyProvider(handler2), config);

            var extractor = new Extractor(fullConfig, new List<IPusher> { pusher1, pusher2 }, client);
            try
            {
                await extractor.RunExtractor(CancellationToken.None, true);
            }
            catch (Exception e)
            {
                if (!CommonTestUtils.TestRunResult(e)) throw;
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
            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(4, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(8, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
            // 1 for root, 1 for MyObject, 1 for asset/timeseries properties
            Assert.Equal(3, (int)CommonTestUtils.GetMetricValue("opcua_browse_operations"));
        }
        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "nodemap")]
        public async Task TestNodeMap()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();
            
            tester.Config.Extraction.NodeMap = new Dictionary<string, ProtoNodeId>
            {
                { "Map1", new ProtoNodeId { NamespaceUri = "http://examples.freeopcua.github.io", NodeId = "i=10" } }
            };

            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Handler.datapoints.ContainsKey("Map1"), 20,
                "Expected the overriden timeseries to create data");

            await tester.TerminateRunTask();

            Assert.True(tester.Handler.datapoints.ContainsKey("Map1"));
            Assert.True(tester.Handler.timeseries.ContainsKey("Map1"));
            Assert.Equal("MyVariable int", tester.Handler.timeseries["Map1"].name);
        }
        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "nonfinite")]
        public async Task TestNonFiniteDatapoints()
        {
            // It is awfully difficult to test anything without a UAClient to use for creating unique-ids etc, unfortunately
            // Perhaps in the future a final rewrite to make the pusher not use NodeId would be in order, it is not that easy, however.
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();
            tester.StartExtractor();
            tester.Config.History.Enabled = false;

            await tester.TerminateRunTask();

            var pusher = tester.Pusher;

            var badPoints = new List<BufferedDataPoint>
            {
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.PositiveInfinity),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.NegativeInfinity),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.NaN),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", 1E100),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", -1E100),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", 1E105),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", -1E105),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.MaxValue),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", double.MinValue)
            };

            Assert.False(tester.Handler.datapoints.ContainsKey("gp.efg:i=2"));
            tester.Handler.StoreDatapoints = true;

            await pusher.PushDataPoints(badPoints, CancellationToken.None);
            Assert.False(tester.Handler.datapoints.ContainsKey("gp.efg:i=2"));
            tester.CogniteConfig.NonFiniteReplacement = -1;

            await pusher.PushDataPoints(badPoints, CancellationToken.None);
            Assert.True(tester.Handler.datapoints.ContainsKey("gp.efg:i=2"));
            Assert.Equal(9, tester.Handler.datapoints["gp.efg:i=2"].Item1.Count);
            Assert.True(tester.Handler.datapoints["gp.efg:i=2"].Item1.TrueForAll(item => Math.Abs(item.Value + 1) < 0.01));

            var badPoints2 = new List<BufferedDataPoint>
            {
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", 1E99),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", -1E99)
            };
            await pusher.PushDataPoints(badPoints2, CancellationToken.None);
            Assert.Equal(11, tester.Handler.datapoints["gp.efg:i=2"].Item1.Count);
        }

        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "badpoints")]
        public async Task TestBadPoints()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();

            tester.Config.History.Enabled = false;
            tester.Config.Extraction.AllowStringVariables = true;

            tester.Handler.timeseries.Add("gp.efg:i=2", new TimeseriesDummy
            {
                id = -1,
                datapoints = new List<DataPoint>(),
                externalId = "gp.efg:i=2",
                isString = true,
                name = "MyVariable"
            });
            tester.Handler.timeseries.Add("gp.efg:i=4", new TimeseriesDummy
            {
                id = -2,
                datapoints = new List<DataPoint>(),
                externalId = "gp.efg:i=4",
                isString = false,
                name = "MyString"
            });

            tester.StartExtractor();
            await tester.TerminateRunTask();

            var pusher = tester.Pusher;
            Assert.False(tester.Handler.datapoints.ContainsKey("gp.efg:i=2"));
            // The extractor does not actually close completely if quitAfterMap is specified,
            // but leaves connections open, including subscriptions
            tester.Handler.StoreDatapoints = true;

            var badPoints = new List<BufferedDataPoint>
            {
                // Too low datetime
                new BufferedDataPoint(new DateTime(1970, 1, 1), "gp.efg:i=3", 0),
                new BufferedDataPoint(new DateTime(1900, 1, 1), "gp.efg:i=3", 0),
                new BufferedDataPoint(DateTime.MinValue, "gp.efg:i=3", 0),
                // Incorrect type
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", 123),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", "123"),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=2", null),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=4", 123),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=4", "123"),
                new BufferedDataPoint(DateTime.Now, "gp.efg:i=4", null)
            };

            await pusher.PushDataPoints(badPoints, CancellationToken.None);
            Assert.False(tester.Handler.datapoints.ContainsKey("gp.efg:i=2"));
            Assert.False(tester.Handler.datapoints.ContainsKey("gp.efg:i=4"));
            Assert.False(tester.Handler.datapoints.ContainsKey("gp.efg:i=3"));

            var badPoints2 = new List<BufferedDataPoint>
            {
                // Remember that this does not test against CDF
                new BufferedDataPoint(new DateTime(1971, 1, 1), "gp.efg:i=3", 0),
                new BufferedDataPoint(new DateTime(2040, 1, 1), "gp.efg:i=3", 0),
                new BufferedDataPoint(new DateTime(1980, 1, 1), "gp.efg:i=3", 0)
            };

            await pusher.PushDataPoints(badPoints2, CancellationToken.None);

            Assert.False(tester.Handler.datapoints.ContainsKey("gp.efg:i=2"));
            Assert.False(tester.Handler.datapoints.ContainsKey("gp.efg:i=4"));
            Assert.True(tester.Handler.datapoints.ContainsKey("gp.efg:i=3"));
            Assert.Equal(3, tester.Handler.datapoints["gp.efg:i=3"].Item1.Count);
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
                LogLevel = "debug",
                QuitAfterMap = false,
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();

            tester.Config.History.Backfill = true;

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Extractor.GetNodeState("gp.efg:i=10") != null
                    && tester.Extractor.GetNodeState("gp.efg:i=10").BackfillDone
                    && tester.Extractor.GetNodeState("gp.efg:i=10").IsStreaming
                    && tester.Handler.datapoints.ContainsKey("gp.efg:i=10")
                    && tester.Handler.datapoints["gp.efg:i=10"].Item1.Any(pt => pt.Timestamp < startTime), 20,
                "Expected integer datapoint to finish backfill and frontfill");

            await tester.TerminateRunTask();

            tester.TestContinuity("gp.efg:i=10");
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_data_count") >= 1);
            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Contains(tester.Handler.datapoints["gp.efg:i=10"].Item1,pt => pt.Timestamp < startTime);
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
                LogLevel = "debug",
                QuitAfterMap = false,
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();

            tester.Config.History.Backfill = true;

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Extractor.GetNodeState("gp.efg:i=10") != null
                    && tester.Extractor.GetNodeState("gp.efg:i=10").BackfillDone
                    && tester.Extractor.GetNodeState("gp.efg:i=10").IsStreaming
                    && tester.Handler.datapoints.ContainsKey("gp.efg:i=10")
                    && tester.Handler.datapoints["gp.efg:i=10"].Item1.Any(pt => pt.Timestamp < startTime), 20,
                "Expected integer datapoint to finish backfill and frontfill");

            tester.TestContinuity("gp.efg:i=10");
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
            Assert.True(CommonTestUtils.GetMetricValue("opcua_backfill_data_count") >= 1);
            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Contains(tester.Handler.datapoints["gp.efg:i=10"].Item1, pt => pt.Timestamp < startTime);
            await tester.Extractor.WaitForNextPush();
            CommonTestUtils.ResetTestMetrics();
            tester.Extractor.RestartExtractor();

            await Task.Delay(500);

            await tester.WaitForCondition(() =>
                    tester.Extractor.GetNodeState("gp.efg:i=10") != null
                    && tester.Extractor.GetNodeState("gp.efg:i=10").BackfillDone
                    && tester.Extractor.GetNodeState("gp.efg:i=10").IsStreaming, 20,
                "Expected integer datapoint to finish backfill and frontfill");

            await tester.TerminateRunTask();

            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 1));
        }
    }
}

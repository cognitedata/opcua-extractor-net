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
        private readonly ILogger log = Log.Logger.ForContext(typeof(CDFPusherTests));

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
            tester.Config.Extraction.DataTypes.AllowStringVariables = false;

            tester.Handler.AllowConnectionTest = mode != CDFMockHandler.MockMode.FailAsset;
            await tester.ClearPersistentData();

            log.Information("Testing with MockMode {TestBasicPushingMockMode}", mode.ToString());
            await tester.StartServer();
            tester.StartExtractor();
            await tester.TerminateRunTask(true, ex => mode == CDFMockHandler.MockMode.FailAsset || CommonTestUtils.TestRunResult(ex));

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(mode == CDFMockHandler.MockMode.FailAsset ? 1 : 0, (int)CommonTestUtils.GetMetricValue("opcua_node_ensure_failures_cdf"));

            if (mode == CDFMockHandler.MockMode.None && serverType == ServerName.Basic)
            {
                Assert.DoesNotContain(tester.Handler.Timeseries.Values, ts => ts.name == "MyString");
                Assert.Contains(tester.Handler.Assets.Values, asset =>
                    asset.name == "BaseRoot" && asset.metadata != null
                    && asset.metadata["Asset Property 1"] == "test"
                    && asset.metadata["Asset Property 2"] == "123.21");
                Assert.Contains(tester.Handler.Timeseries.Values, ts =>
                    ts.name == "Variable 1" && ts.metadata != null
                    && ts.metadata["TS Property 1"] == "test"
                    && ts.metadata["TS Property 2"] == "123.2");
            }

            if (mode != CDFMockHandler.MockMode.FailAsset)
            {
                Assert.Equal(serverType == ServerName.Basic ? 2 : 154, tester.Handler.Assets.Count);
                Assert.Equal(serverType == ServerName.Basic ? 4 : 2000, tester.Handler.Timeseries.Count);
            }
        }
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "debugmode")]
        [Fact]
        public async Task TestDebugMode()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters(){
                Builder = (config, pushers, client, source) =>
                {
                    return new UAExtractor(config, pushers.Append(config.Influx.ToPusher(null)).Append(config.Mqtt.ToPusher(null)), client, null, source.Token);
                }
            });
            await tester.ClearPersistentData();

            tester.Config.Cognite.Debug = true;
            tester.Config.Mqtt.Debug = true;
            tester.Config.Influx.Debug = true;
            tester.Config.Cognite.ApiKey = null;

            await tester.StartServer();
            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Extractor.Pushing, 10, "Expected extractor to start pushing");

            await tester.TerminateRunTask(true);

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
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "customdatatypes")]
        [Fact]
        public async Task TestCustomDataTypes()
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

            tester.Config.Extraction.DataTypes.CustomNumericTypes = new[]
            {
                tester.IdToProtoDataType(tester.Server.Ids.Custom.MysteryType),
                tester.IdToProtoDataType(tester.Server.Ids.Custom.NumberType),
            };
            tester.Config.Extraction.DataTypes.IgnoreDataTypes = new[]
            {
                tester.IdToProto(tester.Server.Ids.Custom.IgnoreType)
            };

            tester.StartExtractor();

            var arrId = tester.UAClient.GetUniqueId(tester.Server.Ids.Custom.Array, 2);
            await tester.WaitForCondition(() =>
                    tester.Handler.Assets.Count == 7
                    && tester.Handler.Timeseries.Count == 15
                    && tester.Handler.Datapoints.ContainsKey(arrId), 20,
                () => $"Expected to get 7 assets and got {tester.Handler.Assets.Count}"
                      + $", 15 timeseries and got {tester.Handler.Timeseries.Count}");

            var mystId = tester.UAClient.GetUniqueId(tester.Server.Ids.Custom.MysteryVar);
            var numId = tester.UAClient.GetUniqueId(tester.Server.Ids.Custom.NumberVar);
            var strId = tester.UAClient.GetUniqueId(tester.Server.Ids.Custom.StringyVar);

            await tester.WaitForCondition(() =>
                    tester.Handler.Datapoints.ContainsKey(mystId)
                    && tester.Handler.Datapoints.ContainsKey(numId)
                    && tester.Handler.Datapoints.ContainsKey(strId)
                    && tester.Handler.Datapoints[mystId].NumericDatapoints.Any()
                    && tester.Handler.Datapoints[numId].NumericDatapoints.Any()
                    && tester.Handler.Datapoints[strId].StringDatapoints.Any(), 20,
                "Expected to get some data");

            await tester.TerminateRunTask(true);

            Assert.Equal(1000, tester.Handler.Datapoints[mystId].NumericDatapoints.DistinctBy(pt => pt.Timestamp).Count());

            var numericTypeVar = tester.Handler.Timeseries.Values.First(ts => ts.name == "MysteryVar");
            Assert.False(numericTypeVar.isString);
            Assert.True(numericTypeVar.metadata.ContainsKey("EngineeringUnits"));
            Assert.Equal("°C: degree Celsius", numericTypeVar.metadata["EngineeringUnits"]);
            Assert.True(numericTypeVar.metadata.ContainsKey("EURange"));
            Assert.Equal("(0, 100)", numericTypeVar.metadata["EURange"]);

            var numericTypeVar2 = tester.Handler.Timeseries.Values.First(ts => ts.name == "NumberVar");
            Assert.False(numericTypeVar2.isString);

            var stringyVar = tester.Handler.Timeseries.Values.First(ts => ts.name == "StringyVar");
            Assert.True(stringyVar.isString);

            Assert.DoesNotContain(tester.Handler.Timeseries.Values, ts => ts.name == "IgnoreVar");

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(7, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(15, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
        }
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "restart")]
        [Fact]
        public async Task TestExtractorRestart()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters());
            await tester.ClearPersistentData();

            await tester.StartServer();

            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Extractor.Pushing, 20,
                () => "Expected extractor to start pushing initially");

            tester.Extractor.RestartExtractor();

            await Task.Delay(500);

            await tester.WaitForCondition(() => tester.Extractor.Started, 20,
                "Expected extractor to start up after restart");

            await tester.TerminateRunTask(false);
        }
        [Fact]
        [Trait("Server", "basic")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "connectiontest")]
        public async Task TestConnectionTest()
        {
            var services = new ServiceCollection();
            var fullConfig = services.AddConfig<FullConfig>("config.events.yml");
            fullConfig.GenerateDefaults();
            var config = fullConfig.Cognite;

            var handler = new CDFMockHandler(config.Project, CDFMockHandler.MockMode.None);
            services.AddMetrics();
            services.AddLogging();
            CommonTestUtils.AddDummyProvider(handler, services);
            services.AddCogniteClient("OpcUa-Extractor", true, true, false);

            using var provider = services.BuildServiceProvider();
            using var pusher = new CDFPusher(provider, config);
            var res = await pusher.TestConnection(fullConfig, CancellationToken.None);
            Assert.True(res);
            Assert.Equal(3, handler.RequestCount);
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
        [Trait("Test", "nodemap")]
        public async Task TestNodeMap()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true
            });
            await tester.ClearPersistentData();

            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.Config.Extraction.NodeMap = new Dictionary<string, ProtoNodeId>
            {
                { "Map1", tester.IdToProto(tester.Server.Ids.Base.IntVar) }
            };

            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Handler.Datapoints.ContainsKey("Map1"), 20,
                "Expected the overriden timeseries to create data");

            await tester.TerminateRunTask(true);

            Assert.True(tester.Handler.Datapoints.ContainsKey("Map1"));
            Assert.True(tester.Handler.Timeseries.ContainsKey("Map1"));
            Assert.Equal("Variable int", tester.Handler.Timeseries["Map1"].name);
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
            tester.Config.History.Enabled = false;

            await tester.StartServer();

            tester.StartExtractor();

            await tester.TerminateRunTask(true);

            var pusher = tester.Pusher;

            var badPoints = new List<UADataPoint>
            {
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", double.PositiveInfinity),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", double.NegativeInfinity),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", double.NaN),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", 1E100),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", -1E100),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", 1E105),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", -1E105),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", double.MaxValue),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", double.MinValue)
            };

            Assert.False(tester.Handler.Datapoints.ContainsKey("gp.efg:i=2"));
            tester.Handler.StoreDatapoints = true;

            await pusher.PushDataPoints(badPoints, CancellationToken.None);
            Assert.True(tester.Handler.Datapoints.ContainsKey("gp.efg:i=2"));
            Assert.Equal(6, tester.Handler.Datapoints["gp.efg:i=2"].NumericDatapoints.Count);
            tester.Config.Cognite.NonFiniteReplacement = -1;

            await pusher.PushDataPoints(badPoints, CancellationToken.None);
            Assert.True(tester.Handler.Datapoints.ContainsKey("gp.efg:i=2"));
            Assert.Equal(15, tester.Handler.Datapoints["gp.efg:i=2"].NumericDatapoints.Count);
            Assert.True(tester.Handler.Datapoints["gp.efg:i=2"].NumericDatapoints.TrueForAll(item => Math.Abs(item.Value + 1) < 0.01
                || item.Value == CogniteUtils.NumericValueMax || item.Value == CogniteUtils.NumericValueMin));

            var badPoints2 = new List<UADataPoint>
            {
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", 1E99),
                new UADataPoint(DateTime.UtcNow, "gp.efg:i=2", -1E99)
            };
            await pusher.PushDataPoints(badPoints2, CancellationToken.None);
            Assert.Equal(17, tester.Handler.Datapoints["gp.efg:i=2"].NumericDatapoints.Count);
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
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;

            await tester.StartServer();
            var numId = "gp.tl:i=2";
            var numId2 = "gp.tl:i=3";
            var strId = "gp.tl:i=4";

            tester.Handler.Timeseries.Add(numId, new TimeseriesDummy
            {
                id = -1,
                datapoints = new List<DataPoint>(),
                externalId = numId,
                isString = true,
                name = "Variable 1"
            });
            tester.Handler.Timeseries.Add(strId, new TimeseriesDummy
            {
                id = -2,
                datapoints = new List<DataPoint>(),
                externalId = strId,
                isString = false,
                name = "Variable string"
            });


            tester.StartExtractor();
            await tester.TerminateRunTask(true);

            var pusher = tester.Pusher;
            Assert.False(tester.Handler.Datapoints.ContainsKey(numId));
            // The extractor does not actually close completely if quitAfterMap is specified,
            // but leaves connections open, including subscriptions
            tester.Handler.StoreDatapoints = true;

            var badPoints = new List<UADataPoint>
            {
                // Too low datetime
                new UADataPoint(new DateTime(1970, 1, 1), numId2, 0),
                new UADataPoint(new DateTime(1900, 1, 1), numId2, 0),
                new UADataPoint(DateTime.MinValue, numId2, 0),
                // Incorrect type
                new UADataPoint(DateTime.UtcNow, numId, 123),
                new UADataPoint(DateTime.UtcNow, numId, "123"),
                new UADataPoint(DateTime.UtcNow, numId, null),
                new UADataPoint(DateTime.UtcNow, strId, 123),
                new UADataPoint(DateTime.UtcNow, strId, "123"),
                new UADataPoint(DateTime.UtcNow, strId, null)
            };

            await pusher.PushDataPoints(badPoints, CancellationToken.None);
            Assert.False(tester.Handler.Datapoints.ContainsKey(numId));
            Assert.False(tester.Handler.Datapoints.ContainsKey(strId));
            Assert.False(tester.Handler.Datapoints.ContainsKey(numId2));

            var badPoints2 = new List<UADataPoint>
            {
                // Remember that this does not test against CDF
                new UADataPoint(new DateTime(1971, 1, 1), numId2, 0),
                new UADataPoint(new DateTime(2040, 1, 1), numId2, 0),
                new UADataPoint(new DateTime(1980, 1, 1), numId2, 0)
            };

            await pusher.PushDataPoints(badPoints2, CancellationToken.None);

            Assert.False(tester.Handler.Datapoints.ContainsKey(numId));
            Assert.False(tester.Handler.Datapoints.ContainsKey(strId));
            Assert.True(tester.Handler.Datapoints.ContainsKey(numId2));
            Assert.Equal(3, tester.Handler.Datapoints[numId2].NumericDatapoints.Count);
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
        [Fact]
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "lateinit")]
        public async Task TestLateInitialization()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                QuitAfterMap = false,
                StoreDatapoints = true,
                ServerName = ServerName.Array
            });
            await tester.ClearPersistentData();
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;

            tester.Handler.BlockAllConnections = true;

            await tester.StartServer();
            tester.Server.PopulateArrayHistory();

            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();
            // Since no pusher is available, expect history to not have been started
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 0));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 0));

            Assert.True(tester.Extractor.State.NodeStates.All(state => state.FrontfillEnabled && state.IsFrontfilling || !state.IsFrontfilling));

            Assert.Empty(tester.Handler.Assets);
            Assert.Empty(tester.Handler.Timeseries);

            tester.Handler.BlockAllConnections = false;

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_tracked_assets", 7));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tracked_timeseries", 16));

            await tester.TerminateRunTask(false);
        }
        [Fact]
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "rawdata")]
        public async Task TestRawMetadata()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true,
                ServerName = ServerName.Array
            });
            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                Database = "metadata",
                AssetsTable = "assets",
                TimeseriesTable = "timeseries"
            };
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;

            await tester.ClearPersistentData();

            await tester.StartServer();
            tester.Server.PopulateArrayHistory();

            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();

            Assert.Empty(tester.Handler.Assets);

            await tester.WaitForCondition(() => tester.Handler.Datapoints.ContainsKey("gp.tl:i=2[0]")
                && tester.Handler.Datapoints["gp.tl:i=2[0]"].NumericDatapoints.DistinctBy(pt => pt.Timestamp).Count() == 1000, 10);

            await tester.TerminateRunTask(false);

            Assert.True(CommonTestUtils.TestMetricValue("opcua_tracked_assets", 7));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_tracked_timeseries", 16));

            Assert.Equal(16, tester.Handler.TimeseriesRaw.Count);
            Assert.Empty(tester.Handler.Assets);

            Assert.True(tester.Handler.TimeseriesRaw["gp.tl:i=10"].metadata.ContainsKey("EURange"));
        }

        [Theory]
        [InlineData(true, true, true, true, false, false, false, false)]
        [InlineData(false, false, false, false, true, true, true, true)]
        [InlineData(true, false, true, false, true, false, true, false)]
        [InlineData(false, true, false, true, false, true, false, true)]
        [InlineData(true, true, true, true, true, true, true, true)]
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "fieldsupdatecdf")]
        public async Task TestUpdateFields(
            bool assetName, bool variableName, 
            bool assetDesc, bool variableDesc,
            bool assetContext, bool variableContext,
            bool assetMeta, bool variableMeta)
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array
            });
            var upd = tester.Config.Extraction.Update;
            upd.Objects.Name = assetName;
            upd.Objects.Description = assetDesc;
            upd.Objects.Context = assetContext;
            upd.Objects.Metadata = assetMeta;
            upd.Variables.Name = variableName;
            upd.Variables.Description = variableDesc;
            upd.Variables.Context = variableContext;
            upd.Variables.Metadata = variableMeta;

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.History.Enabled = false;

            await tester.ClearPersistentData();
            await tester.StartServer();
            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            CommonTestUtils.VerifyStartingConditions(tester.Handler.Assets, tester.Handler.Timeseries, null, false);

            tester.Server.ModifyCustomServer();

            var rebrowseTask = tester.Extractor.Rebrowse();
            await Task.WhenAny(rebrowseTask, Task.Delay(10000));
            Assert.True(rebrowseTask.IsCompleted);

            CommonTestUtils.VerifyStartingConditions(tester.Handler.Assets, tester.Handler.Timeseries, upd, false);
            CommonTestUtils.VerifyModified(tester.Handler.Assets, tester.Handler.Timeseries, upd, false);
        }
        [Theory]
        [InlineData(true, true, true, true, false, false, false, false)]
        [InlineData(false, false, false, false, true, true, true, true)]
        [InlineData(true, false, true, false, true, false, true, false)]
        [InlineData(false, true, false, true, false, true, false, true)]
        [InlineData(true, true, true, true, true, true, true, true)]
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "fieldsupdateraw")]
        public async Task TestUpdateFieldsRaw(
            bool assetName, bool variableName,
            bool assetDesc, bool variableDesc,
            bool assetContext, bool variableContext,
            bool assetMeta, bool variableMeta)
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array
            });
            var upd = tester.Config.Extraction.Update;
            upd.Objects.Name = assetName;
            upd.Objects.Description = assetDesc;
            upd.Objects.Context = assetContext;
            upd.Objects.Metadata = assetMeta;
            upd.Variables.Name = variableName;
            upd.Variables.Description = variableDesc;
            upd.Variables.Context = variableContext;
            upd.Variables.Metadata = variableMeta;

            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                Database = "metadata",
                AssetsTable = "assets",
                TimeseriesTable = "timeseries"
            };

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.History.Enabled = false;

            await tester.ClearPersistentData();
            await tester.StartServer();
            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            CommonTestUtils.VerifyStartingConditions(tester.Handler.AssetRaw, tester.Handler.TimeseriesRaw
                .ToDictionary(kvp => kvp.Key, kvp => (TimeseriesDummy)kvp.Value), null, true);

            tester.Server.ModifyCustomServer();

            await tester.Extractor.Rebrowse();

            CommonTestUtils.VerifyStartingConditions(tester.Handler.AssetRaw, tester.Handler.TimeseriesRaw
                .ToDictionary(kvp => kvp.Key, kvp => (TimeseriesDummy)kvp.Value), upd, true);
            CommonTestUtils.VerifyModified(tester.Handler.AssetRaw, tester.Handler.TimeseriesRaw
                .ToDictionary(kvp => kvp.Key, kvp => (TimeseriesDummy)kvp.Value), upd, true);
        }
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "metadatamapping")]
        [Fact]
        public async Task TestMetadataMapping()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array
            });
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.History.Enabled = false;
            tester.Config.Cognite.MetadataMapping = new MetadataMapConfig
            {
                Assets = new Dictionary<string, string> { { "StringProp", "name" }, { "NumericProp", "description" }, { "EURange", "description" } },
                Timeseries = new Dictionary<string, string> { { "EURange", "description" }, { "EngineeringUnits", "unit" } }
            };

            await tester.ClearPersistentData();
            await tester.StartServer();
            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();

            var obj2 = tester.Handler.Assets[tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.Obj2)];
            Assert.Equal("String prop value", obj2.name);
            Assert.Equal("1234", obj2.description);

            var numVar = tester.Handler.Timeseries[tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.MysteryVar)];
            Assert.Equal("°C: degree Celsius", numVar.unit);
            Assert.Equal("(0, 100)", numVar.description);

            var arrayElem = tester.Handler.Timeseries[tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.Array, 2)];
            Assert.Equal("°C: degree Celsius", arrayElem.unit);
            Assert.Equal("(0, 100)", arrayElem.description);

            var arrayParent = tester.Handler.Assets[tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.Array)];
            Assert.Equal("(0, 100)", arrayParent.description);
        }
        [Trait("Server", "array")]
        [Trait("Target", "CDFPusher")]
        [Trait("Test", "enummapping")]
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task TestEnumMapping(bool enumsAsStrings)
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array,
                StoreDatapoints = true
            });
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.Extraction.DataTypes.EnumsAsStrings = enumsAsStrings;
            tester.Config.Extraction.DataTypes.DataTypeMetadata = true;
            tester.Config.History.Enabled = false;

            await tester.ClearPersistentData();
            await tester.StartServer();
            tester.StartExtractor();

            await tester.Extractor.WaitForSubscriptions();
            await tester.WaitForCondition(() =>
                tester.Handler.Datapoints.TryGetValue(tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.MysteryVar), out var dps)
                && dps.NumericDatapoints.Any(), 10);

            foreach (var variable in tester.Handler.Timeseries.Values)
            {
                Assert.True(variable.metadata.ContainsKey("dataType"));
            }
            Assert.Equal(
                "NumberType",
                tester.Handler.Timeseries[tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.NumberVar)].metadata["dataType"]);

            var enumId1 = tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.EnumVar1);
            var enumId2 = tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.EnumVar2);
            var enumId3 = tester.Extractor.GetUniqueId(tester.Server.Ids.Custom.EnumVar3);

            Assert.Equal(
                "CustomEnumType2",
                tester.Handler.Assets[enumId3].metadata["dataType"]);
            Assert.Equal(
                "CustomEnumType2",
                tester.Handler.Timeseries[enumId3 + "[1]"].metadata["dataType"]);

            if (enumsAsStrings)
            {
                Assert.False(tester.Handler.Timeseries[enumId2].metadata.ContainsKey("123"));
                Assert.False(tester.Handler.Assets[enumId3].metadata.ContainsKey("321"));
                Assert.False(tester.Handler.Timeseries[enumId1].metadata.ContainsKey("1"));

                Assert.Single(tester.Handler.Datapoints[enumId1].StringDatapoints);
                Assert.Single(tester.Handler.Datapoints[enumId2].StringDatapoints);
                Assert.Single(tester.Handler.Datapoints[enumId3 + "[1]"].StringDatapoints);
                Assert.Equal("Enum2", tester.Handler.Datapoints[enumId1].StringDatapoints.First().Value);
                Assert.Equal("VEnum2", tester.Handler.Datapoints[enumId2].StringDatapoints.First().Value);
                Assert.Equal("VEnum2", tester.Handler.Datapoints[enumId3 + "[1]"].StringDatapoints.First().Value);
                Assert.Equal("VEnum1", tester.Handler.Datapoints[enumId3 + "[2]"].StringDatapoints.First().Value);
            }
            else
            {
                Assert.Equal("VEnum1", tester.Handler.Timeseries[enumId2].metadata["321"]);
                Assert.Equal("VEnum2", tester.Handler.Timeseries[enumId2].metadata["123"]);
                Assert.Equal("Enum1", tester.Handler.Timeseries[enumId1].metadata["0"]);
                Assert.Equal("Enum3", tester.Handler.Timeseries[enumId1].metadata["2"]);
                Assert.Equal("VEnum1", tester.Handler.Timeseries[enumId3 + "[1]"].metadata["321"]);

                Assert.Single(tester.Handler.Datapoints[enumId1].NumericDatapoints);
                Assert.Single(tester.Handler.Datapoints[enumId2].NumericDatapoints);
                Assert.Single(tester.Handler.Datapoints[enumId3 + "[1]"].NumericDatapoints);
                Assert.Equal(1, tester.Handler.Datapoints[enumId1].NumericDatapoints.First().Value);
                Assert.Equal(123, tester.Handler.Datapoints[enumId2].NumericDatapoints.First().Value);
                Assert.Equal(123, tester.Handler.Datapoints[enumId3 + "[1]"].NumericDatapoints.First().Value);
                Assert.Equal(321, tester.Handler.Datapoints[enumId3 + "[2]"].NumericDatapoints.First().Value);
            }
        }
        [Fact]
        public async Task TestNodeTypeMapping()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array,
                StoreDatapoints = false,
                QuitAfterMap = true
            });
            tester.Config.Extraction.NodeTypes.Metadata = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;

            await tester.ClearPersistentData();
            await tester.StartServer();
            tester.StartExtractor();

            await tester.TerminateRunTask(false);

            foreach (var asset in tester.Handler.Assets.Values)
            {
                Assert.NotNull(asset.metadata["TypeDefinition"]);
            }
            foreach (var ts in tester.Handler.Timeseries.Values)
            {
                Assert.NotNull(ts.metadata["TypeDefinition"]);
            }
            var root = tester.Handler.Assets["gp.tl:i=1"];
            Assert.Equal("BaseObjectType", root.metadata["TypeDefinition"]);

            var arrayRoot = tester.Handler.Assets["gp.tl:i=2"];
            Assert.Equal("BaseDataVariableType", arrayRoot.metadata["TypeDefinition"]);

            var arrayElem = tester.Handler.Timeseries["gp.tl:i=2[0]"];
            Assert.Equal("BaseDataVariableType", arrayElem.metadata["TypeDefinition"]);

            var someTs = tester.Handler.Timeseries["gp.tl:i=8"];
            Assert.Equal("BaseDataVariableType", someTs.metadata["TypeDefinition"]);
        }
    }
}

using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    public class DataPointTestFixture : BaseExtractorTestFixture
    {
        public DataPointTestFixture() : base()
        {
        }
    }

    public class DataPointTests : IClassFixture<DataPointTestFixture>
    {
        private readonly DataPointTestFixture tester;
        public DataPointTests(ITestOutputHelper output, DataPointTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
            tester.Config.Source.PublishingInterval = 400;
            tester.Config.Extraction.DataPushDelay = "400";
            tester.Config.History.Enabled = false;

            tester.ResetCustomServerValues();
            tester.WipeBaseHistory();
            tester.WipeCustomHistory();
            tester.Client.TypeManager.Reset();
        }

        #region subscriptions
        [Fact]
        public async Task TestBasicSubscriptions()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dataTypes = tester.Config.Extraction.DataTypes;
            var ids = tester.Ids.Custom;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);
            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = 4;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.IgnoreDataTypes = new[]
            {
                CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.IgnoreType, tester.Client)
            };

            var runTask = extractor.RunExtractor();

            await extractor.WaitForSubscriptions();

            await TestUtils.WaitForCondition(() => pusher.DataPoints.Any(kvp => kvp.Value.Count != 0), 5);
            foreach (var kvp in pusher.DataPoints)
            {
                kvp.Value.Clear();
            }

            // Trigger a bunch of subscriptions
            // string variable
            tester.Server.UpdateNode(ids.StringyVar, "value1");
            // string array
            tester.Server.UpdateNode(ids.StringArray, new[] { "test12", "test22" });
            // numeric variable
            tester.Server.UpdateNode(ids.MysteryVar, 123.123);
            // numeric array
            tester.Server.UpdateNode(ids.Array, new[] { 1.1, 2.2, 3.3, 4.4 });
            // enum
            tester.Server.UpdateNode(ids.EnumVar1, 2);
            // ignored variable
            tester.Server.UpdateNode(ids.IgnoreVar, 123.123);
            // enum array
            tester.Server.UpdateNode(ids.EnumVar3, new[] { 123, 321, 321, 321 });

            await TestUtils.WaitForCondition(() => pusher.DataPoints.Count(kvp => kvp.Value.Count != 0) == 13,
                5, () => $"Expected to get values in 13 timeseries, but got {pusher.DataPoints.Count(kvp => kvp.Value.Count != 0)}");

            void TestDataPoints((NodeId, int) id, object expected)
            {
                var dps = pusher.DataPoints[id];
                Assert.Single(dps);
                var dp = dps.First();
                Assert.Equal(tester.Client.GetUniqueId(id.Item1, id.Item2), dp.Id);
                if (dp.IsString)
                {
                    Assert.Equal((string)expected, dp.StringValue);
                }
                else
                {
                    Assert.Equal((double)expected, dp.DoubleValue);
                }
            }

            TestDataPoints((ids.StringyVar, -1), "value1");
            TestDataPoints((ids.StringArray, 0), "test12");
            TestDataPoints((ids.StringArray, 1), "test22");
            TestDataPoints((ids.MysteryVar, -1), 123.123);
            TestDataPoints((ids.Array, 0), 1.1);
            TestDataPoints((ids.Array, 1), 2.2);
            TestDataPoints((ids.Array, 2), 3.3);
            TestDataPoints((ids.Array, 3), 4.4);
            TestDataPoints((ids.EnumVar1, -1), 2.0);
            Assert.False(pusher.DataPoints.ContainsKey((ids.IgnoreVar, -1)));
            TestDataPoints((ids.EnumVar3, 0), 123.0);
            TestDataPoints((ids.EnumVar3, 1), 321.0);
            TestDataPoints((ids.EnumVar3, 2), 321.0);
            TestDataPoints((ids.EnumVar3, 3), 321.0);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }
        [Fact]
        public async Task TestEnumAsString()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dataTypes = tester.Config.Extraction.DataTypes;
            var ids = tester.Ids.Custom;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);
            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = 4;
            dataTypes.EnumsAsStrings = true;
            dataTypes.AutoIdentifyTypes = true;

            var runTask = extractor.RunExtractor();

            await extractor.WaitForSubscriptions();

            try
            {
                await TestUtils.WaitForCondition(() => pusher.DataPoints.Any(kvp => kvp.Value.Count != 0), 5);
            }
            finally
            {
                foreach (var kvp in pusher.DataPoints)
                {
                    kvp.Value.Clear();
                }
            }

            // enum
            tester.Server.UpdateNode(ids.EnumVar1, 2);
            // enum array
            tester.Server.UpdateNode(ids.EnumVar3, new[] { 123, 321, 321, 321 });

            await TestUtils.WaitForCondition(() => pusher.DataPoints.Count(kvp => kvp.Value.Count != 0) == 5,
                5, () => $"Expected to get values in 5 timeseries, but got {pusher.DataPoints.Count(kvp => kvp.Value.Count != 0)}");

            void TestDataPoints((NodeId, int) id, object expected)
            {
                var dps = pusher.DataPoints[id];
                Assert.Single(dps);
                var dp = dps.First();
                Assert.Equal(tester.Client.GetUniqueId(id.Item1, id.Item2), dp.Id);
                if (dp.IsString)
                {
                    Assert.Equal((string)expected, dp.StringValue);
                }
                else
                {
                    Assert.Equal((double)expected, dp.DoubleValue);
                }
            }

            TestDataPoints((ids.EnumVar1, -1), "Enum3");
            TestDataPoints((ids.EnumVar3, 0), "VEnum2");
            TestDataPoints((ids.EnumVar3, 1), "VEnum1");
            TestDataPoints((ids.EnumVar3, 2), "VEnum1");
            TestDataPoints((ids.EnumVar3, 3), "VEnum1");

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }
        [Fact]
        public async Task TestWrongData()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dataTypes = tester.Config.Extraction.DataTypes;
            var ids = tester.Ids.Wrong;

            tester.Server.UpdateNode(ids.WrongDim, new[] { 0, 0 });
            tester.Server.UpdateNode(ids.RankImprecise, new[] { 0, 0, 0, 0, 0, 0 });
            tester.Server.UpdateNode(ids.RankImpreciseNoDim, new[] { 0, 0, 0, 0 });
            tester.Server.UpdateNode(ids.NullType, 0);

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Wrong.Root, tester.Client);
            dataTypes.AllowStringVariables = true;
            dataTypes.UnknownAsScalar = true;
            dataTypes.NullAsNumeric = true;
            dataTypes.MaxArraySize = 4;

            var runTask = extractor.RunExtractor();

            await extractor.WaitForSubscriptions();

            await TestUtils.WaitForCondition(() => pusher.DataPoints.Any(kvp => kvp.Value.Count != 0), 5);
            foreach (var kvp in pusher.DataPoints)
            {
                kvp.Value.Clear();
            }

            CommonTestUtils.ResetMetricValue("opcua_array_points_missed");

            // too small 
            tester.Server.UpdateNode(ids.WrongDim, new[] { 1, 2 });
            // too large
            tester.Server.UpdateNode(ids.RankImprecise, new[] { 1, 2, 3, 4, 5, 6 });
            // Array on scalar
            tester.Server.UpdateNode(ids.RankImpreciseNoDim, new[] { 1, 2, 3, 4 });
            tester.Server.UpdateNode(ids.NullType, 1);

            await TestUtils.WaitForCondition(() => pusher.DataPoints.Count(kvp => kvp.Value.Count != 0) == 8,
                5, () => $"Expected to get values in 8 timeseries, but got {pusher.DataPoints.Count(kvp => kvp.Value.Count != 0)}");

            void TestDataPoints((NodeId, int) id, object expected)
            {
                var dps = pusher.DataPoints[id];
                Assert.Single(dps);
                var dp = dps.First();
                Assert.Equal(tester.Client.GetUniqueId(id.Item1, id.Item2), dp.Id);
                if (dp.IsString)
                {
                    Assert.Equal((string)expected, dp.StringValue);
                }
                else
                {
                    Assert.Equal((double)expected, dp.DoubleValue);
                }
            }

            TestDataPoints((ids.WrongDim, 0), 1.0);
            TestDataPoints((ids.WrongDim, 1), 2.0);
            Assert.Empty(pusher.DataPoints[(ids.WrongDim, 2)]);
            Assert.Empty(pusher.DataPoints[(ids.WrongDim, 3)]);
            TestDataPoints((ids.RankImprecise, 0), 1.0);
            TestDataPoints((ids.RankImprecise, 1), 2.0);
            TestDataPoints((ids.RankImprecise, 2), 3.0);
            TestDataPoints((ids.RankImprecise, 3), 4.0);
            Assert.False(pusher.DataPoints.ContainsKey((ids.RankImprecise, 4)));
            TestDataPoints((ids.RankImpreciseNoDim, -1), 1.0);
            TestDataPoints((ids.NullType, -1), 1.0);
            Assert.True(CommonTestUtils.TestMetricValue("opcua_array_points_missed", 5));

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

            tester.Server.UpdateNode(ids.WrongDim, new[] { 0, 0, 0, 0 });
            tester.Server.UpdateNode(ids.RankImprecise, new[] { 0, 0, 0, 0 });
            tester.Server.UpdateNode(ids.RankImpreciseNoDim, 0);
            tester.Server.UpdateNode(ids.NullType, 0);
        }
        [Fact]
        public async Task TestDataChangeFilter()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dataTypes = tester.Config.Extraction.DataTypes;
            var ids = tester.Ids.Base;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            tester.Config.Subscriptions.DataChangeFilter = new DataSubscriptionConfig
            {
                DeadbandType = DeadbandType.Absolute,
                Trigger = DataChangeTrigger.StatusValue,
                DeadbandValue = 0.6,
            };

            var runTask = extractor.RunExtractor();

            pusher.DataPoints[(ids.DoubleVar1, -1)] = new List<UADataPoint>();

            await extractor.WaitForSubscriptions();
            // Middle value is skipped due to deadband
            tester.Server.UpdateNode(ids.DoubleVar1, 0.0);
            await Task.Delay(100);
            tester.Server.UpdateNode(ids.DoubleVar1, 0.5);
            await Task.Delay(100);
            tester.Server.UpdateNode(ids.DoubleVar1, 1.0);

            await TestUtils.WaitForCondition(() => pusher.DataPoints[(ids.DoubleVar1, -1)].Count == 2, 5,
                () => $"Expected 2 datapoints, got {pusher.DataPoints[(ids.DoubleVar1, -1)].Count}");

            var dps = pusher.DataPoints[(ids.DoubleVar1, -1)];
            Assert.Equal(0.0, dps[0].DoubleValue);
            Assert.Equal(1.0, dps[1].DoubleValue);
        }
        [Fact(Timeout = 10000)]
        public async Task TestDataPointsAsEvents()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Ids.Base;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            tester.Config.Extraction.Transformations = new[]
            {
                new RawNodeTransformation
                {
                    Filter = new NodeFilter
                    {
                        Name = new RegexFieldFilter("Variable bool")
                    },
                    Type = TransformationType.AsEvents
                }
            };

            var runTask = extractor.RunExtractor();

            await extractor.WaitForSubscriptions();

            tester.Server.UpdateNode(ids.BoolVar, true);
            tester.Server.UpdateNode(ids.DoubleVar1, 1.0);
            await Task.Delay(100);
            tester.Server.UpdateNode(ids.BoolVar, false);
            tester.Server.UpdateNode(ids.DoubleVar1, 2.0);

            await TestUtils.WaitForCondition(() =>
            {
                tester.Log.LogDebug("Check condition {Time}", DateTime.UtcNow);
                return pusher.Events.ContainsKey(ids.BoolVar) && pusher.Events[ids.BoolVar].Count >= 2
                && pusher.DataPoints[(ids.DoubleVar1, -1)].Count >= 2;
            }, 5);

            var evts = pusher.Events[ids.BoolVar];
            Assert.All(evts, evt =>
            {
                Assert.Equal(ids.BoolVar, evt.SourceNode);
                Assert.Equal(ids.BoolVar, evt.EmittingNode);
            });
        }

        [Fact]
        public async Task TestIngestDataPointsWithStatus()
        {
            tester.Config.Extraction.StatusCodes.IngestStatusCodes = true;
            tester.Config.Extraction.StatusCodes.StatusCodesToIngest = StatusCodeMode.All;

            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Ids.Base;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            var runTask = extractor.RunExtractor();

            await extractor.WaitForSubscriptions();

            tester.Server.UpdateNode(ids.DoubleVar1, 1.0, StatusCodes.Uncertain);
            await Task.Delay(100);
            tester.Server.UpdateNode(ids.DoubleVar1, 2.0, StatusCodes.GoodClamped);

            await TestUtils.WaitForCondition(() => pusher.DataPoints[(ids.DoubleVar1, -1)].Count >= 2, 5);

            var dps = pusher.DataPoints[(ids.DoubleVar1, -1)];

            Assert.Contains(dps, dp => dp.Status.Code == StatusCodes.Uncertain);
            Assert.Contains(dps, dp => dp.Status.Code == StatusCodes.GoodClamped);
        }

        [Fact]
        public async Task TestVariableDataPointsConfig()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dataTypes = tester.Config.Extraction.DataTypes;
            var ids = tester.Ids.Base;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            tester.Config.Subscriptions.AlternativeConfigs = new[] {
                new FilteredSubscriptionConfig
                {
                    Filter = new SubscriptionConfigFilter
                    {
                        Id = $"i={ids.DoubleVar2.Identifier}"
                    },
                }
            };

            tester.Config.Subscriptions.DataChangeFilter = new DataSubscriptionConfig
            {
                DeadbandType = DeadbandType.Absolute,
                Trigger = DataChangeTrigger.StatusValue,
                DeadbandValue = 0.6,
            };

            tester.Server.UpdateNode(ids.DoubleVar1, 0.0);
            tester.Server.UpdateNode(ids.DoubleVar2, 0.0);

            var runTask = extractor.RunExtractor();

            pusher.DataPoints[(ids.DoubleVar1, -1)] = new List<UADataPoint>();

            await extractor.WaitForSubscriptions();
            // Middle value is skipped due to deadband, but only on DoubleVar1
            tester.Server.UpdateNode(ids.DoubleVar1, 0.0);
            tester.Server.UpdateNode(ids.DoubleVar2, 0.0);
            await Task.Delay(100);
            tester.Server.UpdateNode(ids.DoubleVar1, 0.5);
            tester.Server.UpdateNode(ids.DoubleVar2, 0.5);
            await Task.Delay(100);
            tester.Server.UpdateNode(ids.DoubleVar1, 1.0);
            tester.Server.UpdateNode(ids.DoubleVar2, 1.0);

            await TestUtils.WaitForCondition(() => pusher.DataPoints[(ids.DoubleVar1, -1)].Count == 2, 5,
                () => $"Expected 2 datapoints, got {pusher.DataPoints[(ids.DoubleVar1, -1)].Count}");

            var dps = pusher.DataPoints[(ids.DoubleVar1, -1)];
            Assert.Equal(0.0, dps[0].DoubleValue);
            Assert.Equal(1.0, dps[1].DoubleValue);
            var dps2 = pusher.DataPoints[(ids.DoubleVar2, -1)];
            Assert.True(dps2.Count >= 3);
        }
        #endregion
        #region history
        private static void TestContinuity(IEnumerable<UADataPoint> dps, bool shouldBeString)
        {
            bool isString = dps.First().IsString;
            string id = dps.First().Id;
            Assert.Equal(shouldBeString, isString);
            dps = dps.OrderBy(dp => dp.Timestamp).DistinctBy(dp => dp.Timestamp).ToList();
            var last = -1;
            var lastTs = dps.First().Timestamp.AddMilliseconds(-10);
            foreach (var dp in dps)
            {
                int idx;
                if (isString)
                {
                    idx = Convert.ToInt32(dp.StringValue.Split(' ')[1], CultureInfo.InvariantCulture);
                }
                else
                {
                    idx = (int)dp.DoubleValue;
                }
                Assert.Equal(last + 1, idx);
                Assert.Equal(id, dp.Id);
                Assert.Equal(lastTs.AddMilliseconds(10), dp.Timestamp);
                last = idx;
                lastTs = dp.Timestamp;
            }
        }

        private void CountCustomValues(DummyPusher pusher, int count)
        {
            var ids = tester.Server.Ids.Custom;
            Assert.Equal(count, pusher.DataPoints[(ids.Array, 0)].ToList().DistinctBy(dp => dp.Timestamp).Count());
            Assert.Equal(count, pusher.DataPoints[(ids.Array, 1)].ToList().DistinctBy(dp => dp.Timestamp).Count());
            Assert.Equal(count, pusher.DataPoints[(ids.Array, 2)].ToList().DistinctBy(dp => dp.Timestamp).Count());
            Assert.Equal(count, pusher.DataPoints[(ids.Array, 3)].ToList().DistinctBy(dp => dp.Timestamp).Count());
            Assert.Equal(count, pusher.DataPoints[(ids.StringyVar, -1)].ToList().DistinctBy(dp => dp.Timestamp).Count());
            Assert.Equal(count, pusher.DataPoints[(ids.MysteryVar, -1)].ToList().DistinctBy(dp => dp.Timestamp).Count());

        }
        // Details are tested elsewhere, this is just testing that history is run correctly on startup,
        // and that continuous data is returned when running frontfill and backfill
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestHistory(bool backfill)
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;

            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.History.Backfill = backfill;
            var dataTypes = tester.Config.Extraction.DataTypes;
            dataTypes.AllowStringVariables = true;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.MaxArraySize = 4;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);

            var start = DateTime.UtcNow.AddSeconds(-5);

            tester.WipeCustomHistory();
            tester.Server.PopulateCustomHistory(start);

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                !node.IsFrontfilling && !node.IsBackfilling), 10);

            await extractor.Looper.WaitForNextPush();
            await extractor.Looper.WaitForNextPush();

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

            CountCustomValues(pusher, 1000);

            TestContinuity(pusher.DataPoints[(ids.Array, 0)], false);
            TestContinuity(pusher.DataPoints[(ids.StringyVar, -1)], true);
            TestContinuity(pusher.DataPoints[(ids.MysteryVar, -1)], false);

            tester.Config.History.Enabled = false;
            tester.Config.History.Data = false;
            tester.Config.History.Backfill = false;
            dataTypes.AllowStringVariables = false;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.MaxArraySize = 4;
            tester.WipeCustomHistory();
            tester.ResetCustomServerValues();
        }

        [Theory(Timeout = 10000)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestHistoryContinuation(bool backfill)
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;

            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.History.Backfill = backfill;
            var dataTypes = tester.Config.Extraction.DataTypes;
            dataTypes.AllowStringVariables = true;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.MaxArraySize = 4;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);

            var now = DateTime.UtcNow;

            tester.WipeCustomHistory();
            tester.Server.PopulateCustomHistory(now.AddSeconds(-5));

            var runTask = extractor.RunExtractor();

            await extractor.WaitForSubscriptions();

            await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                !node.IsFrontfilling && !node.IsBackfilling), 10);

            await extractor.Looper.WaitForNextPush();
            await extractor.Looper.WaitForNextPush();

            CountCustomValues(pusher, 1000);

            TestContinuity(pusher.DataPoints[(ids.Array, 0)], false);
            TestContinuity(pusher.DataPoints[(ids.StringyVar, -1)], true);
            TestContinuity(pusher.DataPoints[(ids.MysteryVar, -1)], false);

            tester.Server.PopulateCustomHistory(now.AddSeconds(-15));
            tester.Server.PopulateCustomHistory(now.AddSeconds(5));

            await extractor.RestartHistoryWaitForStop();

            await extractor.Looper.WaitForNextPush();
            await extractor.Looper.WaitForNextPush();

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

            // Only datapoints inserted after end should be returned, backfill is assumed to be complete.
            CountCustomValues(pusher, 2000);

            tester.Config.History.Enabled = false;
            tester.Config.History.Data = false;
            tester.Config.History.Backfill = false;
            dataTypes.AllowStringVariables = false;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.MaxArraySize = 4;
            tester.WipeCustomHistory();
            tester.ResetCustomServerValues();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestLiteDbStateRestart(bool backfill)
        {
            try
            {
                File.Delete("history-data-test-1.db");
            }
            catch { }
            using var stateStore = new LiteDBStateStore(new StateStoreConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = "history-data-test-1.db"
            }, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>());

            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = false });
            var extractor = tester.BuildExtractor(true, stateStore, pusher);

            var ids = tester.Ids.Custom;

            tester.Config.History.Enabled = true;
            tester.Config.StateStorage.Interval = "1000000";
            tester.Config.History.Data = true;
            tester.Config.History.Backfill = backfill;
            var dataTypes = tester.Config.Extraction.DataTypes;
            dataTypes.AllowStringVariables = true;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.MaxArraySize = 4;

            var now = DateTime.UtcNow;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);

            tester.WipeCustomHistory();
            tester.Server.PopulateCustomHistory(now.AddSeconds(-5));

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await TestUtils.WaitForCondition(() => pusher.DataPoints.Values.Any(dps => dps.Count >= 1000), 5);

                await extractor.Looper.StoreState(tester.Source.Token);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);


                CountCustomValues(pusher, 1000);
                pusher.Wipe();
            }
            finally
            {
                await extractor.DisposeAsync();
            }

            tester.Server.PopulateCustomHistory(now.AddSeconds(-15));
            tester.Server.PopulateCustomHistory(now.AddSeconds(5));

            extractor = tester.BuildExtractor(true, stateStore, pusher);

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await TestUtils.WaitForCondition(() => pusher.DataPoints.Values.Any(dps => dps.Count >= 1000), 5);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                CountCustomValues(pusher, 1001);
                pusher.Wipe();
            }
            finally
            {
                await extractor.DisposeAsync();
            }
        }
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestPusherStateRestart(bool backfill)
        {
            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = true });
            var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Ids.Custom;

            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.History.Backfill = backfill;
            var dataTypes = tester.Config.Extraction.DataTypes;
            dataTypes.AllowStringVariables = true;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.MaxArraySize = 4;

            var now = DateTime.UtcNow;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);

            tester.WipeCustomHistory();
            tester.Server.PopulateCustomHistory(now.AddSeconds(-5));

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await TestUtils.WaitForCondition(() => pusher.DataPoints.Values.Any(dps => dps.Count >= 1000), 5);

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                CountCustomValues(pusher, 1000);
            }
            finally
            {
                await extractor.DisposeAsync();
            }

            tester.Server.PopulateCustomHistory(now.AddSeconds(-15));
            tester.Server.PopulateCustomHistory(now.AddSeconds(5));

            extractor = tester.BuildExtractor(true, null, pusher);

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await TestUtils.WaitForCondition(() => pusher.DataPoints.Values.Any(dps => dps.Count >= 2000), 5);

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                if (backfill)
                {
                    // We cannot know if backfill has finished or not based on the pusher state
                    CountCustomValues(pusher, 3000);
                }
                else
                {
                    CountCustomValues(pusher, 2000);
                }
            }
            finally
            {
                await extractor.DisposeAsync();
            }
        }
        [Fact(Timeout = 10000)]
        public async Task TestDisableSubscriptions()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = true });
            using var extractor = tester.BuildExtractor(true, null, pusher);

            tester.Config.Subscriptions.RecreateSubscriptionGracePeriod = "100ms";

            var ids = tester.Ids.Base;

            var now = DateTime.UtcNow;

            async Task Reset()
            {
                extractor.State.Clear();
                extractor.GetType().GetField("subscribed", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, 0);
                extractor.GetType().GetField("subscribeFlag", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(extractor, false);
                var reader = (HistoryReader)extractor.GetType().GetField("historyReader", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(extractor);
                reader.AddIssue(HistoryReader.StateIssue.NodeHierarchyRead);
                await tester.RemoveSubscription(SubscriptionName.DataPoints);
            }

            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = false;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            tester.WipeBaseHistory();
            tester.Server.PopulateBaseHistory(now.AddSeconds(-5));
            CommonTestUtils.ResetMetricValue("opcua_frontfill_data_count");

            var session = (Session)tester.Client.GetType().GetProperty("Session", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(tester.Client);

            // Test everything normal
            await extractor.RunExtractor(true);
            Assert.All(extractor.State.NodeStates, state => { Assert.True(state.ShouldSubscribe); });
            await extractor.WaitForSubscriptions();
            Assert.Equal(4u, session.Subscriptions.First(sub => sub.DisplayName.StartsWith(SubscriptionName.DataPoints.Name(), StringComparison.InvariantCulture)).MonitoredItemCount);
            await TestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1), 5);

            // Test disable subscriptions
            tester.Log.LogDebug("Test disable subscriptions");
            await Reset();
            tester.Config.Subscriptions.DataPoints = false;
            extractor.State.Clear();
            await extractor.RunExtractor(true);
            var state = extractor.State.GetNodeState(ids.DoubleVar1);
            Assert.False(state.ShouldSubscribe);
            state = extractor.State.GetNodeState(ids.IntVar);
            Assert.False(state.ShouldSubscribe);
            await extractor.WaitForSubscriptions();
            Assert.DoesNotContain(session.Subscriptions, sub => sub.DisplayName.StartsWith(SubscriptionName.DataPoints.Name(), StringComparison.InvariantCulture));
            await TestUtils.WaitForCondition(() => CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 2, tester.Log), 5);

            // Test disable specific subscriptions
            tester.Log.LogDebug("Test disable specific subscriptions");
            await Reset();
            var oldTransforms = tester.Config.Extraction.Transformations;
            tester.Config.Extraction.Transformations = new List<RawNodeTransformation>
            {
                new RawNodeTransformation
                {
                    Filter = new NodeFilter
                    {
                        Id = new RegexFieldFilter($"i={ids.DoubleVar1.Identifier}$")
                    },
                    Type = TransformationType.DropSubscriptions
                }
            };

            tester.Config.Subscriptions.DataPoints = true;
            await extractor.RunExtractor(true);
            state = extractor.State.GetNodeState(ids.DoubleVar1);
            Assert.False(state.ShouldSubscribe);
            state = extractor.State.GetNodeState(ids.IntVar);
            Assert.True(state.ShouldSubscribe);
            await extractor.WaitForSubscriptions();
            Assert.Equal(3u, session.Subscriptions.First(sub => sub.DisplayName.StartsWith(SubscriptionName.DataPoints.Name(), StringComparison.InvariantCulture)).MonitoredItemCount);
            await TestUtils.WaitForCondition(() => CommonTestUtils.GetMetricValue("opcua_frontfill_data_count") >= 3, 5);
        }

        [Fact]
        public async Task TestLowServiceLevelStates()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());

            using var stateStore = new DummyStateStore();

            using var extractor = tester.BuildExtractor(true, stateStore, pusher);

            var ids = tester.Server.Ids.Base;

            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.Source.Redundancy.MonitorServiceLevel = true;
            tester.Config.StateStorage.Interval = "10h";

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Base.Root, tester.Client);

            // Need to reset connection to server in order to begin measuring service level
            tester.Server.SetServerRedundancyStatus(190, RedundancySupport.Hot);

            await tester.Client.Run(tester.Source.Token);
            var start = DateTime.UtcNow.AddSeconds(-5);
            tester.WipeBaseHistory();
            tester.Server.PopulateBaseHistory(start);

            try
            {
                var runTask = extractor.RunExtractor();
                await extractor.WaitForSubscriptions();

                var state = extractor.State.GetNodeState(ids.DoubleVar1);
                // Range should be empty
                Assert.True(state.DestinationExtractedRange.First == CogniteTime.DateTimeEpoch);
                Assert.True(state.DestinationExtractedRange.Last == CogniteTime.DateTimeEpoch);
                // Source extracted range should be populated properly.
                Assert.False(state.SourceExtractedRange.IsEmpty);

                // Changes should not trigger state updates
                tester.Server.UpdateBaseNodes(10);
                await TestUtils.WaitForCondition(() =>
                {
                    var dps = pusher.DataPoints[(ids.DoubleVar1, -1)];
                    if (dps.Count == 0) return false;
                    return dps[^1].DoubleValue == 10;
                }, 10);

                Assert.All(extractor.State.NodeStates, state =>
                {
                    if (!state.FrontfillEnabled)
                    {
                        Assert.Equal(state.DestinationExtractedRange, state.SourceExtractedRange);
                    }
                    else
                    {
                        Assert.Equal(CogniteTime.DateTimeEpoch, state.SourceExtractedRange.First);
                        Assert.Equal(CogniteTime.DateTimeEpoch, state.SourceExtractedRange.Last);
                    }

                });

                Assert.True(state.DestinationExtractedRange.First == CogniteTime.DateTimeEpoch);
                Assert.True(state.DestinationExtractedRange.Last == CogniteTime.DateTimeEpoch);

                // Set service level back up and wait for history to catch up.
                tester.Server.SetServerRedundancyStatus(255, RedundancySupport.Hot);
                await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    node.DestinationExtractedRange == node.SourceExtractedRange), 10);
            }
            finally
            {
                foreach (var state in extractor.State.NodeStates)
                {
                    tester.Log.LogDebug("Source: {S}, Dest: {D}. Eq {Eq}", state.SourceExtractedRange, state.DestinationExtractedRange, state.DestinationExtractedRange == state.SourceExtractedRange);
                }
                tester.Config.Source.Redundancy.MonitorServiceLevel = false;
                await tester.Client.Run(tester.Source.Token);
            }
        }


        [Fact]
        public async Task TestRestartHistoryOnReconnect()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());

            var ids = tester.Ids.Base;

            var startTime = DateTime.UtcNow.AddSeconds(-10);
            tester.WipeBaseHistory();
            tester.Server.PopulateBaseHistory(startTime);

            tester.Config.Source.RestartOnReconnect = false;
            tester.Config.History.Backfill = false;
            tester.Config.History.Data = true;
            tester.Config.History.Enabled = true;
            tester.Config.History.EndTime = startTime.AddSeconds(5).ToUnixTimeMilliseconds().ToString();
            tester.Config.Subscriptions.DataPoints = false;
            tester.Config.Extraction.RootNode = tester.Ids.Base.Root.ToProtoNodeId(tester.Client);

            using var extractor = tester.BuildExtractor(true, null, pusher);

            // First start the extractor and read the first half of history.
            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                !node.IsFrontfilling && !node.IsBackfilling) && pusher.DataPoints[(ids.DoubleVar1, -1)].Count == 500, 10);
            var state = extractor.State.GetNodeState(ids.DoubleVar1);

            Assert.Equal(startTime.AddSeconds(5).ToUnixTimeMilliseconds() - 10, state.SourceExtractedRange.Last.ToUnixTimeMilliseconds());
            Assert.Equal(CogniteTime.DateTimeEpoch, state.SourceExtractedRange.First);

            // Change the configured endtime. We do this to simulate more data arriving in the server
            tester.Config.History.EndTime = null;

            // Call the restart callback. Making the client call this is painful, and tested elsewhere
            await extractor.OnServerDisconnect(tester.Client);
            await extractor.OnServerReconnect(tester.Client);
            await TestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                !node.IsFrontfilling && !node.IsBackfilling) && pusher.DataPoints[(ids.DoubleVar1, -1)].Count >= 1000, 10);
            Assert.Equal(startTime.AddSeconds(10).ToUnixTimeMilliseconds() - 10, state.SourceExtractedRange.Last.ToUnixTimeMilliseconds());
            Assert.Equal(CogniteTime.DateTimeEpoch, state.SourceExtractedRange.First);

            Assert.Equal(1000, pusher.DataPoints[(ids.DoubleVar1, -1)].DistinctBy(dp => dp.Timestamp).Count());
        }
        #endregion

        #region buffer
        [Fact]
        public async Task TestFileAutoBuffer()
        {
            try
            {
                File.Delete("datapoint-buffer-test.bin");
            }
            catch { }

            tester.Config.FailureBuffer.DatapointPath = "datapoint-buffer-test.bin";
            tester.Config.FailureBuffer.Enabled = true;

            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = true });
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Base;

            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;

            CommonTestUtils.ResetMetricValues("opcua_buffer_num_points");

            var now = DateTime.UtcNow;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            tester.Server.PopulateBaseHistory(now.AddSeconds(-20));

            pusher.PushDataPointResult = false;

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            Assert.False(runTask.IsFaulted, $"Faulted! {runTask.Exception}");

            // expect no data to arrive in pusher

            await TestUtils.WaitForCondition(
                () => pusher.DataFailing
                && extractor.State.NodeStates.All(state => !state.IsFrontfilling), 5,
                () => $"Pusher is dataFailing: {pusher.DataFailing}");

            Assert.True(pusher.DataPoints.All(dps => dps.Value.Count == 0));

            tester.Server.UpdateNode(ids.DoubleVar1, 1000);
            tester.Server.UpdateNode(ids.DoubleVar2, 1);
            tester.Server.UpdateNode(ids.BoolVar, true);
            tester.Server.UpdateNode(ids.StringVar, "str: 1000");

            await TestUtils.WaitForCondition(() => CommonTestUtils.GetMetricValue("opcua_buffer_num_points") >= 2, 5,
                () => $"Expected at least 2 points to arrive in buffer, but got {CommonTestUtils.GetMetricValue("opcua_buffer_num_points")}");

            tester.Server.UpdateNode(ids.DoubleVar1, 1001);
            tester.Server.UpdateNode(ids.DoubleVar2, 2);
            tester.Server.UpdateNode(ids.BoolVar, false);
            tester.Server.UpdateNode(ids.StringVar, "str: 1001");

            await TestUtils.WaitForCondition(() => CommonTestUtils.GetMetricValue("opcua_buffer_num_points") >= 4, 5,
                () => $"Expected at least 4 points to arrive in buffer, but got {CommonTestUtils.GetMetricValue("opcua_buffer_num_points")}");

            pusher.PushDataPointResult = true;

            await TestUtils.WaitForCondition(() => pusher.DataPoints[(ids.DoubleVar1, -1)].Count >= 1002, 10);

            Assert.Equal(1002, pusher.DataPoints[(ids.DoubleVar1, -1)].DistinctBy(dp => dp.Timestamp).Count());
            Assert.True(pusher.DataPoints[(ids.DoubleVar2, -1)].Count >= 2);
            Assert.True(pusher.DataPoints[(ids.BoolVar, -1)].Count >= 2);
            Assert.Equal(1002, pusher.DataPoints[(ids.StringVar, -1)].DistinctBy(dp => dp.Timestamp).Count());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_buffer_num_points", 0));
        }
        #endregion

        [Fact]
        public async Task TestReadHistoryOnRecreateSub()
        {
            tester.Config.History.Enabled = true;
            tester.Config.History.Data = true;

            tester.Config.Subscriptions.RecreateSubscriptionGracePeriod = "100ms";

            var now = DateTime.UtcNow;
            var ids = tester.Server.Ids.Base;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            tester.Server.PopulateBaseHistory(now.AddSeconds(-20));

            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = true });
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            Assert.False(runTask.IsFaulted, $"Faulted! {runTask.Exception}");

            // Wait for history to arrive properly
            await TestUtils.WaitForCondition(() => pusher.DataPoints[(ids.DoubleVar1, -1)].Count >= 1000 && extractor.State.NodeStates.All(st => !st.IsFrontfilling), 5);

            // Add some historical data, it won't be read since we are already done reading history
            tester.Server.PopulateBaseHistory(now.AddSeconds(10), notifyLast: false);

            // Generate a datapoint, this should be captured
            tester.Server.UpdateNode(ids.DoubleVar1, 2000);
            await TestUtils.WaitForCondition(() => pusher.DataPoints[(ids.DoubleVar1, -1)][^1].DoubleValue == 2000, 5);

            Assert.True(pusher.DataPoints[(ids.DoubleVar1, -1)].Count < 2000);

            tester.Server.Server.DropSubscriptions();

            // Wait for history to be read back once the subscription recovers.
            await TestUtils.WaitForCondition(() => pusher.DataPoints[(ids.DoubleVar1, -1)].Count >= 2000, 20, () =>
                $"Expected 2000 got {pusher.DataPoints[(ids.DoubleVar1, -1)].Count}");
        }
    }
}

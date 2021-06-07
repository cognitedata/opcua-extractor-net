using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    public class DataPointTestFixture : BaseExtractorTestFixture
    {
        public DataPointTestFixture() : base(63300)
        {
            Config.Source.PublishingInterval = 400;
            Config.Extraction.DataPushDelay = 400;
            Config.History.Enabled = false;
        }
        public void ResetCustomServerValues()
        {
            var ids = Server.Ids.Custom;
            Server.UpdateNode(ids.StringyVar, "value");
            Server.UpdateNode(ids.StringArray, new[] { "test1", "test2" });
            Server.UpdateNode(ids.MysteryVar, 0);
            Server.UpdateNode(ids.Array, new[] { 0, 0, 0, 0 });
            Server.UpdateNode(ids.EnumVar1, 1);
            Server.UpdateNode(ids.IgnoreVar, 0);
            Server.UpdateNode(ids.EnumVar2, 123);
            Server.UpdateNode(ids.EnumVar3, new[] { 123, 123, 321, 123 });
        }
        public void WipeCustomHistory()
        {
            var ids = Server.Ids.Custom;
            Server.WipeHistory(ids.StringyVar, null);
            Server.WipeHistory(ids.MysteryVar, 0);
            Server.WipeHistory(ids.Array, new[] { 0, 0, 0, 0 });
        }
        public void WipeBaseHistory()
        {
            var ids = Server.Ids.Base;
            Server.WipeHistory(ids.DoubleVar1, 0);
            Server.WipeHistory(ids.StringVar, null);
            Server.WipeHistory(ids.IntVar, 0);
        }
    }

    public class DataPointTests : MakeConsoleWork, IClassFixture<DataPointTestFixture>
    {
        private readonly DataPointTestFixture tester;
        public DataPointTests(ITestOutputHelper output, DataPointTestFixture tester) : base(output)
        {
            this.tester = tester;
        }

        #region subscriptions
        [Fact]
        public async Task TestBasicSubscriptions()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dataTypes = tester.Config.Extraction.DataTypes;
            var ids = tester.Server.Ids.Custom;

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

            await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Any(kvp => kvp.Value.Any()), 5);
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

            await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Count(kvp => kvp.Value.Any()) == 13,
                5, () => $"Expected to get values in 13 timeseries, but got {pusher.DataPoints.Count(kvp => kvp.Value.Any())}");

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

            dataTypes.AllowStringVariables = false;
            dataTypes.MaxArraySize = 0;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.IgnoreDataTypes = null;
            tester.ResetCustomServerValues();
        }
        [Fact]
        public async Task TestEnumAsString()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dataTypes = tester.Config.Extraction.DataTypes;
            var ids = tester.Server.Ids.Custom;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);
            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = 4;
            dataTypes.EnumsAsStrings = true;
            dataTypes.AutoIdentifyTypes = true;

            var runTask = extractor.RunExtractor();

            await extractor.WaitForSubscriptions();

            await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Any(kvp => kvp.Value.Any()), 5);
            foreach (var kvp in pusher.DataPoints)
            {
                kvp.Value.Clear();
            }

            // enum
            tester.Server.UpdateNode(ids.EnumVar1, 2);
            // enum array
            tester.Server.UpdateNode(ids.EnumVar3, new[] { 123, 321, 321, 321 });

            await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Count(kvp => kvp.Value.Any()) == 5,
                5, () => $"Expected to get values in 5 timeseries, but got {pusher.DataPoints.Count(kvp => kvp.Value.Any())}");

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

            dataTypes.AllowStringVariables = false;
            dataTypes.MaxArraySize = 0;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.EnumsAsStrings = false;
            tester.ResetCustomServerValues();
        }
        [Fact]
        public async Task TestWrongData()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var dataTypes = tester.Config.Extraction.DataTypes;
            var ids = tester.Server.Ids.Wrong;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Wrong.Root, tester.Client);
            dataTypes.AllowStringVariables = true;
            dataTypes.UnknownAsScalar = true;
            dataTypes.NullAsNumeric = true;
            dataTypes.MaxArraySize = 4;

            var runTask = extractor.RunExtractor();

            await extractor.WaitForSubscriptions();

            await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Any(kvp => kvp.Value.Any()), 5);
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

            await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Count(kvp => kvp.Value.Any()) == 8,
                5, () => $"Expected to get values in 8 timeseries, but got {pusher.DataPoints.Count(kvp => kvp.Value.Any())}");

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

            dataTypes.AllowStringVariables = false;
            dataTypes.UnknownAsScalar = false;
            dataTypes.MaxArraySize = 0;

            tester.Server.UpdateNode(ids.WrongDim, new[] { 0, 0, 0, 0 });
            tester.Server.UpdateNode(ids.RankImprecise, new[] { 0, 0, 0, 0 });
            tester.Server.UpdateNode(ids.RankImpreciseNoDim, 0);
            tester.Server.UpdateNode(ids.NullType, 0);
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

            await CommonTestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
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

        [Theory]
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

            await CommonTestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                !node.IsFrontfilling && !node.IsBackfilling), 10);

            await extractor.Looper.WaitForNextPush();
            await extractor.Looper.WaitForNextPush();

            CountCustomValues(pusher, 1000);

            TestContinuity(pusher.DataPoints[(ids.Array, 0)], false);
            TestContinuity(pusher.DataPoints[(ids.StringyVar, -1)], true);
            TestContinuity(pusher.DataPoints[(ids.MysteryVar, -1)], false);

            tester.Server.PopulateCustomHistory(now.AddSeconds(-15));
            tester.Server.PopulateCustomHistory(now.AddSeconds(5));

            foreach (var state in extractor.State.NodeStates)
            {
                state.RestartHistory();
            }

            await extractor.RestartHistory();

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

            var ids = tester.Server.Ids.Custom;

            tester.Config.History.Enabled = true;
            tester.Config.StateStorage.Interval = 1000000;
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

                await CommonTestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Values.Any(dps => dps.Count >= 1000), 5);

                await extractor.Looper.StoreState(tester.Source.Token);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);


                CountCustomValues(pusher, 1000);
                pusher.Wipe();
            }
            finally
            {
                extractor.Dispose();
            }

            tester.Server.PopulateCustomHistory(now.AddSeconds(-15));
            tester.Server.PopulateCustomHistory(now.AddSeconds(5));

            extractor = tester.BuildExtractor(true, stateStore, pusher);

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await CommonTestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Values.Any(dps => dps.Count >= 1000), 5);
                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                CountCustomValues(pusher, 1001);
                pusher.Wipe();
            }
            finally
            {
                extractor.Dispose();
            }

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
        public async Task TestPusherStateRestart(bool backfill)
        {
            using var pusher = new DummyPusher(new DummyPusherConfig() { ReadExtractedRanges = true });
            var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;

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

                await CommonTestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Values.Any(dps => dps.Count >= 1000), 5);

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

                CountCustomValues(pusher, 1000);
            }
            finally
            {
                extractor.Dispose();
            }

            tester.Server.PopulateCustomHistory(now.AddSeconds(-15));
            tester.Server.PopulateCustomHistory(now.AddSeconds(5));

            extractor = tester.BuildExtractor(true, null, pusher);

            try
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();

                await CommonTestUtils.WaitForCondition(() => extractor.State.NodeStates.All(node =>
                    !node.IsFrontfilling && !node.IsBackfilling), 10);

                await extractor.Looper.WaitForNextPush();

                await CommonTestUtils.WaitForCondition(() => pusher.DataPoints.Values.Any(dps => dps.Count >= 2000), 5);

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
                extractor.Dispose();
            }

            tester.Config.History.Enabled = false;
            tester.Config.History.Data = false;
            tester.Config.History.Backfill = false;
            dataTypes.AllowStringVariables = false;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.MaxArraySize = 4;
            tester.WipeCustomHistory();
            tester.ResetCustomServerValues();
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

            await CommonTestUtils.WaitForCondition(
                () => pusher.DataFailing
                && extractor.State.NodeStates.All(state => !state.IsFrontfilling), 5,
                () => $"Pusher is dataFailing: {pusher.DataFailing}");

            Assert.True(pusher.DataPoints.All(dps => !dps.Value.Any()));

            tester.Server.UpdateNode(ids.DoubleVar1, 1000);
            tester.Server.UpdateNode(ids.DoubleVar2, 1);
            tester.Server.UpdateNode(ids.BoolVar, true);
            tester.Server.UpdateNode(ids.StringVar, "str: 1000");

            await CommonTestUtils.WaitForCondition(() => CommonTestUtils.GetMetricValue("opcua_buffer_num_points") >= 2, 5,
                () => $"Expected at least 2 points to arrive in buffer, but got {CommonTestUtils.GetMetricValue("opcua_buffer_num_points")}");

            tester.Server.UpdateNode(ids.DoubleVar1, 1001);
            tester.Server.UpdateNode(ids.DoubleVar2, 2);
            tester.Server.UpdateNode(ids.BoolVar, false);
            tester.Server.UpdateNode(ids.StringVar, "str: 1001");

            await CommonTestUtils.WaitForCondition(() => CommonTestUtils.GetMetricValue("opcua_buffer_num_points") >= 4, 5,
                () => $"Expected at least 4 points to arrive in buffer, but got {CommonTestUtils.GetMetricValue("opcua_buffer_num_points")}");

            pusher.PushDataPointResult = true;

            await CommonTestUtils.WaitForCondition(() => pusher.DataPoints[(ids.DoubleVar1, -1)].Count >= 1002, 10);

            Assert.Equal(1002, pusher.DataPoints[(ids.DoubleVar1, -1)].DistinctBy(dp => dp.Timestamp).Count());
            Assert.True(pusher.DataPoints[(ids.DoubleVar2, -1)].Count >= 2);
            Assert.True(pusher.DataPoints[(ids.BoolVar, -1)].Count >= 2);
            Assert.Equal(1002, pusher.DataPoints[(ids.StringVar, -1)].DistinctBy(dp => dp.Timestamp).Count());

            Assert.True(CommonTestUtils.TestMetricValue("opcua_buffer_num_points", 0));
            tester.ResetCustomServerValues();
        }
        #endregion
    }
}

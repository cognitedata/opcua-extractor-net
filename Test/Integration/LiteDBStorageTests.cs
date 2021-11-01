using AdysTech.InfluxDB.Client.Net;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa;
using Cognite.OpcUa.History;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    public sealed class LiteDbStorageTestFixture : BaseExtractorTestFixture
    {
        public LiteDbStorageTestFixture() : base()
        {
            DeleteFiles("lite-");
        }
    }

    public class LiteDbStorageTests : MakeConsoleWork, IClassFixture<LiteDbStorageTestFixture>
    {
        private readonly LiteDbStorageTestFixture tester;
        private static int index;
        // The influxbuffer tests are left here, the influxdb failure buffer should probably be retired.
        // Nobody is using it, and it is unnecessarily complex. The file buffer is faster and
        // much simpler, which in the end makes it safer.
        public LiteDbStorageTests(ITestOutputHelper output, LiteDbStorageTestFixture tester) : base(output)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.WipeBaseHistory();
            tester.WipeEventHistory();
        }

        [Fact]
        public async Task TestInfluxAutoBufferData()
        {
            int idx = index++;
            var ifSetup = tester.GetInfluxPusher($"testdb-litedb{idx}");
            using var pusher = ifSetup.pusher;
            using var client = ifSetup.client;
            tester.Config.FailureBuffer.DatapointPath = $"lite-buffer-{idx}.bin";

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.History.Enabled = true;
            tester.Config.FailureBuffer.Enabled = true;
            tester.Config.Extraction.RootNode = tester.Ids.Base.Root.ToProtoNodeId(tester.Client);
            tester.Server.PopulateBaseHistory();

            using var extractor = tester.BuildExtractor(true, null, pusher);
            var runTask = extractor.RunExtractor();

            await extractor.Looper.WaitForNextPush();
            await CommonTestUtils.WaitForCondition(() => extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);
            await extractor.Looper.WaitForNextPush();

            var oldHost = tester.Config.Influx.Host;
            tester.Config.Influx.Host = "testWrong";
            pusher.Reconfigure();
            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1000);
            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 1.0);

            await CommonTestUtils.WaitForCondition(() => extractor.FailureBuffer.AnyPoints,
                10, "Failurebuffer must receive some data");

            tester.Config.Influx.Host = oldHost;
            pusher.Reconfigure();

            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1001);
            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 2.0);

            await CommonTestUtils.WaitForCondition(() => extractor.FailureBuffer.AnyPoints,
                10, "FailureBuffer should be emptied");

            await CommonTestUtils.WaitForCondition(() => extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);

            await extractor.Looper.WaitForNextPush();

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);

            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_datapoint_push_failures_influx"));
        }

        [Fact]
        public async Task TestInfluxBufferStateData()
        {
            int idx = index++;
            var ifSetup = tester.GetInfluxPusher($"testdb-litedb{idx}");
            tester.Config.StateStorage = new StateStorageConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = $"lite-state{idx}.db",
                InfluxVariableStore = "influx_variables"
            };
            using var pusher = ifSetup.pusher;
            using var client = ifSetup.client;

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.History.Enabled = false;
            tester.Config.FailureBuffer.Enabled = true;
            tester.Config.FailureBuffer.Influx = true;
            tester.Config.FailureBuffer.InfluxStateStore = true;
            tester.Config.Extraction.RootNode = tester.Ids.Base.Root.ToProtoNodeId(tester.Client);


            var (handler, cdfPusher) = tester.GetCDFPusher();

            List<InfluxBufferState> states;

            using (var stateStore = new LiteDBStateStore(tester.Config.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>()))
            using (var extractor = tester.BuildExtractor(true, stateStore, pusher, cdfPusher))
            {
                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();
                await extractor.Looper.WaitForNextPush();

                handler.AllowPush = false;
                handler.AllowConnectionTest = false;

                tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1);
                tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar1, 1);
                tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 1);
                tester.Server.UpdateNode(tester.Server.Ids.Base.BoolVar, true);
                tester.Server.UpdateNode(tester.Server.Ids.Base.StringVar, "test 1");

                await CommonTestUtils.WaitForCondition(() => extractor.FailureBuffer.AnyPoints,
                    20, "Failurebuffer must receive some data");

                await extractor.Looper.WaitForNextPush();

                states = extractor.State.NodeStates.Where(state => !state.FrontfillEnabled)
                        .Select(state => new InfluxBufferState(state)).ToList();

                await stateStore.RestoreExtractionState(
                    states.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.InfluxVariableStore,
                    false,
                    tester.Source.Token);

                Assert.True(states.All(state =>
                        state.DestinationExtractedRange.First <= state.DestinationExtractedRange.Last
                        && state.DestinationExtractedRange.Last != DateTime.MaxValue));

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
            }

            using (var stateStore = new LiteDBStateStore(tester.Config.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>()))
            using (var extractor = tester.BuildExtractor(true, stateStore, pusher, cdfPusher))
            {
                handler.AllowPush = true;
                handler.AllowConnectionTest = true;

                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();
                await extractor.Looper.WaitForNextPush();

                foreach (var state in states) state.ClearRanges();

                await extractor.StateStorage.RestoreExtractionState(
                    states.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.InfluxVariableStore,
                    false,
                    tester.Source.Token);

                Assert.True(states.All(state =>
                    state.DestinationExtractedRange == TimeRange.Empty));

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
            }
        }

        [Fact]
        public async Task TestInfluxBufferStateEvents()
        {
            int idx = index++;
            var ifSetup = tester.GetInfluxPusher($"testdb-litedb{idx}");
            tester.Config.StateStorage = new StateStorageConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = $"lite-state{idx}.db",
                InfluxEventStore = "influx_events"
            };
            using var pusher = ifSetup.pusher;
            using var client = ifSetup.client;

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.History.Enabled = false;
            tester.Config.Events.History = false;
            tester.Config.Events.Enabled = true;
            tester.Config.FailureBuffer.Enabled = true;
            tester.Config.FailureBuffer.Influx = true;
            tester.Config.FailureBuffer.InfluxStateStore = true;
            tester.Config.Extraction.RootNode = tester.Ids.Event.Root.ToProtoNodeId(tester.Client);

            var (handler, cdfPusher) = tester.GetCDFPusher();

            List<InfluxBufferState> states;

            using (var stateStore = new LiteDBStateStore(tester.Config.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>()))
            using (var extractor = tester.BuildExtractor(true, stateStore, pusher, cdfPusher))
            {
                var runTask = extractor.RunExtractor();
                await extractor.WaitForSubscriptions();
                await extractor.Looper.WaitForNextPush();

                handler.AllowPush = false;
                handler.AllowEvents = false;
                handler.AllowConnectionTest = false;

                tester.Server.TriggerEvents(1);

                await CommonTestUtils.WaitForCondition(() => extractor.FailureBuffer.AnyEvents, 10,
                    "FailureBuffer must receive some events");

                await extractor.Looper.WaitForNextPush();

                states = extractor.State.EmitterStates.Select(state => new InfluxBufferState(state)).ToList();

                await extractor.StateStorage.RestoreExtractionState(
                    states.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.InfluxEventStore,
                    false,
                    tester.Source.Token);

                Assert.True(states.All(state => state.DestinationExtractedRange.First <= state.DestinationExtractedRange.Last
                                                && state.DestinationExtractedRange.Last != DateTime.MaxValue));

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
            }

            using (var stateStore = new LiteDBStateStore(tester.Config.StateStorage, tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>()))
            using (var extractor = tester.BuildExtractor(true, stateStore, pusher, cdfPusher))
            {
                handler.AllowPush = true;
                handler.AllowEvents = true;
                handler.AllowConnectionTest = true;

                var runTask = extractor.RunExtractor();

                await extractor.WaitForSubscriptions();
                await extractor.Looper.WaitForNextPush();

                foreach (var state in states) state.ClearRanges();

                await extractor.StateStorage.RestoreExtractionState(
                    states.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.InfluxEventStore,
                    true,
                    tester.Source.Token);

                Assert.True(states.All(state => state.DestinationExtractedRange == TimeRange.Empty));

                await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
            }
        }

        [Fact]
        public async Task TestEventsInfluxBuffering()
        {
            int idx = index++;
            var ifSetup = tester.GetInfluxPusher($"testdb-litedb{idx}");
            using var pusher = ifSetup.pusher;
            using var client = ifSetup.client;

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.History.Enabled = true;
            tester.Config.FailureBuffer.Enabled = true;
            tester.Config.FailureBuffer.Influx = true;
            tester.Config.Events.Enabled = true;
            tester.Config.Events.History = true;
            tester.Config.Extraction.RootNode = tester.Ids.Event.Root.ToProtoNodeId(tester.Client);
            tester.Server.PopulateEvents();

            var (handler, cdfPusher) = tester.GetCDFPusher();

            using var extractor = tester.BuildExtractor(true, null, pusher, cdfPusher);
            var runTask = extractor.RunExtractor();

            handler.AllowEvents = false;
            handler.AllowPush = false;
            handler.AllowConnectionTest = false;

            await extractor.WaitForSubscriptions();
            await CommonTestUtils.WaitForCondition(() => cdfPusher.EventsFailing, 10, "Expected pusher to start failing");

            tester.Server.TriggerEvents(100);
            tester.Server.TriggerEvents(101);

            await CommonTestUtils.WaitForCondition(() => extractor.FailureBuffer.AnyEvents
                && cdfPusher.EventsFailing,
                10, "Expected failurebuffer to contain some events");
            await extractor.Looper.WaitForNextPush();

            handler.AllowEvents = true;
            handler.AllowPush = true;
            handler.AllowConnectionTest = true;
            await CommonTestUtils.WaitForCondition(() => !extractor.FailureBuffer.AnyEvents,
                10, "Expected FailureBuffer to be emptied");

            await CommonTestUtils.WaitForCondition(() => handler.Events.Count == 1022, 10,
                () => $"Expected to receive 920 events, but got {handler.Events.Count}");

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }
    }
}

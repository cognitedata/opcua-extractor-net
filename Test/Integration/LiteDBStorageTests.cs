using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    [Collection("Extractor tests")]
    public class LiteDbStorageTests : MakeConsoleWork
    {
        // The influxbuffer tests are left here, the influxdb failure buffer should probably be retired.
        // Nobody is using it, and it is unnecessarily complex. The file buffer is faster and
        // much simpler, which in the end makes it safer.
        public LiteDbStorageTests(ITestOutputHelper output) : base(output) { }

        [Trait("Server", "basic")]
        [Trait("Target", "StateStorage")]
        [Trait("Test", "influxautobufferdata")]
        [Fact]
        public async Task TestInfluxAutoBufferData()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                Pusher = "influx",
                DataBufferPath = "buffer.bin"
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;

            await tester.StartServer();
            tester.Server.PopulateBaseHistory();

            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();
            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);
            await tester.Extractor.Looper.WaitForNextPush();

            var oldHost = tester.Config.Influx.Host;
            tester.Config.Influx.Host = "testWrong";
            ((InfluxPusher)tester.Pusher).Reconfigure();
            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1000);
            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 1.0);

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyPoints,
                20, "Failurebuffer must receive some data");

            tester.Config.Influx.Host = oldHost;
            ((InfluxPusher)tester.Pusher).Reconfigure();

            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1001);
            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 2.0);

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.AnyPoints,
                20, "FailureBuffer should be emptied");

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);

            await tester.Extractor.Looper.WaitForNextPush();

            await tester.TerminateRunTask(true);

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_datapoint_push_failures_influx"));
        }

        [Trait("Server", "basic")]
        [Trait("Target", "StateStorage")]
        [Trait("Test", "influxbufferstatedata")]
        [Fact]
        public async Task TestInfluxBufferStateData()
        {
            var states = new List<InfluxBufferState>();
            using (var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Test,
                FailureInflux = true,
                StoreDatapoints = true,
                StateInflux = true
            }))
            {
                await tester.ClearPersistentData();

                tester.Config.Extraction.DataTypes.AllowStringVariables = true;
                tester.Config.History.Enabled = false;

                await tester.StartServer();

                tester.StartExtractor();

                await tester.Extractor.Looper.WaitForNextPush();
                await tester.Extractor.Looper.WaitForNextPush();

                tester.Handler.AllowPush = false;
                tester.Handler.AllowConnectionTest = false;

                tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1);
                tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar1, 1);
                tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 1);
                tester.Server.UpdateNode(tester.Server.Ids.Base.BoolVar, true);
                tester.Server.UpdateNode(tester.Server.Ids.Base.StringVar, "test 1");

                await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyPoints,
                    20, "Failurebuffer must receive some data");

                await tester.Extractor.Looper.WaitForNextPush();

                states = tester.Extractor.State.NodeStates.Where(state => !state.FrontfillEnabled)
                    .Select(state => new InfluxBufferState(state)).ToList();

                await tester.Extractor.StateStorage.RestoreExtractionState(
                    states.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.InfluxVariableStore,
                    false,
                    CancellationToken.None);

                Assert.True(states.All(state =>
                    state.DestinationExtractedRange.First <= state.DestinationExtractedRange.Last
                    && state.DestinationExtractedRange.Last != DateTime.MaxValue));

                await tester.TerminateRunTask(false);
            }

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Test,
                FailureInflux = true,
                StoreDatapoints = true,
                StateInflux = true
            });

            await tester2.StartServer();

            tester2.StartExtractor();
            tester2.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester2.Config.History.Enabled = false;

            await tester2.Extractor.WaitForSubscriptions();

            await tester2.Extractor.Looper.WaitForNextPush();
            await tester2.Extractor.Looper.WaitForNextPush();

            foreach (var state in states) state.ClearRanges();

            await tester2.Extractor.StateStorage.RestoreExtractionState(
                states.ToDictionary(state => state.Id),
                tester2.Config.StateStorage.InfluxVariableStore,
                false,
                CancellationToken.None);

            Assert.True(states.All(state =>
                state.DestinationExtractedRange == TimeRange.Empty));

            await tester2.TerminateRunTask(false);
        }
        [Trait("Server", "basic")]
        [Trait("Target", "StateStorage")]
        [Trait("Test", "influxbufferstateevents")]
        [Fact]
        public async Task TestInfluxBufferStateEvents()
        {
            var states = new List<InfluxBufferState>();
            using (var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events,
                FailureInflux = true,
                StoreDatapoints = true,
                StateInflux = true
            }))
            {
                await tester.ClearPersistentData();

                tester.Config.Events.History = false;

                await tester.StartServer();

                tester.StartExtractor();

                await tester.Extractor.WaitForSubscriptions();
                await tester.Extractor.Looper.WaitForNextPush();

                tester.Handler.AllowPush = false;
                tester.Handler.AllowEvents = false;
                tester.Handler.AllowConnectionTest = false;

                tester.Server.TriggerEvents(1);

                await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyEvents,
                    20, "Failurebuffer must receive some data");

                await tester.Extractor.Looper.WaitForNextPush();

                states = tester.Extractor.State.EmitterStates.Select(state => new InfluxBufferState(state)).ToList();

                await tester.Extractor.StateStorage.RestoreExtractionState(
                    states.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.InfluxEventStore,
                    false,
                    CancellationToken.None);

                Assert.True(states.All(state => state.DestinationExtractedRange.First <= state.DestinationExtractedRange.Last
                                                && state.DestinationExtractedRange.Last != DateTime.MaxValue));

                await tester.TerminateRunTask(false);
            }

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events,
                FailureInflux = true,
                StoreDatapoints = true,
                StateInflux = true
            });
            tester2.Config.Events.History = false;

            await tester2.StartServer();

            tester2.StartExtractor();

            await tester2.Extractor.Looper.WaitForNextPush();

            foreach (var state in states) state.ClearRanges();

            await tester2.Extractor.StateStorage.RestoreExtractionState(
                states.ToDictionary(state => state.Id),
                tester2.Config.StateStorage.InfluxEventStore,
                true,
                CancellationToken.None);

            foreach (var state in states)
            {
                Log.Information("State: {id}, {first}, {last}", state.Id, state.DestinationExtractedRange.First, state.DestinationExtractedRange.Last);
            }

            Assert.True(states.All(state => state.DestinationExtractedRange == TimeRange.Empty));

            await tester2.TerminateRunTask(false);
        }
    }
}

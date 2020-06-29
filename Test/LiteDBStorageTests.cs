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
using Opc.Ua;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [Collection("Extractor tests")]
    public class LiteDbStorageTests : MakeConsoleWork
    {
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

            tester.Config.Extraction.AllowStringVariables = true;

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

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.Any,
                20, "Failurebuffer must receive some data");

            tester.Config.Influx.Host = oldHost;
            ((InfluxPusher)tester.Pusher).Reconfigure();

            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1001);
            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 2.0);

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.Any,
                20, "FailureBuffer should be emptied");

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);

            await tester.Extractor.Looper.WaitForNextPush();

            await tester.TerminateRunTask();

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_datapoint_push_failures_influx"));
        }

        [Trait("Server", "basic")]
        [Trait("Target", "StateStorage")]
        [Trait("Test", "statestoragedata")]
        [Fact]
        public async Task TestStateStorageData()
        {
            using (var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StateStorage = true
            }))
            {
                await tester.ClearPersistentData();

                await tester.StartServer();
                tester.Server.PopulateBaseHistory();

                tester.Config.Cognite.ReadExtractedRanges = false;

                tester.Config.Extraction.AllowStringVariables = true;
                tester.Config.History.Backfill = true;

                var now = DateTime.UtcNow;

                tester.StartExtractor();

                await tester.WaitForCondition(() => 
                        tester.Extractor.State.NodeStates.Any()
                        && tester.Extractor.State.NodeStates.All(state =>
                            !state.FrontfillEnabled || !state.IsBackfilling && !state.IsFrontfilling),
                    20, "Expected history to complete");

                await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.Any(state =>
                    state.LastTimeModified != null && state.LastTimeModified > now),
                    20, "Expected states to become dirty");

                await tester.Extractor.Looper.WaitForNextPush();
                await tester.Extractor.Looper.WaitForNextPush();
                await tester.Extractor.Looper.StoreState(tester.Source.Token);

                await tester.TerminateRunTask();

                var dummyStates = tester.Extractor.State.NodeStates.Select(state => new InfluxBufferState(state))
                    .ToList();

                await tester.Extractor.StateStorage.RestoreExtractionState(
                    dummyStates.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.VariableStore,
                    false,
                    CancellationToken.None);

                foreach (var state in dummyStates)
                {
                    Log.Information("State: {id}, {range}, {hist}", state.Id, state.DestinationExtractedRange, state.Historizing);
                }
                Assert.True(dummyStates.All(state => !state.Historizing
                                                     || state.DestinationExtractedRange.First < state.DestinationExtractedRange.Last
                                                     && state.DestinationExtractedRange.Last != DateTime.MaxValue));
            }

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                StateStorage = true
            });

            await tester2.StartServer();
            tester2.Server.PopulateBaseHistory();

            tester2.Config.Cognite.ReadExtractedRanges = false;
            tester2.Config.Extraction.AllowStringVariables = true;
            tester2.Config.History.Backfill = true;
            CommonTestUtils.ResetTestMetrics();

            tester2.StartExtractor();

            await tester2.Extractor.WaitForSubscriptions();

            await tester2.WaitForCondition(() => tester2.Extractor.State.NodeStates.All(state => !state.FrontfillEnabled
                    || !state.IsBackfilling && !state.IsFrontfilling),
                20, "Expected history to complete");
            await tester2.Extractor.Looper.WaitForNextPush();

            // 0 backfill despite backfill being enabled is strong proof that it works.
            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_data_count", 0));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_data_count", 1));
        }

        [Trait("Server", "events")]
        [Trait("Target", "StateStorage")]
        [Trait("Test", "statestorageevents")]
        [Fact]
        public async Task TestStateStorageEvents()
        {
            var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StateStorage = true,
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events
            });
            await tester.ClearPersistentData();

            await tester.StartServer();
            tester.Server.PopulateEvents();
            tester.Config.History.Enabled = true;
            tester.Config.Cognite.ReadExtractedRanges = false;
            tester.Config.History.Backfill = true;

            var now = DateTime.UtcNow;

            tester.StartExtractor();

            await tester.WaitForCondition(() =>
                    tester.Extractor.State.EmitterStates.Any()
                    && tester.Extractor.State.EmitterStates.All(state =>
                        !state.FrontfillEnabled || !state.IsBackfilling && !state.IsFrontfilling),
                20, "Expected history to complete");

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.Any(state =>
                state.LastTimeModified != null && state.LastTimeModified > now),
                20, "Expected states to become dirty");

            await tester.Extractor.Looper.WaitForNextPush();
            await tester.Extractor.Looper.WaitForNextPush();
            await tester.Extractor.Looper.StoreState(tester.Source.Token);

            var dummyStates = tester.Extractor.State.EmitterStates.Select(state => new InfluxBufferState(state)).ToList();

            foreach (var state in dummyStates)
            {
                state.SetComplete();
            }

            await tester.Extractor.StateStorage.RestoreExtractionState(
                dummyStates.ToDictionary(state => state.Id),
                tester.Config.StateStorage.EventStore,
                false,
                CancellationToken.None);

            await tester.TerminateRunTask();


            Assert.True(dummyStates.All(state => !state.Historizing
                || state.DestinationExtractedRange.First < state.DestinationExtractedRange.Last
                && state.DestinationExtractedRange.Last != DateTime.MaxValue));
            tester.Dispose();

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                StateStorage = true,
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events
            });
            tester2.Config.History.Enabled = true;
            tester2.Config.Cognite.ReadExtractedRanges = false;
            tester2.Config.History.Backfill = true;
            CommonTestUtils.ResetTestMetrics();

            await tester2.StartServer();
            tester2.Server.PopulateEvents();

            tester2.StartExtractor();

            await tester2.Extractor.WaitForSubscriptions();

            await tester2.Extractor.Looper.WaitForNextPush();
            await tester2.WaitForCondition(() => tester2.Extractor.State.EmitterStates.Any() 
                && tester2.Extractor.State.EmitterStates.All(state => !state.FrontfillEnabled
                    || !state.IsBackfilling && !state.IsFrontfilling),
                20, "Expected history to complete");
            await tester2.Extractor.Looper.WaitForNextPush();

            Assert.True(CommonTestUtils.TestMetricValue("opcua_backfill_events_count", 0));
            Assert.True(CommonTestUtils.TestMetricValue("opcua_frontfill_events_count", 1));
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

                tester.Config.Extraction.AllowStringVariables = true;
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

                await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.Any,
                    20, "Failurebuffer must receive some data");

                await tester.Extractor.Looper.WaitForNextPush();

                states = tester.Extractor.State.NodeStates.Where(state => !state.FrontfillEnabled)
                    .Select(state => new InfluxBufferState(state)).ToList();
                foreach (var state in states)
                {
                    state.SetComplete();
                }

                await tester.Extractor.StateStorage.RestoreExtractionState(
                    states.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.InfluxVariableStore,
                    false,
                    CancellationToken.None);

                Assert.True(states.All(state =>
                    state.DestinationExtractedRange.First <= state.DestinationExtractedRange.Last
                    && state.DestinationExtractedRange.Last != DateTime.MaxValue));

                await tester.TerminateRunTask();
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
            tester2.Config.Extraction.AllowStringVariables = true;
            tester2.Config.History.Enabled = false;

            await tester2.Extractor.WaitForSubscriptions();

            await tester2.Extractor.Looper.WaitForNextPush();
            await tester2.Extractor.Looper.WaitForNextPush();

            foreach (var state in states)
            {
                state.SetComplete();
            }

            await tester2.Extractor.StateStorage.RestoreExtractionState(
                states.ToDictionary(state => state.Id),
                tester2.Config.StateStorage.InfluxVariableStore,
                false,
                CancellationToken.None);

            Assert.True(states.All(state =>
                state.DestinationExtractedRange == TimeRange.Empty));

            await tester2.TerminateRunTask();
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

                tester.Config.Events.HistorizingEmitterIds = new List<ProtoNodeId>();

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
                foreach (var state in states)
                {
                    state.SetComplete();
                }

                await tester.Extractor.StateStorage.RestoreExtractionState(
                    states.ToDictionary(state => state.Id),
                    tester.Config.StateStorage.InfluxEventStore,
                    false,
                    CancellationToken.None);

                Assert.True(states.All(state => !state.Historizing
                                                || state.DestinationExtractedRange.First <= state.DestinationExtractedRange.Last
                                                && state.DestinationExtractedRange.Last != DateTime.MaxValue));

                await tester.TerminateRunTask();
            }

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events,
                FailureInflux = true,
                StoreDatapoints = true,
                StateInflux = true
            });
            tester2.Config.Events.HistorizingEmitterIds = new List<ProtoNodeId>();

            await tester2.StartServer();

            tester2.StartExtractor();

            await tester2.Extractor.Looper.WaitForNextPush();

            foreach (var state in states)
            {
                state.SetComplete();
            }

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

            await tester2.TerminateRunTask();
        }
        [Fact]
        [Trait("Server", "events")]
        [Trait("Target", "OldBuffer")]
        [Trait("Test", "eventbyteconversion")]
        public async Task TextEventByteConversion()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events,
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();
            await tester.StartServer();

            tester.StartExtractor();

            await tester.RunTask;

            var evt = new BufferedEvent
            {
                EmittingNode = ObjectIds.Server,
                EventId = "123456789",
                EventType = new NodeId("test", 1),
                Message = "msg",
                MetaData = new Dictionary<string, object>
                {
                    ["dt1"] = "data1",
                    ["dt2"] = "data2"
                },
                SourceNode = tester.Server.Ids.Event.Obj2,
                ReceivedTime = DateTime.UtcNow,
                Time = DateTime.UtcNow
            };

            var evt2 = new BufferedEvent
            {
                EmittingNode = tester.Server.Ids.Event.Obj1,
                EventId = "123456789",
                EventType = new NodeId("test", 1),
                Message = null,
                MetaData = new Dictionary<string, object>(),
                SourceNode = tester.Server.Ids.Event.Var1,
                ReceivedTime = DateTime.UtcNow,
                Time = DateTime.UtcNow
            };

            var bytes = evt.ToStorableBytes(tester.Extractor);

            (var converted, int last) = BufferedEvent.FromStorableBytes(bytes, tester.Extractor, sizeof(ushort));

            Assert.Equal(last, bytes.Length);
            Assert.Equal(evt.EmittingNode, converted.EmittingNode);
            Assert.Equal(evt.EventId, converted.EventId);
            Assert.Equal(tester.Extractor.GetUniqueId(evt.EventType), converted.MetaData["Type"]);
            Assert.Equal(evt.Message, converted.Message);
            foreach (var kvp in evt.MetaData)
            {
                Assert.Equal(kvp.Value, converted.MetaData[kvp.Key]);
            }
            Assert.Equal(evt.SourceNode, converted.SourceNode);
            Assert.Equal(evt.Time, converted.Time);

            var bytes2 = evt2.ToStorableBytes(tester.Extractor);

            (var converted2, int last2) = BufferedEvent.FromStorableBytes(bytes2, tester.Extractor, sizeof(ushort));

            Assert.Equal(last2, bytes2.Length);
            Assert.Equal(evt2.EmittingNode, converted2.EmittingNode);
            Assert.Equal(evt2.EventId, converted2.EventId);
            Assert.Equal(tester.Extractor.GetUniqueId(evt2.EventType), converted2.MetaData["Type"]);
            Assert.Equal(evt2.Message, converted2.Message);
            foreach (var kvp in evt2.MetaData)
            {
                Assert.Equal(kvp.Value, converted2.MetaData[kvp.Key]);
            }
            Assert.Equal(evt2.SourceNode, converted2.SourceNode);
            Assert.Equal(evt2.Time, converted2.Time);

            await tester.TerminateRunTask();
        }
        [Fact]
        [Trait("Target", "OldBuffer")]
        [Trait("Test", "datapointconversion")]
        public void TestDataPointConversion()
        {
            var dp = new BufferedDataPoint(DateTime.UtcNow, "testid", 123.123);
            var dp2 = new BufferedDataPoint(DateTime.UtcNow, "testid2", "testvalue");

            var dpc = dp.ToStorableBytes();
            var dp2c = dp2.ToStorableBytes();

            var dpconv = BufferedDataPoint.FromStorableBytes(dpc, sizeof(ushort));
            var dp2conv = BufferedDataPoint.FromStorableBytes(dp2c, sizeof(ushort));

            Assert.Equal(dpc.Length, BitConverter.ToUInt16(dpc) + sizeof(ushort));
            Assert.Equal(dp.Timestamp, dpconv.DataPoint.Timestamp);
            Assert.Equal(dp.DoubleValue, dpconv.DataPoint.DoubleValue);
            Assert.Equal(dp.Id, dpconv.DataPoint.Id);
            Assert.Equal(dp.IsString, dpconv.DataPoint.IsString);

            Assert.Equal(dp2c.Length, BitConverter.ToUInt16(dp2c) + sizeof(ushort));
            Assert.Equal(dp2.Timestamp, dp2conv.DataPoint.Timestamp);
            Assert.Equal(dp2.DoubleValue, dp2conv.DataPoint.DoubleValue);
            Assert.Equal(dp2.Id, dp2conv.DataPoint.Id);
            Assert.Equal(dp2.IsString, dp2conv.DataPoint.IsString);
        }

        [Fact]
        [Trait("Target", "OldBuffer")]
        [Trait("Test", "datapointconversionwrite")]
        public void TestDataPointConversionWrite()
        {
            var dps = new List<BufferedDataPoint>();
            for (int i = 0; i < 10000; i++)
            {
                dps.Add(new BufferedDataPoint(DateTime.UtcNow, "testid", i));
                dps.Add(new BufferedDataPoint(DateTime.UtcNow, "testid", "test " + i));
            }

            File.Create("datapoints.bin").Close();

            FailureBuffer.WriteDatapointsToFile("datapoints.bin", dps, CancellationToken.None);

            Assert.True(new FileInfo("datapoints.bin").Length > 0);

            long nextPos = 0;

            var readPoints = new List<BufferedDataPoint>();

            int count = 0;

            do
            {
                IEnumerable<BufferedDataPoint> localRead;
                (localRead, nextPos) =
                    FailureBuffer.ReadDatapointsFromFile("datapoints.bin", nextPos, 1000, CancellationToken.None);
                readPoints.AddRange(localRead);
                count++;
            } while (nextPos > 0);

            Assert.Equal(20, count);

            for (int i = 0; i < 10000; i++)
            {
                var dp = dps[i];
                var dpconv = readPoints[i];
                Assert.Equal(dp.Timestamp, dpconv.Timestamp);
                Assert.Equal(dp.DoubleValue, dpconv.DoubleValue);
                Assert.Equal(dp.Id, dpconv.Id);
                Assert.Equal(dp.IsString, dpconv.IsString);
            }
        }
        [Fact]
        [Trait("Server", "events")]
        [Trait("Target", "OldBuffer")]
        [Trait("Test", "eventconversionwrite")]
        public async Task TestEventConversionWrite()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events,
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();
            await tester.StartServer();
            tester.StartExtractor();

            await tester.RunTask;

            var evts = new List<BufferedEvent>();

            for (int i = 0; i < 10000; i++)
            {
                evts.Add(new BufferedEvent
                {
                    EmittingNode = ObjectIds.Server,
                    EventId = "id " + i,
                    EventType = new NodeId("test", 1),
                    Message = "msg " + i,
                    MetaData = new Dictionary<string, object>
                    {
                        ["dt1"] = "data1",
                        ["dt2"] = "data2"
                    },
                    SourceNode = tester.Server.Ids.Event.Obj1,
                    ReceivedTime = DateTime.UtcNow,
                    Time = DateTime.UtcNow
                });
            }

            File.Create("events.bin").Close();

            FailureBuffer.WriteEventsToFile("events.bin", evts, tester.Extractor, CancellationToken.None);

            Assert.True(new FileInfo("events.bin").Length > 0);

            long nextPos = 0;

            var finalRead = new List<BufferedEvent>();
            int count = 0;
            do
            {
                IEnumerable<BufferedEvent> readEvents;
                (readEvents, nextPos) =
                    FailureBuffer.ReadEventsFromFile("events.bin", tester.Extractor, nextPos, 1000,
                        CancellationToken.None);
                finalRead.AddRange(readEvents);
                count++;
            } while (nextPos > 0);

            Assert.Equal(10, count);

            for (int i = 0; i < 10000; i++)
            {
                var evt = evts[i];
                var converted = finalRead[i];
                Assert.Equal(evt.EmittingNode, converted.EmittingNode);
                Assert.Equal(evt.EventId, converted.EventId);
                Assert.Equal(tester.Extractor.GetUniqueId(evt.EventType), converted.MetaData["Type"]);
                Assert.Equal(evt.Message, converted.Message);
                foreach ((string key, var value) in evt.MetaData)
                {
                    Assert.Equal(value, converted.MetaData[key]);
                }
                Assert.Equal(evt.SourceNode, converted.SourceNode);
                Assert.Equal(evt.Time, converted.Time);
            }

            await tester.TerminateRunTask();
        }
        [Trait("Server", "basic")]
        [Trait("Target", "OldBuffer")]
        [Trait("Test", "filebufferdata")]
        [Fact]
        public async Task TestFileBufferData()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true,
                DataBufferPath = "datapoints.bin"
            });
            await tester.ClearPersistentData();
            await tester.StartServer();

            tester.Config.Extraction.AllowStringVariables = true;

            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();
            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);
            await tester.Extractor.Looper.WaitForNextPush();

            tester.Handler.AllowPush = false;
            tester.Handler.AllowConnectionTest = false;

            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 1.0);
            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 1);

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.Any,
                20, "Failurebuffer must receive some data");

            tester.Handler.AllowPush = true;
            tester.Handler.AllowConnectionTest = true;

            tester.Server.UpdateNode(tester.Server.Ids.Base.DoubleVar2, 2.0);
            tester.Server.UpdateNode(tester.Server.Ids.Base.IntVar, 2);

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.Any,
                20, "FailureBuffer should be emptied");

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => !state.IsFrontfilling), 20);
            await tester.Extractor.Looper.WaitForNextPush();

            await tester.TerminateRunTask();

            tester.TestContinuity("gp.tl:i=10");
            tester.TestContinuity("gp.tl:i=3");

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(2, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(5, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_datapoint_push_failures_cdf"));
        }
        [Trait("Server", "events")]
        [Trait("Target", "OldBuffer")]
        [Trait("Test", "filebufferevents")]
        [Fact]
        public async Task TestFileBufferEvents()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true,
                EventBufferPath = "events.bin",
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events
            });
            await tester.ClearPersistentData();

            await tester.StartServer();

            tester.Config.Extraction.AllowStringVariables = true;

            tester.StartExtractor();

            await tester.Extractor.Looper.WaitForNextPush();
            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => !state.IsFrontfilling), 20);
            await tester.Extractor.Looper.WaitForNextPush();

            tester.Handler.AllowPush = false;
            tester.Handler.AllowEvents = false;
            tester.Handler.AllowConnectionTest = false;

            tester.Server.TriggerEvents(0);

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyEvents,
                20, "Failurebuffer must receive some events");

            tester.Handler.AllowPush = true;
            tester.Handler.AllowEvents = true;
            tester.Handler.AllowConnectionTest = true;

            tester.Server.TriggerEvents(1);

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.AnyEvents,
                20, "FailureBuffer should be emptied");

            tester.Server.TriggerEvents(2);

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => !state.IsFrontfilling), 20);

            await tester.Extractor.Looper.WaitForNextPush();

            await tester.TerminateRunTask();

            var events = tester.Handler.Events.Values.ToList();
            Assert.True(events.Any());

            Assert.Equal(21, events.Count);

            var countregex = new Regex("\\d+$");
            var propNumbers = events
                .Where(evt => evt.description.StartsWith("prop ", StringComparison.InvariantCulture))
                .Select(evt => Convert.ToInt32(countregex.Match(evt.description).Value, CultureInfo.InvariantCulture))
                .Distinct()
                .ToList();
            ExtractorTester.TestContinuity(propNumbers);

            var nonHistNumbers = events
                .Where(evt => evt.description.StartsWith("prop-e2 ", StringComparison.InvariantCulture))
                .Select(evt => Convert.ToInt32(countregex.Match(evt.description).Value, CultureInfo.InvariantCulture))
                .Distinct()
                .ToList();
            ExtractorTester.TestContinuity(nonHistNumbers);

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_event_push_failures_cdf"));
        }
    }
}

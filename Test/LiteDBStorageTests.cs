﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Opc.Ua;
using Xunit;
using Xunit.Abstractions;

namespace Test
{
    [CollectionDefinition("State_tests", DisableParallelization = true)]
    public class LiteDbStorageTests : MakeConsoleWork
    {
        public LiteDbStorageTests(ITestOutputHelper output) : base(output) { }

        [Trait("Server", "basic")]
        [Trait("Target", "StateStorage")]
        [Trait("Test", "queuebufferdata")]
        [Fact]
        public async Task TestQueueBufferData()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true,
                BufferQueue = true
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.AllowStringVariables = true;

            tester.StartExtractor();

            await tester.Extractor.WaitForNextPush();

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            tester.Handler.AllowPush = false;
            tester.Handler.AllowConnectionTest = false;

            await tester.WaitForCondition(() => tester.Extractor.StateStorage.AnyPoints,
                20, "Failurebuffer must receive some data");

            tester.Handler.AllowPush = true;
            tester.Handler.AllowConnectionTest = true;

            await tester.WaitForCondition(() => !tester.Extractor.StateStorage.AnyPoints,
                20, "FailureBuffer should be emptied");

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            await tester.TerminateRunTask();

            tester.TestContinuity("gp.efg:i=10");
            tester.TestConstantRate(1000, "gp.efg:i=9");

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.Equal(2, (int)CommonTestUtils.GetMetricValue("opcua_tracked_assets"));
            Assert.Equal(5, (int)CommonTestUtils.GetMetricValue("opcua_tracked_timeseries"));
            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_datapoint_push_failures_cdf"));
        }

        [Trait("Server", "events")]
        [Trait("Target", "StateStorage")]
        [Trait("Test", "queuebufferevents")]
        [Fact]
        public async Task TestQueueBufferEvents()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                StoreDatapoints = true,
                BufferQueue = true,
                ServerName = ServerName.Events,
                ConfigName = ConfigName.Events
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.AllowStringVariables = true;

            tester.StartExtractor();

            await tester.Extractor.WaitForNextPush();

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            // In theory it will work as expected if only pushes to events fail, so long as the connection test also fails,
            // but there is no real reason to test this on an unrealistic scenario.
            // In this case, history will still restart in its entirity once connection is re-established.
            // In theory we could have separated the two, but it is an edge case. Perhaps in the future.
            // The behavior if only the push fails but not the connection test is less clear,
            // what we would expect is that if there are events in the given push, then we end up with double pushes as expected.
            // If not, the pushers will end up getting set to succeeding, but then the read from failurebuffer will fail,
            // so we will continue to read from there.
            // The same conditions still hold: If state storage is enabled on influx, or the queue is used, points will not be lost.
            tester.Handler.AllowPush = false;
            tester.Handler.AllowEvents = false;
            tester.Handler.AllowConnectionTest = false;

            await tester.WaitForCondition(() => tester.Extractor.StateStorage.AnyEvents,
                20, "Failurebuffer must receive some events");

            tester.Handler.AllowPush = true;
            tester.Handler.AllowEvents = true;
            tester.Handler.AllowConnectionTest = true;

            await tester.WaitForCondition(() => !tester.Extractor.StateStorage.AnyEvents,
                20, "FailureBuffer should be emptied");

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            await tester.TerminateRunTask();

            var events = tester.Handler.events.Values.ToList();
            Assert.True(events.Any());

            Assert.Contains(events, ev => ev.description.StartsWith("prop ", StringComparison.InvariantCulture));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            var countregex = new Regex("\\d+$");
            var propNumbers = events
                .Where(evt => evt.description.StartsWith("prop ", StringComparison.InvariantCulture))
                .Select(evt => Convert.ToInt32(countregex.Match(evt.description).Value, CultureInfo.InvariantCulture))
                .ToList();
            ExtractorTester.TestContinuity(propNumbers);

            var nonHistNumbers = events
                .Where(evt => evt.description.StartsWith("propOther ", StringComparison.InvariantCulture))
                .Select(evt => Convert.ToInt32(countregex.Match(evt.description).Value, CultureInfo.InvariantCulture))
                .ToList();
            ExtractorTester.TestContinuity(nonHistNumbers);

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_datapoint_push_failures_cdf"));
        }

        [Trait("Server", "basic")]
        [Trait("Target", "StateStorage")]
        [Trait("Test", "influxautobufferdata")]
        [Fact]
        public async Task TestInfluxAutoBufferData()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                BufferQueue = true,
                ConfigName = ConfigName.Influx
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.AllowStringVariables = true;

            tester.StartExtractor();

            await tester.Extractor.WaitForNextPush();

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            var oldHost = tester.InfluxConfig.Host;
            tester.InfluxConfig.Host = "testWrong";
            ((InfluxPusher)tester.Pusher).Reconfigure();

            await tester.WaitForCondition(() => tester.Extractor.StateStorage.AnyPoints,
                20, "Failurebuffer must receive some data");

            tester.InfluxConfig.Host = oldHost;
            ((InfluxPusher)tester.Pusher).Reconfigure();

            await tester.WaitForCondition(() => !tester.Extractor.StateStorage.AnyPoints,
                20, "FailureBuffer should be emptied");

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

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
                BufferQueue = true,
                StateStorage = true,
                LogLevel = "debug"
            }))
            {
                await tester.ClearPersistentData();

                tester.CogniteConfig.ReadExtractedRanges = false;

                tester.Config.Extraction.AllowStringVariables = true;
                tester.Config.History.Backfill = true;

                tester.StartExtractor();

                await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state =>
                        !state.Historizing
                        || state.BackfillDone
                        && state.IsStreaming),
                    20, "Expected history to complete");

                await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.Any(state => state.IsDirty),
                    20, "Expected states to become dirty");

                await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => 
                        !state.Historizing || state.StatePersisted),
                    20, "Expected states to become clean again");

                await tester.Extractor.WaitForNextPush();

                await tester.TerminateRunTask();

                var dummyStates = tester.Extractor.State.NodeStates.Select(state => new InfluxBufferState(state, false))
                    .ToList();

                foreach (var state in dummyStates)
                {
                    state.DestinationExtractedRange.Start = DateTime.MinValue;
                    state.DestinationExtractedRange.End = DateTime.MaxValue;
                }

                await tester.Extractor.StateStorage.ReadExtractionStates(dummyStates, StateStorage.VariableStates, false,
                    CancellationToken.None);

                Assert.True(dummyStates.All(state => !state.Historizing
                                                     || state.StatePersisted 
                                                     && state.DestinationExtractedRange.Start < state.DestinationExtractedRange.End
                                                     && state.DestinationExtractedRange.End != DateTime.MaxValue));
            }

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                BufferQueue = true,
                StateStorage = true
            });
            tester2.CogniteConfig.ReadExtractedRanges = false;
            tester2.Config.Extraction.AllowStringVariables = true;
            tester2.Config.History.Backfill = true;
            CommonTestUtils.ResetTestMetrics();

            tester2.StartExtractor();
            await tester2.WaitForCondition(() => tester2.Extractor.State.NodeStates.All(state => !state.Historizing
                    || state.BackfillDone && state.IsStreaming),
                20, "Expected history to complete");
            await tester2.Extractor.WaitForNextPush();

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
                BufferQueue = true,
                StateStorage = true,
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events,
                LogLevel = "debug"
            });
            await tester.ClearPersistentData();

            tester.CogniteConfig.ReadExtractedRanges = false;
            tester.Config.History.Backfill = true;

            tester.StartExtractor();

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => !state.Historizing
                    || state.BackfillDone && state.IsStreaming),
                20, "Expected history to complete");

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.Any(state => state.IsDirty),
                20, "Expected states to become dirty");

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.Any(state =>
                    !state.Historizing || state.StatePersisted),
                20, "Expected states to be persisted");

            await tester.Extractor.WaitForNextPush();

            await tester.TerminateRunTask();

            var dummyStates = tester.Extractor.State.EmitterStates.Select(state => new InfluxBufferState(state.Id)).ToList();

            foreach (var state in dummyStates)
            {
                state.DestinationExtractedRange.Start = DateTime.MinValue;
                state.DestinationExtractedRange.End = DateTime.MaxValue;
            }

            await tester.Extractor.StateStorage.ReadExtractionStates(dummyStates, StateStorage.EmitterStates, false,
                CancellationToken.None);

            Assert.True(dummyStates.All(state => !state.Historizing
                || state.StatePersisted && state.DestinationExtractedRange.Start < state.DestinationExtractedRange.End
                && state.DestinationExtractedRange.End != DateTime.MaxValue));
            tester.Dispose();

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                BufferQueue = true,
                StateStorage = true,
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events
            });
            tester2.CogniteConfig.ReadExtractedRanges = false;
            tester2.Config.History.Backfill = true;
            CommonTestUtils.ResetTestMetrics();

            tester2.StartExtractor();
            await tester2.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => !state.Historizing
                    || state.BackfillDone && state.IsStreaming),
                20, "Expected history to complete");
            await tester2.Extractor.WaitForNextPush();

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
                FailureInflux = ConfigName.Influx,
                StoreDatapoints = true,
                FailureInfluxWrite = true,
                StateInflux = true,
                LogLevel = "debug"
            }))
            {
                await tester.ClearPersistentData();

                tester.Config.Extraction.AllowStringVariables = true;
                tester.Config.History.Enabled = false;

                tester.StartExtractor();

                await tester.Extractor.WaitForNextPush();
                await tester.Extractor.WaitForNextPush();

                tester.Handler.AllowPush = false;
                tester.Handler.AllowConnectionTest = false;

                await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.Any,
                    20, "Failurebuffer must receive some data");

                await tester.Extractor.WaitForNextPush();

                await tester.TerminateRunTask();

                states = tester.Extractor.State.NodeStates.Where(state => !state.Historizing)
                    .Select(state => new InfluxBufferState(state, false)).ToList();
                foreach (var state in states)
                {
                    state.DestinationExtractedRange.Start = DateTime.MinValue;
                    state.DestinationExtractedRange.End = DateTime.MaxValue;
                }

                await tester.Extractor.StateStorage.ReadExtractionStates(states, StateStorage.InfluxVariableStates, false,
                    CancellationToken.None);

                Assert.True(states.All(state =>
                    state.DestinationExtractedRange.Start <= state.DestinationExtractedRange.End
                    && state.DestinationExtractedRange.End != DateTime.MaxValue));
            }

            

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Test,
                FailureInflux = ConfigName.Influx,
                StoreDatapoints = true,
                FailureInfluxWrite = true,
                StateInflux = true
            });

            tester2.StartExtractor();
            tester2.Config.Extraction.AllowStringVariables = true;
            tester2.Config.History.Enabled = false;

            await tester2.Extractor.WaitForNextPush();

            await tester2.TerminateRunTask();

            foreach (var state in states)
            {
                state.DestinationExtractedRange.Start = DateTime.MinValue;
                state.DestinationExtractedRange.End = DateTime.MaxValue;
            }

            await tester2.Extractor.StateStorage.ReadExtractionStates(states, StateStorage.InfluxVariableStates, false,
                CancellationToken.None);

            Assert.True(states.All(state =>
                state.DestinationExtractedRange.Start == DateTime.MaxValue
                && state.DestinationExtractedRange.End == DateTime.MinValue));
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
                FailureInflux = ConfigName.Influx,
                StoreDatapoints = true,
                FailureInfluxWrite = true,
                StateInflux = true,
                LogLevel = "debug"
            }))
            {
                await tester.ClearPersistentData();

                tester.Config.Events.HistorizingEmitterIds = new List<ProtoNodeId>();

                tester.StartExtractor();

                await tester.Extractor.WaitForNextPush();
                await tester.Extractor.WaitForNextPush();

                tester.Handler.AllowPush = false;
                tester.Handler.AllowEvents = false;
                tester.Handler.AllowConnectionTest = false;

                await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyEvents,
                20, "Failurebuffer must receive some data");

                await tester.Extractor.WaitForNextPush();

                await tester.TerminateRunTask();

                states = tester.Extractor.State.AllActiveIds.Select(state => new InfluxBufferState(state)).ToList();
                foreach (var state in states)
                {
                    state.DestinationExtractedRange.Start = DateTime.MinValue;
                    state.DestinationExtractedRange.End = DateTime.MaxValue;
                }

                await tester.Extractor.StateStorage.ReadExtractionStates(states, StateStorage.InfluxEventStates, false,
                    CancellationToken.None);

                Assert.True(states.All(state => !state.StatePersisted
                                                || state.DestinationExtractedRange.Start <= state.DestinationExtractedRange.End
                                                && state.DestinationExtractedRange.End != DateTime.MaxValue));
            }

            using var tester2 = new ExtractorTester(new ExtractorTestParameters
            {
                ConfigName = ConfigName.Events,
                ServerName = ServerName.Events,
                FailureInflux = ConfigName.Influx,
                StoreDatapoints = true,
                FailureInfluxWrite = true,
                StateInflux = true,
                LogLevel = "debug"
            });
            tester2.Config.Events.HistorizingEmitterIds = new List<ProtoNodeId>();

            tester2.StartExtractor();

            await tester2.Extractor.WaitForNextPush();

            await tester2.TerminateRunTask();

            foreach (var state in states)
            {
                state.DestinationExtractedRange.Start = DateTime.MinValue;
                state.DestinationExtractedRange.End = DateTime.MaxValue;
            }

            await tester2.Extractor.StateStorage.ReadExtractionStates(states, StateStorage.InfluxEventStates, false,
                CancellationToken.None);

            Assert.True(states.All(state => !state.StatePersisted
                || state.DestinationExtractedRange.Start == DateTime.MaxValue
                && state.DestinationExtractedRange.End == DateTime.MinValue));
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
                LogLevel = "information",
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();

            tester.StartExtractor();

            await tester.TerminateRunTask();

            var evt = new BufferedEvent
            {
                EmittingNode = tester.Extractor.State.EmitterStates.First().Id,
                EventId = "123456789",
                EventType = new NodeId("test", 1),
                Message = "msg",
                MetaData = new Dictionary<string, object>
                {
                    ["dt1"] = "data1",
                    ["dt2"] = "data2"
                },
                SourceNode = tester.Extractor.State.AllActiveIds.First(),
                ReceivedTime = DateTime.Now,
                Time = DateTime.Now
            };

            var evt2 = new BufferedEvent
            {
                EmittingNode = tester.Extractor.State.EmitterStates.First().Id,
                EventId = "123456789",
                EventType = new NodeId("test", 1),
                Message = null,
                MetaData = new Dictionary<string, object>(),
                SourceNode = tester.Extractor.State.AllActiveIds.First(),
                ReceivedTime = DateTime.Now,
                Time = DateTime.Now
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

        }
        [Fact]
        [Trait("Target", "OldBuffer")]
        [Trait("Test", "datapointconversion")]
        public void TestDataPointConversion()
        {
            var dp = new BufferedDataPoint(DateTime.Now, "testid", 123.123);
            var dp2 = new BufferedDataPoint(DateTime.Now, "testid2", "testvalue");

            var dpc = dp.ToStorableBytes();
            var dp2c = dp2.ToStorableBytes();

            var dpconv = BufferedDataPoint.FromStorableBytes(dpc, sizeof(ushort));
            var dp2conv = BufferedDataPoint.FromStorableBytes(dp2c, sizeof(ushort));

            Assert.Equal(dpc.Length, BitConverter.ToUInt16(dpc) + sizeof(ushort));
            Assert.Equal(dp.Timestamp, dpconv.Item1.Timestamp);
            Assert.Equal(dp.DoubleValue, dpconv.Item1.DoubleValue);
            Assert.Equal(dp.Id, dpconv.Item1.Id);
            Assert.Equal(dp.IsString, dpconv.Item1.IsString);

            Assert.Equal(dp2c.Length, BitConverter.ToUInt16(dp2c) + sizeof(ushort));
            Assert.Equal(dp2.Timestamp, dp2conv.Item1.Timestamp);
            Assert.Equal(dp2.DoubleValue, dp2conv.Item1.DoubleValue);
            Assert.Equal(dp2.Id, dp2conv.Item1.Id);
            Assert.Equal(dp2.IsString, dp2conv.Item1.IsString);
        }

        [Fact]
        [Trait("Target", "OldBuffer")]
        [Trait("Test", "datapointconversionwrite")]
        public void TestDataPointConversionWrite()
        {
            var dps = new List<BufferedDataPoint>();
            for (int i = 0; i < 10000; i++)
            {
                dps.Add(new BufferedDataPoint(DateTime.Now, "testid", i));
                dps.Add(new BufferedDataPoint(DateTime.Now, "testid", "test " + i));
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
                LogLevel = "information",
                QuitAfterMap = true
            });
            await tester.ClearPersistentData();

            tester.StartExtractor();

            await tester.TerminateRunTask();

            var evts = new List<BufferedEvent>();

            for (int i = 0; i < 10000; i++)
            {
                evts.Add(new BufferedEvent
                {
                    EmittingNode = tester.Extractor.State.EmitterStates.First().Id,
                    EventId = "id " + i,
                    EventType = new NodeId("test", 1),
                    Message = "msg " + i,
                    MetaData = new Dictionary<string, object>
                    {
                        ["dt1"] = "data1",
                        ["dt2"] = "data2"
                    },
                    SourceNode = tester.Extractor.State.AllActiveIds.First(),
                    ReceivedTime = DateTime.Now,
                    Time = DateTime.Now
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

            tester.Config.Extraction.AllowStringVariables = true;

            tester.StartExtractor();

            await tester.Extractor.WaitForNextPush();

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            tester.Handler.AllowPush = false;
            tester.Handler.AllowConnectionTest = false;

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.Any,
                20, "Failurebuffer must receive some data");

            tester.Handler.AllowPush = true;
            tester.Handler.AllowConnectionTest = true;

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.Any,
                20, "FailureBuffer should be emptied");

            await tester.WaitForCondition(() => tester.Extractor.State.NodeStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            await tester.TerminateRunTask();

            tester.TestContinuity("gp.efg:i=10");
            tester.TestConstantRate(1000, "gp.efg:i=9");

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

            tester.Config.Extraction.AllowStringVariables = true;

            tester.StartExtractor();

            await tester.Extractor.WaitForNextPush();

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            // In theory it will work as expected if only pushes to events fail, so long as the connection test also fails,
            // but there is no real reason to test this on an unrealistic scenario.
            // In this case, history will still restart in its entirity once connection is re-established.
            // In theory we could have separated the two, but it is an edge case. Perhaps in the future.
            // The behavior if only the push fails but not the connection test is less clear,
            // what we would expect is that if there are events in the given push, then we end up with double pushes as expected.
            // If not, the pushers will end up getting set to succeeding, but then the read from failurebuffer will fail,
            // so we will continue to read from there.
            // The same conditions still hold: If state storage is enabled on influx, or the queue is used, points will not be lost.
            tester.Handler.AllowPush = false;
            tester.Handler.AllowEvents = false;
            tester.Handler.AllowConnectionTest = false;

            await tester.WaitForCondition(() => tester.Extractor.FailureBuffer.AnyEvents,
                20, "Failurebuffer must receive some events");

            tester.Handler.AllowPush = true;
            tester.Handler.AllowEvents = true;
            tester.Handler.AllowConnectionTest = true;

            await tester.WaitForCondition(() => !tester.Extractor.FailureBuffer.AnyEvents,
                20, "FailureBuffer should be emptied");

            await tester.WaitForCondition(() => tester.Extractor.State.EmitterStates.All(state => state.IsStreaming), 20);

            await tester.Extractor.WaitForNextPush();

            await tester.TerminateRunTask();

            var events = tester.Handler.events.Values.ToList();
            Assert.True(events.Any());

            Assert.Contains(events, ev => ev.description.StartsWith("prop ", StringComparison.InvariantCulture));
            Assert.Contains(events, ev => ev.description == "prop 0");
            Assert.Contains(events, ev => ev.description == "basicPass 0");
            Assert.Contains(events, ev => ev.description == "basicPassSource 0");
            Assert.Contains(events, ev => ev.description == "basicVarSource 0");
            Assert.Contains(events, ev => ev.description == "mappedType 0");

            var countregex = new Regex("\\d+$");
            var propNumbers = events
                .Where(evt => evt.description.StartsWith("prop ", StringComparison.InvariantCulture))
                .Select(evt => Convert.ToInt32(countregex.Match(evt.description).Value, CultureInfo.InvariantCulture))
                .Distinct()
                .ToList();
            ExtractorTester.TestContinuity(propNumbers);

            var nonHistNumbers = events
                .Where(evt => evt.description.StartsWith("propOther ", StringComparison.InvariantCulture))
                .Select(evt => Convert.ToInt32(countregex.Match(evt.description).Value, CultureInfo.InvariantCulture))
                .Distinct()
                .ToList();
            ExtractorTester.TestContinuity(nonHistNumbers);

            Assert.True(CommonTestUtils.VerifySuccessMetrics());
            Assert.NotEqual(0, (int)CommonTestUtils.GetMetricValue("opcua_event_push_failures"));
        }
    }
}

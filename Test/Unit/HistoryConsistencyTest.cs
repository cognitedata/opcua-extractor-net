using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class HistoryConsistencyTestFixture : BaseExtractorTestFixture
    {
        public DateTime HistoryStart { get; }
        public HistoryConsistencyTestFixture() : base()
        {
            HistoryStart = DateTime.UtcNow.AddSeconds(-20);
        }

        public override async Task InitializeAsync()
        {
            await base.InitializeAsync();

            Server.Server.PopulateHistory(Server.Ids.Custom.Array, 1000, HistoryStart, "custom", 10, i => new int[] { i, i, i, i });
            Server.Server.PopulateHistory(Server.Ids.Custom.MysteryVar, 1000, HistoryStart.AddSeconds(2), "int", 1);
            Server.Server.PopulateHistory(Server.Ids.Custom.StringyVar, 1000, HistoryStart.AddSeconds(4), "string", 5);

            Server.Server.PopulateEventHistory<PropertyEvent>(Ids.Event.PropType, ObjectIds.Server, Ids.Event.Obj1, "prop", 100,
                HistoryStart, 100, (evt, idx) =>
                {
                    var revt = evt as PropertyEvent;
                    revt.PropertyString.Value = "str " + idx;
                    revt.PropertyNum.Value = idx;
                    revt.SubType.Value = "sub-type";
                });
            Server.Server.PopulateEventHistory<PropertyEvent>(Ids.Event.PropType, Ids.Event.Obj1, Ids.Event.Obj1, "prop-e2", 100,
                HistoryStart.AddSeconds(2), 100, (evt, idx) =>
                {
                    var revt = evt as PropertyEvent;
                    revt.PropertyString.Value = "str o2 " + idx;
                    revt.PropertyNum.Value = idx;
                    revt.SubType.Value = "sub-type";
                });
            // Test types
            Server.Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj1, "basic-pass", 100,
                HistoryStart.AddSeconds(4), 100);
            Server.Server.PopulateEventHistory<BasicEvent2>(Ids.Event.BasicType2, ObjectIds.Server, Ids.Event.Obj1, "basic-block", 100,
                HistoryStart.AddSeconds(6), 100);
            Server.Server.PopulateEventHistory<CustomEvent>(Ids.Event.CustomType, ObjectIds.Server, Ids.Event.Obj1, "mapped", 100,
                HistoryStart.AddSeconds(8), 100, (evt, idx) =>
                {
                    var revt = evt as CustomEvent;
                    revt.TypeProp.Value = "CustomType";
                });

            // Test sources
            Server.Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Obj2, "basic-pass-2", 100,
                HistoryStart.AddSeconds(10), 100);
            Server.Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, Ids.Event.Obj1, Ids.Event.Obj2, "basic-pass-3", 100,
                HistoryStart.AddSeconds(12), 50);
            Server.Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.Var1, "basic-varsource", 100,
                HistoryStart.AddSeconds(14), 40);
            Server.Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, null, "basic-nosource", 100,
                HistoryStart.AddSeconds(16), 20);
            Server.Server.PopulateEventHistory<BasicEvent1>(Ids.Event.BasicType1, ObjectIds.Server, Ids.Event.ObjExclude, "basic-excludeobj", 100,
                HistoryStart.AddSeconds(18), 10);
        }
    }
    public class HistoryConsistencyTest : IClassFixture<HistoryConsistencyTestFixture>
    {
        private readonly HistoryConsistencyTestFixture tester;
        public HistoryConsistencyTest(ITestOutputHelper output, HistoryConsistencyTestFixture tester)
        {
            ArgumentNullException.ThrowIfNull(tester);
            this.tester = tester;
            tester.ResetConfig();
            tester.Init(output);
            tester.Client.TypeManager.Reset();
        }

        [Theory(Timeout = 20000)]
        [InlineData(4, 1000, 0, false, 0, false)]
        [InlineData(4, 1000, 0, false, 0, true)]
        [InlineData(4, 1000, 2, false, 0, true)]
        [InlineData(4, 100, 0, true, 0, false)]
        [InlineData(1, 100, 2, true, 0, true)]
        [InlineData(4, 50, 2, false, 1, true)]
        [InlineData(2, 50, 2, true, 0, true)]
        [InlineData(100, 100, 100, false, 2, false)]
        [InlineData(100, 100, 100, true, 2, false)]
        public async Task TestHistoryConsistencyData(
            int nodesChunk, int resultChunk, int nodeParallelism, bool ignoreCps, int maxReadLength, bool backfill)
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Enabled = true,
                Backfill = backfill,
                Data = true,
                IgnoreContinuationPoints = ignoreCps,
                MaxReadLength = maxReadLength.ToString(),
                DataNodesChunk = nodesChunk,
                DataChunk = resultChunk
            };

            var logger = tester.Provider.GetRequiredService<ILogger<HistoryReader>>();

            cfg.Throttling.MaxNodeParallelism = nodeParallelism;
            cfg.StartTime = tester.HistoryStart.AddSeconds(-10).ToUnixTimeMilliseconds().ToString();

            tester.Config.History = cfg;

            using var reader = new HistoryReader(logger, tester.Client, extractor, extractor.TypeManager, tester.Config, tester.Source.Token);

            var dt = new UADataType(DataTypeIds.Double);
            var dt2 = new UADataType(DataTypeIds.String);

            var states = new[] { tester.Server.Ids.Custom.MysteryVar, tester.Server.Ids.Custom.Array,
                tester.Server.Ids.Custom.StringyVar }
                .Select((id, idx) => new VariableExtractionState(
                    extractor,
                    CommonTestUtils.GetSimpleVariable("state",
                        idx == 2 ? dt2 : dt,
                        idx == 1 ? 4 : 0,
                        id),
                    true, true, true))
                .ToList();

            var start = backfill ? tester.HistoryStart.AddSeconds(5) : tester.HistoryStart;


            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetNodeState(state);
            }

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            await CommonTestUtils.RunHistory(reader, states, HistoryReadType.FrontfillData);

            if (backfill)
            {
                await CommonTestUtils.RunHistory(reader, states, HistoryReadType.BackfillData);
            }

            var distinct = queue.ToList().DistinctBy(dp => (dp.Id, dp.Timestamp)).GroupBy(dp => dp.Id)
                .ToDictionary(group => group.Key, group => group.ToArray());

            Assert.Equal(6, distinct.Count);
            foreach (var kvp in distinct)
            {
                Assert.Equal(1000, kvp.Value.Length);
            }
        }


        [Theory(Timeout = 20000)]
        [InlineData(4, 1000, 0, false, 0, false)]
        [InlineData(4, 1000, 0, false, 0, true)]
        [InlineData(4, 1000, 2, false, 0, true)]
        [InlineData(4, 100, 0, true, 0, false)]
        [InlineData(1, 100, 2, true, 0, true)]
        [InlineData(4, 50, 2, false, 1, true)]
        [InlineData(2, 50, 2, true, 0, true)]
        [InlineData(100, 100, 100, false, 2, false)]
        [InlineData(100, 100, 100, true, 2, false)]
        public async Task TestHistoryConsistencyEvents(
            int nodesChunk, int resultChunk, int nodeParallelism, bool ignoreCps, int maxReadLength, bool backfill)
        {
            using var extractor = tester.BuildExtractor();

            var cfg = new HistoryConfig
            {
                Enabled = true,
                Backfill = backfill,
                Data = true,
                IgnoreContinuationPoints = ignoreCps,
                MaxReadLength = maxReadLength.ToString(),
                DataNodesChunk = nodesChunk,
                DataChunk = resultChunk
            };

            var logger = tester.Provider.GetRequiredService<ILogger<HistoryReader>>();

            tester.Config.Events.Enabled = true;
            tester.Config.Events.History = true;

            cfg.Throttling.MaxNodeParallelism = nodeParallelism;
            cfg.StartTime = tester.HistoryStart.AddSeconds(-10).ToUnixTimeMilliseconds().ToString();

            tester.Config.History = cfg;

            using var reader = new HistoryReader(logger, tester.Client, extractor, extractor.TypeManager, tester.Config, tester.Source.Token);

            var states = new[]
            {
                new EventExtractionState(tester.Client, ObjectIds.Server, true, true, true),
                new EventExtractionState(tester.Client, tester.Server.Ids.Event.Obj1, true, true, true)
            };

            var start = backfill ? tester.HistoryStart.AddSeconds(5) : tester.HistoryStart;

            foreach (var state in states)
            {
                state.InitExtractedRange(start, start);
                state.FinalizeRangeInit();
                extractor.State.SetEmitterState(state);
            }

            var uaSource = new UANodeSource(tester.Log, extractor, tester.Client, tester.Client.TypeManager);

            var queue = (Queue<UAEvent>)extractor.Streamer.GetType()
                .GetField("eventQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            await extractor.TypeManager.Initialize(uaSource, tester.Source.Token);
            await extractor.TypeManager.LoadTypeData(uaSource, tester.Source.Token);
            extractor.TypeManager.BuildTypeInfo();
            var fields = extractor.TypeManager.EventFields;
            extractor.State.PopulateActiveEventTypes(fields);

            await CommonTestUtils.RunHistory(reader, states, HistoryReadType.FrontfillEvents);

            if (backfill)
            {
                await CommonTestUtils.RunHistory(reader, states, HistoryReadType.BackfillEvents);
            }

            var distinct = queue.ToList().DistinctBy(evt => evt.Message).GroupBy(evt => evt.EmittingNode)
                .ToDictionary(group => group.Key, group => group.ToArray());

            Assert.Equal(2, distinct.Count);
            Assert.Equal(200, distinct[tester.Ids.Event.Obj1].Length);
            Assert.Equal(800, distinct[ObjectIds.Server].Length);
        }
    }
}

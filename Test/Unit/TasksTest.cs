using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Tasks;
using Cognite.OpcUa.Types;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public class TaskTestFixture : BaseExtractorTestFixture
    {
        public TaskTestFixture() : base() { }
    }

    sealed class DummyErrorReporter : BaseErrorReporter, IIntegrationSink
    {
        public List<ExtractorError> Errors { get; } = new List<ExtractorError>();
        public List<(string, DateTime)> TaskStarts { get; } = new List<(string, DateTime)>();
        public List<(string, DateTime)> TaskEnds { get; } = new List<(string, DateTime)>();
        public Task Flush(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public override ExtractorError NewError(
            ErrorLevel level,
            string description,
            string details = null,
            DateTime? now = null
        )
        {
            return new ExtractorError(level, description, this, details, null, now ?? DateTime.UtcNow);
        }

        public void ReportError(ExtractorError error)
        {
            Errors.Add(error);
        }

        public void ReportTaskEnd(string taskName, TaskUpdatePayload update = null, DateTime? timestamp = null)
        {
            TaskEnds.Add((taskName, timestamp ?? DateTime.UtcNow));
        }

        public void ReportTaskStart(string taskName, TaskUpdatePayload update = null, DateTime? timestamp = null)
        {
            TaskStarts.Add((taskName, timestamp ?? DateTime.UtcNow));
        }

        public Task RunPeriodicCheckIn(CancellationToken token, StartupRequest startupPayload, TimeSpan? interval = null)
        {
            return Task.Delay(Timeout.Infinite, token);
        }
    }

    public class TasksTest : IClassFixture<TaskTestFixture>
    {
        private readonly TaskTestFixture tester;

        public TasksTest(ITestOutputHelper output, TaskTestFixture fixture)
        {
            tester = fixture;
            tester.ResetConfig();
            tester.Init(output);

            tester.ResetCustomServerValues();
            tester.WipeBaseHistory();
            tester.WipeCustomHistory();
            tester.Client.TypeManager.Reset();
        }

        [Fact]
        public async Task TestBasicBrowseTask()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            await using var extractor = tester.BuildExtractor(pusher);

            var browseTask = new BrowseTask(extractor, tester.Client, tester.Config, tester.Provider);
            browseTask.AddNodesToBrowse(new[] { tester.Ids.Base.Root }, isFull: true);
            var reporter = new DummyErrorReporter();
            var result = await browseTask.Run(reporter, CancellationToken.None);
            Assert.Equal("Reading nodes from the OPC-UA server resulted in 1"
            + " source objects, 4 source variables, 1 destination objects, 4 destination variables"
            + ", 0 references", result.Message);

            Assert.Single(pusher.PushedNodes);
            Assert.Equal(4, pusher.PushedVariables.Count);
        }

        [Fact]
        public async Task TestBrowseTaskFromNodeSetSource()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            await using var extractor = tester.BuildExtractor(pusher);
            tester.Config.Source.NodeSetSource = new NodeSetSourceConfig
            {
                NodeSets = new[]
                {
                    new NodeSetConfig
                    {
                        Url = new Uri("https://files.opcfoundation.org/schemas/UA/1.04/Opc.Ua.NodeSet2.xml")
                    },
                    new NodeSetConfig
                    {
                        FileName = "TestServer.NodeSet2.xml"
                    }
                },
                Instance = true,
                Types = true,
            };


            var browseTask = new BrowseTask(extractor, tester.Client, tester.Config, tester.Provider);

            var reporter = new DummyErrorReporter();
            browseTask.AddNodesToBrowse(new[] { new NodeId(1, 2) }, isFull: true);
            var result = await browseTask.Run(reporter, CancellationToken.None);
            Assert.Equal("Reading nodes from local NodeSet2 XML files resulted in 3"
            + " source objects, 0 source variables, 3 destination objects, 0 destination variables"
            + ", 0 references", result.Message);

            Assert.Equal(3, pusher.PushedNodes.Count);
            Assert.Empty(pusher.PushedVariables);
        }

        private void NodeToRaw(UAExtractor extractor, BaseUANode node, CDFMockHandler handler, ConverterType type, bool ts)
        {
            var options = new JsonSerializerOptions();
            var converter = tester.Client.TypeConverter;
            converter.AddConverters(options, type);

            var id = node.GetUniqueId(extractor.Context);

            var json = JsonSerializer.Serialize(node, options);

            var val = JsonSerializer.Deserialize<JsonElement>(json, options);
            tester.Log.LogInformation("{Node}", val);
            if (ts)
            {
                handler.TimeseriesRaw[id] = val;
            }
            else
            {
                handler.AssetsRaw[id] = val;
            }
        }

        [Fact]
        public async Task TestBrowseTaskFromCDFNodeSource()
        {
            var (handler, pusher) = tester.GetCDFPusher();
            await using var extractor = tester.BuildExtractor(pusher);
            tester.Config.Cognite.RawNodeBuffer = new CDFNodeSourceConfig
            {
                AssetsTable = "assets",
                TimeseriesTable = "timeseries",
                Database = "metadata",
                BrowseOnEmpty = true,
                Enable = true,
            };
            tester.Config.Events.Enabled = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            var browseTask = new BrowseTask(extractor, tester.Client, tester.Config, tester.Provider);

            // Add a couple of nodes
            var variable = new UAVariable(new NodeId("test2", 0), "test2", null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            variable.FullAttributes.ValueRank = -1;
            NodeToRaw(extractor, variable, handler, ConverterType.Variable, true);
            // Normal string
            variable = new UAVariable(new NodeId("test3", 0), "test3", null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.String);
            variable.FullAttributes.ValueRank = -1;
            NodeToRaw(extractor, variable, handler, ConverterType.Variable, true);
            // Event emitter
            var node = new UAObject(new NodeId("test5", 0), "test5", null, null, NodeId.Null, null);
            node.FullAttributes.EventNotifier = EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
            NodeToRaw(extractor, node, handler, ConverterType.Node, false);

            var reporter = new DummyErrorReporter();
            browseTask.AddNodesToBrowse(new[] { ObjectIds.ObjectsFolder }, isFull: true);
            var result = await browseTask.Run(reporter, CancellationToken.None);
            Assert.Equal("Reading nodes from CDF raw resulted in 1"
            + " source objects, 2 source variables, 1 destination objects, 2 destination variables"
            + ", 0 references", result.Message);
        }

        [Fact]
        public async Task TestRebrowseDifferentSubset()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            await using var extractor = tester.BuildExtractor(pusher);

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 10;

            var browseTask = new BrowseTask(extractor, tester.Client, tester.Config, tester.Provider);
            browseTask.AddNodesToBrowse(new[] { tester.Ids.Base.Root }, isFull: true);
            var reporter = new DummyErrorReporter();
            var result = await browseTask.Run(reporter, CancellationToken.None);
            Assert.Equal("Reading nodes from the OPC-UA server resulted in 1"
            + " source objects, 5 source variables, 1 destination objects, 5 destination variables"
            + ", 0 references", result.Message);

            Assert.Single(pusher.PushedNodes);
            Assert.Equal(5, pusher.PushedVariables.Count);

            browseTask.AddNodesToBrowse(new[] { tester.Ids.Custom.Root }, isFull: false);
            result = await browseTask.Run(reporter, CancellationToken.None);
            Assert.Equal("Reading nodes from the OPC-UA server resulted in 3"
            + " source objects, 9 source variables, 6 destination objects, 16 destination variables"
            + ", 0 references", result.Message);
            Assert.Equal(7, pusher.PushedNodes.Count);
            Assert.Equal(21, pusher.PushedVariables.Count);
        }

        [Fact]
        public async Task TestBrowseWithReferences()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            await using var extractor = tester.BuildExtractor(pusher);

            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 10;

            var browseTask = new BrowseTask(extractor, tester.Client, tester.Config, tester.Provider);
            browseTask.AddNodesToBrowse(new[] { tester.Ids.Custom.Root }, isFull: true);
            var reporter = new DummyErrorReporter();
            var result = await browseTask.Run(reporter, CancellationToken.None);
            Assert.Equal("Reading nodes from the OPC-UA server resulted in 3"
            + " source objects, 9 source variables, 6 destination objects, 16 destination variables"
            + ", 21 references", result.Message);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(16, pusher.PushedVariables.Count);
            Assert.Equal(21, pusher.PushedReferences.Count);
        }
    }
}

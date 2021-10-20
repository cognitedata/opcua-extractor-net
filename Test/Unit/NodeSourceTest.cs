using Cognite.OpcUa;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class NodeSourceTestFixture : BaseExtractorTestFixture
    {
        public NodeSourceTestFixture() : base() { }
    }
    public class NodeSourceTest : MakeConsoleWork, IClassFixture<NodeSourceTestFixture>
    {
        private readonly NodeSourceTestFixture tester;
        public NodeSourceTest(ITestOutputHelper output, NodeSourceTestFixture tester) : base(output)
        {
            this.tester = tester;
            tester.ResetConfig();
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
                }
            };
        }

        [Fact]
        public async Task TestNodeSetSource()
        {
            tester.Config.Extraction.Relationships.Enabled = true;
            using var extractor = tester.BuildExtractor();
            tester.Config.Extraction.Relationships.Enabled = false;
            var log = tester.Provider.GetRequiredService<ILogger<NodeSetSource>>();
            var source = new NodeSetSource(log, tester.Config, extractor, tester.Client);
            
            // Base, nothing enabled
            source.BuildNodes(new[] { tester.Ids.Custom.Root });
            var result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(3, result.SourceVariables.Count());
            Assert.Equal(3, result.DestinationVariables.Count());
            Assert.Equal(3, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            // Enable arrays
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(5, result.SourceVariables.Count());
            Assert.Equal(11, result.DestinationVariables.Count());
            Assert.Equal(5, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            // Enable strings
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(9, result.SourceVariables.Count());
            Assert.Equal(16, result.DestinationVariables.Count());
            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            // Enable ignore
            tester.Config.Extraction.DataTypes.IgnoreDataTypes = new[]
            {
                CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.IgnoreType, tester.Client)
            };
            extractor.DataTypeManager.Configure();
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(8, result.SourceVariables.Count());
            Assert.Equal(15, result.DestinationVariables.Count());
            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            // Enable non-hierarchical relations
            tester.Config.Extraction.Relationships.Enabled = true;
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(8, result.DestinationReferences.Count());
            Assert.Equal(4, result.DestinationReferences.Count(rel => rel.IsForward));
            Assert.All(result.DestinationReferences, rel =>
            {
                Assert.NotNull(rel.Source);
                Assert.NotNull(rel.Target);
                Assert.False(rel.Source.Id.IsNullNodeId);
                Assert.False(rel.Target.Id.IsNullNodeId);
                Assert.NotNull(rel.Type);
                Assert.NotNull(rel.Type.Id);
                Assert.True(rel.Type.HasName);
                Assert.Contains(result.DestinationReferences, orel => orel.Source.Id == rel.Target.Id
                    && orel.Target.Id == rel.Source.Id && orel.IsForward == !rel.IsForward);
            });


            // Enable forward hierarchical relations
            tester.Config.Extraction.Relationships.Hierarchical = true;
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(18, result.DestinationReferences.Count());
            Assert.Equal(14, result.DestinationReferences.Count(rel => rel.IsForward));
            Assert.All(result.DestinationReferences, rel =>
            {
                Assert.NotNull(rel.Source);
                Assert.NotNull(rel.Target);
                Assert.False(rel.Source.Id.IsNullNodeId);
                Assert.False(rel.Target.Id.IsNullNodeId);
                Assert.NotNull(rel.Type);
                Assert.NotNull(rel.Type.Id);
                Assert.True(rel.Type.HasName);
            });

            // Enable inverse hierarchical relations
            tester.Config.Extraction.Relationships.InverseHierarchical = true;
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(28, result.DestinationReferences.Count());
            Assert.Equal(14, result.DestinationReferences.Count(rel => rel.IsForward));
            Assert.All(result.DestinationReferences, rel =>
            {
                Assert.NotNull(rel.Source);
                Assert.NotNull(rel.Target);
                Assert.False(rel.Source.Id.IsNullNodeId);
                Assert.False(rel.Target.Id.IsNullNodeId);
                Assert.NotNull(rel.Type);
                Assert.NotNull(rel.Type.Id);
                Assert.True(rel.Type.HasName);
                Assert.Contains(result.DestinationReferences, orel => orel.Source.Id == rel.Target.Id
                    && orel.Target.Id == rel.Source.Id && orel.IsForward == !rel.IsForward);
            });
        }


        [Fact]
        public async Task TestNodeSetSourceEvents()
        {
            using var extractor = tester.BuildExtractor();
            var log = tester.Provider.GetRequiredService<ILogger<NodeSetSource>>();
            var source = new NodeSetSource(log, tester.Config, extractor, tester.Client);

            source.BuildNodes(new[] { ObjectIds.ObjectsFolder });

            tester.Config.Events.AllEvents = true;
            tester.Config.Events.Enabled = true;
            var fields = await source.GetEventIdFields(tester.Source.Token);

            Assert.Equal(96, fields.Count);

            // Check that all parent properties are present in a deep event
            Assert.Equal(16, fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields.Count);
            Assert.Contains(new EventField(new QualifiedName("EventType")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);
            Assert.Contains(new EventField(new QualifiedName("ActionTimeStamp")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);
            Assert.Contains(new EventField(new QualifiedName("ParameterDataTypeId")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);
            Assert.Contains(new EventField(new QualifiedName("UpdatedNode")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);
            Assert.Contains(new EventField(new QualifiedName("OldValues")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);

            // Check that nodes in the middle only have higher level properties
            Assert.Equal(13, fields[ObjectTypeIds.AuditHistoryUpdateEventType].CollectedFields.Count);
            Assert.DoesNotContain(new EventField(new QualifiedName("OldValues")),
                fields[ObjectTypeIds.AuditHistoryUpdateEventType].CollectedFields);

            var result = await source.ParseResults(tester.Source.Token);
            var nodeDict = result.DestinationObjects.ToDictionary(obj => obj.Id);

            Assert.True(nodeDict.TryGetValue(tester.Ids.Event.Root, out var root));
            Assert.Equal(0, root.EventNotifier);

            Assert.True(nodeDict.TryGetValue(tester.Ids.Event.Obj1, out var obj));
            Assert.Equal(5, obj.EventNotifier);

            Assert.True(nodeDict.TryGetValue(tester.Ids.Event.Obj2, out obj));
            Assert.Equal(1, obj.EventNotifier);

            Assert.NotNull(extractor.State.GetEmitterState(tester.Ids.Event.Obj1));
            Assert.NotNull(extractor.State.GetEmitterState(tester.Ids.Event.Obj2));
            // Assert.True(false);
        }
        [Fact]
        public async Task TestEstimateArraySize()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            tester.Config.History.Enabled = false;
            var extConfig = tester.Config.Extraction;
            extConfig.RootNode = tester.Ids.Wrong.Root.ToProtoNodeId(tester.Client);
            extConfig.DataTypes.MaxArraySize = 6;
            extConfig.DataTypes.EstimateArraySizes = true;

            tester.Server.UpdateNode(tester.Ids.Wrong.RankImpreciseNoDim, new double[] { 1.0, 2.0, 3.0, 4.0 });

            await extractor.RunExtractor(true);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(5, pusher.PushedNodes.Values.Count(node => node is UAVariable variable && variable.IsArray));
            Assert.Equal(19, pusher.PushedVariables.Count);
        }
        [Fact]
        public async Task TestNodeSetEstimateArraySize()
        {
            using var extractor = tester.BuildExtractor();
            var log = tester.Provider.GetRequiredService<ILogger<NodeSetSource>>();
            var source = new NodeSetSource(log, tester.Config, extractor, tester.Client);

            source.BuildNodes(new[] { tester.Ids.Wrong.Root });
            var extConfig = tester.Config.Extraction;
            extConfig.DataTypes.MaxArraySize = 6;
            extConfig.DataTypes.EstimateArraySizes = true;

            tester.Server.UpdateNode(tester.Ids.Wrong.RankImpreciseNoDim, new double[] { 1.0, 2.0, 3.0, 4.0 });

            var result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(5, result.DestinationObjects.Count(node => node is UAVariable variable && variable.IsArray));
            Assert.Equal(19, result.DestinationVariables.Count());
            Assert.Single(result.SourceObjects);
            Assert.Equal(5, result.SourceVariables.Count());
        }
    }
}

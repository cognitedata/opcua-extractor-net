using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Linq;
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
    public class NodeSourceTest : IClassFixture<NodeSourceTestFixture>
    {
        private readonly NodeSourceTestFixture tester;
        public NodeSourceTest(ITestOutputHelper output, NodeSourceTestFixture tester)
        {
            this.tester = tester;
            tester.ResetConfig();
            tester.Init(output);
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
            tester.Client.TypeManager.Reset();
        }

        [Fact]
        public async Task TestNodeSetSource()
        {
            tester.Config.Extraction.Relationships.Enabled = true;
            using var extractor = tester.BuildExtractor();
            tester.Config.Extraction.Relationships.Enabled = false;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;

            var log = tester.Provider.GetRequiredService<ILogger<NodeSetSource>>();
            var source = new NodeSetSource(log, tester.Config, extractor, tester.Client, extractor.TypeManager);

            // Base, nothing enabled
            source.BuildNodes(new[] { tester.Ids.Custom.Root }, true);
            var result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(3, result.SourceVariables.Count());
            Assert.Equal(3, result.DestinationVariables.Count());
            Assert.Equal(3, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);
            Assert.True(result.CanBeUsedForDeletes);

            // Enable arrays
            extractor.State.Clear();
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            source.BuildNodes(new[] { tester.Ids.Custom.Root }, false);
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(5, result.SourceVariables.Count());
            Assert.Equal(11, result.DestinationVariables.Count());
            Assert.Equal(5, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);
            Assert.False(result.CanBeUsedForDeletes);

            // Enable strings
            extractor.State.Clear();
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            source.BuildNodes(new[] { tester.Ids.Custom.Root }, true);
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(9, result.SourceVariables.Count());
            Assert.Equal(16, result.DestinationVariables.Count());
            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            // Enable ignore
            extractor.State.Clear();
            tester.Config.Extraction.DataTypes.IgnoreDataTypes = new[]
            {
                CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.IgnoreType, tester.Client)
            };
            extractor.TypeManager.BuildTypeInfo();
            source.BuildNodes(new[] { tester.Ids.Custom.Root }, true);
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(8, result.SourceVariables.Count());
            Assert.Equal(15, result.DestinationVariables.Count());
            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            // Map variable children to objects
            extractor.State.Clear();
            tester.Config.Extraction.MapVariableChildren = true;
            source.BuildNodes(new[] { tester.Ids.Custom.Root }, true);
            result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(8, result.SourceVariables.Count());
            Assert.Equal(15, result.DestinationVariables.Count());
            Assert.Equal(9, result.DestinationObjects.Count());
            Assert.Equal(5, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            tester.Config.Extraction.MapVariableChildren = false;

            // Enable non-hierarchical relations
            extractor.State.Clear();
            tester.Config.Extraction.Relationships.Enabled = true;
            source.BuildNodes(new[] { tester.Ids.Custom.Root }, true);
            result = await source.ParseResults(tester.Source.Token);
            foreach (var rf in result.DestinationReferences)
            {
                log.LogDebug("Ref: {Source} {Target} {Type} {IsForward}", rf.Source.Id, rf.Target.Id, rf.Type.Id, rf.IsForward);
            }
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
                Assert.Contains(result.DestinationReferences, orel => orel.Source.Id == rel.Target.Id
                    && orel.Target.Id == rel.Source.Id && orel.IsForward == !rel.IsForward);
            });


            // Enable forward hierarchical relations
            extractor.State.Clear();
            tester.Config.Extraction.Relationships.Hierarchical = true;
            source.BuildNodes(new[] { tester.Ids.Custom.Root }, true);
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
            });

            // Enable inverse hierarchical relations
            extractor.State.Clear();
            tester.Config.Extraction.Relationships.InverseHierarchical = true;
            source.BuildNodes(new[] { tester.Ids.Custom.Root }, true);
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
                Assert.Contains(result.DestinationReferences, orel => orel.Source.Id == rel.Target.Id
                    && orel.Target.Id == rel.Source.Id && orel.IsForward == !rel.IsForward);
            });
            Assert.True(result.CanBeUsedForDeletes);
        }

        [Fact]
        public async Task TestNodeSetWithoutServer()
        {
            tester.Config.Extraction.Relationships.Enabled = true;
            using var extractor = tester.BuildExtractor();
            tester.Config.Extraction.Relationships.Enabled = false;
            tester.Config.Source.EndpointUrl = null;

            var log = tester.Provider.GetRequiredService<ILogger<NodeSetSource>>();
            var source = new NodeSetSource(log, tester.Config, extractor, tester.Client, extractor.TypeManager);

            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.Extraction.NodeTypes.AsNodes = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.MapVariableChildren = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.Relationships.InverseHierarchical = true;
            tester.Config.Extraction.Relationships.Enabled = true;

            source.BuildNodes(new[] { tester.Ids.Custom.Root, ObjectIds.TypesFolder }, true);
            var result = await source.ParseResults(tester.Source.Token);

            Assert.Equal(275, result.SourceVariables.Count());
            Assert.Equal(282, result.DestinationVariables.Count());
            Assert.Equal(447, result.DestinationObjects.Count());
            Assert.Equal(441, result.SourceObjects.Count());
            Assert.Equal(2108, result.DestinationReferences.Count());
            Assert.Equal(1054, result.DestinationReferences.Count(rel => rel.IsForward));
            Assert.All(result.DestinationReferences, rel =>
            {
                Assert.NotNull(rel.Source);
                Assert.NotNull(rel.Target);
                Assert.False(rel.Source.Id.IsNullNodeId);
                Assert.False(rel.Target.Id.IsNullNodeId);
                Assert.NotNull(rel.Type);
                Assert.NotNull(rel.Type.Id);
                Assert.Contains(result.DestinationReferences, orel => orel.Source.Id == rel.Target.Id
                    && orel.Target.Id == rel.Source.Id && orel.IsForward == !rel.IsForward);
            });
            Assert.True(result.CanBeUsedForDeletes);
        }


        [Fact]
        public async Task TestNodeSetSourceEvents()
        {
            using var extractor = tester.BuildExtractor();
            var log = tester.Provider.GetRequiredService<ILogger<NodeSetSource>>();
            var source = new NodeSetSource(log, tester.Config, extractor, tester.Client, extractor.TypeManager);

            source.BuildNodes(new[] { ObjectIds.ObjectsFolder }, true);

            tester.Config.Events.AllEvents = true;
            tester.Config.Events.Enabled = true;
            await extractor.TypeManager.LoadTypeData(tester.Source.Token);
            extractor.TypeManager.BuildTypeInfo();
            var fields = extractor.TypeManager.EventFields;

            Assert.Equal(96, fields.Count);

            // Check that all parent properties are present in a deep event
            Assert.Equal(16, fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields.Count());
            Assert.Contains(new RawTypeField(new QualifiedName("EventType")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("ActionTimeStamp")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("ParameterDataTypeId")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("UpdatedNode")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("OldValues")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields);

            // Check that nodes in the middle only have higher level properties
            Assert.Equal(13, fields[ObjectTypeIds.AuditHistoryUpdateEventType].CollectedFields.Count());
            Assert.DoesNotContain(new RawTypeField(new QualifiedName("OldValues")),
                fields[ObjectTypeIds.AuditHistoryUpdateEventType].CollectedFields);

            var result = await source.ParseResults(tester.Source.Token);
            var nodeDict = result.DestinationObjects.ToDictionary(obj => obj.Id);

            Assert.True(nodeDict.TryGetValue(tester.Ids.Event.Root, out var root));
            Assert.Equal(0, (root as UAObject).FullAttributes.EventNotifier);

            Assert.True(nodeDict.TryGetValue(tester.Ids.Event.Obj1, out var obj));
            Assert.Equal(5, (obj as UAObject).FullAttributes.EventNotifier);

            Assert.True(nodeDict.TryGetValue(tester.Ids.Event.Obj2, out obj));
            Assert.Equal(1, (obj as UAObject).FullAttributes.EventNotifier);

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
            var source = new NodeSetSource(log, tester.Config, extractor, tester.Client, extractor.TypeManager);

            source.BuildNodes(new[] { tester.Ids.Wrong.Root }, true);
            var extConfig = tester.Config.Extraction;
            extConfig.DataTypes.MaxArraySize = 6;
            extConfig.DataTypes.EstimateArraySizes = true;

            tester.Server.UpdateNode(tester.Ids.Wrong.RankImpreciseNoDim, new double[] { 1.0, 2.0, 3.0, 4.0 });

            var result = await source.ParseResults(tester.Source.Token);
            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(5, result.DestinationObjects.Count(node => node is UAVariable variable && variable.IsArray));
            Assert.Equal(21, result.DestinationVariables.Count());
            Assert.Single(result.SourceObjects);
            Assert.Equal(5, result.SourceVariables.Count());
        }
    }
}

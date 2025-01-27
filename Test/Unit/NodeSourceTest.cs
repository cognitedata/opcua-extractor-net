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
            tester.Config.Extraction.Relationships.Enabled = false;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;

            using var extractor = tester.BuildExtractor();

            NodeHierarchyBuilder GetBuilder(params NodeId[] nodesToRead)
            {
                var log = tester.Provider.GetRequiredService<ILogger<NodeSetNodeSource>>();
                var source = new NodeSetNodeSource(log, tester.Config, extractor, tester.Client, tester.Client.TypeManager);

                return new NodeHierarchyBuilder(
                    source, source, tester.Config, nodesToRead, tester.Client,
                    extractor, extractor.Transformations, log);
            }

            // Base, nothing enabled
            var builder = GetBuilder(tester.Ids.Custom.Root);
            var result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Equal(3, result.SourceVariables.Count());
            Assert.Equal(3, result.DestinationVariables.Count());
            Assert.Equal(3, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);
            Assert.True(result.CanBeUsedForDeletes);

            // Enable arrays
            extractor.State.Clear();
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            builder = GetBuilder(tester.Ids.Custom.Root);
            result = await builder.LoadNodeHierarchy(false, tester.Source.Token);
            Assert.Equal(5, result.SourceVariables.Count());
            Assert.Equal(11, result.DestinationVariables.Count());
            Assert.Equal(5, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);
            Assert.False(result.CanBeUsedForDeletes);

            // Enable strings
            extractor.State.Clear();
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            builder = GetBuilder(tester.Ids.Custom.Root);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Equal(9, result.SourceVariables.Count());
            Assert.Equal(16, result.DestinationVariables.Count());
            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            // Enable ignore
            extractor.State.Clear();
            extractor.TypeManager.GetDataType(tester.Server.Ids.Custom.IgnoreType)
                .ShouldIgnore = true;
            builder = GetBuilder(tester.Ids.Custom.Root);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Equal(8, result.SourceVariables.Count());
            Assert.Equal(15, result.DestinationVariables.Count());
            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(3, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            // Map variable children to objects
            extractor.State.Clear();
            tester.Config.Extraction.MapVariableChildren = true;
            builder = GetBuilder(tester.Ids.Custom.Root);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            Assert.Equal(8, result.SourceVariables.Count());
            Assert.Equal(15, result.DestinationVariables.Count());
            Assert.Equal(9, result.DestinationObjects.Count());
            Assert.Equal(5, result.SourceObjects.Count());
            Assert.Empty(result.DestinationReferences);

            tester.Config.Extraction.MapVariableChildren = false;

            // Enable non-hierarchical relations
            extractor.State.Clear();
            tester.Config.Extraction.Relationships.Enabled = true;
            builder = GetBuilder(tester.Ids.Custom.Root);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
            foreach (var rf in result.DestinationReferences)
            {
                tester.Log.LogDebug("{X}", rf);
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
            builder = GetBuilder(tester.Ids.Custom.Root);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
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
            builder = GetBuilder(tester.Ids.Custom.Root);
            result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
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

            var log = tester.Provider.GetRequiredService<ILogger<NodeSetNodeSource>>();
            var source = new NodeSetNodeSource(log, tester.Config, extractor, tester.Client, tester.Client.TypeManager);

            var builder = new NodeHierarchyBuilder(
                source, source, tester.Config, new[] { tester.Ids.Custom.Root, ObjectIds.TypesFolder },
                tester.Client, extractor, extractor.Transformations, log);


            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.Extraction.NodeTypes.AsNodes = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.MapVariableChildren = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.Relationships.InverseHierarchical = true;
            tester.Config.Extraction.Relationships.Enabled = true;

            var result = await builder.LoadNodeHierarchy(true, tester.Source.Token);

            Assert.Equal(275, result.SourceVariables.Count());
            Assert.Equal(282, result.DestinationVariables.Count());
            Assert.Equal(767, result.DestinationObjects.Count());
            Assert.Equal(761, result.SourceObjects.Count());
            Assert.Equal(3152, result.DestinationReferences.Count());
            Assert.Equal(1576, result.DestinationReferences.Count(rel => rel.IsForward));
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
            var log = tester.Provider.GetRequiredService<ILogger<NodeSetNodeSource>>();
            var source = new NodeSetNodeSource(log, tester.Config, extractor, tester.Client, tester.Client.TypeManager);

            tester.Config.Events.AllEvents = true;
            tester.Config.Events.Enabled = true;
            await extractor.TypeManager.Initialize(source, tester.Source.Token);
            await extractor.TypeManager.LoadTypeData(source, tester.Source.Token);
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

            var builder = new NodeHierarchyBuilder(
                source, source, tester.Config, new[] { tester.Ids.Event.Root, ObjectIds.TypesFolder },
                tester.Client, extractor, extractor.Transformations, log);
            var result = await builder.LoadNodeHierarchy(true, tester.Source.Token);
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
            using var extractor = tester.BuildExtractor(pusher);

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
            var log = tester.Provider.GetRequiredService<ILogger<NodeSetNodeSource>>();
            var source = new NodeSetNodeSource(log, tester.Config, extractor, tester.Client, tester.Client.TypeManager);

            var builder = new NodeHierarchyBuilder(
                source, source, tester.Config, new[] { tester.Ids.Wrong.Root },
                tester.Client, extractor, extractor.Transformations, log);

            var extConfig = tester.Config.Extraction;
            extConfig.DataTypes.MaxArraySize = 6;
            extConfig.DataTypes.EstimateArraySizes = true;

            tester.Server.UpdateNode(tester.Ids.Wrong.RankImpreciseNoDim, new double[] { 1.0, 2.0, 3.0, 4.0 });

            var result = await builder.LoadNodeHierarchy(true, tester.Source.Token);

            Assert.Equal(6, result.DestinationObjects.Count());
            Assert.Equal(5, result.DestinationObjects.Count(node => node is UAVariable variable && variable.IsArray));

            // RankImprecise: 4, RankImpreciseNoDim: 4 (above), WrongDim: 4, NoDim: 5, DimInProp: 6
            Assert.Equal(23, result.DestinationVariables.Count());
            Assert.Single(result.SourceObjects);
            Assert.Equal(5, result.SourceVariables.Count());
        }
    }
}

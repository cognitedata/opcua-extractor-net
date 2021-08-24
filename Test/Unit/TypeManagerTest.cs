using Cognite.OpcUa;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
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
    public sealed class TypeManagerTestFixture : BaseExtractorTestFixture
    {
        public TypeManagerTestFixture() : base() { }
    }
    public class TypeManagerTest : MakeConsoleWork, IClassFixture<TypeManagerTestFixture>
    {
        private readonly TypeManagerTestFixture tester;
        public TypeManagerTest(ITestOutputHelper output, TypeManagerTestFixture tester) : base(output)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
            this.tester = tester;
            tester.ResetConfig();
        }
        #region datatypemanager
        [Fact]
        public void TestDataTypeManagerConfigure()
        {
            var config = new DataTypeConfig();
            var mgr = new DataTypeManager(tester.Client, config);
            var types = (Dictionary<NodeId, UADataType>)mgr.GetType()
                .GetField("dataTypes", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(mgr);
            var ignoreTypes = (HashSet<NodeId>)mgr.GetType()
                .GetField("ignoreDataTypes", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(mgr);
            // basic
            mgr.Configure();
            Assert.Empty(types);
            Assert.Empty(ignoreTypes);

            config.CustomNumericTypes = new List<ProtoDataType>
            {
                new ProtoDataType { Enum = true, NodeId = new NodeId("enum").ToProtoNodeId(tester.Client) },
                new ProtoDataType { NodeId = new NodeId("test").ToProtoNodeId(tester.Client) },
                new ProtoDataType { NodeId = new ProtoNodeId { NamespaceUri = "some.missing.uri", NodeId = "i=123" } }
            };
            // with custom numeric types
            mgr.Configure();
            Assert.Equal(2, types.Count);
            Assert.Empty(ignoreTypes);

            config.IgnoreDataTypes = new List<ProtoNodeId>
            {
                new NodeId("enum").ToProtoNodeId(tester.Client),
                new NodeId("test").ToProtoNodeId(tester.Client),
                new ProtoNodeId { NamespaceUri = "some.missing.uri", NodeId = "i=123" }
            };
            // with ignore data types
            mgr.Configure();
            Assert.Equal(2, types.Count);
            Assert.Equal(2, ignoreTypes.Count);
        }
        [Fact]
        public void TestGetDataType()
        {
            var config = new DataTypeConfig();
            var mgr = new DataTypeManager(tester.Client, config);
            var types = (Dictionary<NodeId, UADataType>)mgr.GetType()
                .GetField("dataTypes", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(mgr);
            var parentIds = (Dictionary<NodeId, NodeId>)mgr.GetType()
                .GetField("parentIds", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(mgr);

            // child of number
            parentIds[DataTypeIds.Number] = DataTypeIds.BaseDataType;
            parentIds[new NodeId("dt1")] = DataTypeIds.Number;
            var dt1 = mgr.GetDataType(new NodeId("dt1"));
            Assert.False(dt1.IsString);
            Assert.False(dt1.IsStep);
            Assert.Equal(new NodeId("dt1"), dt1.Raw);
            Assert.Single(types);

            // Grandchild of number
            types.Clear();
            parentIds[new NodeId("dt2")] = new NodeId("dt1");
            var dt2 = mgr.GetDataType(new NodeId("dt2"));
            Assert.False(dt2.IsString);
            Assert.False(dt2.IsStep);
            Assert.Equal(new NodeId("dt2"), dt2.Raw);
            Assert.Single(types);

            // Child of unknown
            parentIds[new NodeId("udt")] = DataTypeIds.BaseDataType;
            parentIds[new NodeId("dt3")] = new NodeId("udt");
            var dt3 = mgr.GetDataType(new NodeId("dt3"));
            Assert.True(dt3.IsString);
            Assert.False(dt3.IsStep);
            Assert.Equal(new NodeId("dt3"), dt3.Raw);
            Assert.Equal(2, types.Count);

            // Child of known
            parentIds[new NodeId("dt4")] = new NodeId("dt2");
            var dt4 = mgr.GetDataType(new NodeId("dt4"));
            Assert.False(dt4.IsString);
            Assert.False(dt4.IsStep);
            Assert.Equal(new NodeId("dt4"), dt4.Raw);
            Assert.Equal(3, types.Count);

            // Child of bool
            parentIds[new NodeId("dt5")] = DataTypeIds.Boolean;
            var dt5 = mgr.GetDataType(new NodeId("dt5"));
            Assert.False(dt5.IsString);
            Assert.True(dt5.IsStep);
            Assert.Equal(new NodeId("dt5"), dt5.Raw);
            Assert.Equal(4, types.Count);

            // Child of enum
            parentIds[new NodeId("dt6")] = DataTypeIds.Enumeration;
            var dt6 = mgr.GetDataType(new NodeId("dt6"));
            Assert.False(dt6.IsString);
            Assert.True(dt6.IsStep);
            Assert.Equal(new NodeId("dt6"), dt6.Raw);
            Assert.Equal(5, types.Count);

            // Null nodeId
            config.NullAsNumeric = true;
            var dt7 = mgr.GetDataType(NodeId.Null);
            Assert.False(dt7.IsString);
            Assert.False(dt7.IsStep);
            Assert.Equal(NodeId.Null, dt7.Raw);
            Assert.Equal(6, types.Count);

            // Recognized NodeId
            parentIds.Clear();
            var dt8 = mgr.GetDataType(new NodeId("dt6"));
            Assert.False(dt8.IsString);
            Assert.True(dt8.IsStep);
            Assert.Equal(new NodeId("dt6"), dt8.Raw);
            Assert.Equal(6, types.Count);

            types.Clear();
            // Null nodeId
            config.NullAsNumeric = false;
            var dt9 = mgr.GetDataType(NodeId.Null);
            Assert.True(dt9.IsString);
            Assert.False(dt9.IsStep);
            Assert.Equal(NodeId.Null, dt9.Raw);
            Assert.Single(types);
        }
        [Fact]
        public void TestAllowTsMap()
        {
            var config = new DataTypeConfig();
            config.IgnoreDataTypes = new List<ProtoNodeId>
            {
                new NodeId("ignore").ToProtoNodeId(tester.Client)
            };
            var node = new UAVariable(new NodeId("node"), "node", NodeId.Null);
            node.VariableAttributes.ValueRank = ValueRanks.Scalar;
            var mgr = new DataTypeManager(tester.Client, config);
            mgr.Configure();

            // Basic, passing
            node.VariableAttributes.DataType = new UADataType(DataTypeIds.Double);
            Assert.True(mgr.AllowTSMap(node));

            // String, failing
            node.VariableAttributes.DataType = new UADataType(DataTypeIds.String);
            Assert.False(mgr.AllowTSMap(node));

            // Override string
            Assert.True(mgr.AllowTSMap(node, null, true));

            // Allow strings
            config.AllowStringVariables = true;
            Assert.True(mgr.AllowTSMap(node));

            // Ignored datatype
            node.VariableAttributes.DataType = new UADataType(new NodeId("ignore"));
            Assert.False(mgr.AllowTSMap(node));

            // Non-scalar value rank
            node.VariableAttributes.DataType = new UADataType(DataTypeIds.Double);
            node.VariableAttributes.ValueRank = ValueRanks.Any;
            Assert.False(mgr.AllowTSMap(node));

            // Set unknown-as-scalar
            config.UnknownAsScalar = true;
            Assert.True(mgr.AllowTSMap(node));

            // Missing dimensions
            node.VariableAttributes.ValueRank = ValueRanks.OneDimension;
            Assert.False(mgr.AllowTSMap(node));

            // Too high dimension
            node.VariableAttributes.ArrayDimensions = new [] { 4, 4 };
            Assert.False(mgr.AllowTSMap(node));

            // Too large array
            node.VariableAttributes.ArrayDimensions = new [] { 4 };
            Assert.False(mgr.AllowTSMap(node));

            // Override size
            Assert.True(mgr.AllowTSMap(node, 4));

            // Set max size to infinite
            config.MaxArraySize = -1;
            Assert.True(mgr.AllowTSMap(node));

            // Set max size to smaller
            config.MaxArraySize = 3;
            Assert.False(mgr.AllowTSMap(node));

            // Set to equal
            config.MaxArraySize = 4;
            Assert.True(mgr.AllowTSMap(node));
        }
        [Fact]
        public void TestAdditionalDataTypeMetadata()
        {
            var config = new DataTypeConfig();
            var node = new UAVariable(new NodeId("node"), "node", NodeId.Null);
            var mgr = new DataTypeManager(tester.Client, config);
            var customTypeNames = (Dictionary<NodeId, string>)mgr.GetType()
                .GetField("customTypeNames", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(mgr);

            Assert.Null(mgr.GetAdditionalMetadata(node));

            node.VariableAttributes.DataType = mgr.GetDataType(DataTypeIds.Double);
            Assert.Null(mgr.GetAdditionalMetadata(node));

            // Built in type
            config.DataTypeMetadata = true;
            var meta = mgr.GetAdditionalMetadata(node);
            Assert.Single(meta);
            Assert.Equal("Double", meta["dataType"]);

            // Custom type
            customTypeNames[new NodeId("type", 2)] = "SomeType";
            node.VariableAttributes.DataType = mgr.GetDataType(new NodeId("type", 2));
            meta = mgr.GetAdditionalMetadata(node);
            Assert.Equal("SomeType", meta["dataType"]);

            // Custom type, not mapped
            node.VariableAttributes.DataType = mgr.GetDataType(new NodeId("type2", 2));
            meta = mgr.GetAdditionalMetadata(node);
            Assert.Equal("gp.tl:s=type2", meta["dataType"]);

            // Enum type
            node.VariableAttributes.DataType = mgr.GetDataType(new NodeId("enum", 2));
            customTypeNames[new NodeId("enum", 2)] = "EnumType";
            node.DataType.EnumValues = new Dictionary<long, string>
            {
                { 123, "field1" },
                { 321, "field2" },
                { -3, "field3" },
                { 0, "field4" }
            };
            meta = mgr.GetAdditionalMetadata(node);
            Assert.Equal(5, meta.Count);
            Assert.Equal("EnumType", meta["dataType"]);
            Assert.Equal("field1", meta["123"]);
            Assert.Equal("field2", meta["321"]);
            Assert.Equal("field3", meta["-3"]);
            Assert.Equal("field4", meta["0"]);
        }
        [Fact]
        public async Task TestReadDataTypes()
        {
            var config = new DataTypeConfig();
            config.CustomNumericTypes = new List<ProtoDataType>
            {
                new ProtoDataType { NodeId = tester.Server.Ids.Custom.NumberType.ToProtoNodeId(tester.Client) }
            };
            var mgr = new DataTypeManager(tester.Client, config);
            var customTypeNames = (Dictionary<NodeId, string>)mgr.GetType()
                .GetField("customTypeNames", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(mgr);
            mgr.Configure();

            config.AutoIdentifyTypes = true;
            await mgr.GetDataTypeStructureAsync(tester.Source.Token);

            Assert.Equal(6, customTypeNames.Count);

            var type = mgr.GetDataType(tester.Server.Ids.Custom.IgnoreType);
            Assert.True(type.IsString);
            Assert.False(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.IgnoreType, type.Raw);
            Assert.Null(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.StringyType);
            Assert.True(type.IsString);
            Assert.False(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.StringyType, type.Raw);
            Assert.Null(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.MysteryType);
            Assert.False(type.IsString);
            Assert.False(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.MysteryType, type.Raw);
            Assert.Null(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.NumberType);
            Assert.False(type.IsString);
            Assert.False(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.NumberType, type.Raw);
            Assert.Null(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.EnumType1);
            Assert.False(type.IsString);
            Assert.True(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.EnumType1, type.Raw);
            Assert.Empty(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.EnumType2);
            Assert.False(type.IsString);
            Assert.True(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.EnumType2, type.Raw);
            Assert.Empty(type.EnumValues);

            await mgr.GetDataTypeMetadataAsync(new[]
            {
                tester.Server.Ids.Custom.IgnoreType,
                tester.Server.Ids.Custom.StringyType,
                tester.Server.Ids.Custom.MysteryType,
                tester.Server.Ids.Custom.NumberType,
                tester.Server.Ids.Custom.EnumType1,
                tester.Server.Ids.Custom.EnumType2
            }, tester.Source.Token);

            var et1 = mgr.GetDataType(tester.Server.Ids.Custom.EnumType1);
            Assert.Equal(3, et1.EnumValues.Count);
            Assert.Equal("Enum1", et1.EnumValues[0]);
            Assert.Equal("Enum2", et1.EnumValues[1]);
            Assert.Equal("Enum3", et1.EnumValues[2]);

            var et2 = mgr.GetDataType(tester.Server.Ids.Custom.EnumType2);
            Assert.Equal(2, et2.EnumValues.Count);
            Assert.Equal("VEnum1", et2.EnumValues[321]);
            Assert.Equal("VEnum2", et2.EnumValues[123]);
        }
        #endregion
        #region EventFieldCollector
        [Fact]
        public void TestEventFieldEquality()
        {
            var field1 = new EventField(new QualifiedName("baseNsName"));
            var field2 = new EventField(new QualifiedName("baseNsName"));

            Assert.Equal(field1, field2);
            Assert.Equal(field1.GetHashCode(), field2.GetHashCode());

            var field3 = new EventField(new QualifiedName("baseNsName2"));
            Assert.NotEqual(field1, field3);

            var field4 = new EventField(new QualifiedName("baseNsName", 2));
            Assert.NotEqual(field1, field4);

            var field5 = new EventField(new QualifiedNameCollection { new QualifiedName("otherName"), new QualifiedName("baseNsName") });
            Assert.NotEqual(field1, field5);

            var field6 = new EventField(new QualifiedName("baseNsName", 2));
            Assert.Equal(field4, field6);

            var field7 = new EventField(new QualifiedNameCollection { new QualifiedName("otherName"), new QualifiedName("baseNsName") });
            Assert.Equal(field5, field7);
            Assert.Equal(field5.GetHashCode(), field7.GetHashCode());
        }
        [Fact]
        public void TestCollectCustomOnly()
        {
            tester.Client.Browser.ResetVisitedNodes();
            var config = new EventConfig() { Enabled = true, AllEvents = false };
            var collector = new EventFieldCollector(tester.Client, config);

            var fields = collector.GetEventIdFields(tester.Source.Token);
            Assert.Equal(5, fields.Count);
            var eventIds = tester.Server.Ids.Event;
            Assert.Equal(7, fields[eventIds.BasicType1].Count);
            Assert.Contains(new EventField(new QualifiedName("SourceNode")), fields[eventIds.BasicType1]);
            Assert.Contains(new EventField(new QualifiedName("Time")), fields[eventIds.BasicType1]);
            Assert.Contains(new EventField(new QualifiedName("Severity")), fields[eventIds.BasicType1]);
            Assert.Contains(new EventField(new QualifiedName("Message")), fields[eventIds.BasicType1]);
            Assert.Contains(new EventField(new QualifiedName("EventId")), fields[eventIds.BasicType1]);
            Assert.Contains(new EventField(new QualifiedName("EventType")), fields[eventIds.BasicType1]);

            Assert.Equal(7, fields[eventIds.BasicType2].Count);
            foreach (var field in fields[eventIds.BasicType1])
            {
                Assert.Contains(field, fields[eventIds.BasicType2]);
            }

            Assert.Equal(8, fields[eventIds.CustomType].Count);
            Assert.Contains(new EventField(new QualifiedName("TypeProp")), fields[eventIds.CustomType]);

            Assert.Equal(10, fields[eventIds.PropType].Count);
            Assert.Contains(new EventField(new QualifiedName("PropertyNum")), fields[eventIds.PropType]);
            Assert.Contains(new EventField(new QualifiedName("PropertyString")), fields[eventIds.PropType]);
            Assert.Contains(new EventField(new QualifiedName("SubType")), fields[eventIds.PropType]);

            Assert.Equal(11, fields[eventIds.DeepType].Count);
            Assert.Contains(new EventField(new QualifiedNameCollection { new QualifiedName("DeepObj", 2), new QualifiedName("DeepProp") }),
                fields[eventIds.DeepType]);
        }
        [Fact]
        public void TestCollectAllEvents()
        {
            tester.Client.Browser.ResetVisitedNodes();
            var config = new EventConfig { Enabled = true, AllEvents = true };
            var collector = new EventFieldCollector(tester.Client, config);

            var fields = collector.GetEventIdFields(tester.Source.Token);

            Assert.Equal(96, fields.Count);

            // Check that all parent properties are present in a deep event
            Assert.Equal(16, fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].Count);
            Assert.Contains(new EventField(new QualifiedName("EventType")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType]);
            Assert.Contains(new EventField(new QualifiedName("ActionTimeStamp")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType]);
            Assert.Contains(new EventField(new QualifiedName("ParameterDataTypeId")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType]);
            Assert.Contains(new EventField(new QualifiedName("UpdatedNode")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType]);
            Assert.Contains(new EventField(new QualifiedName("OldValues")),
                fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType]);

            // Check that nodes in the middle only have higher level properties
            Assert.Equal(13, fields[ObjectTypeIds.AuditHistoryUpdateEventType].Count);
            Assert.DoesNotContain(new EventField(new QualifiedName("OldValues")),
                fields[ObjectTypeIds.AuditHistoryUpdateEventType]);
        }
        [Fact]
        public void TestIgnoreEvents()
        {
            tester.Client.Browser.ResetVisitedNodes();
            // Audit and conditions/alarms account for most of the event types in the base namespace
            // Also check if we still get child events once the parent is excluded (should this be how it works?)
            var config = new EventConfig { Enabled = true, AllEvents = true, ExcludeEventFilter = "Audit|Condition|Alarm|SystemEventType" };
            var collector = new EventFieldCollector(tester.Client, config);

            var fields = collector.GetEventIdFields(tester.Source.Token);

            Assert.Equal(21, fields.Count);

            Assert.False(fields.ContainsKey(ObjectTypeIds.SystemEventType));
            Assert.True(fields.ContainsKey(ObjectTypeIds.DeviceFailureEventType));
        }
        [Fact]
        public void TestEventExcludeProperties()
        {
            tester.Client.Browser.ResetVisitedNodes();
            var config = new EventConfig { Enabled = true, AllEvents = false, ExcludeProperties = new List<string> { "SubType" } };
            var collector = new EventFieldCollector(tester.Client, config);

            var fields = collector.GetEventIdFields(tester.Source.Token);

            Assert.Equal(5, fields.Count);

            var eventIds = tester.Server.Ids.Event;
            Assert.Equal(9, fields[eventIds.PropType].Count);
            Assert.Contains(new EventField(new QualifiedName("PropertyNum")), fields[eventIds.PropType]);
            Assert.Contains(new EventField(new QualifiedName("PropertyString")), fields[eventIds.PropType]);
            Assert.DoesNotContain(new EventField(new QualifiedName("SubType")), fields[eventIds.PropType]);
        }
        [Fact]
        public void TestEventWhitelist()
        {
            tester.Client.Browser.ResetVisitedNodes();
            var eventIds = tester.Server.Ids.Event;
            var config = new EventConfig
            {
                Enabled = true,
                AllEvents = false,
                EventIds = new List<ProtoNodeId>
                {
                    eventIds.BasicType1.ToProtoNodeId(tester.Client),
                    eventIds.PropType.ToProtoNodeId(tester.Client),
                    ObjectTypeIds.AuditHistoryAtTimeDeleteEventType.ToProtoNodeId(tester.Client)
                }
            };
            var collector = new EventFieldCollector(tester.Client, config);

            var fields = collector.GetEventIdFields(tester.Source.Token);

            Assert.Equal(3, fields.Count);
            Assert.Equal(16, fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].Count);
            Assert.Equal(10, fields[eventIds.PropType].Count);
            Assert.Equal(7, fields[eventIds.BasicType1].Count);
        }
        #endregion
        #region nodetypemanager
        [Fact]
        public async Task TestNodeTypeManager()
        {
            var mgr = new NodeTypeManager(tester.Client);
            var type1 = mgr.GetObjectType(ObjectTypeIds.BaseObjectType, false);
            var type2 = mgr.GetObjectType(ObjectTypeIds.FolderType, false);
            var type3 = mgr.GetObjectType(VariableTypeIds.AudioVariableType, true);
            var type4 = mgr.GetObjectType(tester.Server.Ids.Custom.ObjectType, false);
            var type5 = mgr.GetObjectType(tester.Server.Ids.Custom.VariableType, true);

            await mgr.GetObjectTypeMetadataAsync(tester.Source.Token);

            Assert.Equal("BaseObjectType", type1.Name);
            Assert.Equal("FolderType", type2.Name);
            Assert.Equal("AudioVariableType", type3.Name);
            Assert.Equal("CustomObjectType", type4.Name);
            Assert.Equal("CustomVariableType", type5.Name);
        }
        #endregion
        #region referencetypemanager
        [Fact]
        public async Task TestReferenceTypeMeta()
        {
            using var extractor = tester.BuildExtractor();
            var mgr = new ReferenceTypeManager(tester.Client, extractor);
            var type1 = mgr.GetReferenceType(ReferenceTypeIds.Organizes);
            var type2 = mgr.GetReferenceType(ReferenceTypeIds.HasComponent);
            var type3 = mgr.GetReferenceType(tester.Server.Ids.Custom.RefType1);
            var type4 = mgr.GetReferenceType(tester.Server.Ids.Custom.RefType2);

            await mgr.GetReferenceTypeDataAsync(tester.Source.Token);

            Assert.Equal("Organizes", type1.GetName(false));
            Assert.Equal("OrganizedBy", type1.GetName(true));
            Assert.Equal("HasComponent", type2.GetName(false));
            Assert.Equal("ComponentOf", type2.GetName(true));
            Assert.Equal("HasCustomRelation", type3.GetName(false));
            Assert.Equal("IsCustomRelationOf", type3.GetName(true));
            Assert.Equal("HasSymmetricRelation", type4.GetName(false));
            Assert.Equal("HasSymmetricRelation", type4.GetName(true));
        }

        private async Task TestGetReferencesGroup(NodeId referenceTypeId, int results, params NodeId[] ids)
        {
            using var extractor = tester.BuildExtractor();
            var mgr = new ReferenceTypeManager(tester.Client, extractor);

            var nodes = ids.Select(id => new UANode(id, "Node", NodeId.Null, NodeClass.Object)).ToList();
            foreach (var node in nodes)
            {
                extractor.State.RegisterNode(node.Id, tester.Client.GetUniqueId(node.Id));
                extractor.State.AddActiveNode(node, new TypeUpdateConfig(), false, false);
            }

            var references = await mgr.GetReferencesAsync(nodes, referenceTypeId, tester.Source.Token);
            Assert.All(references, reference => Assert.True(reference.Type != null && !reference.Type.Id.IsNullNodeId));
            Assert.Equal(results, references.Count());
        }
        [Fact]
        public async Task TestGetReferences()
        {
            var ids = tester.Server.Ids.Custom;
            await TestGetReferencesGroup(ReferenceTypeIds.NonHierarchicalReferences, 10,
                ids.Root, ids.Array, ids.StringArray, ids.StringyVar, ids.MysteryVar, ids.IgnoreVar);
            await TestGetReferencesGroup(ids.RefType2, 6,
                ids.Root, ids.Array, ids.StringArray, ids.StringyVar, ids.MysteryVar, ids.IgnoreVar);
            await TestGetReferencesGroup(ReferenceTypeIds.HierarchicalReferences, 4,
                ids.Root, ids.Array, ids.StringArray);
            await TestGetReferencesGroup(ReferenceTypeIds.References, 20,
                ids.Root, ids.Array, ids.StringArray, ids.StringyVar, ids.MysteryVar, ids.IgnoreVar);
        }

        #endregion
    }
}

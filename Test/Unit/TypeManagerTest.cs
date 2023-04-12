﻿using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
    [Collection("Shared server tests")]
    public class TypeManagerTest
    {
        private readonly StaticServerTestFixture tester;
        public TypeManagerTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
        }
        #region datatypemanager
        [Fact]
        public void TestDataTypeManagerConfigure()
        {
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);
            // basic
            mgr.BuildTypeInfo();
            Assert.Empty(mgr.NodeMap.Values.OfType<UADataType>());

            var config = tester.Config.Extraction.DataTypes;
            config.CustomNumericTypes = new List<ProtoDataType>
            {
                new ProtoDataType { Enum = true, NodeId = new NodeId("enum").ToProtoNodeId(tester.Client) },
                new ProtoDataType { NodeId = new NodeId("test").ToProtoNodeId(tester.Client) },
                new ProtoDataType { NodeId = new ProtoNodeId { NamespaceUri = "some.missing.uri", NodeId = "i=123" } }
            };
            // with custom numeric types
            mgr.BuildTypeInfo();
            Assert.Equal(2, mgr.NodeMap.Values.OfType<UADataType>().Count());

            config.IgnoreDataTypes = new List<ProtoNodeId>
            {
                new NodeId("enum").ToProtoNodeId(tester.Client),
                new NodeId("test").ToProtoNodeId(tester.Client),
                new ProtoNodeId { NamespaceUri = "some.missing.uri", NodeId = "i=123" }
            };
            // with ignore data types
            mgr.BuildTypeInfo();
            Assert.Equal(2, mgr.NodeMap.Values.OfType<UADataType>().Count());
        }
        [Fact]
        public void TestGetDataType()
        {
            var config = tester.Config.Extraction.DataTypes;
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);

            // child of number
            var dt1 = mgr.GetDataType(new NodeId("dt1"));
            dt1.Parent = mgr.GetDataType(DataTypeIds.Number);
            mgr.BuildTypeInfo();
            Assert.False(dt1.IsString);
            Assert.False(dt1.IsStep);
            Assert.Equal(2, mgr.NodeMap.OfType<UADataType>().Count());

            // Grandchild of number
            var dt2 = mgr.GetDataType(new NodeId("dt2"));
            dt2.Parent = dt1;
            mgr.BuildTypeInfo();
            Assert.False(dt2.IsString);
            Assert.False(dt2.IsStep);
            Assert.Equal(3, mgr.NodeMap.OfType<UADataType>().Count());

            // Child of unknown
            var dt3 = mgr.GetDataType(new NodeId("dt3"));
            dt3.Parent = mgr.GetDataType(new NodeId("udt"));
            mgr.BuildTypeInfo();
            Assert.True(dt3.IsString);
            Assert.False(dt3.IsStep);
            Assert.Equal(5, mgr.NodeMap.OfType<UADataType>().Count());

            // Child of known
            var dt4 = mgr.GetDataType(new NodeId("dt4"));
            dt4.Parent = dt2;
            mgr.BuildTypeInfo();
            Assert.False(dt4.IsString);
            Assert.False(dt4.IsStep);
            Assert.Equal(6, mgr.NodeMap.OfType<UADataType>().Count());

            // Child of bool
            var dt5 = mgr.GetDataType(new NodeId("dt5"));
            dt5.Parent = mgr.GetDataType(DataTypeIds.Boolean);
            mgr.BuildTypeInfo();
            Assert.False(dt5.IsString);
            Assert.True(dt5.IsStep);
            Assert.Equal(8, mgr.NodeMap.OfType<UADataType>().Count());

            // Child of enum
            var dt6 = mgr.GetDataType(new NodeId("dt6"));
            dt6.Parent = mgr.GetDataType(DataTypeIds.Enumeration);
            mgr.BuildTypeInfo();
            Assert.False(dt6.IsString);
            Assert.True(dt6.IsStep);
            Assert.NotNull(dt6.EnumValues);
            Assert.Equal(10, mgr.NodeMap.OfType<UADataType>().Count());

            // Null nodeId
            config.NullAsNumeric = true;
            var dt7 = mgr.GetDataType(NodeId.Null);
            mgr.BuildTypeInfo();
            Assert.False(dt7.IsString);
            Assert.False(dt7.IsStep);
            Assert.Equal(11, mgr.NodeMap.OfType<UADataType>().Count());

            // Recognized NodeId
            var dt8 = mgr.GetDataType(new NodeId("dt6"));
            mgr.BuildTypeInfo();
            Assert.False(dt8.IsString);
            Assert.True(dt8.IsStep);
            Assert.Equal(11, mgr.NodeMap.OfType<UADataType>().Count());
        }
        [Fact]
        public void TestAllowTsMap()
        {
            tester.Config.Extraction.DataTypes = new DataTypeConfig
            {
                IgnoreDataTypes = new List<ProtoNodeId>
                {
                    new NodeId("ignore").ToProtoNodeId(tester.Client)
                }
            };
            var node = new UAVariable(new NodeId("node"), "node", null, null, NodeId.Null, null);
            node.FullAttributes.ValueRank = ValueRanks.Scalar;
            var config = tester.Config.Extraction.DataTypes;

            // Basic, passing
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            Assert.True(node.AllowTSMap(tester.Log, config));

            // String, failing
            node.FullAttributes.DataType = new UADataType(DataTypeIds.String);
            Assert.False(node.AllowTSMap(tester.Log, config));

            // Override string
            Assert.True(node.AllowTSMap(tester.Log, config));

            // Allow strings
            config.AllowStringVariables = true;
            Assert.True(node.AllowTSMap(tester.Log, config));

            // Ignored datatype
            node.FullAttributes.DataType = new UADataType(new NodeId("ignore"));
            Assert.False(node.AllowTSMap(tester.Log, config));

            // Non-scalar value rank
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            node.FullAttributes.ValueRank = ValueRanks.Any;
            Assert.False(node.AllowTSMap(tester.Log, config));

            // Set unknown-as-scalar
            config.UnknownAsScalar = true;
            Assert.True(node.AllowTSMap(tester.Log, config));

            // Missing dimensions
            node.FullAttributes.ValueRank = ValueRanks.OneDimension;
            Assert.False(node.AllowTSMap(tester.Log, config));

            // Too high dimension
            node.FullAttributes.ArrayDimensions = new[] { 4, 4 };
            Assert.False(node.AllowTSMap(tester.Log, config));

            // Too large array
            node.FullAttributes.ArrayDimensions = new[] { 4 };
            Assert.False(node.AllowTSMap(tester.Log, config));

            // Override size
            Assert.True(node.AllowTSMap(tester.Log, config));

            // Set max size to infinite
            config.MaxArraySize = -1;
            Assert.True(node.AllowTSMap(tester.Log, config));

            // Set max size to smaller
            config.MaxArraySize = 3;
            Assert.False(node.AllowTSMap(tester.Log, config));

            // Set to equal
            config.MaxArraySize = 4;
            Assert.True(node.AllowTSMap(tester.Log, config));
        }
        [Fact]
        public async Task TestReadDataTypes()
        {
            var config = tester.Config.Extraction.DataTypes;
            config.CustomNumericTypes = new List<ProtoDataType>
            {
                new ProtoDataType { NodeId = tester.Server.Ids.Custom.NumberType.ToProtoNodeId(tester.Client) }
            };
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);


            config.AutoIdentifyTypes = true;
            await mgr.LoadTypeData(tester.Source.Token);


            var type = mgr.GetDataType(tester.Server.Ids.Custom.IgnoreType);
            Assert.True(type.IsString);
            Assert.False(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.IgnoreType, type.Id);
            Assert.Null(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.StringyType);
            Assert.True(type.IsString);
            Assert.False(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.StringyType, type.Id);
            Assert.Null(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.MysteryType);
            Assert.False(type.IsString);
            Assert.False(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.MysteryType, type.Id);
            Assert.Null(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.NumberType);
            Assert.False(type.IsString);
            Assert.False(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.NumberType, type.Id);
            Assert.Null(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.EnumType1);
            Assert.False(type.IsString);
            Assert.True(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.EnumType1, type.Id);
            Assert.Empty(type.EnumValues);

            type = mgr.GetDataType(tester.Server.Ids.Custom.EnumType2);
            Assert.False(type.IsString);
            Assert.True(type.IsStep);
            Assert.Equal(tester.Server.Ids.Custom.EnumType2, type.Id);
            Assert.Empty(type.EnumValues);

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
            var field1 = new RawTypeField(new QualifiedName("baseNsName"));
            var field2 = new RawTypeField(new QualifiedName("baseNsName"));

            Assert.Equal(field1, field2);
            Assert.Equal(field1.GetHashCode(), field2.GetHashCode());

            var field3 = new RawTypeField(new QualifiedName("baseNsName2"));
            Assert.NotEqual(field1, field3);

            var field4 = new RawTypeField(new QualifiedName("baseNsName", 2));
            Assert.NotEqual(field1, field4);

            var field5 = new RawTypeField(new QualifiedNameCollection { new QualifiedName("otherName"), new QualifiedName("baseNsName") });
            Assert.NotEqual(field1, field5);

            var field6 = new RawTypeField(new QualifiedName("baseNsName", 2));
            Assert.Equal(field4, field6);

            var field7 = new RawTypeField(new QualifiedNameCollection { new QualifiedName("otherName"), new QualifiedName("baseNsName") });
            Assert.Equal(field5, field7);
            Assert.Equal(field5.GetHashCode(), field7.GetHashCode());
        }
        [Fact]
        public async Task TestCollectCustomOnly()
        {
            var config = tester.Config.Events;
            config.Enabled = true;
            config.AllEvents = false;
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);
            await mgr.LoadTypeData(tester.Source.Token);
            mgr.BuildTypeInfo();

            var fields = mgr.EventFields;
            Assert.Equal(5, fields.Count);
            var eventIds = tester.Server.Ids.Event;
            Assert.Equal(7, fields[eventIds.BasicType1].CollectedFields.Count());
            Assert.Contains(new RawTypeField(new QualifiedName("SourceNode")), fields[eventIds.BasicType1].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("Time")), fields[eventIds.BasicType1].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("Severity")), fields[eventIds.BasicType1].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("Message")), fields[eventIds.BasicType1].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("EventId")), fields[eventIds.BasicType1].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("EventType")), fields[eventIds.BasicType1].CollectedFields);

            Assert.Equal(7, fields[eventIds.BasicType2].CollectedFields.Count());
            foreach (var field in fields[eventIds.BasicType1].CollectedFields)
            {
                Assert.Contains(field, fields[eventIds.BasicType2].CollectedFields);
            }

            Assert.Equal(8, fields[eventIds.CustomType].CollectedFields.Count());
            Assert.Contains(new RawTypeField(new QualifiedName("TypeProp")), fields[eventIds.CustomType].CollectedFields);

            Assert.Equal(10, fields[eventIds.PropType].CollectedFields.Count());
            Assert.Contains(new RawTypeField(new QualifiedName("PropertyNum")), fields[eventIds.PropType].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("PropertyString")), fields[eventIds.PropType].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("SubType")), fields[eventIds.PropType].CollectedFields);

            Assert.Equal(11, fields[eventIds.DeepType].CollectedFields.Count());
            Assert.Contains(new RawTypeField(new QualifiedNameCollection { new QualifiedName("DeepObj", 2), new QualifiedName("DeepProp") }),
                fields[eventIds.DeepType].CollectedFields);
        }
        [Fact]
        public async Task TestCollectAllEvents()
        {
            var config = tester.Config.Events;
            config.Enabled = true;
            config.AllEvents = true;
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);
            await mgr.LoadTypeData(tester.Source.Token);
            mgr.BuildTypeInfo();

            var fields = mgr.EventFields;

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
        }
        [Fact]
        public async Task TestIgnoreEvents()
        {
            // Audit and conditions/alarms account for most of the event types in the base namespace
            // Also check if we still get child events once the parent is excluded (should this be how it works?)
            var config = tester.Config.Events;
            config.Enabled = true;
            config.AllEvents = true;
            config.ExcludeEventFilter = "Audit|Condition|Alarm|SystemEventType";
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);
            await mgr.LoadTypeData(tester.Source.Token);
            mgr.BuildTypeInfo();

            var fields = mgr.EventFields;

            Assert.Equal(21, fields.Count);

            Assert.False(fields.ContainsKey(ObjectTypeIds.SystemEventType));
            Assert.True(fields.ContainsKey(ObjectTypeIds.DeviceFailureEventType));
        }
        [Fact]
        public async Task TestEventExcludeProperties()
        {
            var config = tester.Config.Events;
            config.Enabled = true;
            config.AllEvents = false;
            config.ExcludeProperties = new[] { "SubType" };
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);
            await mgr.LoadTypeData(tester.Source.Token);
            mgr.BuildTypeInfo();

            var fields = mgr.EventFields;

            Assert.Equal(5, fields.Count);

            var eventIds = tester.Server.Ids.Event;
            Assert.Equal(9, fields[eventIds.PropType].CollectedFields.Count());
            Assert.Contains(new RawTypeField(new QualifiedName("PropertyNum")), fields[eventIds.PropType].CollectedFields);
            Assert.Contains(new RawTypeField(new QualifiedName("PropertyString")), fields[eventIds.PropType].CollectedFields);
            Assert.DoesNotContain(new RawTypeField(new QualifiedName("SubType")), fields[eventIds.PropType].CollectedFields);
        }
        [Fact]
        public async Task TestEventWhitelist()
        {
            var eventIds = tester.Server.Ids.Event;
            var config = tester.Config.Events;
            config.Enabled = true;
            config.AllEvents = false;
            config.EventIds = new[]
            {
                eventIds.BasicType1.ToProtoNodeId(tester.Client),
                eventIds.PropType.ToProtoNodeId(tester.Client),
                ObjectTypeIds.AuditHistoryAtTimeDeleteEventType.ToProtoNodeId(tester.Client)
            };
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);
            await mgr.LoadTypeData(tester.Source.Token);
            mgr.BuildTypeInfo();

            var fields = mgr.EventFields;

            Assert.Equal(3, fields.Count);
            Assert.Equal(16, fields[ObjectTypeIds.AuditHistoryAtTimeDeleteEventType].CollectedFields.Count());
            Assert.Equal(10, fields[eventIds.PropType].CollectedFields.Count());
            Assert.Equal(7, fields[eventIds.BasicType1].CollectedFields.Count());
        }
        #endregion
        #region nodetypemanager
        [Fact]
        public async Task TestNodeTypeManager()
        {
            tester.Config.Extraction.NodeTypes.Metadata = true;
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);
            var type1 = mgr.GetObjectType(ObjectTypeIds.BaseObjectType);
            var type2 = mgr.GetObjectType(ObjectTypeIds.FolderType);
            var type3 = mgr.GetVariableType(VariableTypeIds.AudioVariableType);
            var type4 = mgr.GetObjectType(tester.Server.Ids.Custom.ObjectType);
            var type5 = mgr.GetVariableType(tester.Server.Ids.Custom.VariableType);

            await mgr.LoadTypeData(tester.Source.Token);

            Assert.Equal("BaseObjectType", type1.Attributes.DisplayName);
            Assert.Equal("FolderType", type2.Attributes.DisplayName);
            Assert.Equal("AudioVariableType", type3.Attributes.DisplayName);
            Assert.Equal("CustomObjectType", type4.Attributes.DisplayName);
            Assert.Equal("CustomVariableType", type5.Attributes.DisplayName);
        }
        #endregion
        #region referencetypemanager
        [Fact]
        public async Task TestReferenceTypeMeta()
        {
            tester.Config.Extraction.Relationships.Enabled = true;
            var mgr = new TypeManager(tester.Config, tester.Client, tester.Log);
            var type1 = mgr.GetReferenceType(ReferenceTypeIds.Organizes);
            var type2 = mgr.GetReferenceType(ReferenceTypeIds.HasComponent);
            var type3 = mgr.GetReferenceType(tester.Server.Ids.Custom.RefType1);
            var type4 = mgr.GetReferenceType(tester.Server.Ids.Custom.RefType2);

            await mgr.LoadTypeData(tester.Source.Token);

            Assert.Equal("Organizes", type1.GetName(false));
            Assert.Equal("OrganizedBy", type1.GetName(true));
            Assert.Equal("HasComponent", type2.GetName(false));
            Assert.Equal("ComponentOf", type2.GetName(true));
            Assert.Equal("HasCustomRelation", type3.GetName(false));
            Assert.Equal("IsCustomRelationOf", type3.GetName(true));
            Assert.Equal("HasSymmetricRelation", type4.GetName(false));
            Assert.Equal("HasSymmetricRelation", type4.GetName(true));
        }
        #endregion
    }
}

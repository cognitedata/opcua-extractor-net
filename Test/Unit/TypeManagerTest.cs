using Cognite.OpcUa;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class TypeManagerTestFixture : BaseExtractorTestFixture
    {
        public TypeManagerTestFixture() : base(62700) { }
    }
    public class TypeManagerTest : MakeConsoleWork, IClassFixture<TypeManagerTestFixture>
    {
        private readonly TypeManagerTestFixture tester;
        public TypeManagerTest(ITestOutputHelper output, TypeManagerTestFixture tester) : base(output)
        {
            this.tester = tester;
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
            node.ValueRank = ValueRanks.Scalar;
            var mgr = new DataTypeManager(tester.Client, config);
            mgr.Configure();

            // Basic, passing
            node.DataType = new UADataType(DataTypeIds.Double);
            Assert.True(mgr.AllowTSMap(node));

            // String, failing
            node.DataType = new UADataType(DataTypeIds.String);
            Assert.False(mgr.AllowTSMap(node));

            // Override string
            Assert.True(mgr.AllowTSMap(node, null, true));

            // Allow strings
            config.AllowStringVariables = true;
            Assert.True(mgr.AllowTSMap(node));

            // Ignored datatype
            node.DataType = new UADataType(new NodeId("ignore"));
            Assert.False(mgr.AllowTSMap(node));

            // Non-scalar value rank
            node.DataType = new UADataType(DataTypeIds.Double);
            node.ValueRank = ValueRanks.Any;
            Assert.False(mgr.AllowTSMap(node));

            // Set unknown-as-scalar
            config.UnknownAsScalar = true;
            Assert.True(mgr.AllowTSMap(node));

            // Missing dimensions
            node.ValueRank = ValueRanks.OneDimension;
            Assert.False(mgr.AllowTSMap(node));

            // Too high dimension
            node.ArrayDimensions = new System.Collections.ObjectModel.Collection<int> { 4, 4 };
            Assert.False(mgr.AllowTSMap(node));

            // Too large array
            node.ArrayDimensions = new System.Collections.ObjectModel.Collection<int> { 4 };
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

            node.DataType = mgr.GetDataType(DataTypeIds.Double);
            Assert.Null(mgr.GetAdditionalMetadata(node));

            // Built in type
            config.DataTypeMetadata = true;
            var meta = mgr.GetAdditionalMetadata(node);
            Assert.Single(meta);
            Assert.Equal("Double", meta["dataType"]);

            // Custom type
            customTypeNames[new NodeId("type", 2)] = "SomeType";
            node.DataType = mgr.GetDataType(new NodeId("type", 2));
            meta = mgr.GetAdditionalMetadata(node);
            Assert.Equal("SomeType", meta["dataType"]);

            // Custom type, not mapped
            node.DataType = mgr.GetDataType(new NodeId("type2", 2));
            meta = mgr.GetAdditionalMetadata(node);
            Assert.Equal("gp.tl:s=type2", meta["dataType"]);

            // Enum type
            node.DataType = mgr.GetDataType(new NodeId("enum", 2));
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
    }
}

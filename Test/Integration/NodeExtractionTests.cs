using Cognite.OpcUa;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    public sealed class NodeExtractionTestFixture : BaseExtractorTestFixture
    {
        public NodeExtractionTestFixture() : base(63200) { }
    }

    // Tests for various configurations for extracting nodes and pushing to dummy pusher
    public class NodeExtractionTests : MakeConsoleWork, IClassFixture<NodeExtractionTestFixture>
    {
        NodeExtractionTestFixture tester;
        public NodeExtractionTests(ITestOutputHelper output, NodeExtractionTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        #region datatypeconfig
        [Fact]
        public async Task TestNoDataTypeConfig()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            await extractor.RunExtractor(true);

            Assert.Equal(3, pusher.PushedNodes.Count);
            var node = pusher.PushedNodes[ids.Root];
            Assert.Equal("CustomRoot", node.DisplayName);
            Assert.True(node.ParentId == null || node.ParentId.IsNullNodeId);
            Assert.True(node.Properties == null || !node.Properties.Any());
            Assert.False(node.IsVariable);

            node = pusher.PushedNodes[ids.Obj1];
            Assert.Equal("ChildObject", node.DisplayName);
            Assert.Equal(ids.Root, node.ParentId);
            Assert.True(node.Properties == null || !node.Properties.Any());
            Assert.False(node.IsVariable);

            node = pusher.PushedNodes[ids.Obj2];
            Assert.Equal("ChildObject2", node.DisplayName);
            Assert.Equal(ids.Root, node.ParentId);
            Assert.Equal(2, node.Properties.Count);
            var prop = node.Properties.First(prop => prop.DisplayName == "NumericProp");
            Assert.Equal(DataTypeIds.Int64, prop.DataType.Raw);
            Assert.Equal("1234", prop.Value.StringValue);
            prop = node.Properties.First(prop => prop.DisplayName == "StringProp");
            Assert.Equal(DataTypeIds.String, prop.DataType.Raw);
            Assert.Equal("String prop value", prop.Value.StringValue);
            Assert.False(node.IsVariable);

            // No normal datatypes here
            Assert.Empty(pusher.PushedVariables);
        }

        [Fact]
        public async Task TestAllowStringTypes()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            await extractor.RunExtractor(true);

            Assert.Equal(3, pusher.PushedNodes.Count);
            Assert.Equal(6, pusher.PushedVariables.Count);
            Assert.All(pusher.PushedVariables.Values, variable => Assert.True(variable.DataType.IsString));

            var vnode = pusher.PushedVariables[(ids.MysteryVar, -1)];
            Assert.Equal("MysteryVar", vnode.DisplayName);
            Assert.Equal(ids.MysteryType, vnode.DataType.Raw);
            Assert.Equal(ids.Root, vnode.ParentId);
            Assert.Equal(2, vnode.Properties.Count);
            var prop = vnode.Properties.First(prop => prop.DisplayName == "EngineeringUnits");
            Assert.Equal(DataTypeIds.EUInformation, prop.DataType.Raw);
            Assert.Equal("°C: degree Celsius", prop.Value.StringValue);
            prop = vnode.Properties.First(prop => prop.DisplayName == "EURange");
            Assert.Equal(DataTypeIds.Range, prop.DataType.Raw);
            Assert.Equal("(0, 100)", prop.Value.StringValue);

            Assert.All(pusher.PushedVariables.Values.Where(variable => variable.DisplayName != "MysteryVar"),
                variable => Assert.True(variable.Properties == null || !variable.Properties.Any()));

            dataTypes.AllowStringVariables = false;
        }
        [Fact]
        public async Task TestAllowSmallArrays()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = 2;
            await extractor.RunExtractor(true);

            Assert.Equal(4, pusher.PushedNodes.Count);
            Assert.Equal(8, pusher.PushedVariables.Count);
            Assert.All(pusher.PushedVariables.Values, variable => Assert.True(variable.DataType.IsString));

            var node = pusher.PushedNodes[ids.StringArray];
            Assert.Equal("Variable StringArray", node.DisplayName);
            var arr = Assert.IsType<UAVariable>(node);
            Assert.True(arr.IsVariable);
            Assert.True(arr.Properties == null || !arr.Properties.Any());
            Assert.True(arr.IsArray);
            Assert.Equal(2, arr.ArrayDimensions[0]);
            Assert.Equal(2, arr.ArrayChildren.Count());
            Assert.Equal(DataTypeIds.String, arr.DataType.Raw);
            Assert.Equal(-1, arr.Index);

            var vnode = pusher.PushedVariables[(ids.StringArray, 0)];
            Assert.True(vnode.IsArray);
            Assert.Equal(0, vnode.Index);
            Assert.Contains(vnode, arr.ArrayChildren);
            Assert.Equal(2, vnode.ArrayDimensions[0]);
            Assert.Equal(arr, vnode.ArrayParent);
            Assert.Equal(DataTypeIds.String, vnode.DataType.Raw);
            vnode = pusher.PushedVariables[(ids.StringArray, 1)];
            Assert.Contains(vnode, arr.ArrayChildren);
            Assert.Equal(1, vnode.Index);
            Assert.False(pusher.PushedVariables.ContainsKey((ids.StringArray, 2)));

            dataTypes.AllowStringVariables = false;
            dataTypes.MaxArraySize = 0;
        }
        [Fact]
        public async Task TestAllowLargerArrays()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = -1;
            await extractor.RunExtractor(true);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(16, pusher.PushedVariables.Count);
            Assert.All(pusher.PushedVariables.Values, variable =>
                Assert.True(variable.DataType.IsString
                    || variable.DisplayName.StartsWith("Variable Array", StringComparison.InvariantCulture)));

            var node = pusher.PushedNodes[ids.Array];
            Assert.Equal("Variable Array", node.DisplayName);
            var arr = Assert.IsType<UAVariable>(node);
            Assert.True(arr.IsVariable);
            Assert.Equal(2, arr.Properties.Count);
            var prop = arr.Properties.First(prop => prop.DisplayName == "EngineeringUnits");
            Assert.Equal(DataTypeIds.EUInformation, prop.DataType.Raw);
            Assert.Equal("°C: degree Celsius", prop.Value.StringValue);
            prop = arr.Properties.First(prop => prop.DisplayName == "EURange");
            Assert.Equal(DataTypeIds.Range, prop.DataType.Raw);
            Assert.Equal("(0, 100)", prop.Value.StringValue);
            Assert.True(arr.IsArray);
            Assert.Equal(4, arr.ArrayDimensions[0]);
            Assert.Equal(4, arr.ArrayChildren.Count());
            Assert.Equal(DataTypeIds.Double, arr.DataType.Raw);

            dataTypes.AllowStringVariables = false;
            dataTypes.MaxArraySize = 0;
        }
        [Fact]
        public async Task TestAutoIdentifyTypes()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = -1;
            dataTypes.AutoIdentifyTypes = true;
            await extractor.RunExtractor(true);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(16, pusher.PushedVariables.Count);
            Assert.Equal(6, pusher.PushedVariables.Count(vb => vb.Value.DataType.EnumValues != null));
            Assert.Equal(5, pusher.PushedVariables.Count(vb => vb.Value.DataType.IsString));

            var enumv = pusher.PushedVariables[(ids.EnumVar1, -1)];
            Assert.Equal(ids.EnumType1, enumv.DataType.Raw);
            Assert.Equal(3, enumv.DataType.EnumValues.Count);
            var dp = enumv.DataType.ToDataPoint(extractor, 1, DateTime.UtcNow, "test");
            Assert.Equal(1, dp.DoubleValue);

            var node = pusher.PushedNodes[ids.EnumVar3];
            var enumArr = Assert.IsType<UAVariable>(node);
            Assert.Equal(ids.EnumType2, enumArr.DataType.Raw);
            Assert.Equal(2, enumArr.DataType.EnumValues.Count);
            dp = enumArr.DataType.ToDataPoint(extractor, 123, DateTime.UtcNow, "test");
            Assert.Equal(123, dp.DoubleValue);

            var vnode = pusher.PushedVariables[(ids.MysteryVar, -1)];
            Assert.False(vnode.DataType.IsString);

            dataTypes.AllowStringVariables = false;
            dataTypes.MaxArraySize = 0;
            dataTypes.AutoIdentifyTypes = false;
        }

        [Fact]
        public async Task TestEnumsAsStrings()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = -1;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.EnumsAsStrings = true;
            await extractor.RunExtractor(true);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(16, pusher.PushedVariables.Count);
            Assert.Equal(6, pusher.PushedVariables.Count(vb => vb.Value.DataType.EnumValues != null));
            Assert.Equal(11, pusher.PushedVariables.Count(vb => vb.Value.DataType.IsString));

            var enumv = pusher.PushedVariables[(ids.EnumVar1, -1)];
            Assert.Equal(ids.EnumType1, enumv.DataType.Raw);
            Assert.Equal(3, enumv.DataType.EnumValues.Count);
            var dp = enumv.DataType.ToDataPoint(extractor, 1, DateTime.UtcNow, "test");
            Assert.Equal("Enum2", dp.StringValue);

            var node = pusher.PushedNodes[ids.EnumVar3];
            var enumArr = Assert.IsType<UAVariable>(node);
            Assert.Equal(ids.EnumType2, enumArr.DataType.Raw);
            Assert.Equal(2, enumArr.DataType.EnumValues.Count);
            dp = enumArr.DataType.ToDataPoint(extractor, 123, DateTime.UtcNow, "test");
            Assert.Equal("VEnum2", dp.StringValue);

            var vnode = pusher.PushedVariables[(ids.MysteryVar, -1)];
            Assert.False(vnode.DataType.IsString);

            dataTypes.AllowStringVariables = false;
            dataTypes.MaxArraySize = 0;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.EnumsAsStrings = false;
        }
        [Fact]
        public async Task TestIgnoreDataType()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = -1;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.IgnoreDataTypes = new List<ProtoNodeId> { CommonTestUtils.ToProtoNodeId(ids.IgnoreType, tester.Client) };
            await extractor.RunExtractor(true);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(15, pusher.PushedVariables.Count);

            Assert.False(pusher.PushedVariables.ContainsKey((ids.IgnoreVar, -1)));

            dataTypes.AllowStringVariables = false;
            dataTypes.MaxArraySize = 0;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.IgnoreDataTypes = null;
        }
        [Fact]
        public async Task TestNullDataType()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Wrong;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.AutoIdentifyTypes = true;
            await extractor.RunExtractor(true);

            Assert.Single(pusher.PushedNodes);
            Assert.Single(pusher.PushedVariables);

            var node = pusher.PushedVariables.Values.First();
            Assert.Equal("NullType", node.DisplayName);
            Assert.Equal(NodeId.Null, node.DataType.Raw);
            Assert.True(node.DataType.IsString);

            tester.Client.ResetVisitedNodes();
            tester.Client.DataTypeManager.Reset();
            pusher.Wipe();
            dataTypes.NullAsNumeric = true;
            await extractor.RunExtractor(true);
            Assert.Single(pusher.PushedNodes);
            Assert.Single(pusher.PushedVariables);

            node = pusher.PushedVariables.Values.First();
            Assert.Equal("NullType", node.DisplayName);
            Assert.Equal(NodeId.Null, node.DataType.Raw);
            Assert.False(node.DataType.IsString);

            dataTypes.AllowStringVariables = false;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.NullAsNumeric = false;
        }
        [Fact]
        public async Task TestUnknownAsScalar()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Wrong;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.UnknownAsScalar = true;
            dataTypes.MaxArraySize = -1;
            await extractor.RunExtractor(true);

            Assert.Equal(3, pusher.PushedNodes.Count);
            Assert.Equal(10, pusher.PushedVariables.Count);

            var vnode = pusher.PushedVariables[(ids.RankImpreciseNoDim, -1)];
            Assert.Equal("RankImpreciseNoDim", vnode.DisplayName);

            var node = pusher.PushedNodes[ids.RankImprecise];
            var arr = Assert.IsType<UAVariable>(node);
            Assert.Equal(4, arr.ArrayDimensions[0]);

            dataTypes.AllowStringVariables = false;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.UnknownAsScalar = false;
            dataTypes.MaxArraySize = 0;
        }

        #endregion
    }
}

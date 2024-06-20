using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    public sealed class NodeExtractionTestFixture : BaseExtractorTestFixture
    {
        public NodeExtractionTestFixture() : base()
        {
        }
    }

    // Tests for various configurations for extracting nodes and pushing to dummy pusher
    public class NodeExtractionTests : IClassFixture<NodeExtractionTestFixture>
    {
        private readonly NodeExtractionTestFixture tester;
        private readonly ITestOutputHelper _output;

        public NodeExtractionTests(ITestOutputHelper output, NodeExtractionTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.Init(output);
            tester.ResetConfig();
            tester.Config.History.Enabled = false;
            tester.Client.TypeManager.Reset();
            _output = output;
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
            Assert.Equal("CustomRoot", node.Name);
            Assert.True(node.ParentId.IsNullNodeId);
            Assert.True(node.Properties == null || !node.Properties.Any());
            Assert.True(node is UAObject);

            node = pusher.PushedNodes[ids.Obj1];
            Assert.Equal("ChildObject", node.Name);
            Assert.Equal(ids.Root, node.ParentId);
            Assert.True(node.Properties == null || !node.Properties.Any());
            Assert.True(node is UAObject);

            node = pusher.PushedNodes[ids.Obj2];
            Assert.Equal("ChildObject2", node.Name);
            Assert.Equal(ids.Root, node.ParentId);
            Assert.Equal(2, node.Properties.Count());
            var prop = node.Properties.First(prop => prop.Name == "NumericProp") as UAVariable;
            Assert.Equal(DataTypeIds.Int64, prop.FullAttributes.DataType.Id);
            Assert.Equal(new Variant(1234L), prop.Value);
            prop = node.Properties.First(prop => prop.Name == "StringProp") as UAVariable;
            Assert.Equal(DataTypeIds.String, prop.FullAttributes.DataType.Id);
            Assert.Equal(new Variant("String prop value"), prop.Value);
            Assert.True(node is UAObject);

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
            Assert.All(pusher.PushedVariables.Values, variable => Assert.True(variable.FullAttributes.DataType.IsString));

            var vnode = pusher.PushedVariables[(ids.MysteryVar, -1)];
            Assert.Equal("MysteryVar", vnode.Name);
            Assert.Equal(ids.MysteryType, vnode.FullAttributes.DataType.Id);
            Assert.Equal(ids.Root, vnode.ParentId);
            Assert.Equal(2, vnode.Properties.Count());
            var prop = vnode.Properties.First(prop => prop.Name == "EngineeringUnits") as UAVariable;
            Assert.Equal(DataTypeIds.EUInformation, prop.FullAttributes.DataType.Id);
            Assert.Equal("°C: degree Celsius", extractor.StringConverter.ConvertToString(prop.Value));
            prop = vnode.Properties.First(prop => prop.Name == "EURange") as UAVariable;
            Assert.Equal(DataTypeIds.Range, prop.FullAttributes.DataType.Id);
            Assert.Equal("(0, 100)", extractor.StringConverter.ConvertToString(prop.Value));

            Assert.All(pusher.PushedVariables.Values.Where(variable => variable.Name != "MysteryVar"
                && variable.Name != "NumberVar"),
                variable => Assert.True(variable.Properties == null || !variable.Properties.Any()));
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
            Assert.All(pusher.PushedVariables.Values, variable => Assert.True(variable.FullAttributes.DataType.IsString));

            var node = pusher.PushedNodes[ids.StringArray];
            Assert.Equal("Variable StringArray", node.Name);
            var arr = Assert.IsType<UAVariable>(node);
            Assert.NotNull(arr);
            Assert.True(arr.Properties == null || !arr.Properties.Any());
            Assert.True(arr.IsArray);
            Assert.Equal(2, arr.ArrayDimensions[0]);
            Assert.Equal(2, arr.ArrayChildren.Count());
            Assert.Equal(DataTypeIds.String, arr.FullAttributes.DataType.Id);
            Assert.False(arr is UAVariableMember);

            var vnode = pusher.PushedVariables[(ids.StringArray, 0)];
            Assert.True(vnode.IsArray);
            Assert.Equal(0, (vnode as UAVariableMember).Index);
            Assert.Contains(vnode, arr.ArrayChildren);
            Assert.Equal(2, vnode.ArrayDimensions[0]);
            Assert.Equal(arr, (vnode as UAVariableMember).TSParent);
            Assert.Equal(DataTypeIds.String, vnode.FullAttributes.DataType.Id);
            vnode = pusher.PushedVariables[(ids.StringArray, 1)];
            Assert.Contains(vnode, arr.ArrayChildren);
            Assert.Equal(1, (vnode as UAVariableMember).Index);
            Assert.False(pusher.PushedVariables.ContainsKey((ids.StringArray, 2)));
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
                Assert.True(variable.FullAttributes.DataType.IsString
                    || variable.Name.StartsWith("Variable Array", StringComparison.InvariantCulture)));

            var node = pusher.PushedNodes[ids.Array];
            Assert.Equal("Variable Array", node.Name);
            var arr = Assert.IsType<UAVariable>(node);
            Assert.NotNull(arr);
            Assert.Equal(2, arr.Properties.Count());
            var prop = arr.Properties.First(prop => prop.Name == "EngineeringUnits") as UAVariable;
            Assert.Equal(DataTypeIds.EUInformation, prop.FullAttributes.DataType.Id);
            Assert.Equal("°C: degree Celsius", extractor.StringConverter.ConvertToString(prop.Value));
            prop = arr.Properties.First(prop => prop.Name == "EURange") as UAVariable;
            Assert.Equal(DataTypeIds.Range, prop.FullAttributes.DataType.Id);
            Assert.Equal("(0, 100)", extractor.StringConverter.ConvertToString(prop.Value));
            Assert.True(arr.IsArray);
            Assert.Equal(4, arr.ArrayDimensions[0]);
            Assert.Equal(4, arr.ArrayChildren.Count());
            Assert.Equal(DataTypeIds.Double, arr.FullAttributes.DataType.Id);
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
            Assert.Equal(6, pusher.PushedVariables.Count(vb => vb.Value.FullAttributes.DataType.EnumValues != null));
            Assert.Equal(5, pusher.PushedVariables.Count(vb => vb.Value.FullAttributes.DataType.IsString));

            var enumv = pusher.PushedVariables[(ids.EnumVar1, -1)];
            Assert.Equal(ids.EnumType1, enumv.FullAttributes.DataType.Id);
            Assert.Equal(3, enumv.FullAttributes.DataType.EnumValues.Count);
            var dp = enumv.FullAttributes.DataType.ToDataPoint(extractor, 1, DateTime.UtcNow, "test", StatusCodes.Good);
            Assert.Equal(1, dp.DoubleValue);

            var node = pusher.PushedNodes[ids.EnumVar3];
            var enumArr = Assert.IsType<UAVariable>(node);
            Assert.Equal(ids.EnumType2, enumArr.FullAttributes.DataType.Id);
            Assert.Equal(2, enumArr.FullAttributes.DataType.EnumValues.Count);
            dp = enumArr.FullAttributes.DataType.ToDataPoint(extractor, 123, DateTime.UtcNow, "test", StatusCodes.Good);
            Assert.Equal(123, dp.DoubleValue);

            var vnode = pusher.PushedVariables[(ids.MysteryVar, -1)];
            Assert.False(vnode.FullAttributes.DataType.IsString);

            var nnode = pusher.PushedVariables[(ids.NumberVar, -1)];
            Assert.Equal(4, nnode.GetAllProperties().Count());
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
            Assert.Equal(6, pusher.PushedVariables.Count(vb => vb.Value.FullAttributes.DataType.EnumValues != null));
            Assert.Equal(11, pusher.PushedVariables.Count(vb => vb.Value.FullAttributes.DataType.IsString));

            var enumv = pusher.PushedVariables[(ids.EnumVar1, -1)];
            Assert.Equal(ids.EnumType1, enumv.FullAttributes.DataType.Id);
            Assert.Equal(3, enumv.FullAttributes.DataType.EnumValues.Count);
            var dp = enumv.FullAttributes.DataType.ToDataPoint(extractor, 1, DateTime.UtcNow, "test", StatusCodes.Good);
            Assert.Equal("Enum2", dp.StringValue);

            var node = pusher.PushedNodes[ids.EnumVar3];
            var enumArr = Assert.IsType<UAVariable>(node);
            Assert.Equal(ids.EnumType2, enumArr.FullAttributes.DataType.Id);
            Assert.Equal(2, enumArr.FullAttributes.DataType.EnumValues.Count);
            dp = enumArr.FullAttributes.DataType.ToDataPoint(extractor, 123, DateTime.UtcNow, "test", StatusCodes.Good);
            Assert.Equal("VEnum2", dp.StringValue);

            var vnode = pusher.PushedVariables[(ids.MysteryVar, -1)];
            Assert.False(vnode.FullAttributes.DataType.IsString);
        }
        [Fact]
        public async Task TestIgnoreDataType()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = -1;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.IgnoreDataTypes = new List<ProtoNodeId> { CommonTestUtils.ToProtoNodeId(ids.IgnoreType, tester.Client) };
            using var extractor = tester.BuildExtractor(true, null, pusher);
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
        public async Task TestCustomDataType()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = -1;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.CustomNumericTypes = new List<ProtoDataType> {
                new ProtoDataType
                {
                    IsStep = true,
                    NodeId = CommonTestUtils.ToProtoNodeId(ids.NumberType, tester.Client)
                }
            };
            await extractor.RunExtractor(true);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(16, pusher.PushedVariables.Count);

            var node = pusher.PushedVariables[(ids.NumberVar, -1)];
            Assert.True(node.FullAttributes.DataType.IsStep);
            Assert.False(node.FullAttributes.DataType.IsString);
            Assert.Equal(ids.NumberType, node.FullAttributes.DataType.Id);
        }
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestNullDataType(bool nullAsNumeric)
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Wrong;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var dataTypes = tester.Config.Extraction.DataTypes;

            dataTypes.AllowStringVariables = true;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.NullAsNumeric = nullAsNumeric;
            await extractor.RunExtractor(true);

            Assert.Single(pusher.PushedNodes);
            Assert.Single(pusher.PushedVariables);

            var node = pusher.PushedVariables.Values.First();
            Assert.Equal("NullType", node.Name);
            Assert.Equal(NodeId.Null, node.FullAttributes.DataType.Id);
            Assert.NotEqual(nullAsNumeric, node.FullAttributes.DataType.IsString);

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
            Assert.Equal("RankImpreciseNoDim", vnode.Name);

            var node = pusher.PushedNodes[ids.RankImprecise];
            var arr = Assert.IsType<UAVariable>(node);
            Assert.Equal(4, arr.ArrayDimensions[0]);
        }

        #endregion

        #region structureconfig
        [Fact]
        public async Task TestIgnoreName()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var extraction = tester.Config.Extraction;

            extraction.IgnoreName = new[] { "Child", "ChildObject", "Variable", "IgnoreVar", "Variable Array", "EURange", "NumericProp" };
            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;

            await extractor.RunExtractor(true);

            Assert.Equal(4, pusher.PushedNodes.Count);
            Assert.Equal(11, pusher.PushedVariables.Count);
            Assert.True(pusher.PushedNodes.ContainsKey(ids.StringArray));
            Assert.False(pusher.PushedNodes.ContainsKey(ids.Array));
            Assert.True(pusher.PushedVariables.ContainsKey((ids.StringArray, 1)));
            Assert.True(pusher.PushedVariables.ContainsKey((ids.MysteryVar, -1)));
            Assert.False(pusher.PushedVariables.ContainsKey((ids.IgnoreVar, -1)));
            Assert.False(pusher.PushedVariables.ContainsKey((ids.Array, 1)));

            var vnode = pusher.PushedVariables[(ids.MysteryVar, -1)];
            Assert.Single(vnode.Properties);
            Assert.DoesNotContain(vnode.Properties, prop => prop.Name == "EURange");

            var node = pusher.PushedNodes[ids.Obj2];
            Assert.Single(node.Properties);
        }
        [Fact]
        public async Task TestIgnoreNamePrefix()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);
            var extraction = tester.Config.Extraction;

            extraction.IgnoreNamePrefix = new[] { "Variable", "Ignore", "EUR", "Numeric" };
            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;

            await extractor.RunExtractor(true);

            Assert.Equal(4, pusher.PushedNodes.Count);
            Assert.Equal(9, pusher.PushedVariables.Count);
            Assert.False(pusher.PushedNodes.ContainsKey(ids.StringArray));
            Assert.False(pusher.PushedNodes.ContainsKey(ids.Array));
            Assert.False(pusher.PushedVariables.ContainsKey((ids.StringArray, 1)));
            Assert.True(pusher.PushedVariables.ContainsKey((ids.MysteryVar, -1)));
            Assert.False(pusher.PushedVariables.ContainsKey((ids.IgnoreVar, -1)));
            Assert.False(pusher.PushedVariables.ContainsKey((ids.Array, 1)));

            var vnode = pusher.PushedVariables[(ids.MysteryVar, -1)];
            Assert.Single(vnode.Properties);
            Assert.DoesNotContain(vnode.Properties, prop => prop.Name == "EURange");

            var node = pusher.PushedNodes[ids.Obj2];
            Assert.Single(node.Properties);
        }
        [Fact]
        public async Task TestPropertyNameFilter()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            extraction.PropertyNameFilter = "ble Str|ble Arr|r[0-9]$";
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;

            await extractor.RunExtractor(true);

            Assert.Equal(3, pusher.PushedNodes.Count);
            Assert.Equal(4, pusher.PushedVariables.Count);
            Assert.False(pusher.PushedNodes.ContainsKey(ids.StringArray));
            Assert.False(pusher.PushedNodes.ContainsKey(ids.Array));
            Assert.False(pusher.PushedVariables.ContainsKey((ids.StringArray, 1)));
            Assert.True(pusher.PushedVariables.ContainsKey((ids.MysteryVar, -1)));
            Assert.False(pusher.PushedVariables.ContainsKey((ids.Array, 1)));

            var node = pusher.PushedNodes[ids.Root];
            Assert.Equal(5, node.Properties.Count());
            var prop = node.Properties.First(prop => prop.Name == "Variable StringArray") as UAVariable;
            Assert.Equal(@"[""test1"",""test2""]", extractor.StringConverter.ConvertToString(prop.Value));
            prop = node.Properties.First(prop => prop.Name == "Variable Array") as UAVariable;
            Assert.Equal("[0,0,0,0]", extractor.StringConverter.ConvertToString(prop.Value));
            prop = node.Properties.First(prop => prop.Name == "EnumVar1") as UAVariable;
            Assert.Equal("Enum2", extractor.StringConverter.ConvertToString(prop.Value, prop.FullAttributes.DataType.EnumValues));
            prop = node.Properties.First(prop => prop.Name == "EnumVar2") as UAVariable;
            Assert.Equal("VEnum2", extractor.StringConverter.ConvertToString(prop.Value, prop.FullAttributes.DataType.EnumValues));
            prop = node.Properties.First(prop => prop.Name == "EnumVar3") as UAVariable;
            Assert.Equal(@"[""VEnum2"",""VEnum2"",""VEnum1"",""VEnum2""]",
                extractor.StringConverter.ConvertToString(prop.Value, prop.FullAttributes.DataType.EnumValues));
        }
        [Fact]
        public async Task TestPropertyIdFilter()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            extraction.PropertyIdFilter = "enum";
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;

            await extractor.RunExtractor(true);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(15, pusher.PushedVariables.Count);
            Assert.False(pusher.PushedVariables.ContainsKey((ids.EnumVar2, -1)));

            var node = pusher.PushedNodes[ids.Root];
            Assert.Single(node.Properties);
            var prop = node.Properties.First(prop => prop.Name == "EnumVar2") as UAVariable;
            Assert.Equal("VEnum2", extractor.StringConverter.ConvertToString(prop.Value, prop.FullAttributes.DataType.EnumValues));
        }
        [Fact]
        public async Task TestMultipleSourceNodes()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            // Duplicates should be handled
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);
            tester.Config.Extraction.RootNodes = new[]
            {
                CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client),
                CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Base.Root, tester.Client),
            };

            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;

            await extractor.RunExtractor(true);

            Assert.Equal(7, pusher.PushedNodes.Count);
            Assert.Equal(21, pusher.PushedVariables.Count);
        }
        [Fact]
        public async Task TestMapVariableChildren()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);

            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;
            extraction.MapVariableChildren = true;

            await extractor.RunExtractor(true);

            Assert.Equal(9, pusher.PushedNodes.Count);
            Assert.Equal(16, pusher.PushedVariables.Count);

            var numberVar = pusher.PushedVariables[(ids.NumberVar, -1)];
            var numberVarObj = pusher.PushedNodes[ids.NumberVar];
            Assert.Equal(ids.NumberVar, numberVar.ParentId);
        }

        [Fact]
        public async Task TestMapVariableChildrenSkipParent()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);

            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;
            extraction.MapVariableChildren = true;
            extraction.DataTypes.IgnoreDataTypes = new[]
            {
                CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.NumberType, tester.Client)
            };

            await extractor.RunExtractor(true);

            Assert.Equal(9, pusher.PushedNodes.Count);
            Assert.Equal(15, pusher.PushedVariables.Count);

            var numberVarObj = pusher.PushedNodes[ids.NumberVar];
            Assert.False(pusher.PushedVariables.ContainsKey((ids.NumberVar, -1)));

        }
        #endregion

        #region custommetadata
        [Fact]
        public async Task TestExtraMetadata()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var ids = tester.Server.Ids.Custom;
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ids.Root, tester.Client);

            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;
            extraction.DataTypes.DataTypeMetadata = true;
            extraction.NodeTypes.Metadata = true;

            await extractor.RunExtractor(true);

            Assert.Equal(6, pusher.PushedNodes.Count);
            Assert.Equal(16, pusher.PushedVariables.Count);

            var node = pusher.PushedNodes[ids.Root];
            var metadata = node.GetExtraMetadata(tester.Config, extractor.Context, extractor.StringConverter);
            Assert.Single(metadata);
            Assert.Equal("BaseObjectType", metadata["TypeDefinition"]);

            node = pusher.PushedNodes[ids.Array];
            metadata = node.GetExtraMetadata(tester.Config, extractor.Context, extractor.StringConverter);
            Assert.Equal(2, metadata.Count);
            Assert.Equal("BaseDataVariableType", metadata["TypeDefinition"]);
            Assert.Equal("Double", metadata["dataType"]);

            node = pusher.PushedNodes[ids.EnumVar3];
            metadata = node.GetExtraMetadata(tester.Config, extractor.Context, extractor.StringConverter);
            Assert.Equal(4, metadata.Count);
            Assert.Equal("BaseDataVariableType", metadata["TypeDefinition"]);
            Assert.Equal("CustomEnumType2", metadata["dataType"]);
            Assert.Equal("VEnum1", metadata["321"]);
            Assert.Equal("VEnum2", metadata["123"]);

            var vb = pusher.PushedVariables[(ids.EnumVar3, 1)];
            metadata = node.GetExtraMetadata(tester.Config, extractor.Context, extractor.StringConverter);
            Assert.Equal(4, metadata.Count);
            Assert.Equal("BaseDataVariableType", metadata["TypeDefinition"]);
            Assert.Equal("CustomEnumType2", metadata["dataType"]);
            Assert.Equal("VEnum1", metadata["321"]);
            Assert.Equal("VEnum2", metadata["123"]);

            vb = pusher.PushedVariables[(ids.MysteryVar, -1)];
            metadata = vb.GetExtraMetadata(tester.Config, extractor.Context, extractor.StringConverter);
            Assert.Equal(2, metadata.Count);
            Assert.Equal("BaseDataVariableType", metadata["TypeDefinition"]);
            Assert.Equal("MysteryType", metadata["dataType"]);
        }
        #endregion

        #region references
        private async Task RunReferenceExtraction(UAExtractor extractor)
        {
            var dataTypes = tester.Config.Extraction.DataTypes;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);
            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = 4;
            dataTypes.AutoIdentifyTypes = true;
            dataTypes.IgnoreDataTypes = new[]
            {
                CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.IgnoreType, tester.Client)
            };

            await extractor.RunExtractor(true);

            dataTypes.AllowStringVariables = false;
            dataTypes.MaxArraySize = 0;
            dataTypes.AutoIdentifyTypes = false;
            dataTypes.IgnoreDataTypes = null;
        }
        [Fact]
        public async Task TestBasicReferences()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.Relationships.Enabled = true;

            using var extractor = tester.BuildExtractor(true, null, pusher);
            await RunReferenceExtraction(extractor);

            Assert.Equal(8, pusher.PushedReferences.Count);
            Assert.Equal(4, pusher.PushedReferences.Count(rel => rel.IsForward));

            Assert.All(pusher.PushedReferences, rel =>
            {
                Assert.NotNull(rel.Source);
                Assert.NotNull(rel.Target);
                Assert.False(rel.Source.Id.IsNullNodeId);
                Assert.False(rel.Target.Id.IsNullNodeId);
                Assert.NotNull(rel.Type);
                Assert.NotNull(rel.Type.Id);
                Assert.Contains(pusher.PushedReferences, orel => orel.Source.Id == rel.Target.Id
                    && orel.Target.Id == rel.Source.Id && orel.IsForward == !rel.IsForward);
            });
        }
        [Fact]
        public async Task TestHierarchicalReferences()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;

            using var extractor = tester.BuildExtractor(true, null, pusher);
            await RunReferenceExtraction(extractor);

            foreach (var rf in pusher.PushedReferences)
            {
                tester.Log.LogDebug("{S}", rf);
            }
            Assert.Equal(18, pusher.PushedReferences.Count);
            Assert.Equal(14, pusher.PushedReferences.Count(rel => rel.IsForward));

            Assert.All(pusher.PushedReferences, rel =>
            {
                Assert.NotNull(rel.Source);
                Assert.NotNull(rel.Target);
                Assert.False(rel.Source.Id.IsNullNodeId);
                Assert.False(rel.Target.Id.IsNullNodeId);
                Assert.NotNull(rel.Type);
                Assert.NotNull(rel.Type.Id);
            });
        }
        [Fact]
        public async Task TestInverseHierarchicalReferences()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.Relationships.InverseHierarchical = true;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await RunReferenceExtraction(extractor);

            Assert.Equal(28, pusher.PushedReferences.Count);
            Assert.Equal(14, pusher.PushedReferences.Count(rel => rel.IsForward));
            Assert.All(pusher.PushedReferences, rel =>
            {
                Assert.NotNull(rel.Source);
                Assert.NotNull(rel.Target);
                Assert.False(rel.Source.Id.IsNullNodeId);
                Assert.False(rel.Target.Id.IsNullNodeId);
                Assert.NotNull(rel.Type);
                Assert.NotNull(rel.Type.Id);
                Assert.Contains(pusher.PushedReferences, orel => orel.Source.Id == rel.Target.Id
                    && orel.Target.Id == rel.Source.Id && orel.IsForward == !rel.IsForward);
            });
        }
        #endregion

        #region lateinit
        [Fact]
        public async Task TestLateInitInitialFail()
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.Relationships.InverseHierarchical = true;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            pusher.NoInit = true;
            pusher.TestConnectionResult = false;

            var dataTypes = tester.Config.Extraction.DataTypes;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);
            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = 4;
            dataTypes.AutoIdentifyTypes = true;

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            Assert.Empty(pusher.PushedNodes);
            Assert.Empty(pusher.PushedVariables);
            Assert.Equal(6, pusher.PendingNodes.Objects.Count());
            Assert.Equal(16, pusher.PendingNodes.Variables.Count());
            Assert.Equal(32, pusher.PendingNodes.References.Count());

            Assert.False(pusher.Initialized);

            pusher.TestConnectionResult = true;

            await TestUtils.WaitForCondition(() =>
                pusher.PushedNodes.Count == 6
                && pusher.PushedReferences.Count == 32
                && pusher.PushedVariables.Count == 16, 5, () => $"Expected to get 6 nodes, got {pusher.PushedNodes.Count}, " +
                    $"16 variables and got {pusher.PushedVariables.Count}, 32 references and got {pusher.PushedReferences.Count}");

            Assert.False(pusher.NoInit);
            Assert.True(pusher.Initialized);

            await extractor.Close(false);

            var comp = await Task.WhenAny(runTask, Task.Delay(10000));
            Assert.Equal(comp, runTask);
        }
        [Theory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task TestLateInitLateFail(bool failNodes, bool failReferences)
        {
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.Relationships.InverseHierarchical = true;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            pusher.NoInit = false;
            pusher.TestConnectionResult = false;
            pusher.PushNodesResult = !failNodes;
            pusher.PushReferenceResult = !failReferences;

            var dataTypes = tester.Config.Extraction.DataTypes;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);
            dataTypes.AllowStringVariables = true;
            dataTypes.MaxArraySize = 4;
            dataTypes.AutoIdentifyTypes = true;

            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            if (failNodes)
            {
                Assert.Empty(pusher.PushedNodes);
                Assert.Empty(pusher.PushedVariables);
                Assert.Equal(6, pusher.PendingNodes.Objects.Count());
                Assert.Equal(16, pusher.PendingNodes.Variables.Count());
            }
            if (failReferences)
            {
                Assert.Empty(pusher.PushedReferences);
                Assert.Equal(32, pusher.PendingNodes.References.Count());
            }


            Assert.False(pusher.Initialized);

            pusher.TestConnectionResult = true;
            pusher.PushNodesResult = true;
            pusher.PushReferenceResult = true;

            await TestUtils.WaitForCondition(() =>
                pusher.PushedNodes.Count == 6
                && pusher.PushedReferences.Count == 32
                && pusher.PushedVariables.Count == 16, 5, () => $"Expected to get 6 nodes, got {pusher.PushedNodes.Count}, " +
                    $"16 variables and got {pusher.PushedVariables.Count}, 32 references and got {pusher.PushedReferences.Count}");

            Assert.True(pusher.Initialized);

            await extractor.Close(false);

            await runTask;
        }
        #endregion

        #region updates
        [Theory]
        [InlineData(true, true, true, true, false, false, false, false)]
        [InlineData(false, false, false, false, true, true, true, true)]
        [InlineData(true, false, true, false, true, false, true, false)]
        [InlineData(false, true, false, true, false, true, false, true)]
        [InlineData(true, true, true, true, true, true, true, true)]
        public async Task TestUpdateFields(
            bool assetName, bool variableName,
            bool assetDesc, bool variableDesc,
            bool assetContext, bool variableContext,
            bool assetMeta, bool variableMeta)
        {
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Assets = true,
                    Timeseries = true
                }
            };
            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            var upd = tester.Config.Extraction.Update;
            upd.Objects.Name = assetName;
            upd.Objects.Description = assetDesc;
            upd.Objects.Context = assetContext;
            upd.Objects.Metadata = assetMeta;
            upd.Variables.Name = variableName;
            upd.Variables.Description = variableDesc;
            upd.Variables.Context = variableContext;
            upd.Variables.Metadata = variableMeta;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.History.Enabled = false;

            var runTask = extractor.RunExtractor();

            await TestUtils.WaitForCondition(() => handler.Assets.Count != 0 && handler.Timeseries.Count != 0, 5);

            CommonTestUtils.VerifyStartingConditions(handler.Assets, handler.Timeseries, null, extractor, tester.Server.Ids.Custom, false);

            tester.Server.ModifyCustomServer();

            var rebrowseTask = extractor.Rebrowse();
            await Task.WhenAny(rebrowseTask, Task.Delay(10000));
            Assert.True(rebrowseTask.IsCompleted);

            CommonTestUtils.VerifyStartingConditions(handler.Assets, handler.Timeseries, upd, extractor, tester.Server.Ids.Custom, false);
            CommonTestUtils.VerifyModified(handler.Assets, handler.Timeseries, upd, extractor, tester.Server.Ids.Custom, false);

            tester.Server.ResetCustomServer();
            tester.Config.Extraction.Update = new UpdateConfig();
            tester.Config.Extraction.DataTypes.AllowStringVariables = false;
            tester.Config.Extraction.DataTypes.MaxArraySize = 0;

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }
        [Theory]
        [InlineData(true, false)]
        // [InlineData(false, true)]
        // [InlineData(true, true)]
        public async Task TestUpdateFieldsRaw(bool assets, bool timeseries)
        {
            var upd = tester.Config.Extraction.Update;
            upd.Objects.Name = assets;
            upd.Objects.Description = assets;
            upd.Objects.Context = assets;
            upd.Objects.Metadata = assets;
            upd.Variables.Name = timeseries;
            upd.Variables.Description = timeseries;
            upd.Variables.Context = timeseries;
            upd.Variables.Metadata = timeseries;

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);

            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Relationships = true,
                    Assets = false,
                    Timeseries = false
                },
                Raw = new RawMetadataTargetConfig
                {
                    Database = "metadata",
                    AssetsTable = "assets",
                    TimeseriesTable = "timeseries"
                }
            };
            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.History.Enabled = false;

            var runTask = extractor.RunExtractor();

            await TestUtils.WaitForCondition(() => handler.AssetsRaw.Count != 0 && handler.TimeseriesRaw.Count != 0, 5);

            CommonTestUtils.VerifyStartingConditions(
                handler.AssetsRaw
                .ToDictionary(kvp => kvp.Key, kvp => (AssetDummy)JsonSerializer.Deserialize<AssetDummyJson>(kvp.Value.ToString())),
                handler.TimeseriesRaw
                .ToDictionary(kvp => kvp.Key, kvp => (TimeseriesDummy)
                    JsonSerializer.Deserialize<StatelessTimeseriesDummy>(kvp.Value.ToString())), null, extractor, tester.Server.Ids.Custom, true);

            tester.Server.ModifyCustomServer();

            await extractor.Rebrowse();

            CommonTestUtils.VerifyStartingConditions(
                handler.AssetsRaw
                .ToDictionary(kvp => kvp.Key, kvp => (AssetDummy)JsonSerializer.Deserialize<AssetDummyJson>(kvp.Value.ToString())),
                handler.TimeseriesRaw
                .ToDictionary(kvp => kvp.Key, kvp => (TimeseriesDummy)
                    JsonSerializer.Deserialize<StatelessTimeseriesDummy>(kvp.Value.ToString())), upd, extractor, tester.Server.Ids.Custom, true);
            CommonTestUtils.VerifyModified(
                handler.AssetsRaw
                .ToDictionary(kvp => kvp.Key, kvp => (AssetDummy)JsonSerializer.Deserialize<AssetDummyJson>(kvp.Value.ToString())),
                handler.TimeseriesRaw
                .ToDictionary(kvp => kvp.Key, kvp => (TimeseriesDummy)
                    JsonSerializer.Deserialize<StatelessTimeseriesDummy>(kvp.Value.ToString())), upd, extractor, tester.Server.Ids.Custom, true);

            tester.Server.ResetCustomServer();
            tester.Config.Extraction.Update = new UpdateConfig();
            tester.Config.Extraction.DataTypes.AllowStringVariables = false;
            tester.Config.Extraction.DataTypes.MaxArraySize = 0;

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }
        // Test for a specific bug
        [Fact]
        public async Task TestUpdateNullPropertyValue()
        {
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Wrong.Root, tester.Client);

            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.Extraction.Update = new UpdateConfig
            {
                Objects = new TypeUpdateConfig
                {
                    Metadata = true
                },
                Variables = new TypeUpdateConfig
                {
                    Metadata = true
                }
            };
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                Clean = new CleanMetadataTargetConfig
                {
                    Assets = true,
                    Timeseries = true
                }
            };
            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);


            tester.Server.Server.MutateNode(tester.Server.Ids.Wrong.TooLargeProp, state =>
            {
                var varState = state as PropertyState;
                varState.ArrayDimensions = new ReadOnlyList<uint>(new List<uint> { 5 });
                varState.Value = null;
            });

            var runTask = extractor.RunExtractor();

            await TestUtils.WaitForCondition(() => handler.Assets.Count != 0 && handler.Timeseries.Count != 0, 5);

            var id = tester.Client.GetUniqueId(tester.Server.Ids.Wrong.RankImprecise);

            Assert.True(string.IsNullOrEmpty(handler.Assets[id].metadata["TooLargeDim"]));

            await extractor.Rebrowse();

            Assert.True(string.IsNullOrEmpty(handler.Assets[id].metadata["TooLargeDim"]));

            tester.Server.Server.MutateNode(tester.Server.Ids.Wrong.TooLargeProp, state =>
            {
                var varState = state as PropertyState;
                varState.ArrayDimensions = new ReadOnlyList<uint>(new List<uint> { 5 });
                varState.Value = Enumerable.Range(0, 5).ToArray();
            });

            await extractor.Rebrowse();

            Assert.Equal("[0,1,2,3,4]", handler.Assets[id].metadata["TooLargeDim"]);

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }
        #endregion

        #region transformations
        [Fact]
        public async Task TestPropertyInheritance()
        {
            // The IsProperty attribute is inherited by deeper nodes.
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            extraction.Transformations = new List<RawNodeTransformation>
            {
                new RawNodeTransformation
                {
                    Filter = new NodeFilter
                    {
                        Name = new RegexFieldFilter("^CustomRoot$")
                    },
                    Type = TransformationType.Property
                }
            };

            using var extractor = tester.BuildExtractor(true, null, pusher);

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ObjectIds.ObjectsFolder, tester.Client);

            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;

            await extractor.RunExtractor(true);

            var root = pusher.PushedNodes[ObjectIds.ObjectsFolder] as UAObject;
            var meta = root.BuildMetadata(tester.Config, extractor, false);
            Assert.Equal(17, meta.Count);
            // Verify that the metadata fields get values
            Assert.Equal("[0,0,0,0]", meta["CustomRoot_Variable Array"]);
            Assert.Equal("String prop value", meta["CustomRoot_ChildObject2_StringProp"]);

            var log = tester.Provider.GetRequiredService<ILogger<NodeExtractionTests>>();

            // ... and that the JSON looks right
            var metaElem = root.ToJson(log, extractor.StringConverter, ConverterType.Node);
            var metaString = CommonTestUtils.JsonElementToString(metaElem.RootElement.GetProperty("metadata"));
            // This wouldn't work in clean, since there is only a single very large metadata field, but it is a much more useful input to Raw.
            Assert.Equal(@"{""CustomRoot"":{""ChildObject"":null,""ChildObject2"":{""NumericProp"":1234,""StringProp"":""String prop value""},"
            + @"""Variable Array"":{""Value"":[0,0,0,0],""EngineeringUnits"":""°C: degree Celsius"",""EURange"":""(0, 100)""},"
            + @"""Variable StringArray"":[""test1"",""test2""],""StringyVar"":null,""IgnoreVar"":null,"
            + @"""MysteryVar"":{""Value"":null,""EngineeringUnits"":""°C: degree Celsius"",""EURange"":""(0, 100)""},"
            + @"""NumberVar"":{""Value"":null,""DeepProp"":{""DeepProp2"":{""val1"":""value 1"",""val2"":""value 2""}}},"
            + @"""EnumVar1"":""Enum2"",""EnumVar3"":[""VEnum2"",""VEnum2"",""VEnum1"",""VEnum2""],""EnumVar2"":""VEnum2""}}", metaString);
        }
        [Fact]
        public async Task TestArrayPropertiesWithoutMaxArraySize()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            extraction.Transformations = new List<RawNodeTransformation>
            {
                new RawNodeTransformation
                {
                    Filter = new NodeFilter
                    {
                        Name = new RegexFieldFilter("^CustomRoot$")
                    },
                    Type = TransformationType.Property
                }
            };

            using var extractor = tester.BuildExtractor(true, null, pusher);

            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(ObjectIds.ObjectsFolder, tester.Client);

            extraction.DataTypes.AllowStringVariables = false;
            extraction.DataTypes.MaxArraySize = 0;
            extraction.DataTypes.AutoIdentifyTypes = false;

            await extractor.RunExtractor(true);

            var root = pusher.PushedNodes[ObjectIds.ObjectsFolder] as UAObject;
            var meta = root.BuildMetadata(tester.Config, extractor, false);
            Assert.Equal(17, meta.Count);
            // Verify that the metadata fields get values
            Assert.Equal("[0,0,0,0]", meta["CustomRoot_Variable Array"]);
            Assert.Equal("String prop value", meta["CustomRoot_ChildObject2_StringProp"]);

            var log = tester.Provider.GetRequiredService<ILogger<NodeExtractionTests>>();

            // ... and that the JSON looks right
            var metaElem = root.ToJson(log, extractor.StringConverter, ConverterType.Node);
            var metaString = CommonTestUtils.JsonElementToString(metaElem.RootElement.GetProperty("metadata"));
            // This wouldn't work in clean, since there is only a single very large metadata field, but it is a much more useful input to Raw.
            Assert.Equal(@"{""CustomRoot"":{""ChildObject"":null,""ChildObject2"":{""NumericProp"":1234,""StringProp"":""String prop value""},"
            + @"""Variable Array"":{""Value"":[0,0,0,0],""EngineeringUnits"":""°C: degree Celsius"",""EURange"":""(0, 100)""},"
            + @"""Variable StringArray"":[""test1"",""test2""],""StringyVar"":null,""IgnoreVar"":null,"
            + @"""MysteryVar"":{""Value"":null,""EngineeringUnits"":""°C: degree Celsius"",""EURange"":""(0, 100)""},"
            + @"""NumberVar"":{""Value"":null,""DeepProp"":{""DeepProp2"":{""val1"":""value 1"",""val2"":""value 2""}}},"
            + @"""EnumVar1"":1,""EnumVar3"":[123,123,321,123],""EnumVar2"":123}}", metaString);
        }
        [Fact]
        public async Task TestLateIgnore()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            extraction.Transformations = new List<RawNodeTransformation>
            {
                new RawNodeTransformation
                {
                    Filter = new NodeFilter
                    {
                        Parent = new NodeFilter
                        {
                            Name = new RegexFieldFilter("^CustomRoot$")
                        }
                    },
                    Type = TransformationType.Ignore
                }
            };
            using var extractor = tester.BuildExtractor(true, null, pusher);
            tester.Config.Extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Server.Ids.Custom.Root, tester.Client);
            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;
            await extractor.RunExtractor(true);

            Assert.Single(pusher.PushedNodes);
            Assert.Empty(pusher.PushedVariables);
        }

        #endregion
        #region types
        [Fact]
        public async Task TestReadTypes()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            using var extractor = tester.BuildExtractor(true, null, pusher);
            extraction.RootNode = CommonTestUtils.ToProtoNodeId(ObjectIds.TypesFolder, tester.Client);
            extraction.NodeTypes.AsNodes = true;
            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;
            await extractor.RunExtractor(true);

            Assert.Equal(884, pusher.PushedNodes.Count);
            Assert.Equal(2, pusher.PushedVariables.Count);
            var customVarType = pusher.PushedNodes[tester.Server.Ids.Custom.VariableType] as UAVariableType;
            Assert.Equal("CustomVariableType", customVarType.Name);

            Assert.Equal(NodeClass.VariableType, customVarType.NodeClass);
            var meta = customVarType.BuildMetadata(tester.Config, extractor, true);
            Assert.Single(meta);
            Assert.Equal("123.123", meta["Value"]);
            var customObjType = pusher.PushedNodes[tester.Server.Ids.Custom.ObjectType];
            Assert.Equal("CustomObjectType", customObjType.Name);
            Assert.Equal(NodeClass.ObjectType, customObjType.NodeClass);
        }
        [Fact]
        public async Task TestReadTypesWithReferences()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            using var extractor = tester.BuildExtractor(true, null, pusher);
            extraction.RootNode = CommonTestUtils.ToProtoNodeId(ObjectIds.TypesFolder, tester.Client);
            extraction.NodeTypes.AsNodes = true;
            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = -1;
            extraction.DataTypes.AutoIdentifyTypes = true;
            extraction.Relationships.Enabled = true;
            extraction.Relationships.Hierarchical = false;
            await extractor.RunExtractor(true);

            Assert.Equal(884, pusher.PushedNodes.Count);
            Assert.Equal(2, pusher.PushedVariables.Count);
            Assert.Equal(409, pusher.PushedReferences.Count);
        }
        #endregion

        #region nodeset
        [Fact]
        public async Task TestNodeSetSource()
        {
            using var pusher = new DummyPusher(new DummyPusherConfig());
            var extraction = tester.Config.Extraction;
            extraction.Relationships.Enabled = true;
            extraction.Relationships.Hierarchical = true;
            extraction.Relationships.InverseHierarchical = true;
            using var extractor = tester.BuildExtractor(true, null, pusher);

            extraction.RootNode = CommonTestUtils.ToProtoNodeId(tester.Ids.Custom.Root, tester.Client);
            extraction.DataTypes.AllowStringVariables = true;
            extraction.DataTypes.MaxArraySize = 4;
            extraction.DataTypes.DataTypeMetadata = true;
            extraction.NodeTypes.Metadata = true;

            tester.Config.History.Enabled = true;
            tester.Config.Events.Enabled = true;
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

            // Nothing enabled, default run, copy results
            await extractor.RunExtractor(true);
            var assets = pusher.PushedNodes.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            var tss = pusher.PushedVariables.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            var refs = pusher.PushedReferences.ToHashSet();

            void CompareProperties(BaseUANode node, BaseUANode other)
            {
                var props = node.GetAllProperties();
                var otherProps = other.GetAllProperties();
                Assert.Equal(otherProps.Count(), props.Count());
                var dict = otherProps.ToDictionary(prop => prop.Id);
                foreach (var prop in props)
                {
                    Assert.True(dict.TryGetValue(prop.Id, out var otherProp));
                    Assert.Equal(prop.Name, otherProp.Name);
                    Assert.True(prop.IsProperty);
                    Assert.True(otherProp.IsProperty);
                    if (otherProp is UAVariable otherVar)
                    {
                        var propVar = Assert.IsType<UAVariable>(prop);
                        Assert.Equal(otherVar.FullAttributes.DataType.Id, propVar.FullAttributes.DataType.Id);
                        Assert.Equal(otherVar.Value, propVar.Value);
                    }
                }
            }


            void Compare(IEnumerable<BaseUANode> nodes, IEnumerable<UAVariable> variables, HashSet<UAReference> references)
            {
                Assert.Equal(assets.Count, nodes.Count());
                Assert.Equal(tss.Count, variables.Count());
                Assert.Equal(refs.Count, references.Count);
                foreach (var node in nodes)
                {
                    Assert.True(assets.TryGetValue(node.Id, out var other));
                    Assert.Equal(other.Name, node.Name);
                    if (other is UAObject oobj)
                    {
                        var obj = node as UAObject;
                        Assert.Equal(oobj.FullAttributes.ShouldSubscribeToEvents(tester.Config), obj.FullAttributes.ShouldSubscribeToEvents(tester.Config));
                        Assert.Equal(oobj.FullAttributes.TypeDefinition.Id, obj.FullAttributes.TypeDefinition.Id);
                        Assert.Equal(oobj.FullAttributes.EventNotifier, obj.FullAttributes.EventNotifier);
                    }
                    Assert.False(node.IsProperty);
                    Assert.Equal(other.ParentId, node.ParentId);
                    CompareProperties(node, other);
                }

                foreach (var node in variables)
                {
                    Assert.True(tss.TryGetValue(node.DestinationId(), out var other));
                    Assert.Equal(other.Name, node.Name);
                    Assert.Equal(other.FullAttributes.ShouldSubscribe(tester.Config), node.FullAttributes.ShouldSubscribe(tester.Config));
                    Assert.Equal(other.FullAttributes.TypeDefinition.Id, node.FullAttributes.TypeDefinition.Id);
                    Assert.False(node.IsProperty);
                    Assert.Equal(other.ParentId, node.ParentId);
                    Assert.Equal(other.FullAttributes.DataType.Id, node.FullAttributes.DataType.Id);
                    // This is a really, really weird issue. Turns out there is a discrepancy in the C# language and
                    // .NET runtime, so an array can be uint under the hood, but int in code, which the language
                    // doesn't allow, but the runtime is fine with. Shouldn't matter when running, but we get this magic.
                    // Your IDE/compiler might complain about meaningless casts here, but it's wrong.
                    Assert.Equal(other.FullAttributes.ArrayDimensions?.Select(i => i),
                        node.FullAttributes.ArrayDimensions?.Select(i => i));
                    Assert.Equal(other.FullAttributes.ShouldReadHistory(tester.Config), node.FullAttributes.ShouldReadHistory(tester.Config));
                    Assert.Equal(other.FullAttributes.ValueRank, node.FullAttributes.ValueRank);
                    Assert.Equal(other.IsArray, node.IsArray);
                    CompareProperties(node, other);
                }

                foreach (var rf in references)
                {
                    Assert.Contains(rf, refs);
                }
            }

            pusher.Wipe();

            // Enable types only
            tester.Log.LogInformation("BEGIN TYPE RUN");
            tester.Client.TypeManager.Reset();
            tester.Config.Source.NodeSetSource.Types = true;
            await extractor.RunExtractor(true);
            Compare(pusher.PushedNodes.Values, pusher.PushedVariables.Values, pusher.PushedReferences);

            // Enable instance as well

            pusher.Wipe();
            tester.Log.LogInformation("BEGIN INSTANCE RUN");
            tester.Client.TypeManager.Reset();
            tester.Config.Source.NodeSetSource.Instance = true;
            await extractor.RunExtractor(true);
            foreach (var node in pusher.PushedNodes.Values)
            {
                tester.Log.LogInformation("{Node}", node);
            }

            Compare(pusher.PushedNodes.Values, pusher.PushedVariables.Values, pusher.PushedReferences);
        }
        #endregion
    }
}

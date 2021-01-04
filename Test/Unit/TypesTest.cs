using Cognite.OpcUa;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class TypesTestFixture : BaseExtractorTestFixture
    {
        public TypesTestFixture() : base(62600) { }
    }
    public class TypesTest : MakeConsoleWork, IClassFixture<TypesTestFixture>
    {
        private readonly TypesTestFixture tester;
        public TypesTest(ITestOutputHelper output, TypesTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        #region uanode
        [Theory]
        [InlineData(true, true, true, true, true)]
        [InlineData(true, true, true, true, false)]
        [InlineData(true, true, true, false, false)]
        [InlineData(true, true, false, false, false)]
        [InlineData(true, false, false, false, false)]
        [InlineData(false, true, false, false, false)]
        [InlineData(false, false, true, false, false)]
        [InlineData(false, false, false, true, true)]
        [InlineData(false, false, false, false, false)]
        [InlineData(true, false, true, false, true)]
        public void TestChecksum(bool context, bool description, bool name, bool metadata, bool ntMeta)
        {
            var update = new TypeUpdateConfig
            {
                Context = context,
                Description = description,
                Name = name,
                Metadata = metadata
            };
            int csA, csB;
            void AssertNotEqualIf(bool cond)
            {
                if (cond)
                {
                    Assert.NotEqual(csA, csB);
                }
                else
                {
                    Assert.Equal(csA, csB);
                }
            }

            var nodeA = new UANode(new NodeId("node"), null, NodeId.Null);
            var nodeB = new UANode(new NodeId("node"), null, NodeId.Null);

            (int, int) Update(UANode nodeA, UANode nodeB)
            {
                int csA = nodeA.GetUpdateChecksum(update, false, ntMeta);
                int csB = nodeB.GetUpdateChecksum(update, false, ntMeta);
                return (csA, csB);
            }

            (csA, csB) = Update(nodeA, nodeB);

            Assert.Equal(csA, csB);

            // Test name
            nodeA = new UANode(new NodeId("node"), "name", NodeId.Null);
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Name);
            nodeB = new UANode(new NodeId("node"), "name", NodeId.Null);
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test context
            nodeA = new UANode(new NodeId("node"), "name", new NodeId("parent"));
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Context);
            nodeB = new UANode(new NodeId("node"), "name", new NodeId("parent"));
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test description
            nodeA.Description = "description";
            nodeB.Description = "otherDesc";
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Description);
            nodeB.Description = "description";
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);


            var propA = new UAVariable(new NodeId("propA"), "propA", NodeId.Null);
            propA.SetDataPoint("valueA", DateTime.UtcNow, tester.Client);
            var propB = new UAVariable(new NodeId("propB"), "propB", NodeId.Null);
            propB.SetDataPoint("valueB", DateTime.UtcNow, tester.Client);

            var propC = new UAVariable(new NodeId("propA"), "propA", NodeId.Null);
            propC.SetDataPoint("valueA", DateTime.UtcNow, tester.Client);
            var propD = new UAVariable(new NodeId("propB"), "propB", NodeId.Null);
            propD.SetDataPoint("valueC", DateTime.UtcNow, tester.Client);

            // Test metadata
            nodeA.Properties = new List<UAVariable>
            {
                propA, propB
            };
            nodeB.Properties = new List<UAVariable>
            {
                propC, propD
            };
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Metadata);
            nodeB.Properties[1].SetDataPoint("valueB", DateTime.UtcNow, tester.Client);
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test NodeType metadata
            nodeA.NodeType = new UANodeType(new NodeId("type"), false);
            nodeB.NodeType = new UANodeType(new NodeId("type2"), false);
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(ntMeta && update.Metadata);
            nodeB.NodeType = new UANodeType(new NodeId("type"), false);
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test nested metadata
            var nestProp = new UAVariable(new NodeId("nestProp"), "nestProp", NodeId.Null);
            var nestProp2 = new UAVariable(new NodeId("nestProp"), "nestProp", NodeId.Null);

            nestProp.Properties = new List<UAVariable> { propA };
            nestProp2.Properties = new List<UAVariable> { propB };
            nodeA.Properties.Add(nestProp);
            nodeB.Properties.Add(nestProp2);

            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Metadata);
            nestProp2.Properties = nestProp.Properties;
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);
        }
        [Fact]
        public void TestDebugDescription()
        {
            // Super basic
            var node = new UANode(new NodeId("test"), "name", NodeId.Null);
            var str = node.ToDebugDescription();
            var refStr = "Object: name\n"
                       + "Id: s=test\n";
            Assert.Equal(refStr, str);

            // Full
            node = new UANode(new NodeId("test"), "name", new NodeId("parent"));
            node.Description = "description";
            node.EventNotifier = EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
            var propA = new UAVariable(new NodeId("propA"), "propA", NodeId.Null);
            propA.SetDataPoint("valueA", DateTime.UtcNow, tester.Client);
            var propB = new UAVariable(new NodeId("propB"), "propB", NodeId.Null);
            var nestedProp = new UAVariable(new NodeId("propN"), "propN", NodeId.Null);
            nestedProp.SetDataPoint("nProp", DateTime.UtcNow, tester.Client);
            nestedProp.Properties = new List<UAVariable> { propA };

            node.Properties = new List<UAVariable>
            {
                propA, nestedProp, propB
            };
            node.NodeType = new UANodeType(new NodeId("type"), false);

            str = node.ToDebugDescription();
            refStr = "Object: name\n"
                   + "Id: s=test\n"
                   + "ParentId: s=parent\n"
                   + "Description: description\n"
                   + "EventNotifier: 5\n"
                   + "NodeType: s=type\n"
                   + "Properties: {\n"
                   + "    propA: valueA\n"
                   + "    propN: nProp\n"
                   + "        propA: valueA\n"
                   + "    propB: ??\n"
                   + "}";
            Assert.Equal(refStr, str);
        }
        #endregion

        #region uavariable
        [Fact]
        public void TestVariableDebugDescription()
        {
            // basic
            var node = new UAVariable(new NodeId("test"), "name", NodeId.Null);
            node.ValueRank = ValueRanks.Scalar;
            var str = node.ToDebugDescription();
            var refStr = "Variable: name\n"
                       + "Id: s=test\n";
            Assert.Equal(refStr, str);

            // full
            node = new UAVariable(new NodeId("test"), "name", new NodeId("parent"));
            node.Description = "description";
            node.DataType = new UADataType(DataTypeIds.Double);
            node.Historizing = true;
            node.ValueRank = ValueRanks.Any;
            node.ArrayDimensions = new System.Collections.ObjectModel.Collection<int>(new int[] { 4 });
            node.NodeType = new UANodeType(new NodeId("type"), false);

            var propA = new UAVariable(new NodeId("propA"), "propA", NodeId.Null);
            propA.SetDataPoint("valueA", DateTime.UtcNow, tester.Client);
            var propB = new UAVariable(new NodeId("propB"), "propB", NodeId.Null);
            var nestedProp = new UAVariable(new NodeId("propN"), "propN", NodeId.Null);
            nestedProp.SetDataPoint("nProp", DateTime.UtcNow, tester.Client);
            nestedProp.Properties = new List<UAVariable> { propA };

            node.Properties = new List<UAVariable>
            {
                propA, nestedProp, propB
            };

            str = node.ToDebugDescription();
            refStr = "Variable: name\n"
                   + "Id: s=test\n"
                   + "ParentId: s=parent\n"
                   + "Description: description\n"
                   + "DataType: {\n"
                   + $"    NodeId: i={DataTypes.Double}\n"
                   + "    isStep: False\n"
                   + "    isString: False\n"
                   + "}\n"
                   + "Historizing: True\n"
                   + "ValueRank: -2\n"
                   + "Dimension: 4\n"
                   + "NodeType: s=type\n"
                   + "Properties: {\n"
                   + "    propA: valueA\n"
                   + "    propN: nProp\n"
                   + "        propA: valueA\n"
                   + "    propB: ??\n"
                   + "}";
            Assert.Equal(refStr, str);
        }
        [Fact]
        public void TestSetDatapoint()
        {
            // Property
            var node = new UAVariable(new NodeId("test"), "name", NodeId.Null);
            node.IsProperty = true;
            var now = DateTime.UtcNow;
            node.SetDataPoint(123.4, now, tester.Client);
            Assert.Equal(now, node.Value.Timestamp);
            Assert.True(node.Value.IsString);
            Assert.Equal("123.4", node.Value.StringValue);
            node.SetDataPoint("test", now, tester.Client);
            Assert.Equal(now, node.Value.Timestamp);
            Assert.True(node.Value.IsString);
            Assert.Equal("test", node.Value.StringValue);

            // Double datatype
            node.IsProperty = false;
            node.DataType = new UADataType(DataTypeIds.Double);
            node.SetDataPoint(123.0, now, tester.Client);
            Assert.Equal(now, node.Value.Timestamp);
            Assert.False(node.Value.IsString);
            Assert.Equal(123.0, node.Value.DoubleValue);
            node.SetDataPoint("test", now, tester.Client);
            Assert.Equal(now, node.Value.Timestamp);
            Assert.False(node.Value.IsString);
            Assert.Equal(0, node.Value.DoubleValue);

            // String datatype
            node.DataType = new UADataType(DataTypeIds.String);
            node.SetDataPoint(123.4, now, tester.Client);
            Assert.Equal(now, node.Value.Timestamp);
            Assert.True(node.Value.IsString);
            Assert.Equal("123.4", node.Value.StringValue);
            node.SetDataPoint("test", now, tester.Client);
            Assert.Equal(now, node.Value.Timestamp);
            Assert.True(node.Value.IsString);
            Assert.Equal("test", node.Value.StringValue);
        }
        [Fact]
        public void TestGetArrayChildren()
        {
            var id = new NodeId("test");
            var node = new UAVariable(id, "name", NodeId.Null);
            Assert.Empty(node.CreateArrayChildren());
            Assert.Null(node.ArrayChildren);

            node.Historizing = true;
            node.DataType = new UADataType(DataTypeIds.Double);
            node.NodeType = new UANodeType(new NodeId("test"), true);
            node.ValueRank = ValueRanks.OneDimension;
            node.ArrayDimensions = new System.Collections.ObjectModel.Collection<int>(new int[] { 4 });

            var children = node.CreateArrayChildren().ToList();
            Assert.Equal(4, children.Count);
            Assert.Equal(children, node.ArrayChildren);

            for (int i = 0; i < 4; i++)
            {
                var child = children[i];
                Assert.True(child.Historizing);
                Assert.Equal($"name[{i}]", child.DisplayName);
                Assert.Equal(node.Id, child.ParentId);
                Assert.Equal(node, child.ArrayParent);
                Assert.Equal(node.DataType, child.DataType);
                Assert.Equal(node.NodeType, child.NodeType);
                Assert.Equal(node.ValueRank, child.ValueRank);
                Assert.Equal(node.ArrayDimensions, child.ArrayDimensions);
                Assert.Equal(i, child.Index);
            }

        }
        #endregion

        #region uadatapoint
        [Fact]
        public void TestDataPointConstructors()
        {
            var now = DateTime.UtcNow;
            var dt = new UADataPoint(now, "id", 123.123);
            Assert.Equal(now, dt.Timestamp);
            Assert.Equal("id", dt.Id);
            Assert.False(dt.IsString);
            Assert.Equal(123.123, dt.DoubleValue);
            Assert.Null(dt.StringValue);

            dt = new UADataPoint(dt, 12.34);
            Assert.Equal(now, dt.Timestamp);
            Assert.Equal("id", dt.Id);
            Assert.False(dt.IsString);
            Assert.Equal(12.34, dt.DoubleValue);
            Assert.Null(dt.StringValue);

            dt = new UADataPoint(now, "id", "value");
            Assert.Equal(now, dt.Timestamp);
            Assert.Equal("id", dt.Id);
            Assert.True(dt.IsString);
            Assert.Equal("value", dt.StringValue);
            Assert.Null(dt.DoubleValue);

            dt = new UADataPoint(dt, "value2");
            Assert.Equal(now, dt.Timestamp);
            Assert.Equal("id", dt.Id);
            Assert.True(dt.IsString);
            Assert.Equal("value2", dt.StringValue);
            Assert.Null(dt.DoubleValue);
        }
        [Theory]
        [InlineData("id", 123.123)]
        [InlineData("longwæeirdæid", 123.123)]
        [InlineData("id", -123.123)]
        [InlineData("id", "stringvalue")]
        [InlineData("id", null)]
        [InlineData("id", "longwæirdævalue")]
        public void TestDataPointSerialization(string id, object value)
        {
            UADataPoint dt;
            var ts = DateTime.UtcNow;
            if (value is string || value == null)
            {
                dt = new UADataPoint(ts, id, value as string);
            }
            else
            {
                dt = new UADataPoint(ts, id, UAClient.ConvertToDouble(value));
            }
            var bytes = dt.ToStorableBytes();
            using (var stream = new MemoryStream(bytes))
            {
                var convDt = UADataPoint.FromStream(stream);
                Assert.Equal(dt.Timestamp, convDt.Timestamp);
                Assert.Equal(dt.Id, convDt.Id);
                Assert.Equal(dt.IsString, convDt.IsString);
                Assert.Equal(dt.StringValue, convDt.StringValue);
                Assert.Equal(dt.DoubleValue, convDt.DoubleValue);
            }
        }
        [Fact]
        public void TestDataPointDebugDescription()
        {
            var ts = DateTime.UtcNow;
            var dt = new UADataPoint(ts, "id", 123.123);
            var str = dt.ToDebugDescription();
            var refStr = $"Update timeseries id to 123.123 at {ts.ToString(CultureInfo.InvariantCulture)}";
            Assert.Equal(refStr, str);

            dt = new UADataPoint(ts, "id", "value");
            str = dt.ToDebugDescription();
            refStr = $"Update timeseries id to \"value\" at {ts.ToString(CultureInfo.InvariantCulture)}";
            Assert.Equal(refStr, str);
        }

        #endregion
    }
}

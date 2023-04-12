using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    [Collection("Shared server tests")]
    public class TypesTest
    {
        private readonly StaticServerTestFixture tester;
        public TypesTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
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

            var nodeA = new UAObject(new NodeId("node"), null, null, null, NodeId.Null, null);
            var nodeB = new UAObject(new NodeId("node"), null, null, null, NodeId.Null, null);

            (int, int) Update(BaseUANode nodeA, BaseUANode nodeB)
            {
                int csA = nodeA.GetUpdateChecksum(update, false, ntMeta);
                int csB = nodeB.GetUpdateChecksum(update, false, ntMeta);
                return (csA, csB);
            }

            (csA, csB) = Update(nodeA, nodeB);

            Assert.Equal(csA, csB);

            // Test name
            nodeA = new UAObject(new NodeId("node"), "name", null, null, NodeId.Null, null);
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Name);
            nodeB = new UAObject(new NodeId("node"), "name", null, null, NodeId.Null, null);
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test context
            nodeA = new UAObject(new NodeId("node"), "name", null, null, new NodeId("parent"), null);
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Context);
            nodeB = new UAObject(new NodeId("node"), "name", null, null, new NodeId("parent"), null);
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test description
            nodeA.Attributes.Description = "description";
            nodeB.Attributes.Description = "otherDesc";
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Description);
            nodeB.Attributes.Description = "description";
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            var pdt = new UADataType(DataTypeIds.String);

            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            propA.FullAttributes.Value = new Variant("valueA");
            var propB = CommonTestUtils.GetSimpleVariable("propB", pdt);
            propB.FullAttributes.Value = new Variant("valueB");

            var propC = CommonTestUtils.GetSimpleVariable("propA", pdt);
            propC.FullAttributes.Value = new Variant("valueA");
            var propD = CommonTestUtils.GetSimpleVariable("propB", pdt);
            propD.FullAttributes.Value = new Variant("valueC");

            // Test metadata
            nodeA.Attributes.Properties = new List<BaseUANode>
            {
                propA, propB
            };
            nodeB.Attributes.Properties = new List<BaseUANode>
            {
                propC, propD
            };
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Metadata);
            (nodeB.Attributes.Properties[1] as UAVariable).FullAttributes.Value = new Variant("valueB");
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test NodeType metadata
            nodeA.FullAttributes.TypeDefinition = new UAObjectType(new NodeId("type"));
            nodeB.FullAttributes.TypeDefinition = new UAObjectType(new NodeId("type2"));
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(ntMeta && update.Metadata);
            nodeB.FullAttributes.TypeDefinition = new UAObjectType(new NodeId("type"));
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test nested metadata
            var nestProp = CommonTestUtils.GetSimpleVariable("nestProp", pdt);
            var nestProp2 = CommonTestUtils.GetSimpleVariable("nestProp", pdt);

            nestProp.Attributes.Properties = new List<BaseUANode> { propA };
            nestProp2.Attributes.Properties = new List<BaseUANode> { propB };
            nodeA.Attributes.AddProperty(nestProp);
            nodeB.Attributes.AddProperty(nestProp2);

            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Metadata);
            nestProp2.Attributes.Properties = nestProp.Attributes.Properties;
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test variable types
            var typeA = new UAVariableType(new NodeId("typeA"), "typeA", null, null, NodeId.Null);
            typeA.FullAttributes.DataType = pdt;
            typeA.FullAttributes.Value = new Variant("value1");
            var typeB = new UAVariableType(new NodeId("typeA"), "typeA", null, null, NodeId.Null);
            typeB.FullAttributes.DataType = pdt;
            typeB.FullAttributes.Value = new Variant("value2");
            (csA, csB) = Update(typeA, typeB);
            AssertNotEqualIf(update.Metadata);
            typeB.FullAttributes.Value = new Variant("value1");
            (csA, csB) = Update(typeA, typeB);
            Assert.Equal(csA, csB);
        }
        [Fact]
        public void TestDebugDescription()
        {
            // Super basic
            var node = new UAObject(new NodeId("test"), "name", null, null, NodeId.Null, null);
            var str = node.ToString();
            var refStr = "Object: name\n"
                       + "Id: s=test\n";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());

            // Full
            var pdt = new UADataType(DataTypeIds.String);

            node = new UAObject(new NodeId("test"), "name", null, null, new NodeId("parent"), null);
            node.Attributes.Description = "description";
            node.FullAttributes.EventNotifier = EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            propA.FullAttributes.Value = new Variant("valueA");
            var propB = CommonTestUtils.GetSimpleVariable("propB", pdt);
            var nestedProp = CommonTestUtils.GetSimpleVariable("propN", pdt);
            nestedProp.FullAttributes.Value = new Variant("nProp");
            nestedProp.Attributes.Properties = new List<BaseUANode> { propA };

            node.Attributes.Properties = new List<BaseUANode>
            {
                propA, nestedProp, propB
            };
            node.FullAttributes.TypeDefinition = new UAObjectType(new NodeId("type"));

            str = node.ToString();
            refStr = "Object: name\n"
                   + "Id: s=test\n"
                   + "ParentId: s=parent\n"
                   + "Description: description\n"
                   + "EventNotifier: 5\n"
                   + "NodeType: s=type\n"
                   + "Properties: {\n"
                   + "    propA: valueA\n"
                   + "    propN: nProp\n"
                   + "    propN_propA: valueA\n"
                   + "    propB: \n"
                   + "}";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());
        }

        [Fact]
        public void TestBuildMetadata()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UAObject(new NodeId("test"), "test", null, null, NodeId.Null, null);
            node.FullAttributes.TypeDefinition.Attributes.DisplayName = "SomeType";
            Assert.Empty(node.BuildMetadata(tester.Config, extractor, false));
            Assert.Empty(node.BuildMetadata(tester.Config, extractor, true));
            tester.Config.Extraction.NodeTypes.Metadata = true;
            node.FullAttributes.TypeDefinition = new UAObjectType(new NodeId("type"));
            node.FullAttributes.TypeDefinition.Attributes.DisplayName = "SomeType";
            // Test extras only
            Assert.Single(node.BuildMetadata(tester.Config, extractor, true));

            // Test properties only
            var pdt = new UADataType(DataTypeIds.String);

            tester.Config.Extraction.NodeTypes.Metadata = false;
            var ts = DateTime.UtcNow;
            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            var propB = CommonTestUtils.GetSimpleVariable("propB", pdt);
            propA.FullAttributes.Value = new Variant("valueA");
            propB.FullAttributes.Value = new Variant("valueB");

            node.Attributes.Properties = new List<BaseUANode>
            {
                propA, propB
            };
            var meta = node.BuildMetadata(tester.Config, extractor, true);
            Assert.Equal(2, meta.Count);
            Assert.Equal("valueA", meta["propA"]);
            Assert.Equal("valueB", meta["propB"]);

            // Test both
            tester.Config.Extraction.NodeTypes.Metadata = true;
            Assert.Equal(3, node.BuildMetadata(tester.Config, extractor, true).Count);

            // Test nested properties
            var nestedProp = CommonTestUtils.GetSimpleVariable("nestedProp", pdt); ;
            nestedProp.FullAttributes.Value = new Variant("nestedValue");
            propB.Attributes.Properties = new List<BaseUANode>
            {
                nestedProp
            };
            meta = node.BuildMetadata(tester.Config, extractor, true);
            Assert.Equal(4, meta.Count);
            Assert.Equal("nestedValue", meta["propB_nestedProp"]);

            // Test null name
            var nullNameProp = new UAVariable(new NodeId("nullName"), null, null, null, NodeId.Null, null);
            nullNameProp.FullAttributes.DataType = pdt;
            node.Attributes.AddProperty(nullNameProp);
            meta = node.BuildMetadata(tester.Config, extractor, true);
            Assert.Equal(4, meta.Count);

            // Test null value
            var nullValueProp = new UAVariable(new NodeId("nullValue"), "nullValue", null, null, NodeId.Null, null);
            nullValueProp.FullAttributes.DataType = pdt;
            node.Attributes.AddProperty(nullValueProp);
            meta = node.BuildMetadata(tester.Config, extractor, true);
            Assert.Equal(5, meta.Count);
            Assert.Equal("", meta["nullValue"]);

            // Test duplicated properties
            var propA2 = new UAVariable(new NodeId("propA2"), "propA", null, null, NodeId.Null, null);
            propA2.FullAttributes.DataType = pdt;
            node.Attributes.AddProperty(propA2);
            propA2.FullAttributes.Value = new Variant("valueA2");
            meta = node.BuildMetadata(tester.Config, extractor, true);
            Assert.Equal(5, meta.Count);
            Assert.Equal("valueA2", meta["propA"]);

            // Test overwrite extras
            Assert.Equal("SomeType", meta["TypeDefinition"]);
            var propNT = new UAVariable(new NodeId("TypeDef"), "TypeDefinition", null, null, NodeId.Null, null);
            propNT.FullAttributes.DataType = pdt;
            propNT.FullAttributes.Value = new Variant("SomeOtherType");
            node.Attributes.AddProperty(propNT);
            meta = node.BuildMetadata(tester.Config, extractor, true);
            Assert.Equal(5, meta.Count);
            Assert.Equal("SomeOtherType", meta["TypeDefinition"]);
        }

        [Fact]
        public void TestToCDFAsset()
        {
            using var extractor = tester.BuildExtractor();

            var node = new UAObject(new NodeId("test"), "test", null, null, new NodeId("parent"), null);
            node.Attributes.Description = "description";
            var ts = DateTime.UtcNow;
            var pdt = new UADataType(DataTypeIds.String);

            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            var propB = CommonTestUtils.GetSimpleVariable("propB", pdt);
            propA.FullAttributes.Value = new Variant("valueA");
            propB.FullAttributes.Value = new Variant("valueB");

            node.Attributes.Properties = new List<BaseUANode>
            {
                propA, propB
            };

            var poco = node.ToCDFAsset(tester.Config, extractor, 123, null);
            Assert.Equal(node.Attributes.Description, poco.Description);
            Assert.Equal(123, poco.DataSetId);
            Assert.Equal("test", poco.Name);
            Assert.Equal("gp.base:s=test", poco.ExternalId);
            Assert.Equal("gp.base:s=parent", poco.ParentExternalId);
            Assert.Equal(2, poco.Metadata.Count);

            // Test meta-map
            var propC = CommonTestUtils.GetSimpleVariable("propC", pdt); ;
            propC.FullAttributes.Value = new Variant("valueC");
            node.Attributes.AddProperty(propC);

            var metaMap = new Dictionary<string, string>
            {
                { "propA", "description" },
                { "propB", "name" },
                { "propC", "parentId" }
            };
            poco = node.ToCDFAsset(tester.Config, extractor, 123, metaMap);
            Assert.Equal("valueA", poco.Description);
            Assert.Equal(123, poco.DataSetId);
            Assert.Equal("valueB", poco.Name);
            Assert.Equal("gp.base:s=test", poco.ExternalId);
            Assert.Equal("valueC", poco.ParentExternalId);
            Assert.Equal(3, poco.Metadata.Count);
        }

        private static string MetadataToJson(ILogger log, BaseUANode node, UAExtractor extractor)
        {
            var json = node.ToJson(log, extractor.StringConverter, ConverterType.Node);
            return json.RootElement.GetProperty("metadata").ToString();
        }

        [Fact]
        public void TestToJson()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UAObject(new NodeId("test"), "test", null, null, NodeId.Null, null);
            var converter = tester.Client.StringConverter;
            var log = tester.Provider.GetRequiredService<ILogger<TypesTest>>();
            Assert.Equal("", MetadataToJson(log, node, extractor));

            // Extras only
            tester.Config.Extraction.NodeTypes.Metadata = true;
            node.FullAttributes.TypeDefinition = new UAObjectType(new NodeId("type"));
            node.FullAttributes.TypeDefinition.Attributes.DisplayName = "SomeType";
            Assert.Equal(@"{""TypeDefinition"":""SomeType""}", MetadataToJson(log, node, extractor));

            // Properties only
            var pdt = new UADataType(DataTypeIds.String);

            tester.Config.Extraction.NodeTypes.Metadata = false;
            var ts = DateTime.UtcNow;
            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            var propB = CommonTestUtils.GetSimpleVariable("propB", pdt);
            propA.FullAttributes.Value = new Variant("valueA");
            propB.FullAttributes.Value = new Variant("valueB");

            node.Attributes.Properties = new List<BaseUANode>
            {
                propA, propB
            };
            Assert.Equal(@"{""propA"":""valueA"",""propB"":""valueB""}", MetadataToJson(log, node, extractor));

            tester.Config.Extraction.NodeTypes.Metadata = true;
            Assert.Equal(@"{""TypeDefinition"":""SomeType"",""propA"":""valueA"",""propB"":""valueB""}", MetadataToJson(log, node, extractor));

            // Test nested properties
            var nestedProp = CommonTestUtils.GetSimpleVariable("nestedProp", pdt); ;
            nestedProp.FullAttributes.Value = new Variant("nestedValue");
            propB.Attributes.Properties = new List<BaseUANode>
            {
                nestedProp
            };
            Assert.Equal(@"{""TypeDefinition"":""SomeType"",""propA"":""valueA"","
                + @"""propB"":{""Value"":""valueB"",""nestedProp"":""nestedValue""}}",
                MetadataToJson(log, node, extractor));

            // Test null name
            var nullNameProp = new UAVariable(new NodeId("nullName"), null, null, null, NodeId.Null, null);
            nullNameProp.FullAttributes.DataType = pdt;
            node.Attributes.AddProperty(nullNameProp);
            Assert.Equal(@"{""TypeDefinition"":""SomeType"",""propA"":""valueA"","
                + @"""propB"":{""Value"":""valueB"",""nestedProp"":""nestedValue""}}",
                MetadataToJson(log, node, extractor));

            // Test null value
            var nullValueProp = new UAVariable(new NodeId("nullValue"), "nullValue", null, null, NodeId.Null, null);
            nullValueProp.FullAttributes.DataType = pdt;
            node.Attributes.AddProperty(nullValueProp);
            Assert.Equal(@"{""TypeDefinition"":""SomeType"",""propA"":""valueA"","
                + @"""propB"":{""Value"":""valueB"",""nestedProp"":""nestedValue""},""nullValue"":null}",
                MetadataToJson(log, node, extractor));

            // Test duplicated properties
            var propA2 = new UAVariable(new NodeId("propA2"), "propA", null, null, NodeId.Null, null);
            propA2.FullAttributes.DataType = pdt;
            node.Attributes.AddProperty(propA2);
            propA2.FullAttributes.Value = new Variant("valueA2");
            Assert.Equal(@"{""TypeDefinition"":""SomeType"",""propA"":""valueA"","
                + @"""propB"":{""Value"":""valueB"",""nestedProp"":""nestedValue""},""nullValue"":null,""propA0"":""valueA2""}",
                MetadataToJson(log, node, extractor));
        }
        [Fact]
        public void TestToJsonComplexTypes()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UAObject(new NodeId("test"), "test", null, null, NodeId.Null, null);
            var converter = tester.Client.StringConverter;
            var log = tester.Provider.GetRequiredService<ILogger<TypesTest>>();

            var pdt = new UADataType(DataTypeIds.ReadValueId);
            var prop = new UAVariable(new NodeId("readvalueid"), "readvalueid", null, null, NodeId.Null, null);

            // Test simple value
            prop.FullAttributes.DataType = pdt;
            var value = new ReadValueId { NodeId = new NodeId("test"), AttributeId = Attributes.Value };
            prop.FullAttributes.Value = new Variant(value);
            node.Attributes.AddProperty(prop);

            Assert.Equal(@"{""readvalueid"":{""NodeId"":{""IdType"":1,""Id"":""test""},""AttributeId"":13}}",
                MetadataToJson(log, node, extractor));

            // Test nested
            node.Attributes.Properties.Clear();
            var outerProp = new UAObject(new NodeId("outer"), "outer", null, null, NodeId.Null, null);
            outerProp.Attributes.AddProperty(prop);
            node.Attributes.AddProperty(outerProp);
            Assert.Equal(@"{""outer"":{""readvalueid"":{""NodeId"":{""IdType"":1,""Id"":""test""},""AttributeId"":13}}}",
                MetadataToJson(log, node, extractor));

            // Test array
            prop.FullAttributes.Value = new Variant(new ReadValueIdCollection(new[] { value, value }));
            Assert.Equal(@"{""outer"":{""readvalueid"":["
            + @"{""NodeId"":{""IdType"":1,""Id"":""test""},""AttributeId"":13},"
            + @"{""NodeId"":{""IdType"":1,""Id"":""test""},""AttributeId"":13}]}}",
                MetadataToJson(log, node, extractor));
        }
        #endregion

        #region uavariable
        [Fact]
        public void TestVariableDebugDescription()
        {
            var pdt = new UADataType(DataTypeIds.String);

            // basic
            var node = new UAVariable(new NodeId("test"), "name", null, null, NodeId.Null, null);
            node.FullAttributes.ValueRank = ValueRanks.Scalar;
            var str = node.ToString();
            var refStr = "Variable: name\n"
                       + "Id: s=test\n"
                       + "AccessLevel: 0\n";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());

            // full
            node = new UAVariable(new NodeId("test"), "name", null, null, new NodeId("parent"), null);
            node.Attributes.Description = "description";
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            node.FullAttributes.AccessLevel = AccessLevels.CurrentRead | AccessLevels.HistoryRead;
            node.FullAttributes.ValueRank = ValueRanks.Any;
            node.FullAttributes.ArrayDimensions = new int[] { 4 };
            node.FullAttributes.TypeDefinition = new UAVariableType(new NodeId("type"));
            node.AsEvents = true;

            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            propA.FullAttributes.Value = new Variant("valueA");
            var propB = CommonTestUtils.GetSimpleVariable("propB", pdt);
            var nestedProp = CommonTestUtils.GetSimpleVariable("propN", pdt); ;

            nestedProp.FullAttributes.Value = new Variant("nProp");
            nestedProp.Attributes.Properties = new List<BaseUANode> { propA };

            node.Attributes.Properties = new List<BaseUANode>
            {
                propA, nestedProp, propB
            };

            str = node.ToString();
            refStr = "Variable: name\n"
                   + "    Id: s=test\n"
                   + "    ParentId: s=parent\n"
                   + "    Description: description\n"
                   + "    DataType: {\n"
                   + $"        Id: i={DataTypes.Double}\n"
                   + "        IsString: False\n"
                   + "}\n"
                   + "    History: True\n"
                   + "    AccessLevel: 5\n"
                   + "    ValueRank: Any\n"
                   + "    Dimension: 4\n"
                   + "    NodeType: s=type\n"
                   + "    Written as events to destinations\n"
                   + "    Properties: {\n"
                   + "        propA: valueA\n"
                   + "        propN: nProp\n"
                   + "        propN_propA: valueA\n"
                   + "        propB: \n"
                   + "}";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());
        }
        [Fact]
        public void TestGetArrayChildren()
        {
            var id = new NodeId("test");
            var node = new UAVariable(id, "name", null, null, NodeId.Null, new UAVariableType(new NodeId("test")));
            Assert.Single(node.CreateTimeseries());
            Assert.Null(node.ArrayChildren);

            node.FullAttributes.AccessLevel = AccessLevels.CurrentRead | AccessLevels.HistoryRead;
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            node.FullAttributes.ValueRank = ValueRanks.OneDimension;
            node.FullAttributes.ArrayDimensions = new int[] { 4 };

            var children = node.CreateTimeseries().ToList();
            Assert.Equal(4, children.Count);
            Assert.Equal(children, node.ArrayChildren);

            for (int i = 0; i < 4; i++)
            {
                var child = children[i];
                Assert.True(child.FullAttributes.ShouldReadHistory(tester.Config));
                Assert.Equal($"name[{i}]", child.Attributes.DisplayName);
                Assert.Equal(node.Id, child.ParentId);
                Assert.Equal(node.FullAttributes.AccessLevel, child.FullAttributes.AccessLevel);
                Assert.Equal(node, (child as UAVariableMember).TSParent);
                Assert.Equal(node.FullAttributes.DataType, child.FullAttributes.DataType);
                Assert.Equal(node.FullAttributes.TypeDefinition.Id, child.FullAttributes.TypeDefinition.Id);
                Assert.Equal(node.ValueRank, child.ValueRank);
                Assert.Equal(node.ArrayDimensions, child.ArrayDimensions);
                Assert.Equal(i, (child as UAVariableMember).Index);
            }
        }
        [Fact]
        public void TestGetTimeseries()
        {
            var id = new NodeId("test");
            var node = new UAVariable(id, "name", null, null, NodeId.Null, new UAVariableType(new NodeId("test")));

            node.FullAttributes.AccessLevel = AccessLevels.CurrentRead | AccessLevels.HistoryRead;
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            node.FullAttributes.ValueRank = ValueRanks.OneDimension;

            node.IsObject = true;

            var children = node.CreateTimeseries().ToList();
            Assert.Single(children);

            var child = children.Single();
            Assert.True(child.FullAttributes.ShouldReadHistory(tester.Config));
            Assert.Equal(node.FullAttributes.DisplayName, child.FullAttributes.DisplayName);
            Assert.Equal(node.Id, child.ParentId);
            Assert.Equal(node.FullAttributes.AccessLevel, child.FullAttributes.AccessLevel);
            Assert.Equal(node.TimeSeries, child);
            Assert.Equal(node.FullAttributes.DataType.Id, child.FullAttributes.DataType.Id);
            Assert.Equal(node.FullAttributes.TypeDefinition.Id, child.FullAttributes.TypeDefinition.Id);
            Assert.Equal(node.ValueRank, child.ValueRank);

            Assert.Equal(child, node.CreateTimeseries().First());
        }

        [Fact]
        public void TestToStatelessTimeseries()
        {
            using var extractor = tester.BuildExtractor();

            var pdt = new UADataType(DataTypeIds.String);

            var node = new UAVariable(new NodeId("test"), "test", null, null, new NodeId("parent"), null);
            node.Attributes.Description = "description";
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Boolean);
            node.Attributes.Properties = new List<BaseUANode>();
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = CommonTestUtils.GetSimpleVariable($"prop{i}", pdt);
                prop.FullAttributes.Value = new Variant($"value{i}");
                node.Attributes.AddProperty(prop);
            }

            var ts = node.ToStatelessTimeSeries(tester.Config, extractor, 123, null);
            Assert.Equal("gp.base:s=test", ts.ExternalId);
            Assert.Equal(123, ts.DataSetId);
            Assert.Equal("test", ts.Name);
            Assert.Equal("gp.base:s=test", ts.LegacyName);
            Assert.Equal("gp.base:s=parent", ts.AssetExternalId);
            Assert.True(ts.IsStep);
            Assert.False(ts.IsString);
            Assert.Equal(4, ts.Metadata.Count);
            for (int i = 1; i <= 4; i++) Assert.Equal($"value{i}", ts.Metadata[$"prop{i}"]);
            Assert.Null(ts.Unit);
            Assert.Equal("description", ts.Description);


            var metaMap = new Dictionary<string, string>
            {
                { "prop1", "description" },
                { "prop2", "name" },
                { "prop3", "unit" },
                { "prop4", "parentId" }
            };
            ts = node.ToStatelessTimeSeries(tester.Config, extractor, 123, metaMap);
            Assert.Equal("gp.base:s=test", ts.ExternalId);
            Assert.Equal(123, ts.DataSetId);
            Assert.Equal("value2", ts.Name);
            Assert.Equal("gp.base:s=test", ts.LegacyName);
            Assert.Equal("value4", ts.AssetExternalId);
            Assert.True(ts.IsStep);
            Assert.False(ts.IsString);
            Assert.Equal(4, ts.Metadata.Count);
            for (int i = 1; i <= 4; i++) Assert.Equal($"value{i}", ts.Metadata[$"prop{i}"]);
            Assert.Equal("value1", ts.Description);
            Assert.Equal("value3", ts.Unit);
        }
        [Fact]
        public void TestToTimeseries()
        {
            using var extractor = tester.BuildExtractor();

            var node = new UAVariable(new NodeId("test"), "test", null, null, new NodeId("parent"), null);
            node.Attributes.Description = "description";
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Boolean);
            node.Attributes.Properties = new List<BaseUANode>();

            var pdt = new UADataType(DataTypeIds.String);

            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = CommonTestUtils.GetSimpleVariable($"prop{i}", pdt);
                prop.FullAttributes.Value = new Variant($"value{i}");
                node.Attributes.AddProperty(prop);
            }

            var nodeToAssetIds = new Dictionary<NodeId, long>
            {
                { new NodeId("parent"), 111 },
                { new NodeId("parent2"), 222 }
            };
            extractor.State.RegisterNode(new NodeId("parent2"), "value4");

            var ts = node.ToTimeseries(tester.Config, extractor, extractor, 123, nodeToAssetIds, null);
            Assert.Equal("gp.base:s=test", ts.ExternalId);
            Assert.Equal(123, ts.DataSetId);
            Assert.Equal("test", ts.Name);
            Assert.Equal("gp.base:s=test", ts.LegacyName);
            Assert.Equal(111, ts.AssetId);
            Assert.True(ts.IsStep);
            Assert.False(ts.IsString);
            Assert.Equal(4, ts.Metadata.Count);
            Assert.Null(ts.Unit);
            Assert.Equal("description", ts.Description);

            ts = node.ToTimeseries(tester.Config, extractor, extractor, 123, nodeToAssetIds, null, true);
            Assert.Null(ts.Name);
            Assert.Null(ts.Metadata);
            Assert.Null(ts.AssetId);
            Assert.Equal("gp.base:s=test", ts.ExternalId);
            Assert.True(ts.IsStep);
            Assert.False(ts.IsString);

            var metaMap = new Dictionary<string, string>
            {
                { "prop1", "description" },
                { "prop2", "name" },
                { "prop3", "unit" },
                { "prop4", "parentId" }
            };
            ts = node.ToTimeseries(tester.Config, extractor, extractor, 123, nodeToAssetIds, metaMap);
            Assert.Equal("gp.base:s=test", ts.ExternalId);
            Assert.Equal(123, ts.DataSetId);
            Assert.Equal("value2", ts.Name);
            Assert.Equal("gp.base:s=test", ts.LegacyName);
            Assert.Equal(222, ts.AssetId);
            Assert.True(ts.IsStep);
            Assert.False(ts.IsString);
            Assert.Equal(4, ts.Metadata.Count);
            Assert.Equal("value1", ts.Description);
            Assert.Equal("value3", ts.Unit);
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

            using var stream = new MemoryStream(bytes);

            var convDt = UADataPoint.FromStream(stream);
            Assert.Equal(dt.Timestamp, convDt.Timestamp);
            Assert.Equal(dt.Id, convDt.Id);
            Assert.Equal(dt.IsString, convDt.IsString);
            Assert.Equal(dt.StringValue, convDt.StringValue);
            Assert.Equal(dt.DoubleValue, convDt.DoubleValue);
        }
        [Fact]
        public void TestDataPointDebugDescription()
        {
            var ts = DateTime.UtcNow;
            var dt = new UADataPoint(ts, "id", 123.123);
            var str = dt.ToString();
            var refStr = $"Update timeseries id to 123.123 at {ts.ToString(CultureInfo.InvariantCulture)}";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());

            dt = new UADataPoint(ts, "id", "value");
            str = dt.ToString();
            refStr = $"Update timeseries id to \"value\" at {ts.ToString(CultureInfo.InvariantCulture)}";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());
        }
        #endregion

        #region uadatatype
        [Fact]
        public void TestDataTypeConstructors()
        {
            // Base constructor
            // Native type, double
            var dt = new UADataType(DataTypeIds.Double);
            Assert.False(dt.IsStep);
            Assert.False(dt.IsString);

            // Native type, integer
            dt = new UADataType(DataTypeIds.Integer);
            Assert.False(dt.IsStep);
            Assert.False(dt.IsString);

            // Native type, string
            dt = new UADataType(DataTypeIds.String);
            Assert.False(dt.IsStep);
            Assert.True(dt.IsString);

            // Native type, bool
            dt = new UADataType(DataTypeIds.Boolean);
            Assert.True(dt.IsStep);
            Assert.False(dt.IsString);

            // Custom type
            dt = new UADataType(new NodeId("test"));
            Assert.False(dt.IsStep);
            Assert.True(dt.IsString);

            // From proto
            var config = new DataTypeConfig();

            // Override step
            dt = new UADataType(new ProtoDataType { IsStep = true }, new NodeId("test"), config);
            Assert.True(dt.IsStep);
            Assert.False(dt.IsString);

            // Override enum, strings disabled
            dt = new UADataType(new ProtoDataType { Enum = true }, new NodeId("test"), config);
            Assert.True(dt.IsStep);
            Assert.False(dt.IsString);
            Assert.NotNull(dt.EnumValues);

            // Override enum, strings enabled
            config.EnumsAsStrings = true;
            dt = new UADataType(new ProtoDataType { Enum = true }, new NodeId("test"), config);
            Assert.False(dt.IsStep);
            Assert.True(dt.IsString);
            Assert.NotNull(dt.EnumValues);


            // Child constructor
            var rootDt = new UADataType(DataTypeIds.Boolean);
            dt = new UADataType(new NodeId("test"), rootDt);
            Assert.True(dt.IsStep);
            Assert.False(dt.IsString);

            rootDt.EnumValues = new Dictionary<long, string>
            {
                [123] = "test"
            };
            dt = new UADataType(new NodeId("test"), rootDt);
            Assert.True(dt.IsStep);
            Assert.False(dt.IsString);
            Assert.NotNull(dt.EnumValues);
            Assert.Empty(dt.EnumValues);
        }
        [Fact]
        public void TestTypeToDataPoint()
        {
            // Normal double
            using var extractor = tester.BuildExtractor();
            var now = DateTime.UtcNow;
            var dt = new UADataType(DataTypeIds.Double);
            var dp = dt.ToDataPoint(extractor, 123.123, now, "id");
            Assert.Equal("id", dp.Id);
            Assert.Equal(123.123, dp.DoubleValue);
            Assert.Equal(now, dp.Timestamp);

            // Normal string
            dt = new UADataType(DataTypeIds.String);
            dp = dt.ToDataPoint(extractor, 123.123, now, "id");
            Assert.Equal("id", dp.Id);
            Assert.Equal("123.123", dp.StringValue);
            Assert.Equal(now, dp.Timestamp);

            // Enum double
            var config = new DataTypeConfig();
            dt = new UADataType(new ProtoDataType { Enum = true }, new NodeId("test"), config);
            dp = dt.ToDataPoint(extractor, 123, now, "id");
            Assert.Equal("id", dp.Id);
            Assert.Equal(123, dp.DoubleValue);
            Assert.Equal(now, dp.Timestamp);

            // Enum string
            config.EnumsAsStrings = true;
            dt = new UADataType(new ProtoDataType { Enum = true }, new NodeId("test"), config);
            dt.EnumValues[123] = "enum";
            dp = dt.ToDataPoint(extractor, 123, now, "id");
            Assert.Equal("id", dp.Id);
            Assert.Equal("enum", dp.StringValue);
            Assert.Equal(now, dp.Timestamp);

            dp = dt.ToDataPoint(extractor, 124, now, "id");
            Assert.Equal("id", dp.Id);
            Assert.Equal("124", dp.StringValue);
            Assert.Equal(now, dp.Timestamp);

            // Use variant
            dt = new UADataType(DataTypeIds.String);
            dp = dt.ToDataPoint(extractor, new Variant("test"), now, "id");
            Assert.Equal("id", dp.Id);
            Assert.Equal("test", dp.StringValue);
            Assert.Equal(now, dp.Timestamp);

            // Test complex type
            dt = new UADataType(DataTypeIds.ReadValueId);
            var value = new Variant(new ReadValueId { AttributeId = Attributes.Value, NodeId = new NodeId("test") });
            dp = dt.ToDataPoint(extractor, value, now, "id");
            Assert.Equal("id", dp.Id);
            Assert.Equal(@"{""NodeId"":{""IdType"":1,""Id"":""test""},""AttributeId"":13}", dp.StringValue);
            Assert.Equal(now, dp.Timestamp);
        }
        [Fact]
        public void TestDataTypeDebugDescription()
        {
            // plain
            var dt = new UADataType(DataTypeIds.String);
            var str = dt.ToString();
            var refStr = "DataType: {\n"
                       + "    NodeId: i=12\n"
                       + "    String: True\n"
                       + "}";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());

            // full
            dt = new UADataType(new NodeId("test"))
            {
                IsString = false,
                IsStep = true,
                EnumValues = new Dictionary<long, string>
                {
                    { 123, "test" },
                    { 321, "test2" },
                    { 1, "test3" }
                }
            };
            str = dt.ToString();
            refStr = "DataType: {\n"
                   + "    NodeId: s=test\n"
                   + "    Step: True\n"
                   + "    String: False\n"
                   + "    EnumValues: [[123, test], [321, test2], [1, test3]]\n"
                   + "}";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());
        }
        #endregion

        #region uaevent
        [Fact]
        public void TestEventDebugDescription()
        {
            var now = DateTime.UtcNow;
            var evt = new UAEvent
            {
                EventId = "test.test",
                Time = now,
                EmittingNode = new NodeId("emitter"),
                EventType = new UAObjectType(new NodeId("type"))
            };
            var str = evt.ToString();
            var refStr = "Event: test.test\n"
                       + $"Time: {now.ToString(CultureInfo.InvariantCulture)}\n"
                       + "Type: EventType\n"
                       + "Emitter: s=emitter\n";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());

            evt.Message = "message";
            evt.SourceNode = new NodeId("source");
            evt.MetaData = new Dictionary<string, string>
            {
                { "key", "value1" },
                { "key2", "123" },
                { "key3", "value2" }
            };

            str = evt.ToString();
            refStr = "Event: test.test\n"
                   + $"Time: {now.ToString(CultureInfo.InvariantCulture)}\n"
                   + "Type: EventType\n"
                   + "Emitter: s=emitter\n"
                   + "Message: message\n"
                   + "SourceNode: s=source\n"
                   + "MetaData: {\n"
                   + "    key: value1\n"
                   + "    key2: 123\n"
                   + "    key3: value2\n"
                   + "}";
            Assert.Equal(refStr.ReplaceLineEndings(), str.ReplaceLineEndings());
        }
        [Fact]
        public void TestEventSerialization()
        {
            // minimal
            var now = DateTime.UtcNow;
            using var extractor = tester.BuildExtractor();
            var state = new EventExtractionState(tester.Client, new NodeId("emitter"), true, true, true);
            extractor.State.SetEmitterState(state);
            extractor.State.RegisterNode(new NodeId("type"), tester.Client.GetUniqueId(new NodeId("type")));
            var type = new UAObjectType(new NodeId("type"));
            extractor.State.ActiveEvents[type.Id] = type;
            extractor.State.RegisterNode(new NodeId("emitter"), tester.Client.GetUniqueId(new NodeId("emitter")));

            ILogger log = tester.Provider.GetRequiredService<ILogger<TypesTest>>();
            // No event should be created without all of these
            var evt = new UAEvent
            {
                EventId = "test.test",
                Time = DateTime.MinValue,
                EmittingNode = new NodeId("emitter"),
                EventType = type,
                SourceNode = NodeId.Null
            };

            var bytes = evt.ToStorableBytes(extractor);
            using (var stream = new MemoryStream(bytes))
            {
                var convEvt = UAEvent.FromStream(stream, extractor);
                Assert.Equal(convEvt.EventId, evt.EventId);
                Assert.Equal(convEvt.Time, evt.Time);
                Assert.Equal(convEvt.EmittingNode, evt.EmittingNode);
                Assert.Equal(convEvt.EventType, evt.EventType);
                Assert.Empty(convEvt.MetaData);
                Assert.Equal(convEvt.Message, evt.Message);
                Assert.Equal(convEvt.SourceNode, evt.SourceNode);
            }

            // full
            extractor.State.RegisterNode(new NodeId("source"), tester.Client.GetUniqueId(new NodeId("source")));
            evt.Message = "message";
            evt.Time = now;
            evt.SourceNode = new NodeId("source");
            evt.SetMetadata(extractor.StringConverter, new[]
            {
                new EventFieldValue(new RawTypeField("key1"), "value1"),
                new EventFieldValue(new RawTypeField("key1"), 123),
                new EventFieldValue(new RawTypeField("key1"), Variant.Null),
                new EventFieldValue(new RawTypeField("key1"), new NodeId("meta")),
            }, log);

            bytes = evt.ToStorableBytes(extractor);
            using (var stream = new MemoryStream(bytes))
            {
                var convEvt = UAEvent.FromStream(stream, extractor);
                Assert.Equal(convEvt.EventId, evt.EventId);
                Assert.Equal(convEvt.Time, evt.Time);
                Assert.Equal(convEvt.EmittingNode, evt.EmittingNode);
                Assert.Equal(convEvt.EventType, evt.EventType);
                Assert.Equal(convEvt.Message, evt.Message);
                Assert.Equal(convEvt.SourceNode, evt.SourceNode);
                Assert.Equal(convEvt.MetaData.Count, evt.MetaData.Count);
                foreach (var kvp in convEvt.MetaData)
                {
                    Assert.Equal(kvp.Value ?? "", tester.Client.StringConverter.ConvertToString(evt.MetaData[kvp.Key]));
                }
            }
        }
        [Fact]
        public void TestToStatelessCDFEvent()
        {
            using var extractor = tester.BuildExtractor();

            var ts = DateTime.UtcNow;

            var evt = new UAEvent
            {
                EmittingNode = new NodeId("emitter"),
                MetaData = new Dictionary<string, string>(),
                EventId = "eventid",
                EventType = new UAObjectType(new NodeId("type")),
                Message = "message",
                SourceNode = new NodeId("source"),
                Time = ts
            };
            evt.MetaData["field"] = "value";

            // Plain
            var conv = evt.ToStatelessCDFEvent(extractor, 123, null);
            Assert.Equal("gp.base:s=emitter", conv.Metadata["Emitter"]);
            Assert.Equal("gp.base:s=source", conv.Metadata["SourceNode"]);
            Assert.Equal(4, conv.Metadata.Count);
            Assert.Equal("value", conv.Metadata["field"]);
            Assert.Equal("EventType", conv.Metadata["TypeName"]);
            Assert.Equal("gp.base:s=type", conv.Type);
            Assert.Equal("eventid", conv.ExternalId);
            Assert.Equal("message", conv.Description);
            Assert.Equal(ts.ToUnixTimeMilliseconds(), conv.StartTime);
            Assert.Equal(ts.ToUnixTimeMilliseconds(), conv.EndTime);
            Assert.Equal(123, conv.DataSetId);
            Assert.Equal(new[] { "gp.base:s=source" }, conv.AssetExternalIds);

            // With parentId mapping
            conv = evt.ToStatelessCDFEvent(extractor, 123, new Dictionary<NodeId, string>
            {
                { new NodeId("source"), "source" }
            });
            Assert.Equal(new[] { "source" }, conv.AssetExternalIds);

            // With mapped metadata
            evt.MetaData["SubType"] = "SomeSubType";
            evt.MetaData["StartTime"] = ts.AddDays(-1).ToUnixTimeMilliseconds().ToString();
            evt.MetaData["EndTime"] = ts.AddDays(1).ToUnixTimeMilliseconds().ToString();
            evt.MetaData["Type"] = "SomeOtherType";

            conv = evt.ToStatelessCDFEvent(extractor, 123, null);
            Assert.Equal("gp.base:s=emitter", conv.Metadata["Emitter"]);
            Assert.Equal("gp.base:s=source", conv.Metadata["SourceNode"]);
            Assert.Equal(4, conv.Metadata.Count);
            Assert.Equal("value", conv.Metadata["field"]);
            Assert.Equal("EventType", conv.Metadata["TypeName"]);
            Assert.Equal("SomeOtherType", conv.Type);
            Assert.Equal("SomeSubType", conv.Subtype);
            Assert.Equal("eventid", conv.ExternalId);
            Assert.Equal("message", conv.Description);
            Assert.Equal(ts.AddDays(-1).ToUnixTimeMilliseconds(), conv.StartTime);
            Assert.Equal(ts.AddDays(1).ToUnixTimeMilliseconds(), conv.EndTime);
            Assert.Equal(123, conv.DataSetId);
            Assert.Equal(new[] { "gp.base:s=source" }, conv.AssetExternalIds);
        }
        [Fact]
        public void TestToCDFEvent()
        {
            using var extractor = tester.BuildExtractor();

            var ts = DateTime.UtcNow;

            var evt = new UAEvent
            {
                EmittingNode = new NodeId("emitter"),
                MetaData = new Dictionary<string, string>(),
                EventId = "eventid",
                EventType = new UAObjectType(new NodeId("type")),
                Message = "message",
                SourceNode = new NodeId("source"),
                Time = ts
            };
            evt.MetaData["field"] = "value";

            // Plain
            var nodeToAsset = new Dictionary<NodeId, long>
            {
                { new NodeId("source"), 111 }
            };

            var conv = evt.ToCDFEvent(extractor, 123, null);
            Assert.Equal("gp.base:s=emitter", conv.Metadata["Emitter"]);
            Assert.Equal("gp.base:s=source", conv.Metadata["SourceNode"]);
            Assert.Equal(4, conv.Metadata.Count);
            Assert.Equal("value", conv.Metadata["field"]);
            Assert.Equal("EventType", conv.Metadata["TypeName"]);
            Assert.Equal("gp.base:s=type", conv.Type);
            Assert.Equal("eventid", conv.ExternalId);
            Assert.Equal("message", conv.Description);
            Assert.Equal(ts.ToUnixTimeMilliseconds(), conv.StartTime);
            Assert.Equal(ts.ToUnixTimeMilliseconds(), conv.EndTime);
            Assert.Equal(123, conv.DataSetId);
            Assert.Null(conv.AssetIds);

            // With mapped metadata
            evt.MetaData["SubType"] = "SomeSubType";
            evt.MetaData["StartTime"] = ts.AddDays(-1).ToUnixTimeMilliseconds().ToString();
            evt.MetaData["EndTime"] = ts.AddDays(1).ToUnixTimeMilliseconds().ToString();
            evt.MetaData["Type"] = "SomeOtherType";

            conv = evt.ToCDFEvent(extractor, 123, nodeToAsset);
            Assert.Equal("gp.base:s=emitter", conv.Metadata["Emitter"]);
            Assert.Equal("gp.base:s=source", conv.Metadata["SourceNode"]);
            Assert.Equal(4, conv.Metadata.Count);
            Assert.Equal("value", conv.Metadata["field"]);
            Assert.Equal("EventType", conv.Metadata["TypeName"]);
            Assert.Equal("SomeOtherType", conv.Type);
            Assert.Equal("SomeSubType", conv.Subtype);
            Assert.Equal("eventid", conv.ExternalId);
            Assert.Equal("message", conv.Description);
            Assert.Equal(ts.AddDays(-1).ToUnixTimeMilliseconds(), conv.StartTime);
            Assert.Equal(ts.AddDays(1).ToUnixTimeMilliseconds(), conv.EndTime);
            Assert.Equal(123, conv.DataSetId);
            Assert.Equal(new long[] { 111 }, conv.AssetIds);
        }
        [Fact]
        public void TestDeepEventMetadata()
        {
            using var extractor = tester.BuildExtractor();

            var ts = DateTime.UtcNow;

            var evt = new UAEvent
            {
                EmittingNode = new NodeId("emitter"),
                EventId = "eventid",
                EventType = new UAObjectType(new NodeId("type")),
                Message = "message",
                SourceNode = new NodeId("source"),
                Time = ts
            };

            ILogger log = tester.Provider.GetRequiredService<ILogger<TypesTest>>();

            var rawMeta = new[]
            {
                new EventFieldValue(new RawTypeField("test-simple"), new NodeId("test")),
                new EventFieldValue(new RawTypeField("test-complex"), new Variant(new ReadValueId { AttributeId = 1, NodeId = new NodeId("test2") })),
                new EventFieldValue(new RawTypeField(new QualifiedNameCollection { "deep", "deep-2", "deep-simple" }), 123.123),
                new EventFieldValue(new RawTypeField(new QualifiedNameCollection { "deep", "deep-2", "deep-complex" }),
                    new Variant(new ReadValueId { AttributeId = 1, NodeId = new NodeId("test2") })),
                new EventFieldValue(new RawTypeField(new QualifiedNameCollection { "deep", "deep-2" }), new [] { 1, 2, 3, 4 }),
                new EventFieldValue(new RawTypeField(new QualifiedNameCollection { "deep", "deep-2", "Value" }), 123.321)
            };
            evt.SetMetadata(extractor.StringConverter, rawMeta, log);
            var meta = evt.MetaData;

            Assert.Equal(3, meta.Count);
            Assert.Equal("gp.base:s=test", meta["test-simple"]);
            Assert.Equal(@"{""NodeId"":{""IdType"":1,""Id"":""test2""},""AttributeId"":1}", meta["test-complex"]);
            Assert.Equal(@"{""deep-2"":{""deep-simple"":123.123,""deep-complex"":{""NodeId"":{""IdType"":1,""Id"":""test2""},""AttributeId"":1},"
                + @"""Value1"":123.321,""Value"":[1,2,3,4]}}", meta["deep"]);

        }
        #endregion

        #region uareference
        [Fact]
        public void TestReferenceDebugDescription()
        {
            using var extractor = tester.BuildExtractor();
            // asset - asset
            var reference = new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), false, false, true, extractor.TypeManager);
            reference.Type.Attributes.DisplayName = "Organizes";
            reference.Type.FullAttributes.InverseName = "IsOrganizedBy";
            Assert.Equal("Reference: Asset s=source Organizes Asset s=target", reference.ToString());
            // inverse
            reference = new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source"), new NodeId("target"), false, false, true, extractor.TypeManager);
            Assert.Equal("Reference: Asset s=source IsOrganizedBy Asset s=target", reference.ToString());

            // ts - asset
            reference = new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), true, false, true, extractor.TypeManager);
            Assert.Equal("Reference: TimeSeries s=source Organizes Asset s=target", reference.ToString());

            reference = new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source"), new NodeId("target"), false, true, true, extractor.TypeManager);
            Assert.Equal("Reference: Asset s=source IsOrganizedBy TimeSeries s=target", reference.ToString());

            reference = new UAReference(ReferenceTypeIds.HasComponent, true, new NodeId("source"), new NodeId("target"), false, false, true, extractor.TypeManager);
            Assert.Equal("Reference: Asset s=source i=47 Forward Asset s=target", reference.ToString());

            reference = new UAReference(ReferenceTypeIds.HasComponent, false, new NodeId("source"), new NodeId("target"), false, false, true, extractor.TypeManager);
            Assert.Equal("Reference: Asset s=source i=47 Inverse Asset s=target", reference.ToString());
        }
        [Fact]
        public void TestReferenceEquality()
        {
            using var extractor = tester.BuildExtractor();
            var reference = new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), false, false, true, extractor.TypeManager);
            Assert.Equal(reference, reference);
            // Different due to different type only
            var reference2 = new UAReference(ReferenceTypeIds.HasComponent, true, new NodeId("source"), new NodeId("target"), false, false, true, extractor.TypeManager);
            Assert.NotEqual(reference, reference2);
            // Different due to different source vertex type
            reference2 = new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), true, false, true, extractor.TypeManager);
            Assert.NotEqual(reference, reference2);
            // Different due to different target vertex type
            reference2 = new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), false, true, true, extractor.TypeManager);
            Assert.NotEqual(reference, reference2);
            // Different due to different direction
            reference2 = new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("source"), new NodeId("target"), false, false, true, extractor.TypeManager);
            Assert.NotEqual(reference, reference2);
            // Equal
            reference2 = new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), false, false, true, extractor.TypeManager);
            Assert.Equal(reference, reference2);
            Assert.Equal(reference.GetHashCode(), reference2.GetHashCode());
        }
        [Fact]
        public void TestToRelationship()
        {
            using var extractor = tester.BuildExtractor();
            var reference = new UAReference(ReferenceTypeIds.Organizes, true, new NodeId("source"), new NodeId("target"), false, true, true, extractor.TypeManager);
            reference.Type.Attributes.DisplayName = "Organizes";
            reference.Type.FullAttributes.InverseName = "IsOrganizedBy";
            var rel = reference.ToRelationship(123, extractor);
            Assert.Equal(123, rel.DataSetId);
            Assert.Equal(RelationshipVertexType.Asset, rel.SourceType);
            Assert.Equal(RelationshipVertexType.TimeSeries, rel.TargetType);
            Assert.Equal("gp.base:s=source", rel.SourceExternalId);
            Assert.Equal("gp.base:s=target", rel.TargetExternalId);
            Assert.Equal("gp.Organizes;base:s=source;base:s=target", rel.ExternalId);

            reference = new UAReference(ReferenceTypeIds.Organizes, false, new NodeId("target"), new NodeId("source"), true, false, true, extractor.TypeManager);
            rel = reference.ToRelationship(123, extractor);
            Assert.Equal(123, rel.DataSetId);
            Assert.Equal(RelationshipVertexType.TimeSeries, rel.SourceType);
            Assert.Equal(RelationshipVertexType.Asset, rel.TargetType);
            Assert.Equal("gp.base:s=target", rel.SourceExternalId);
            Assert.Equal("gp.base:s=source", rel.TargetExternalId);
            Assert.Equal("gp.OrganizedBy;base:s=target;base:s=source", rel.ExternalId);
        }
        #endregion
    }
}

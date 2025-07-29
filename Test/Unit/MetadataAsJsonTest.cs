using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Opc.Ua;
using System;
using System.Collections.Generic;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    [Collection("Shared server tests")]
    public class MetadataAsJsonTest
    {
        private readonly StaticServerTestFixture tester;

        public MetadataAsJsonTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
            tester.Client.TypeManager.Reset();
        }

        #region BuildMetadataAsJson Tests

        [Fact]
        public void TestBuildMetadataAsJsonEmptyNode()
        {
            using var extractor = tester.BuildExtractor();
            var node = CommonTestUtils.GetSimpleVariable("test", new UADataType(DataTypeIds.String));

            var jsonMetadata = node.BuildMetadataAsJson(tester.Config, extractor, false);
            Assert.NotNull(jsonMetadata);
            Assert.Empty(jsonMetadata);

            var jsonMetadataWithExtras = node.BuildMetadataAsJson(tester.Config, extractor, true);
            Assert.NotNull(jsonMetadataWithExtras);
            Assert.Empty(jsonMetadataWithExtras);
        }

        [Fact]
        public void TestBuildMetadataAsJsonWithProperties()
        {
            using var extractor = tester.BuildExtractor();
            var pdt = new UADataType(DataTypeIds.String);
            var node = CommonTestUtils.GetSimpleVariable("test", pdt);

            // Add simple properties
            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            var propB = CommonTestUtils.GetSimpleVariable("propB", pdt);
            propA.FullAttributes.Value = new Variant("valueA");
            propB.FullAttributes.Value = new Variant("valueB");

            node.Attributes.Properties = new List<BaseUANode> { propA, propB };

            var jsonMetadata = node.BuildMetadataAsJson(tester.Config, extractor, false);

            Assert.Equal(2, jsonMetadata.Count);
            Assert.True(jsonMetadata.ContainsKey("propA"));
            Assert.True(jsonMetadata.ContainsKey("propB"));

            // Verify JsonNode values
            Assert.NotNull(jsonMetadata["propA"]);
            Assert.NotNull(jsonMetadata["propB"]);
            Assert.Equal("valueA", jsonMetadata["propA"]?.GetValue<string>());
            Assert.Equal("valueB", jsonMetadata["propB"]?.GetValue<string>());
        }

        [Fact]
        public void TestBuildMetadataAsJsonWithNestedProperties()
        {
            using var extractor = tester.BuildExtractor();
            var pdt = new UADataType(DataTypeIds.String);
            var node = CommonTestUtils.GetSimpleVariable("test", pdt);

            // Create nested structure: propA -> nestedProp
            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            var nestedProp = CommonTestUtils.GetSimpleVariable("nestedProp", pdt);

            propA.FullAttributes.Value = new Variant("valueA");
            nestedProp.FullAttributes.Value = new Variant("nestedValue");

            propA.Attributes.Properties = new List<BaseUANode> { nestedProp };
            node.Attributes.Properties = new List<BaseUANode> { propA };

            var jsonMetadata = node.BuildMetadataAsJson(tester.Config, extractor, false);

            Assert.Equal(2, jsonMetadata.Count);
            Assert.True(jsonMetadata.ContainsKey("propA"));
            Assert.True(jsonMetadata.ContainsKey("propA_nestedProp"));
            Assert.Equal("valueA", jsonMetadata["propA"]?.GetValue<string>());
            Assert.Equal("nestedValue", jsonMetadata["propA_nestedProp"]?.GetValue<string>());
        }

        [Fact]
        public void TestBuildMetadataAsJsonWithExtras()
        {
            using var extractor = tester.BuildExtractor();
            var node = CommonTestUtils.GetSimpleVariable("test", new UADataType(DataTypeIds.String));

            // Enable node type metadata
            tester.Config.Extraction.NodeTypes.Metadata = true;
            node.FullAttributes.TypeDefinition = new UAVariableType(new NodeId("type", 0));
            node.FullAttributes.TypeDefinition.Attributes.DisplayName = "CustomType";

            var jsonMetadata = node.BuildMetadataAsJson(tester.Config, extractor, true);

            Assert.Single(jsonMetadata);
            Assert.True(jsonMetadata.ContainsKey("TypeDefinition"));
            Assert.Equal("CustomType", jsonMetadata["TypeDefinition"]?.GetValue<string>());
        }

        [Fact]
        public void TestBuildMetadataAsJsonNullValues()
        {
            using var extractor = tester.BuildExtractor();
            var pdt = new UADataType(DataTypeIds.String);
            var node = CommonTestUtils.GetSimpleVariable("test", pdt);

            // Property with null name
            var nullNameProp = CommonTestUtils.GetSimpleVariable("nullName", pdt);
            nullNameProp.Attributes.DisplayName = null; // Set name to null for testing

            // Property with null value  
            var nullValueProp = CommonTestUtils.GetSimpleVariable("nullValue", pdt);
            nullValueProp.FullAttributes.Value = null;

            node.Attributes.Properties = new List<BaseUANode> { nullNameProp, nullValueProp };

            var jsonMetadata = node.BuildMetadataAsJson(tester.Config, extractor, false);

            // Null name property should be ignored
            Assert.Single(jsonMetadata);
            Assert.True(jsonMetadata.ContainsKey("nullValue"));
        }

        [Fact]
        public void TestBuildMetadataAsJsonNumericValues()
        {
            using var extractor = tester.BuildExtractor();
            var node = CommonTestUtils.GetSimpleVariable("test", new UADataType(DataTypeIds.String));

            // Add numeric properties
            var intProp = CommonTestUtils.GetSimpleVariable("intProp", new UADataType(DataTypeIds.Int32));
            var doubleProp = CommonTestUtils.GetSimpleVariable("doubleProp", new UADataType(DataTypeIds.Double));
            var boolProp = CommonTestUtils.GetSimpleVariable("boolProp", new UADataType(DataTypeIds.Boolean));

            intProp.FullAttributes.Value = new Variant(42);
            doubleProp.FullAttributes.Value = new Variant(3.14);
            boolProp.FullAttributes.Value = new Variant(true);

            node.Attributes.Properties = new List<BaseUANode> { intProp, doubleProp, boolProp };

            var jsonMetadata = node.BuildMetadataAsJson(tester.Config, extractor, false);

            Assert.Equal(3, jsonMetadata.Count);
            Assert.Equal(42, jsonMetadata["intProp"]?.GetValue<int>());
            Assert.Equal(3.14, jsonMetadata["doubleProp"]?.GetValue<double>());
            Assert.Equal(true, jsonMetadata["boolProp"]?.GetValue<bool>());
        }

        #endregion

        #region ToIdmTimeSeries Tests

        [Fact]
        public void TestToIdmTimeSeriesMetadataAsJsonFalse()
        {
            using var extractor = tester.BuildExtractor();
            var pdt = new UADataType(DataTypeIds.String);
            var variable = CommonTestUtils.GetSimpleVariable("testVar", new UADataType(DataTypeIds.Double));

            // Add properties
            var propA = CommonTestUtils.GetSimpleVariable("propA", pdt);
            propA.FullAttributes.Value = new Variant("valueA");
            variable.Attributes.Properties = new List<BaseUANode> { propA };

            // Configure MetadataAsJson = false
            tester.Config.Cognite = new CognitePusherConfig
            {
                MetadataTargets = new MetadataTargetsConfig
                {
                    Clean = new CleanMetadataTargetConfig
                    {
                        MetadataAsJson = false
                    }
                }
            };

            var result = variable.ToIdmTimeSeries(extractor, "testSpace", "testSource", tester.Config, null);

            Assert.NotNull(result);
            Assert.NotNull(result.Properties);
            Assert.NotNull(result.Properties.extractedData);
            Assert.Single(result.Properties.extractedData);

            // Should contain string value as JsonNode
            Assert.True(result.Properties.extractedData.ContainsKey("propA"));
            var propValue = result.Properties.extractedData["propA"];
            Assert.NotNull(propValue);
            Assert.Equal("valueA", propValue.GetValue<string>());
        }

        [Fact]
        public void TestToIdmTimeSeriesMetadataAsJsonTrue()
        {
            using var extractor = tester.BuildExtractor();
            var pdt = new UADataType(DataTypeIds.String);
            var variable = CommonTestUtils.GetSimpleVariable("testVar", new UADataType(DataTypeIds.Double));

            // Add mixed type properties
            var stringProp = CommonTestUtils.GetSimpleVariable("stringProp", pdt);
            var intProp = CommonTestUtils.GetSimpleVariable("intProp", new UADataType(DataTypeIds.Int32));
            var boolProp = CommonTestUtils.GetSimpleVariable("boolProp", new UADataType(DataTypeIds.Boolean));

            stringProp.FullAttributes.Value = new Variant("testValue");
            intProp.FullAttributes.Value = new Variant(123);
            boolProp.FullAttributes.Value = new Variant(true);

            variable.Attributes.Properties = new List<BaseUANode> { stringProp, intProp, boolProp };

            // Configure MetadataAsJson = true
            tester.Config.Cognite = new CognitePusherConfig
            {
                MetadataTargets = new MetadataTargetsConfig
                {
                    Clean = new CleanMetadataTargetConfig
                    {
                        MetadataAsJson = true
                    }
                }
            };

            var result = variable.ToIdmTimeSeries(extractor, "testSpace", "testSource", tester.Config, null);

            Assert.NotNull(result);
            Assert.NotNull(result.Properties);
            Assert.NotNull(result.Properties.extractedData);
            Assert.Equal(3, result.Properties.extractedData.Count);

            // Verify different value types are preserved
            Assert.True(result.Properties.extractedData.ContainsKey("stringProp"));
            Assert.True(result.Properties.extractedData.ContainsKey("intProp"));
            Assert.True(result.Properties.extractedData.ContainsKey("boolProp"));

            var stringValue = result.Properties.extractedData["stringProp"];
            var intValue = result.Properties.extractedData["intProp"];
            var boolValue = result.Properties.extractedData["boolProp"];

            Assert.NotNull(stringValue);
            Assert.Equal("testValue", stringValue.GetValue<string>());

            Assert.NotNull(intValue);
            Assert.Equal(123, intValue.GetValue<int>());

            Assert.NotNull(boolValue);
            Assert.True(boolValue.GetValue<bool>());
        }

        [Fact]
        public void TestToIdmTimeSeriesMetadataAsJsonWithNestedProperties()
        {
            using var extractor = tester.BuildExtractor();
            var pdt = new UADataType(DataTypeIds.String);
            var variable = CommonTestUtils.GetSimpleVariable("testVar", new UADataType(DataTypeIds.Double));

            // Create nested properties
            var parentProp = CommonTestUtils.GetSimpleVariable("parent", pdt);
            var childProp = CommonTestUtils.GetSimpleVariable("child", pdt);

            parentProp.FullAttributes.Value = new Variant("parentValue");
            childProp.FullAttributes.Value = new Variant("childValue");

            parentProp.Attributes.Properties = new List<BaseUANode> { childProp };
            variable.Attributes.Properties = new List<BaseUANode> { parentProp };

            // Configure MetadataAsJson = true
            tester.Config.Cognite = new CognitePusherConfig
            {
                MetadataTargets = new MetadataTargetsConfig
                {
                    Clean = new CleanMetadataTargetConfig
                    {
                        MetadataAsJson = true
                    }
                }
            };

            var result = variable.ToIdmTimeSeries(extractor, "testSpace", "testSource", tester.Config, null);

            Assert.NotNull(result);
            Assert.Equal(2, result.Properties.extractedData.Count);

            // Check nested property naming
            Assert.True(result.Properties.extractedData.ContainsKey("parent"));
            Assert.True(result.Properties.extractedData.ContainsKey("parent_child"));

            Assert.Equal("parentValue", result.Properties.extractedData["parent"]?.GetValue<string>());
            Assert.Equal("childValue", result.Properties.extractedData["parent_child"]?.GetValue<string>());
        }

        [Fact]
        public void TestToIdmTimeSeriesDefaultConfig()
        {
            using var extractor = tester.BuildExtractor();
            var variable = CommonTestUtils.GetSimpleVariable("testVar", new UADataType(DataTypeIds.Double));

            // Test with default config (MetadataAsJson should default to false)
            var result = variable.ToIdmTimeSeries(extractor, "testSpace", "testSource", tester.Config, null);

            Assert.NotNull(result);
            Assert.NotNull(result.Properties);
            Assert.Equal("testSpace", result.Space);
            Assert.Equal(variable.GetUniqueId(extractor.Context), result.ExternalId);
        }

        [Fact]
        public void TestToIdmTimeSeriesNullCogniteConfig()
        {
            using var extractor = tester.BuildExtractor();
            var variable = CommonTestUtils.GetSimpleVariable("testVar", new UADataType(DataTypeIds.Double));

            // Ensure config is null
            tester.Config.Cognite = null;

            var result = variable.ToIdmTimeSeries(extractor, "testSpace", "testSource", tester.Config, null);

            Assert.NotNull(result);
            Assert.NotNull(result.Properties);
            // Should default to MetadataAsJson = false when config is null
        }

        [Fact]
        public void TestToIdmTimeSeriesEmptyExtractedData()
        {
            using var extractor = tester.BuildExtractor();
            var variable = CommonTestUtils.GetSimpleVariable("testVar", new UADataType(DataTypeIds.Double));

            // No properties added - extractedData should be empty but not null
            tester.Config.Cognite = new CognitePusherConfig
            {
                MetadataTargets = new MetadataTargetsConfig
                {
                    Clean = new CleanMetadataTargetConfig
                    {
                        MetadataAsJson = true
                    }
                }
            };

            var result = variable.ToIdmTimeSeries(extractor, "testSpace", "testSource", tester.Config, null);

            Assert.NotNull(result);
            Assert.NotNull(result.Properties);
            Assert.NotNull(result.Properties.extractedData);
            Assert.Empty(result.Properties.extractedData);
        }

        #endregion

        #region Edge Cases and Error Handling

        [Fact]
        public void TestBuildMetadataAsJsonPropertyOverwritesExtras()
        {
            using var extractor = tester.BuildExtractor();
            var pdt = new UADataType(DataTypeIds.String);
            var node = CommonTestUtils.GetSimpleVariable("test", pdt);

            // Enable extras
            tester.Config.Extraction.NodeTypes.Metadata = true;
            node.FullAttributes.TypeDefinition = new UAVariableType(new NodeId("type", 0));
            node.FullAttributes.TypeDefinition.Attributes.DisplayName = "ExtraType";

            // Add property with same name as extra
            var typeDefProp = CommonTestUtils.GetSimpleVariable("TypeDefinition", pdt);
            typeDefProp.FullAttributes.Value = new Variant("PropertyType");
            node.Attributes.Properties = new List<BaseUANode> { typeDefProp };

            var jsonMetadata = node.BuildMetadataAsJson(tester.Config, extractor, true);

            Assert.Single(jsonMetadata);
            Assert.True(jsonMetadata.ContainsKey("TypeDefinition"));
            // Property should overwrite extra
            Assert.Equal("PropertyType", jsonMetadata["TypeDefinition"]?.GetValue<string>());
        }

        [Fact]
        public void TestToIdmTimeSeriesComplexJsonStructure()
        {
            using var extractor = tester.BuildExtractor();
            var variable = CommonTestUtils.GetSimpleVariable("testVar", new UADataType(DataTypeIds.Double));

            // Create properties that would result in complex JSON structures
            var arrayProp = CommonTestUtils.GetSimpleVariable("arrayProp", new UADataType(DataTypeIds.Double), 3);
            arrayProp.FullAttributes.Value = new Variant(new double[] { 1.0, 2.0, 3.0 });

            variable.Attributes.Properties = new List<BaseUANode> { arrayProp };

            tester.Config.Cognite = new CognitePusherConfig
            {
                MetadataTargets = new MetadataTargetsConfig
                {
                    Clean = new CleanMetadataTargetConfig
                    {
                        MetadataAsJson = true
                    }
                }
            };

            var result = variable.ToIdmTimeSeries(extractor, "testSpace", "testSource", tester.Config, null);

            Assert.NotNull(result);
            Assert.NotNull(result.Properties.extractedData);
            Assert.True(result.Properties.extractedData.ContainsKey("arrayProp"));

            // The exact structure will depend on how TypeConverter.ConvertToJson handles arrays
            var arrayValue = result.Properties.extractedData["arrayProp"];
            Assert.NotNull(arrayValue);
        }

        #endregion

        #region Configuration Tests

        [Fact]
        public void TestCleanMetadataTargetConfigDefaultValue()
        {
            var config = new CleanMetadataTargetConfig();
            Assert.False(config.MetadataAsJson); // Should default to false
        }

        [Fact]
        public void TestCleanMetadataTargetConfigSetValue()
        {
            var config = new CleanMetadataTargetConfig
            {
                MetadataAsJson = true
            };
            Assert.True(config.MetadataAsJson);
        }

        #endregion
    }
}

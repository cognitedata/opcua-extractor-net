using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class PusherUtilsTestFixture : BaseExtractorTestFixture
    {
        public PusherUtilsTestFixture() : base() { }
    }
    public class PusherUtilsTest : MakeConsoleWork, IClassFixture<PusherUtilsTestFixture>
    {
        private readonly PusherUtilsTestFixture tester;
        public PusherUtilsTest(ITestOutputHelper output, PusherUtilsTestFixture tester) : base(output)
        {
            if (tester == null) throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            this.tester = tester;
        }
        [Fact]
        public void TestGetTimestampValue()
        {
            var ts = DateTime.UtcNow;
            var result = ts.ToUnixTimeMilliseconds();
            Assert.Equal(result, PusherUtils.GetTimestampValue(ts));
            Assert.Equal(result, PusherUtils.GetTimestampValue(result));
            Assert.Equal(result, PusherUtils.GetTimestampValue(result.ToString(CultureInfo.InvariantCulture)));
            Assert.Equal(0, PusherUtils.GetTimestampValue("Hey there"));
        }
        private static string GetStringValue(JsonElement? json, string name)
        {
            return json.Value.GetProperty(name).GetString();
        }

        private static RawRow ToRawRow(JsonElement raw)
        {
            var columns = new Dictionary<string, JsonElement>();
            foreach (var field in raw.EnumerateObject())
            {
                columns[field.Name] = field.Value;
            }
            return new RawRow
            {
                Columns = columns,
                LastUpdatedTime = 0,
                Key = GetStringValue(raw, "externalId")
            };
        }

        [Fact]
        public void TestGetTsUpdate()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UAVariable(new NodeId("test"), "test", new NodeId("parent"));
            node.Attributes.Description = "description";
            node.Attributes.Properties = new List<UANode>();
            node.VariableAttributes.DataType = new UADataType(DataTypeIds.Boolean);
            var pdt = new UADataType(DataTypeIds.String);
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = new UAVariable(new NodeId($"prop{i}"), $"prop{i}", NodeId.Null);
                prop.VariableAttributes.DataType = pdt;
                prop.SetDataPoint($"value{i}");
                node.AddProperty(prop);
            }
            // Need to create this manually to match
            var ts = new TimeSeries
            {
                ExternalId = "gp.base:s=test",
                Name = "test",
                AssetId = 111,
                Description = "description",
                Metadata = new Dictionary<string, string>
                {
                    { "prop1", "value1" },
                    { "prop2", "value2" },
                    { "prop3", "value3" },
                    { "prop4", "value4" }
                }
            };
            var nodeToAssetIds = new Dictionary<NodeId, long>
            {
                { new NodeId("parent"), 111 },
                { new NodeId("parent2"), 222 }
            };

            var update = new TypeUpdateConfig();

            Assert.Null(PusherUtils.GetTSUpdate(tester.Config.Extraction, extractor.DataTypeManager, extractor.StringConverter, null, node, update, nodeToAssetIds));
            var result = PusherUtils.GetTSUpdate(tester.Config.Extraction, extractor.DataTypeManager, extractor.StringConverter, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            update.Context = true;
            update.Description = true;
            update.Metadata = true;
            update.Name = true;
            result = PusherUtils.GetTSUpdate(tester.Config.Extraction, extractor.DataTypeManager, extractor.StringConverter, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            // Update everything
            var oldProperties = node.Properties.ToList();
            node = new UAVariable(new NodeId("test2"), "test2", new NodeId("parent2"));
            node.Attributes.Description = "description2";
            node.Attributes.Properties = oldProperties;
            oldProperties.RemoveAt(1);
            oldProperties.Add(CommonTestUtils.GetSimpleVariable("prop-new", pdt));
            (oldProperties[3] as UAVariable).SetDataPoint("value-new");
            (oldProperties[2] as UAVariable).SetDataPoint("value4-new");

            result = PusherUtils.GetTSUpdate(tester.Config.Extraction,
                extractor.DataTypeManager, extractor.StringConverter, ts, node, new TypeUpdateConfig(), nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            result = PusherUtils.GetTSUpdate(tester.Config.Extraction,
                extractor.DataTypeManager, extractor.StringConverter, ts, node, update, nodeToAssetIds);
            Assert.Equal("description2", result.Description.Set);
            Assert.Equal("test2", result.Name.Set);
            Assert.Equal(222, result.AssetId.Set);
            Assert.Equal(4, result.Metadata.Set.Count);
            Assert.Equal("value1", result.Metadata.Set["prop1"]);
            Assert.False(result.Metadata.Set.ContainsKey("prop2"));
            Assert.Equal("value3", result.Metadata.Set["prop3"]);
            Assert.Equal("value4-new", result.Metadata.Set["prop4"]);
            Assert.Equal("value-new", result.Metadata.Set["prop-new"]);
            Assert.Null(result.ExternalId);

            // Update with null values, and missing asset
            node = new UAVariable(new NodeId("test3"), null, new NodeId("parent3"));
            result = PusherUtils.GetTSUpdate(tester.Config.Extraction, extractor.DataTypeManager, extractor.StringConverter, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);
        }
        [Fact]
        public void TestGetAssetUpdate()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UANode(new NodeId("test"), "test", new NodeId("parent"), NodeClass.Object);
            node.Attributes.Description = "description";
            node.Attributes.Properties = new List<UANode>();
            var pdt = new UADataType(DataTypeIds.String);
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = new UAVariable(new NodeId($"prop{i}"), $"prop{i}", NodeId.Null);
                prop.VariableAttributes.DataType = pdt;
                prop.SetDataPoint($"value{i}");
                node.AddProperty(prop);
            }

            var asset = new Asset
            {
                ExternalId = "gp.base:s=test",
                Name = "test",
                ParentExternalId = "gp.base:s=parent",
                Description = "description",
                Metadata = new Dictionary<string, string>
                {
                    { "prop1", "value1" },
                    { "prop2", "value2" },
                    { "prop3", "value3" },
                    { "prop4", "value4" }
                }
            };
            var update = new TypeUpdateConfig();

            Assert.Null(PusherUtils.GetAssetUpdate(tester.Config.Extraction, asset, null, extractor, update));
            var result = PusherUtils.GetAssetUpdate(tester.Config.Extraction, asset, node, extractor, update);
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            update.Context = true;
            update.Description = true;
            update.Metadata = true;
            update.Name = true;
            result = PusherUtils.GetAssetUpdate(tester.Config.Extraction, asset, node, extractor, update);
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            // Update everything
            var oldProperties = node.Properties.ToList();
            node = new UANode(new NodeId("test2"), "test2", new NodeId("parent2"), NodeClass.Object);
            node.Attributes.Description = "description2";
            node.Attributes.Properties = oldProperties;
            oldProperties.RemoveAt(1);
            oldProperties.Add(CommonTestUtils.GetSimpleVariable("prop-new", pdt));
            (oldProperties[3] as UAVariable).SetDataPoint("value-new");
            (oldProperties[2] as UAVariable).SetDataPoint("value4-new");

            result = PusherUtils.GetAssetUpdate(tester.Config.Extraction, asset, node, extractor, new TypeUpdateConfig());
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            result = PusherUtils.GetAssetUpdate(tester.Config.Extraction, asset, node, extractor, update);
            Assert.Equal("description2", result.Description.Set);
            Assert.Equal("test2", result.Name.Set);
            Assert.Equal("gp.base:s=parent2", result.ParentExternalId.Set);
            Assert.Equal(4, result.Metadata.Set.Count);
            Assert.Equal("value1", result.Metadata.Set["prop1"]);
            Assert.False(result.Metadata.Set.ContainsKey("prop2"));
            Assert.Equal("value3", result.Metadata.Set["prop3"]);
            Assert.Equal("value4-new", result.Metadata.Set["prop4"]);
            Assert.Equal("value-new", result.Metadata.Set["prop-new"]);
            Assert.Null(result.ExternalId);

            node = new UANode(new NodeId("test3"), null, NodeId.Null, NodeClass.Object);
            result = PusherUtils.GetAssetUpdate(tester.Config.Extraction, asset, node, extractor, update);
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);
        }

        [Fact]
        public void TestRawUpdate()
        {
            var log = tester.Provider.GetRequiredService<ILogger<PusherUtilsTest>>();

            // Test null
            Assert.Null(PusherUtils.CreateRawUpdate(log, null, null, null, ConverterType.Node));

            var properties = new List<UANode>
            {
                new UAVariable(new NodeId("prop1"), "prop1", NodeId.Null, NodeClass.Variable),
                new UANode(new NodeId("prop2"), "prop2", NodeId.Null, NodeClass.Object),
            };
            var deepProp = new UAVariable(new NodeId("prop3"), "prop3", NodeId.Null, NodeClass.Variable);
            properties[1].AddProperty(deepProp);
            deepProp.SetDataPoint("value3");
            (properties[0] as UAVariable).SetDataPoint(new[] { 1, 2, 3, 4 });

            // Test create asset
            var node = new UANode(new NodeId("test"), "test", new NodeId("parent"), NodeClass.Object);
            foreach (var prop in properties) node.AddProperty(prop);
            var result = PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, node, null, ConverterType.Node);
            Assert.Equal(@"{""externalId"":""gp.base:s=test"",""name"":""test"",""description"":null,""metadata"":{""prop1"":[1,2,3,4]," +
                @"""prop2"":{""prop3"":""value3""}},""parentExternalId"":""gp.base:s=parent""}", result.ToString());

            var nodeRow = ToRawRow(result.Value);

            Assert.Null(PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, node, nodeRow, ConverterType.Node));

            // Modified description
            node.Attributes.Description = "desc";
            result = PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, node, nodeRow, ConverterType.Node);
            Assert.Equal(@"{""externalId"":""gp.base:s=test"",""name"":""test"",""description"":""desc"",""metadata"":{""prop1"":[1,2,3,4]," +
                @"""prop2"":{""prop3"":""value3""}},""parentExternalId"":""gp.base:s=parent""}", result.ToString());

            var variable = new UAVariable(new NodeId("test"), "test", new NodeId("parent"), NodeClass.Variable);
            variable.VariableAttributes.DataType = new UADataType(new NodeId("dt"));
            variable.SetDataPoint("test");

            foreach (var prop in properties) variable.AddProperty(prop);
            result = PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, variable, null, ConverterType.Variable);
            Assert.Equal(@"{""externalId"":""gp.base:s=test"",""name"":""test"",""description"":null,""metadata"":{""prop1"":[1,2,3,4]," +
                @"""prop2"":{""prop3"":""value3""}},""assetExternalId"":""gp.base:s=parent"",""isString"":true,""isStep"":false}", result.ToString());

            nodeRow = ToRawRow(result.Value);

            Assert.Null(PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, variable, nodeRow, ConverterType.Variable));

            variable.VariableAttributes.DataType = new UADataType(DataTypeIds.Boolean);
            result = PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, variable, nodeRow, ConverterType.Variable);
            Assert.Equal(@"{""externalId"":""gp.base:s=test"",""name"":""test"",""description"":null,""metadata"":{""prop1"":[1,2,3,4]," +
                @"""prop2"":{""prop3"":""value3""}},""assetExternalId"":""gp.base:s=parent"",""isString"":false,""isStep"":true}", result.ToString());
        }
    }
}

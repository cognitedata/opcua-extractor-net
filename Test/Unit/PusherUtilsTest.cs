using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
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
    [Collection("Shared server tests")]
    public class PusherUtilsTest
    {
        private readonly StaticServerTestFixture tester;
        public PusherUtilsTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            ArgumentNullException.ThrowIfNull(tester);
            tester.ResetConfig();
            tester.Init(output);
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

        private static RawRow<Dictionary<string, JsonElement>> ToRawRow(JsonElement raw)
        {
            var columns = new Dictionary<string, JsonElement>();
            foreach (var field in raw.EnumerateObject())
            {
                columns[field.Name] = field.Value;
            }
            return new RawRow<Dictionary<string, JsonElement>>
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
            var node = new UAVariable(new NodeId("test", 0), "test", null, null, new NodeId("parent", 0), null);
            node.Attributes.Description = "description";
            node.Attributes.Properties = new List<BaseUANode>();
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Boolean);
            var pdt = new UADataType(DataTypeIds.String);
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = new UAVariable(new NodeId($"prop{i}", 0), $"prop{i}", null, null, NodeId.Null, null);
                prop.FullAttributes.DataType = pdt;
                prop.FullAttributes.Value = new Variant($"value{i}");
                node.Attributes.Properties.Add(prop);
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
                { new NodeId("parent", 0), 111 },
                { new NodeId("parent2", 0), 222 }
            };

            var update = new TypeUpdateConfig();

            Assert.Null(PusherUtils.GetTSUpdate(tester.Config, extractor, null, node, update, nodeToAssetIds));
            var result = PusherUtils.GetTSUpdate(tester.Config, extractor, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            update.Context = true;
            update.Description = true;
            update.Metadata = true;
            update.Name = true;
            result = PusherUtils.GetTSUpdate(tester.Config, extractor, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            // Update everything
            var oldProperties = node.Properties.ToList();
            node = new UAVariable(new NodeId("test2", 0), "test2", null, null, new NodeId("parent2", 0), null);
            node.Attributes.Description = "description2";
            node.Attributes.Properties = oldProperties;
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            oldProperties.RemoveAt(1);
            oldProperties.Add(CommonTestUtils.GetSimpleVariable("prop-new", pdt));
            (oldProperties[3] as UAVariable).FullAttributes.Value = new Variant("value-new");
            (oldProperties[2] as UAVariable).FullAttributes.Value = new Variant("value4-new");

            result = PusherUtils.GetTSUpdate(tester.Config, extractor, ts, node, new TypeUpdateConfig(), nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            result = PusherUtils.GetTSUpdate(tester.Config, extractor, ts, node, update, nodeToAssetIds);
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
            node = new UAVariable(new NodeId("test3", 0), null, null, null, new NodeId("parent3", 0), null);
            node.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            result = PusherUtils.GetTSUpdate(tester.Config, extractor, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);
        }
        [Fact]
        public void TestGetAssetUpdate()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UAObject(new NodeId("test", 0), "test", null, null, new NodeId("parent", 0), null);
            node.Attributes.Description = "description";
            node.Attributes.Properties = new List<BaseUANode>();
            var pdt = new UADataType(DataTypeIds.String);
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = new UAVariable(new NodeId($"prop{i}", 0), $"prop{i}", null, null, NodeId.Null, null);
                prop.FullAttributes.DataType = pdt;
                prop.FullAttributes.Value = new Variant($"value{i}");
                node.FullAttributes.Properties.Add(prop);
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

            Assert.Null(PusherUtils.GetAssetUpdate(tester.Config, asset, null, extractor, update));
            var result = PusherUtils.GetAssetUpdate(tester.Config, asset, node, extractor, update);
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            update.Context = true;
            update.Description = true;
            update.Metadata = true;
            update.Name = true;
            result = PusherUtils.GetAssetUpdate(tester.Config, asset, node, extractor, update);
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            // Update everything
            var oldProperties = node.Properties.ToList();
            node = new UAObject(new NodeId("test2", 0), "test2", null, null, new NodeId("parent2", 0), null);
            node.Attributes.Description = "description2";
            node.Attributes.Properties = oldProperties;
            oldProperties.RemoveAt(1);
            oldProperties.Add(CommonTestUtils.GetSimpleVariable("prop-new", pdt));
            (oldProperties[3] as UAVariable).FullAttributes.Value = new Variant("value-new");
            (oldProperties[2] as UAVariable).FullAttributes.Value = new Variant("value4-new");

            result = PusherUtils.GetAssetUpdate(tester.Config, asset, node, extractor, new TypeUpdateConfig());
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            result = PusherUtils.GetAssetUpdate(tester.Config, asset, node, extractor, update);
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

            node = new UAObject(new NodeId("test3", 0), null, null, null, NodeId.Null, null);
            result = PusherUtils.GetAssetUpdate(tester.Config, asset, node, extractor, update);
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

            var properties = new List<BaseUANode>
            {
                new UAVariable(new NodeId("prop1", 0), "prop1", null, null, NodeId.Null, null),
                new UAObject(new NodeId("prop2", 0), "prop2", null, null, NodeId.Null, null),
            };
            var deepProp = new UAVariable(new NodeId("prop3", 0), "prop3", null, null, NodeId.Null, null);
            properties[1].Attributes.AddProperty(deepProp);
            deepProp.FullAttributes.Value = new Variant("value3");
            (properties[0] as UAVariable).FullAttributes.Value = new Variant(new[] { 1, 2, 3, 4 });

            // Test create asset
            var node = new UAObject(new NodeId("test", 0), "test", null, null, new NodeId("parent", 0), null);
            foreach (var prop in properties) node.Attributes.AddProperty(prop);
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

            var variable = new UAVariable(new NodeId("test", 0), "test", null, null, new NodeId("parent", 0), null);
            variable.FullAttributes.DataType = new UADataType(new NodeId("dt", 0));
            variable.FullAttributes.Value = new Variant("test");

            foreach (var prop in properties) variable.Attributes.AddProperty(prop);
            result = PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, variable, null, ConverterType.Variable);
            Assert.Equal(@"{""externalId"":""gp.base:s=test"",""name"":""test"",""description"":null,""metadata"":{""prop1"":[1,2,3,4]," +
                @"""prop2"":{""prop3"":""value3""}},""assetExternalId"":""gp.base:s=parent"",""isString"":true,""isStep"":false}", result.ToString());

            nodeRow = ToRawRow(result.Value);

            Assert.Null(PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, variable, nodeRow, ConverterType.Variable));

            variable.FullAttributes.DataType = new UADataType(DataTypeIds.Boolean);
            result = PusherUtils.CreateRawUpdate(log, tester.Client.StringConverter, variable, nodeRow, ConverterType.Variable);
            Assert.Equal(@"{""externalId"":""gp.base:s=test"",""name"":""test"",""description"":null,""metadata"":{""prop1"":[1,2,3,4]," +
                @"""prop2"":{""prop3"":""value3""}},""assetExternalId"":""gp.base:s=parent"",""isString"":false,""isStep"":true}", result.ToString());
        }
    }
}

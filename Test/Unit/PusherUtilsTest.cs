using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class PusherUtilsTestFixture : BaseExtractorTestFixture
    {
        public PusherUtilsTestFixture() : base(62800) { }
    }
    public class PusherUtilsTest : MakeConsoleWork, IClassFixture<PusherUtilsTestFixture>
    {
        private PusherUtilsTestFixture tester;
        public PusherUtilsTest(ITestOutputHelper output, PusherUtilsTestFixture tester) : base(output)
        {
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
        public void TestCreateRawTsUpdate()
        {
            using var extractor = tester.BuildExtractor();
            // First get the result directly, when the passed RawRow is null
            var node = new UAVariable(new NodeId("test"), "test", new NodeId("parent"));
            node.Description = "description";
            node.Properties = new List<UAVariable>();
            node.DataType = new UADataType(DataTypeIds.Boolean);
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = new UAVariable(new NodeId($"prop{i}"), $"prop{i}", NodeId.Null);
                prop.SetDataPoint($"value{i}", now, tester.Client);
                node.Properties.Add(prop);
            }

            var update = new TypeUpdateConfig();

            Assert.Null(PusherUtils.CreateRawTsUpdate(null, extractor, null, update, null));
            var result1 = PusherUtils.CreateRawTsUpdate(node, extractor, null, update, null);
            Assert.NotNull(result1);
            Assert.Equal("test", GetStringValue(result1, "name"));
            Assert.Equal("description", GetStringValue(result1, "description"));
            Assert.Equal("gp.base:s=test", GetStringValue(result1, "externalId"));
            Assert.Equal("gp.base:s=parent", GetStringValue(result1, "assetExternalId"));
            Assert.True(result1.Value.GetProperty("isStep").GetBoolean());
            Assert.False(result1.Value.GetProperty("isString").GetBoolean());
            var meta = result1.Value.GetProperty("metadata");
            Assert.Equal("value1", GetStringValue(meta, "prop1"));
            Assert.Equal("value4", GetStringValue(meta, "prop4"));


            // Update, but keep the TypeUpdateConfig at default
            var oldProperties = node.Properties;
            node = new UAVariable(new NodeId("test2"), "test2", new NodeId("parent2"));
            node.Description = "description2";
            node.Properties = oldProperties;
            oldProperties.RemoveAt(1);
            oldProperties.Add(new UAVariable(new NodeId("prop-new"), "prop-new", NodeId.Null));
            oldProperties[3].SetDataPoint("value-new", now, tester.Client);
            oldProperties[2].SetDataPoint("value4-new", now, tester.Client);

            var result2 = PusherUtils.CreateRawTsUpdate(node, extractor, ToRawRow(result1.Value), update, null);
            Assert.Null(result2);

            // Update, set all TypeUpdateConfig fields
            update.Context = true;
            update.Description = true;
            update.Metadata = true;
            update.Name = true;
            result2 = PusherUtils.CreateRawTsUpdate(node, extractor, ToRawRow(result1.Value), update, null);

            Assert.NotNull(result2);
            Assert.Equal("test2", GetStringValue(result2, "name"));
            Assert.Equal("description2", GetStringValue(result2, "description"));
            Assert.Equal("gp.base:s=test", GetStringValue(result2, "externalId"));
            Assert.Equal("gp.base:s=parent2", GetStringValue(result2, "assetExternalId"));
            meta = result2.Value.GetProperty("metadata");
            Assert.Equal("value1", GetStringValue(meta, "prop1"));
            Assert.False(meta.TryGetProperty("value2", out var _));
            Assert.Equal("value3", GetStringValue(meta, "prop3"));
            Assert.Equal("value4-new", GetStringValue(meta, "prop4"));
            Assert.Equal("value-new", GetStringValue(meta, "prop-new"));
            Assert.True(result2.Value.GetProperty("isStep").GetBoolean());
            Assert.False(result2.Value.GetProperty("isString").GetBoolean());

            // Try to update, but all fields are null except description
            node = new UAVariable(new NodeId("test3"), null, NodeId.Null);
            node.Description = "description3";
            result2 = PusherUtils.CreateRawTsUpdate(node, extractor, ToRawRow(result1.Value), update, null);
            Assert.NotNull(result2);
            Assert.Equal("test", GetStringValue(result2, "name"));
            Assert.Equal("description3", GetStringValue(result2, "description"));
            Assert.Equal("gp.base:s=test", GetStringValue(result2, "externalId"));
            Assert.Equal("gp.base:s=parent", GetStringValue(result2, "assetExternalId"));
            Assert.True(result2.Value.GetProperty("isStep").GetBoolean());
            Assert.False(result2.Value.GetProperty("isString").GetBoolean());
            meta = result2.Value.GetProperty("metadata");
            Assert.Equal("value1", GetStringValue(meta, "prop1"));
            Assert.Equal("value4", GetStringValue(meta, "prop4"));
        }
        [Fact]
        public void TestCreateRawAssetUpdate()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UANode(new NodeId("test"), "test", new NodeId("parent"));
            node.Description = "description";
            node.Properties = new List<UAVariable>();
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = new UAVariable(new NodeId($"prop{i}"), $"prop{i}", NodeId.Null);
                prop.SetDataPoint($"value{i}", now, tester.Client);
                node.Properties.Add(prop);
            }

            var update = new TypeUpdateConfig();

            Assert.Null(PusherUtils.CreateRawAssetUpdate(null, extractor, null, update, null));
            var result1 = PusherUtils.CreateRawAssetUpdate(node, extractor, null, update, null);
            Assert.NotNull(result1);
            Assert.Equal("test", GetStringValue(result1, "name"));
            Assert.Equal("description", GetStringValue(result1, "description"));
            Assert.Equal("gp.base:s=test", GetStringValue(result1, "externalId"));
            Assert.Equal("gp.base:s=parent", GetStringValue(result1, "parentExternalId"));
            var meta = result1.Value.GetProperty("metadata");
            Assert.Equal("value1", GetStringValue(meta, "prop1"));
            Assert.Equal("value4", GetStringValue(meta, "prop4"));

            // Update, but keep TypeUpdateConfig at default
            var oldProperties = node.Properties;
            node = new UANode(new NodeId("test2"), "test2", new NodeId("parent2"));
            node.Description = "description2";
            node.Properties = oldProperties;
            oldProperties.RemoveAt(1);
            oldProperties.Add(new UAVariable(new NodeId("prop-new"), "prop-new", NodeId.Null));
            oldProperties[3].SetDataPoint("value-new", now, tester.Client);
            oldProperties[2].SetDataPoint("value4-new", now, tester.Client);

            var result2 = PusherUtils.CreateRawAssetUpdate(node, extractor, ToRawRow(result1.Value), update, null);
            Assert.Null(result2);

            // Update, set all TypeUpdateConfig fields
            update.Context = true;
            update.Description = true;
            update.Metadata = true;
            update.Name = true;
            result2 = PusherUtils.CreateRawAssetUpdate(node, extractor, ToRawRow(result1.Value), update, null);
            Assert.NotNull(result2);
            Assert.Equal("test2", GetStringValue(result2, "name"));
            Assert.Equal("description2", GetStringValue(result2, "description"));
            Assert.Equal("gp.base:s=test", GetStringValue(result2, "externalId"));
            Assert.Equal("gp.base:s=parent2", GetStringValue(result2, "parentExternalId"));
            meta = result2.Value.GetProperty("metadata");
            Assert.Equal("value1", GetStringValue(meta, "prop1"));
            Assert.False(meta.TryGetProperty("value2", out var _));
            Assert.Equal("value3", GetStringValue(meta, "prop3"));
            Assert.Equal("value4-new", GetStringValue(meta, "prop4"));
            Assert.Equal("value-new", GetStringValue(meta, "prop-new"));

            // Try to update, but all fields are null except description
            node = new UANode(new NodeId("test3"), null, NodeId.Null);
            node.Description = "description3";
            result2 = PusherUtils.CreateRawAssetUpdate(node, extractor, ToRawRow(result1.Value), update, null);
            Assert.NotNull(result2);
            Assert.Equal("test", GetStringValue(result2, "name"));
            Assert.Equal("description3", GetStringValue(result2, "description"));
            Assert.Equal("gp.base:s=test", GetStringValue(result2, "externalId"));
            Assert.Equal("gp.base:s=parent", GetStringValue(result2, "parentExternalId"));
            meta = result2.Value.GetProperty("metadata");
            Assert.Equal("value1", GetStringValue(meta, "prop1"));
            Assert.Equal("value4", GetStringValue(meta, "prop4"));
        }
        [Fact]
        public void TestGetTsUpdate()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UAVariable(new NodeId("test"), "test", new NodeId("parent"));
            node.Description = "description";
            node.Properties = new List<UAVariable>();
            node.DataType = new UADataType(DataTypeIds.Boolean);
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = new UAVariable(new NodeId($"prop{i}"), $"prop{i}", NodeId.Null);
                prop.SetDataPoint($"value{i}", now, tester.Client);
                node.Properties.Add(prop);
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

            Assert.Null(PusherUtils.GetTSUpdate(extractor, null, node, update, nodeToAssetIds));
            var result = PusherUtils.GetTSUpdate(extractor, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            update.Context = true;
            update.Description = true;
            update.Metadata = true;
            update.Name = true;
            result = PusherUtils.GetTSUpdate(extractor, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            // Update everything
            var oldProperties = node.Properties;
            node = new UAVariable(new NodeId("test2"), "test2", new NodeId("parent2"));
            node.Description = "description2";
            node.Properties = oldProperties;
            oldProperties.RemoveAt(1);
            oldProperties.Add(new UAVariable(new NodeId("prop-new"), "prop-new", NodeId.Null));
            oldProperties[3].SetDataPoint("value-new", now, tester.Client);
            oldProperties[2].SetDataPoint("value4-new", now, tester.Client);

            result = PusherUtils.GetTSUpdate(extractor, ts, node, new TypeUpdateConfig(), nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            result = PusherUtils.GetTSUpdate(extractor, ts, node, update, nodeToAssetIds);
            Assert.Equal("description2", result.Description.Set);
            Assert.Equal("test2", result.Name.Set);
            Assert.Equal(222, result.AssetId.Set);
            Assert.Equal(4, result.Metadata.Add.Count);
            Assert.Equal("value1", result.Metadata.Add["prop1"]);
            Assert.False(result.Metadata.Add.ContainsKey("prop2"));
            Assert.Equal("value3", result.Metadata.Add["prop3"]);
            Assert.Equal("value4-new", result.Metadata.Add["prop4"]);
            Assert.Equal("value-new", result.Metadata.Add["prop-new"]);
            Assert.Null(result.ExternalId);

            // Update with null values, and missing asset
            node = new UAVariable(new NodeId("test3"), null, new NodeId("parent3"));
            result = PusherUtils.GetTSUpdate(extractor, ts, node, update, nodeToAssetIds);
            Assert.Null(result.AssetId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);
        }
        [Fact]
        public void TestGetAssetUpdate()
        {
            using var extractor = tester.BuildExtractor();
            var node = new UANode(new NodeId("test"), "test", new NodeId("parent"));
            node.Description = "description";
            node.Properties = new List<UAVariable>();
            var now = DateTime.UtcNow;
            for (int i = 1; i < 5; i++)
            {
                var prop = new UAVariable(new NodeId($"prop{i}"), $"prop{i}", NodeId.Null);
                prop.SetDataPoint($"value{i}", now, tester.Client);
                node.Properties.Add(prop);
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

            Assert.Null(PusherUtils.GetAssetUpdate(asset, null, extractor, update));
            var result = PusherUtils.GetAssetUpdate(asset, node, extractor, update);
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            update.Context = true;
            update.Description = true;
            update.Metadata = true;
            update.Name = true;
            result = PusherUtils.GetAssetUpdate(asset, node, extractor, update);
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            // Update everything
            var oldProperties = node.Properties;
            node = new UANode(new NodeId("test2"), "test2", new NodeId("parent2"));
            node.Description = "description2";
            node.Properties = oldProperties;
            oldProperties.RemoveAt(1);
            oldProperties.Add(new UAVariable(new NodeId("prop-new"), "prop-new", NodeId.Null));
            oldProperties[3].SetDataPoint("value-new", now, tester.Client);
            oldProperties[2].SetDataPoint("value4-new", now, tester.Client);

            result = PusherUtils.GetAssetUpdate(asset, node, extractor, new TypeUpdateConfig());
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);

            result = PusherUtils.GetAssetUpdate(asset, node, extractor, update);
            Assert.Equal("description2", result.Description.Set);
            Assert.Equal("test2", result.Name.Set);
            Assert.Equal("gp.base:s=parent2", result.ParentExternalId.Set);
            Assert.Equal(4, result.Metadata.Add.Count);
            Assert.Equal("value1", result.Metadata.Add["prop1"]);
            Assert.False(result.Metadata.Add.ContainsKey("prop2"));
            Assert.Equal("value3", result.Metadata.Add["prop3"]);
            Assert.Equal("value4-new", result.Metadata.Add["prop4"]);
            Assert.Equal("value-new", result.Metadata.Add["prop-new"]);
            Assert.Null(result.ExternalId);

            node = new UANode(new NodeId("test3"), null, NodeId.Null);
            result = PusherUtils.GetAssetUpdate(asset, node, extractor, update);
            Assert.Null(result.ParentExternalId);
            Assert.Null(result.Description);
            Assert.Null(result.Metadata);
            Assert.Null(result.Name);
        }
    }
}

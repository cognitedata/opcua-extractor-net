using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Xml;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;
using JsonSerializer = Newtonsoft.Json.JsonSerializer;

namespace Test.Unit
{
    [Collection("Shared server tests")]
    public class StringConversionTest : MakeConsoleWork
    {
        private readonly StaticServerTestFixture tester;
        public StringConversionTest(ITestOutputHelper output, StaticServerTestFixture tester) : base(output)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
        }
        [Fact]
        public void TestConvertToString()
        {
            var log = tester.Provider.GetRequiredService<ILogger<StringConverter>>();
            var converter = new StringConverter(log, tester.Client, tester.Config);

            Assert.Equal("", converter.ConvertToString(null));
            Assert.Equal("gp.tl:s=abc", converter.ConvertToString(new NodeId("abc", 2)));
            Assert.Equal("gp.tl:s=abc", converter.ConvertToString(new ExpandedNodeId("abc", tester.Client.NamespaceTable.GetString(2))));
            Assert.Equal("test", converter.ConvertToString(new LocalizedText("EN-US", "test")));
            Assert.Equal("(0, 100)", converter.ConvertToString(new Opc.Ua.Range(100, 0)));
            Assert.Equal("N: Newton", converter.ConvertToString(new EUInformation { DisplayName = "N", Description = "Newton" }));
            Assert.Equal("N: Newton", converter.ConvertToString(new ExtensionObject(new EUInformation { DisplayName = "N", Description = "Newton" })));
            Assert.Equal("key: 1", converter.ConvertToString(new EnumValueType { DisplayName = "key", Value = 1 }));
            Assert.Equal("1234", converter.ConvertToString(1234));
            Assert.Equal("[123,1234]", converter.ConvertToString(new[] { 123, 1234 }));
            Assert.Equal(@"[""gp.tl:i=123"",""gp.tl:i=1234"",""gp.tl:s=abc""]", converter.ConvertToString(new[]
            {
                new NodeId(123u, 2), new NodeId(1234u, 2), new NodeId("abc", 2)
            }));
            Assert.Equal("somekey: gp.tl:s=abc", converter.ConvertToString(new Opc.Ua.KeyValuePair
            {
                Key = "somekey",
                Value = new NodeId("abc", 2)
            }));
            var readValueId = new ReadValueId { AttributeId = Attributes.Value, NodeId = new NodeId("test") };
            var readValueIdStr = @"{""NodeId"":{""IdType"":1,""Id"":""test""},""AttributeId"":13}";
            Assert.Equal(readValueIdStr, converter.ConvertToString(new Variant(readValueId)));
            var ids = new ReadValueIdCollection { readValueId, readValueId };
            // Results in Variant(ExtensionObject[])
            Assert.Equal($"[{readValueIdStr},{readValueIdStr}]", converter.ConvertToString(new Variant(ids)));
            var ids2 = new[] { readValueId, readValueId };
            // Results in [Variant(ExtensionObject), Variant(ExtensionObject)], so it ends up using our system
            Assert.Equal($"[{readValueIdStr},{readValueIdStr}]", converter.ConvertToString(new Variant(ids2)));
            // Simple matrix
#pragma warning disable CA1814 // Prefer jagged arrays over multidimensional
            var m1 = new Matrix(new int[3, 3] { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } }, BuiltInType.Int32);
            Assert.Equal("[[1,2,3],[4,5,6],[7,8,9]]", converter.ConvertToString(new Variant(m1)));
            // Complex matrix
            var m2 = new Matrix(new Variant[2, 2] {
                { new Variant(readValueId), new Variant(readValueId) },
                { new Variant(readValueId), new Variant(readValueId) } }, BuiltInType.Variant);
            Assert.Equal($"[[{readValueIdStr},{readValueIdStr}],[{readValueIdStr},{readValueIdStr}]]", converter.ConvertToString(new Variant(m2)));
#pragma warning restore CA1814 // Prefer jagged arrays over multidimensional
        }
        [Fact]
        public void TestConvertToStringJson()
        {
            var log = tester.Provider.GetRequiredService<ILogger<StringConverter>>();
            var converter = new StringConverter(log, tester.Client, tester.Config);

            Assert.Equal("null", converter.ConvertToString(null, null, null, true));
            Assert.Equal(@"""gp.tl:s=abc""", converter.ConvertToString(new NodeId("abc", 2), null, null, true));
            Assert.Equal(@"""gp.tl:s=abc""", converter.ConvertToString(new ExpandedNodeId("abc", tester.Client.NamespaceTable.GetString(2)), null, null, true));
            Assert.Equal(@"""test""", converter.ConvertToString(new LocalizedText("EN-US", "test"), null, null, true));
            Assert.Equal(@"""(0, 100)""", converter.ConvertToString(new Opc.Ua.Range(100, 0), null, null, true));
            Assert.Equal(@"""N: Newton""", converter.ConvertToString(new EUInformation { DisplayName = "N", Description = "Newton" }, null, null, true));
            Assert.Equal(@"""N: Newton""", converter.ConvertToString(new ExtensionObject(new EUInformation { DisplayName = "N", Description = "Newton" }),
                null, null, true));
            Assert.Equal(@"{""key"":1}", converter.ConvertToString(new EnumValueType { DisplayName = "key", Value = 1 }, null, null, true));
            Assert.Equal("1234", converter.ConvertToString(1234, null, null, true));
            Assert.Equal("[123,1234]", converter.ConvertToString(new[] { 123, 1234 }, null, null, true));
            Assert.Equal(@"[""gp.tl:i=123"",""gp.tl:i=1234"",""gp.tl:s=abc""]", converter.ConvertToString(new[]
            {
                new NodeId(123u, 2), new NodeId(1234u, 2), new NodeId("abc", 2)
            }, null, null, true));
            Assert.Equal(@"{""somekey"":""gp.tl:s=abc""}", converter.ConvertToString(new Opc.Ua.KeyValuePair
            {
                Key = "somekey",
                Value = new NodeId("abc", 2)
            }, null, null, true));
            Assert.Equal(@"{""enumkey"":1}", converter.ConvertToString(new Opc.Ua.EnumValueType
            {
                DisplayName = "enumkey",
                Value = 1
            }, null, null, true));
            var xml = new XmlDocument();
            xml.LoadXml("<?xml version='1.0' ?>" +
                "<test1 key1='val1' key2='val2'>" +
                "   <test2 key3='val3' key4='val4'>Content</test2>" +
                "</test1>");
            var xmlJson = converter.ConvertToString(xml.DocumentElement, null, null, true);
            Assert.Equal(@"{""test1"":{""@key1"":""val1"",""@key2"":""val2"",""test2"":"
                + @"{""@key3"":""val3"",""@key4"":""val4"",""#text"":""Content""}}}", xmlJson);
#pragma warning disable CA1814 // Prefer jagged arrays over multidimensional
            var m1 = new Matrix(new int[3, 3] { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } }, BuiltInType.Int32);
#pragma warning restore CA1814 // Prefer jagged arrays over multidimensional
            Assert.Equal("[[1,2,3],[4,5,6],[7,8,9]]", converter.ConvertToString(new Variant(m1), null, null, true));

            Assert.Equal(@"""Anonymous""", converter.ConvertToString(new Variant(UserTokenType.Anonymous), null, null, true));
            Assert.Equal(@"""bcabfe0c-1fe6-42c4-8dad-2d72e50e2dbd""", converter.ConvertToString(new Guid("bcabfe0c-1fe6-42c4-8dad-2d72e50e2dbd"), null, null, true));
            Assert.Equal(@"""Good""", converter.ConvertToString(new Variant(StatusCodes.Good, new TypeInfo(BuiltInType.StatusCode, -1)), null, null, true));

        }
        [Fact]
        public void TestConvertToStringJsonIssues()
        {
            // The OPC-UA JsonEncoder can be a bit unreliable, this is a brute-force way to check that it behaves properly
            // for all types, or they are handled externally.
            var log = tester.Provider.GetRequiredService<ILogger<StringConverter>>();
            var converter = new StringConverter(log, tester.Client, tester.Config);
            var failedTypes = new List<Type>();
            void TestJsonEncoder(Type type, Variant variant)
            {
                var builder = new StringBuilder("{");
                builder.Append(@"""value"":");
                try
                {
                    builder.Append(converter.ConvertToString(variant, null, null, true));
                }
                catch
                {
                    Console.WriteLine("Type: " + type + ", could not be serialized");
                    throw;
                }
                builder.Append('}');

                try
                {
                    JsonDocument.Parse(builder.ToString());
                }
                catch
                {
                    Console.WriteLine($"Type {type} produced invalid JSON: " + builder.ToString());
                    failedTypes.Add(type);
                }
            }

            foreach (var type in typeof(EnumValueType).Assembly.GetTypes())
            {
                if (type.IsAbstract || type.IsInterface) continue;
                Variant variant;
                // If we can't create it, we don't care
                try
                {
                    var obj = Activator.CreateInstance(type);
                    variant = new Variant(obj);
                }
                catch
                {
                    // None of the encodable types are unassignable, keeping this here just in case
                    if (typeof(IEncodeable).IsAssignableFrom(type))
                    {
                        throw;
                    }
                    continue;
                }
                TestJsonEncoder(type, variant);
            }

            Assert.Empty(failedTypes);
        }
        [Fact]
        public void TestNodeIdSerialization()
        {
            var serializer = new JsonSerializer();
            var converter = tester.Client.StringConverter;
            converter.AddConverters(serializer, ConverterType.Node);

            void TestConvert(NodeId id, string expected)
            {
                var sb = new StringBuilder();
                using var sw = new StringWriter(sb);
                using var writer = new JsonTextWriter(sw);
                serializer.Serialize(writer, id);
                writer.Flush();
                var result = sb.ToString();

                Assert.Equal(expected, result);

                using var sr = new StringReader(result);
                using var reader = new JsonTextReader(sr);
                var retId = serializer.Deserialize<NodeId>(reader);

                Assert.Equal(id, retId);
            }

            TestConvert(new NodeId(123u), @"{""idType"":0,""identifier"":123}");
            TestConvert(new NodeId("test"), @"{""idType"":1,""identifier"":""test""}");
            TestConvert(new NodeId(Guid.Parse("123e4567-e89b-12d3-a456-426614174000")),
                @"{""idType"":2,""identifier"":""123e4567-e89b-12d3-a456-426614174000""}");
            TestConvert(new NodeId(new byte[] { 6, 45, 213, 93 }), @"{""idType"":3,""identifier"":""Bi3VXQ==""}");
            TestConvert(NodeId.Null, @"{""idType"":0,""identifier"":0}");

            TestConvert(new NodeId(123u, 1),
                @"{""namespace"":""" + tester.Client.NamespaceTable.GetString(1)
                + @""",""idType"":0,""identifier"":123}");
            TestConvert(new NodeId("test", 2),
                @"{""namespace"":""" + tester.Client.NamespaceTable.GetString(2)
                + @""",""idType"":1,""identifier"":""test""}");
            TestConvert(new NodeId(Guid.Parse("123e4567-e89b-12d3-a456-426614174000"), 1),
                @"{""namespace"":""" + tester.Client.NamespaceTable.GetString(1)
                + @""",""idType"":2,""identifier"":""123e4567-e89b-12d3-a456-426614174000""}");
            TestConvert(new NodeId(new byte[] { 6, 45, 213, 93 }, 2),
                @"{""namespace"":""" + tester.Client.NamespaceTable.GetString(2)
                + @""",""idType"":3,""identifier"":""Bi3VXQ==""}");


            // Test bad conversions
            void TestBadConvert(string input)
            {
                using var sr = new StringReader(input);
                using var reader = new JsonTextReader(sr);
                var retId = serializer.Deserialize<NodeId>(reader);

                Assert.True(retId.IsNullNodeId);
            }
            TestBadConvert("{}");
            TestBadConvert(@"{""idType"":3}");
            TestBadConvert(@"{""identifier"":123}");
            TestBadConvert(@"{""idType"":4,""identifier"":123}");
            TestBadConvert(@"{""idType"":0,""identifier"":""test""}");
            TestBadConvert(@"{""idType"":-1,""identifier"":123}");
            TestBadConvert(@"{""idType"":2,""identifier"":""bleh""}");
            TestBadConvert(@"{""idType"":3,""identifier"":""åååå""}");
            TestBadConvert(@"{""idType"":0,""identifier"":123,""namespace"":""does-not-exist-space""}");
        }
        [Fact]
        public void TestWriteNodeIds()
        {
            var serializer = new JsonSerializer();
            var converter = tester.Client.StringConverter;
            converter.AddConverters(serializer, ConverterType.Node);

            void TestConvert(UANode node, string expected)
            {
                var sb = new StringBuilder();
                using var sw = new StringWriter(sb);
                using var writer = new JsonTextWriter(sw);
                serializer.Serialize(writer, node);
                writer.Flush();
                var result = sb.ToString();

                Assert.Equal(expected, result);
            }

            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;
            var node = new UANode(new NodeId("test"), "test", NodeId.Null, NodeClass.Object);
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":null,"
                + @"""NodeId"":{""idType"":1,""identifier"":""test""}}");

            node.SetNodeType(tester.Client, new NodeId("test-type"));
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":null,"
                + @"""NodeId"":{""idType"":1,""identifier"":""test""},"
                + @"""TypeDefinitionId"":{""idType"":1,""identifier"":""test-type""}}");

            node = new UANode(new NodeId("test"), "test", new NodeId("parent"), NodeClass.Object);
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":""gp.base:s=parent"","
                + @"""NodeId"":{""idType"":1,""identifier"":""test""},"
                + @"""ParentNodeId"":{""idType"":1,""identifier"":""parent""}}");

            serializer = new JsonSerializer();
            converter.AddConverters(serializer, ConverterType.Variable);

            var variable = new UAVariable(new NodeId("test"), "test", NodeId.Null, NodeClass.Variable);
            TestConvert(variable,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""assetExternalId"":null,"
                + @"""isString"":false,""isStep"":false,"
                + @"""NodeId"":{""idType"":1,""identifier"":""test""}}");

            variable.VariableAttributes.DataType = new UADataType(DataTypeIds.Boolean);

            TestConvert(variable,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""assetExternalId"":null,"
                + @"""isString"":false,""isStep"":true,"
                + @"""NodeId"":{""idType"":1,""identifier"":""test""},"
                + @"""DataTypeId"":{""idType"":0,""identifier"":1}}");
        }
        [Fact]
        public void TestWriteInternals()
        {
            var serializer = new JsonSerializer();
            var converter = tester.Client.StringConverter;
            converter.AddConverters(serializer, ConverterType.Node);

            void TestConvert(UANode node, string expected)
            {
                var sb = new StringBuilder();
                using var sw = new StringWriter(sb);
                using var writer = new JsonTextWriter(sw);
                serializer.Serialize(writer, node);
                writer.Flush();
                var result = sb.ToString();

                Assert.Equal(expected, result);
            }

            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            var node = new UANode(new NodeId("test"), "test", NodeId.Null, NodeClass.Object);
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":null,"
                + @"""InternalInfo"":{""EventNotifier"":0,""ShouldSubscribe"":true,""NodeClass"":1}}");

            node.Attributes.EventNotifier |= EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":null,"
                + @"""InternalInfo"":{""EventNotifier"":5,""ShouldSubscribe"":true,""NodeClass"":1}}");

            var variable = new UAVariable(new NodeId("test"), "test", NodeId.Null, NodeClass.Variable);
            serializer = new JsonSerializer();
            converter.AddConverters(serializer, ConverterType.Variable);
            variable.VariableAttributes.AccessLevel |= AccessLevels.CurrentRead | AccessLevels.HistoryRead;
            variable.VariableAttributes.Historizing = true;
            variable.VariableAttributes.ValueRank = -1;
            TestConvert(variable,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""assetExternalId"":null,"
                + @"""isString"":false,""isStep"":false,"
                + @"""InternalInfo"":{""EventNotifier"":0,""ShouldSubscribe"":true,""NodeClass"":2,""AccessLevel"":5,"
                + @"""Historizing"":true,""ValueRank"":-1}}");

            variable.VariableAttributes.ValueRank = ValueRanks.OneDimension;
            variable.VariableAttributes.ArrayDimensions = new[] { 5 };
            TestConvert(variable,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""assetExternalId"":null,"
                + @"""isString"":false,""isStep"":false,"
                + @"""InternalInfo"":{""EventNotifier"":0,""ShouldSubscribe"":true,""NodeClass"":2,""AccessLevel"":5,"
                + @"""Historizing"":true,""ValueRank"":1,""ArrayDimensions"":[5],""Index"":-1}}"
                );
        }
        [Fact]
        public void TestNodeDeserialization()
        {
            // Objects
            var serializer = new JsonSerializer();
            var converter = tester.Client.StringConverter;
            converter.AddConverters(serializer, ConverterType.Node);

            var node = new UANode(new NodeId("test", 2), "test", new NodeId("parent"), NodeClass.Object);
            node.Attributes.EventNotifier = 5;
            node.Attributes.ShouldSubscribe = true;

            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;

            SavedNode Convert(UANode node)
            {
                var sb = new StringBuilder();
                using var sw = new StringWriter(sb);
                using var writer = new JsonTextWriter(sw);
                serializer.Serialize(writer, node);
                writer.Flush();
                var json = sb.ToString();
                Console.WriteLine(json);
                var sr = new StringReader(json);
                using var reader = new JsonTextReader(sr);
                return serializer.Deserialize<SavedNode>(reader);
            }

            var saved = Convert(node);
            Assert.Equal(node.Id, saved.NodeId);
            Assert.Equal(node.NodeClass, saved.InternalInfo.NodeClass);
            Assert.Equal(node.ParentId, saved.ParentNodeId);
            Assert.Equal(node.ShouldSubscribe, saved.InternalInfo.ShouldSubscribe);
            Assert.Equal(node.EventNotifier, saved.InternalInfo.EventNotifier);

            // Variables
            serializer = new JsonSerializer();
            converter.AddConverters(serializer, ConverterType.Variable);

            var variable = new UAVariable(new NodeId("test", 2), "test", new NodeId("parent"), NodeClass.Variable);
            variable.VariableAttributes.AccessLevel = 5;
            variable.VariableAttributes.EventNotifier = 5;
            variable.VariableAttributes.DataType = new UADataType(DataTypeIds.Double);
            variable.VariableAttributes.ValueRank = -1;

            saved = Convert(variable);
            Assert.Equal(variable.Id, saved.NodeId);
            Assert.Equal(variable.NodeClass, saved.InternalInfo.NodeClass);
            Assert.Equal(variable.ParentId, saved.ParentNodeId);
            Assert.Equal(variable.ShouldSubscribe, saved.InternalInfo.ShouldSubscribe);
            Assert.Equal(variable.EventNotifier, saved.InternalInfo.EventNotifier);
            Assert.Equal(variable.DisplayName, saved.Name);
            Assert.Equal(variable.DataType.Raw, saved.DataTypeId);
            Assert.Equal(variable.AccessLevel, saved.InternalInfo.AccessLevel);
            Assert.Equal(variable.VariableAttributes.Historizing, saved.InternalInfo.Historizing);
            Assert.Equal(variable.ValueRank, saved.InternalInfo.ValueRank);

            variable.VariableAttributes.ValueRank = 2;
            variable.VariableAttributes.ArrayDimensions = new[] { 3, 4 };

            saved = Convert(variable);
            Assert.Equal(variable.ArrayDimensions, saved.InternalInfo.ArrayDimensions);
            Assert.Equal(variable.ValueRank, saved.InternalInfo.ValueRank);
        }
    }
}

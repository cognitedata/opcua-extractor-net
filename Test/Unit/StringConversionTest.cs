﻿using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Xml;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    [Collection("Shared server tests")]
    public class StringConversionTest
    {
        private readonly StaticServerTestFixture tester;
        public StringConversionTest(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
            tester.Client.TypeManager.Reset();
        }
        [Fact]
        public void TestConvertToString()
        {
            var log = tester.Provider.GetRequiredService<ILogger<TypeConverter>>();
            var converter = new TypeConverter(log, tester.Client, tester.Config);

            string ToString(object obj)
            {
                var variant = obj is Variant v ? v : new Variant(obj);
                return converter.ConvertToString(variant);
            }

            Assert.Equal("", ToString(null));
            Assert.Equal("gp.tl:s=abc", ToString(new NodeId("abc", 2)));
            Assert.Equal("gp.tl:s=abc", ToString(new ExpandedNodeId("abc", tester.Client.NamespaceTable.GetString(2))));
            Assert.Equal("test", ToString(new LocalizedText("EN-US", "test")));
            Assert.Equal("(0, 100)", ToString(new Opc.Ua.Range(100, 0)));
            Assert.Equal("N: Newton", ToString(new EUInformation { DisplayName = "N", Description = "Newton" }));
            Assert.Equal("N: Newton", ToString(new ExtensionObject(new EUInformation { DisplayName = "N", Description = "Newton" })));
            Assert.Equal("key: 1", ToString(new EnumValueType { DisplayName = "key", Value = 1 }));
            Assert.Equal("1234", ToString(1234));
            Assert.Equal("[123,1234]", ToString(new[] { 123, 1234 }));
            Assert.Equal(@"[""gp.tl:i=123"",""gp.tl:i=1234"",""gp.tl:s=abc""]", ToString(new[]
            {
                new NodeId(123u, 2), new NodeId(1234u, 2), new NodeId("abc", 2)
            }));
            Assert.Equal("somekey: gp.tl:s=abc", ToString(new Opc.Ua.KeyValuePair
            {
                Key = "somekey",
                Value = new NodeId("abc", 2)
            }));
            var readValueId = new ReadValueId { AttributeId = Attributes.Value, NodeId = new NodeId("test", 0) };
            var readValueIdStr = @"{""NodeId"":{""IdType"":1,""Id"":""test""},""AttributeId"":13}";
            Assert.Equal(readValueIdStr, ToString(new Variant(readValueId)));
            var ids = new ReadValueIdCollection { readValueId, readValueId };
            // Results in Variant(ExtensionObject[])
            Assert.Equal($"[{readValueIdStr},{readValueIdStr}]", ToString(new Variant(ids)));
            var ids2 = new[] { readValueId, readValueId };
            // Results in [Variant(ExtensionObject), Variant(ExtensionObject)], so it ends up using our system
            Assert.Equal($"[{readValueIdStr},{readValueIdStr}]", ToString(new Variant(ids2)));
            // Simple matrix
#pragma warning disable CA1814 // Prefer jagged arrays over multidimensional
            var m1 = new Matrix(new int[3, 3] { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } }, BuiltInType.Int32);
            Assert.Equal("[[1,2,3],[4,5,6],[7,8,9]]", ToString(new Variant(m1)));
            // Complex matrix
            var m2 = new Matrix(new Variant[2, 2] {
                { new Variant(readValueId), new Variant(readValueId) },
                { new Variant(readValueId), new Variant(readValueId) } }, BuiltInType.Variant);
            Assert.Equal($"[[{readValueIdStr},{readValueIdStr}],[{readValueIdStr},{readValueIdStr}]]", converter.ConvertToString(new Variant(m2)));
#pragma warning restore CA1814 // Prefer jagged arrays over multidimensional


            Assert.Equal("Good", ToString(new Variant(StatusCodes.Good, new TypeInfo(BuiltInType.StatusCode, ValueRanks.Scalar))));
            Assert.Equal("f5a7af86-0989-48dc-a535-7789483c5d04", ToString(Guid.Parse("f5a7af86-0989-48dc-a535-7789483c5d04")));
            var xml = new XmlDocument();
            xml.LoadXml("<?xml version='1.0' ?>" +
                "<test1 key1='val1' key2='val2'>" +
                "   <test2 key3='val3' key4='val4'>Content</test2>" +
                "</test1>");
            var xmlJson = ToString(xml.DocumentElement);
            Assert.Equal(@"{""test1"":{""@key1"":""val1"",""@key2"":""val2"",""test2"":"
                + @"{""@key3"":""val3"",""@key4"":""val4"",""#text"":""Content""}}}", xmlJson);
            var diagInfo = new DiagnosticInfo
            {
                LocalizedText = 5,
                InnerStatusCode = StatusCodes.BadAlreadyExists,
                AdditionalInfo = "Some additional info",
                InnerDiagnosticInfo = new DiagnosticInfo
                {
                    LocalizedText = 6,
                    InnerStatusCode = StatusCodes.BadNotConnected,
                    AdditionalInfo = "Inner additional info"
                }
            };
            var diagInfoStr = @"{""LocalizedText"":5,""InnerStatusCode"":""BadAlreadyExists""," +
                @"""AdditionalInfo"":""Some additional info""," +
                @"""InnerDiagnosticInfo"":{""LocalizedText"":6," +
                @"""InnerStatusCode"":""BadNotConnected""," +
                @"""AdditionalInfo"":""Inner additional info""}}";
            Assert.Equal(diagInfoStr, ToString(diagInfo));

            Assert.Equal("N: Newton", ToString(new Variant(
                new ExtensionObject(new EUInformation { DisplayName = "N", Description = "Newton" })
            )));
            Assert.Equal("Anonymous", ToString(UserTokenType.Anonymous));
            Assert.Equal("VGhlIHF1aWNrIGJyb3c=", ToString(new Variant(
                new ExtensionObject(new NodeId("abc", 2), new byte[] { 0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77 })
            )));
            var xmlJson2 = ToString(new ExtensionObject(new NodeId("abc", 2), xml.DocumentElement));
            Assert.Equal(@"{""test1"":{""@key1"":""val1"",""@key2"":""val2"",""test2"":"
                + @"{""@key3"":""val3"",""@key4"":""val4"",""#text"":""Content""}}}", xmlJson2);
        }
        [Fact]
        public void TestConvertToStringJson()
        {
            var log = tester.Provider.GetRequiredService<ILogger<TypeConverter>>();
            var converter = new TypeConverter(log, tester.Client, tester.Config);

            string ToJsonString(object obj)
            {
                return converter.ConvertToJson(new Variant(obj), null, null, JsonMode.Json)?.ToJsonString() ?? "null";
            }

            Assert.Equal("null", ToJsonString(null));
            Assert.Equal(@"""gp.tl:s=abc""", ToJsonString(new NodeId("abc", 2)));
            Assert.Equal(@"""gp.tl:s=abc""", ToJsonString(new ExpandedNodeId("abc", tester.Client.NamespaceTable.GetString(2))));
            Assert.Equal(@"""test""", ToJsonString(new LocalizedText("EN-US", "test")));
            Assert.Equal(@"{""Low"":0,""High"":100}", ToJsonString(new Opc.Ua.Range(100, 0)));
            var euInfoStr = @"{""UnitId"":0,""DisplayName"":""N"",""Description"":""Newton""}";
            Assert.Equal(euInfoStr, ToJsonString(new EUInformation { DisplayName = "N", Description = "Newton" }));
            Assert.Equal(euInfoStr, ToJsonString(new ExtensionObject(new EUInformation { DisplayName = "N", Description = "Newton" })));
            Assert.Equal(@"{""key"":1}", ToJsonString(new EnumValueType { DisplayName = "key", Value = 1 }));
            Assert.Equal("1234", ToJsonString(1234));
            Assert.Equal("[123,1234]", ToJsonString(new[] { 123, 1234 }));
            Assert.Equal(@"[""gp.tl:i=123"",""gp.tl:i=1234"",""gp.tl:s=abc""]", ToJsonString(new[]
            {
                new NodeId(123u, 2), new NodeId(1234u, 2), new NodeId("abc", 2)
            }));
            Assert.Equal(@"{""somekey"":""gp.tl:s=abc""}", ToJsonString(new Opc.Ua.KeyValuePair
            {
                Key = "somekey",
                Value = new NodeId("abc", 2)
            }));
            Assert.Equal(@"{""enumkey"":1}", ToJsonString(new Opc.Ua.EnumValueType
            {
                DisplayName = "enumkey",
                Value = 1
            }));
            var xml = new XmlDocument();
            xml.LoadXml("<?xml version='1.0' ?>" +
                "<test1 key1='val1' key2='val2'>" +
                "   <test2 key3='val3' key4='val4'>Content</test2>" +
                "</test1>");
            var xmlJson = ToJsonString(xml.DocumentElement);
            Assert.Equal(@"{""test1"":{""@key1"":""val1"",""@key2"":""val2"",""test2"":"
                + @"{""@key3"":""val3"",""@key4"":""val4"",""#text"":""Content""}}}", xmlJson);
#pragma warning disable CA1814 // Prefer jagged arrays over multidimensional
            var m1 = new Matrix(new int[3, 3] { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } }, BuiltInType.Int32);
#pragma warning restore CA1814 // Prefer jagged arrays over multidimensional
            Assert.Equal("[[1,2,3],[4,5,6],[7,8,9]]", ToJsonString(new Variant(m1)));

            Assert.Equal(@"""Anonymous""", ToJsonString(new Variant(UserTokenType.Anonymous)));
            Assert.Equal(@"""bcabfe0c-1fe6-42c4-8dad-2d72e50e2dbd""", ToJsonString(new Guid("bcabfe0c-1fe6-42c4-8dad-2d72e50e2dbd")));
            Assert.Equal(@"""Good""", ToJsonString(new StatusCode(StatusCodes.Good)));
            Assert.Equal(@"""VGhlIHF1aWNrIGJyb3c=""", ToJsonString(new Variant(
                new byte[] { 0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77 },
                new TypeInfo(BuiltInType.ByteString, ValueRanks.Scalar))
            ));

            Assert.Equal("123", ToJsonString((sbyte)123));
            Assert.Equal("123", ToJsonString((byte)123));
            Assert.Equal("123", ToJsonString((short)123));
            Assert.Equal("123", ToJsonString((uint)123));
            Assert.Equal("123", ToJsonString((ulong)123));
            Assert.Equal("true", ToJsonString(true));
            Assert.Equal(@"""2023-10-01 12:00:00.000""", ToJsonString(new DateTime(2023, 10, 1, 12, 0, 0, DateTimeKind.Utc)));
            Assert.Equal(@"""f5a7af86-0989-48dc-a535-7789483c5d04""", ToJsonString(Guid.Parse("f5a7af86-0989-48dc-a535-7789483c5d04")));

            // Raw extension object bodies. Not a lot we can do about these once we get here, so we just serialize them in their raw form.
            Assert.Equal(@"""VGhlIHF1aWNrIGJyb3c=""", ToJsonString(new Variant(
                new ExtensionObject(new NodeId("abc", 2), new byte[] { 0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77 })
            )));
            var xmlJson2 = ToJsonString(new ExtensionObject(new NodeId("abc", 2), xml.DocumentElement));
            Assert.Equal(@"{""test1"":{""@key1"":""val1"",""@key2"":""val2"",""test2"":"
                + @"{""@key3"":""val3"",""@key4"":""val4"",""#text"":""Content""}}}", xmlJson2);
            Assert.Equal(@"""Anonymous""", ToJsonString(UserTokenType.Anonymous));
        }
        [Fact]
        public void TestConvertToStringJsonIssues()
        {
            // The OPC-UA JsonEncoder can be a bit unreliable, this is a brute-force way to check that it behaves properly
            // for all types, or they are handled externally.
            var canBeNull = new HashSet<Type>
            {
                typeof(ExtensionObject), typeof(Variant)
            };

            var log = tester.Provider.GetRequiredService<ILogger<TypeConverter>>();
            var converter = new TypeConverter(log, tester.Client, tester.Config);
            var failedTypes = new List<Type>();
            void TestJsonEncoder(Type type, Variant variant)
            {
                var builder = new StringBuilder("{");
                builder.Append(@"""value"":");
                try
                {
                    var conv = converter.ConvertToJson(variant, null, null, JsonMode.Json)?.ToJsonString();
                    if (conv == null && !canBeNull.Contains(type))
                    {
                        throw new InvalidOperationException($"Type {type} produced null JSON");
                    }
                    builder.Append(conv ?? "null");
                }
                catch
                {
                    tester.Log.LogWarning("Type: {Type}, could not be serialized", type);
                    throw;
                }
                builder.Append('}');

                try
                {
                    JsonDocument.Parse(builder.ToString());
                }
                catch
                {
                    tester.Log.LogWarning("Type {Type} produced invalid JSON: {Json}", type, builder);
                    failedTypes.Add(type);
                }
            }

            foreach (var type in typeof(EnumValueType).Assembly.GetTypes())
            {
                if (type.IsAbstract || type.IsInterface || type == typeof(Opc.Ua.Export.LocalizedText)) continue;
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
            var options = new JsonSerializerOptions();
            var converter = tester.Client.TypeConverter;
            converter.AddConverters(options, ConverterType.Node);

            void TestConvert(NodeId id, string expected)
            {
                var result = JsonSerializer.Serialize(id, options);

                Assert.Equal(expected, result);

                var retId = JsonSerializer.Deserialize<NodeId>(result, options);

                Assert.Equal(id, retId);
            }

            TestConvert(new NodeId(123u), @"{""idType"":0,""identifier"":123}");
            TestConvert(new NodeId("test", 0), @"{""idType"":1,""identifier"":""test""}");
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
                var retId = JsonSerializer.Deserialize<NodeId>(input);

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
            var options = new JsonSerializerOptions();
            var converter = tester.Client.TypeConverter;
            converter.AddConverters(options, ConverterType.Node);

            void TestConvert(BaseUANode node, string expected)
            {
                var result = JsonSerializer.Serialize(node, options);

                Assert.Equal(expected, result);
            }

            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;
            var node = new UAObject(new NodeId("test", 0), "test", null, null, NodeId.Null, null);
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":null,"
                + @"""NodeId"":{""idType"":1,""identifier"":""test""}}");

            node.FullAttributes.TypeDefinition = new UAObjectType(new NodeId("test-type", 0));
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":null,"
                + @"""NodeId"":{""idType"":1,""identifier"":""test""},"
                + @"""TypeDefinitionId"":{""idType"":1,""identifier"":""test-type""}}");

            node = new UAObject(new NodeId("test", 0), "test", null, null, new NodeId("parent", 0), null);
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":""gp.base:s=parent"","
                + @"""NodeId"":{""idType"":1,""identifier"":""test""},"
                + @"""ParentNodeId"":{""idType"":1,""identifier"":""parent""}}");

            options = new JsonSerializerOptions();
            converter.AddConverters(options, ConverterType.Variable);

            var variable = new UAVariable(new NodeId("test", 0), "test", null, null, NodeId.Null, null);
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.Boolean);

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
            var options = new JsonSerializerOptions();
            var converter = tester.Client.TypeConverter;
            converter.AddConverters(options, ConverterType.Node);

            void TestConvert(BaseUANode node, string expected)
            {
                var result = JsonSerializer.Serialize(node, options);

                Assert.Equal(expected, result);
            }

            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Events.Enabled = true;
            tester.Config.Events.History = true;
            var node = new UAObject(new NodeId("test", 0), "test", null, null, NodeId.Null, null);
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":null,"
                + @"""InternalInfo"":{""EventNotifier"":0,""ShouldSubscribeEvents"":false,"
                + @"""ShouldReadHistoryEvents"":false,""NodeClass"":1}}");

            node.FullAttributes.EventNotifier |= EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
            TestConvert(node,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""parentExternalId"":null,"
                + @"""InternalInfo"":{""EventNotifier"":5,""ShouldSubscribeEvents"":true,"
                + @"""ShouldReadHistoryEvents"":true,""NodeClass"":1}}");

            var variable = new UAVariable(new NodeId("test", 0), "test", null, null, NodeId.Null, null);
            options = new JsonSerializerOptions();
            converter.AddConverters(options, ConverterType.Variable);
            variable.FullAttributes.AccessLevel |= AccessLevels.CurrentRead | AccessLevels.HistoryRead;
            variable.FullAttributes.Historizing = true;
            variable.FullAttributes.ValueRank = -1;
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            TestConvert(variable,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""assetExternalId"":null,"
                + @"""isString"":false,""isStep"":false,"
                + @"""InternalInfo"":{""NodeClass"":2,""AccessLevel"":5,"
                + @"""Historizing"":true,""ValueRank"":-1,""ShouldSubscribeData"":true,""ShouldReadHistoryData"":true}}");

            variable.FullAttributes.ValueRank = ValueRanks.OneDimension;
            variable.FullAttributes.ArrayDimensions = new[] { 5 };
            variable.AsEvents = true;
            TestConvert(variable,
                @"{""externalId"":""gp.base:s=test"",""name"":""test"","
                + @"""description"":null,""metadata"":null,""assetExternalId"":null,"
                + @"""isString"":false,""isStep"":false,"
                + @"""InternalInfo"":{""NodeClass"":2,""AccessLevel"":5,"
                + @"""Historizing"":true,""ValueRank"":1,""ShouldSubscribeData"":true,""ShouldReadHistoryData"":true,"
                + @"""ArrayDimensions"":[5],""Index"":-1,""AsEvents"":true}}"
                );
        }
        [Fact]
        public void TestNodeDeserialization()
        {
            // Objects
            var options = new JsonSerializerOptions();
            var converter = tester.Client.TypeConverter;
            converter.AddConverters(options, ConverterType.Node);

            var node = new UAObject(new NodeId("test", 2), "test", null, null, new NodeId("parent", 0), null);
            node.FullAttributes.EventNotifier = 5;

            tester.Config.Extraction.DataTypes.AppendInternalValues = true;
            tester.Config.Extraction.DataTypes.ExpandNodeIds = true;

            SavedNode Convert(BaseUANode node)
            {
                var json = JsonSerializer.Serialize(node, options);

                tester.Log.LogDebug("Produced JSON: {Json}", json);

                return JsonSerializer.Deserialize<SavedNode>(json, options);
            }

            var saved = Convert(node);
            Assert.Equal(node.Id, saved.NodeId);
            Assert.Equal(node.NodeClass, saved.InternalInfo.NodeClass);
            Assert.Equal(node.ParentId, saved.ParentNodeId);
            Assert.Equal(node.FullAttributes.ShouldSubscribeToEvents(tester.Config), saved.InternalInfo.ShouldSubscribeEvents);
            Assert.Equal(node.FullAttributes.EventNotifier, saved.InternalInfo.EventNotifier);

            // Variables
            options = new JsonSerializerOptions();
            converter.AddConverters(options, ConverterType.Variable);

            var variable = new UAVariable(new NodeId("test", 2), "test", null, null, new NodeId("parent", 0), null);
            variable.FullAttributes.AccessLevel = 5;
            variable.FullAttributes.DataType = new UADataType(DataTypeIds.Double);
            variable.FullAttributes.ValueRank = -1;

            saved = Convert(variable);
            Assert.Equal(variable.Id, saved.NodeId);
            Assert.Equal(variable.NodeClass, saved.InternalInfo.NodeClass);
            Assert.Equal(variable.ParentId, saved.ParentNodeId);
            Assert.Equal(variable.FullAttributes.ShouldSubscribe(tester.Config), saved.InternalInfo.ShouldSubscribeData);
            Assert.Equal(variable.Name, saved.Name);
            Assert.Equal(variable.FullAttributes.DataType.Id, saved.DataTypeId);
            Assert.Equal(variable.FullAttributes.AccessLevel, saved.InternalInfo.AccessLevel);
            Assert.Equal(variable.FullAttributes.Historizing, saved.InternalInfo.Historizing);
            Assert.Equal(variable.ValueRank, saved.InternalInfo.ValueRank);

            variable.FullAttributes.ValueRank = 2;
            variable.FullAttributes.ArrayDimensions = new[] { 3, 4 };

            saved = Convert(variable);
            Assert.Equal(variable.ArrayDimensions, saved.InternalInfo.ArrayDimensions);
            Assert.Equal(variable.ValueRank, saved.InternalInfo.ValueRank);
        }
    }
}

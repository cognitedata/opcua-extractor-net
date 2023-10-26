﻿using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Server;
using System;
using System.CommandLine;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public class FDMTestFixture : BaseExtractorTestFixture
    {
        public FDMTestFixture() : base(new[] { PredefinedSetup.Types }) { }
    }

    public class FDMTests : IClassFixture<FDMTestFixture>
    {
        private readonly FDMTestFixture tester;
        public FDMTests(ITestOutputHelper output, FDMTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.Init(output);
            tester.ResetConfig();
            tester.Config.Cognite.MetadataTargets = new MetadataTargetsConfig
            {
                DataModels = new FdmDestinationConfig
                {
                    Enabled = true,
                    ModelSpace = "modelspace",
                    InstanceSpace = "instancespace",
                    ModelVersion = "1"
                }
            };
            tester.Config.Extraction.RootNode = new ProtoNodeId
            {
                NamespaceUri = "http://opcfoundation.org/UA/",
                NodeId = "i=85"
            };
            tester.Config.Extraction.NodeTypes.AsNodes = true;
            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.Relationships.CreateReferencedNodes = true;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
        }

        private static T GetProperty<T>(JsonNode node, string property, string view) where T : class
        {
            return node["sources"]?.AsArray()?.FirstOrDefault(f => f["source"]["externalId"].ToString() == view)
                ?["properties"]?[property]?.GetValue<T>();
        }

        private static T? GetPropertyStruct<T>(JsonNode node, string property, string view) where T : struct
        {
            return node["sources"]?.AsArray()?.FirstOrDefault(f => f["source"]["externalId"].ToString() == view)
                ?["properties"]?[property]?.GetValue<T>();
        }



        [Fact]
        public async Task TestMapCustomTypes()
        {
            tester.Config.Cognite.MetadataTargets.DataModels.SkipSimpleTypes = false;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Equal(2, handler.Spaces.Count);
            // FolderType, BaseObjectType, BaseVariableType, BaseDataVariableType, PropertyType,
            // 4 custom object types, 1 custom variable types
            // BaseNode, BaseType, +4 type types

            foreach (var view in handler.Views)
            {
                tester.Log.LogDebug("{Key}", view.Key);
            }
            Assert.Equal(18, handler.Views.Count);
            // 8 base types, 2 custom object types, 1 custom variable type have container data
            Assert.Equal(13, handler.Containers.Count);
            /* foreach (var inst in handler.Instances)
            {
                tester.Log.LogDebug("{Id}: {Name}", inst.Key, inst.Value
                    ?["sources"]?.AsArray()?.FirstOrDefault(r => r["source"]["externalId"].GetValue<string>() == "BaseNode")
                        ?["properties"]?["DisplayName"]);
            } */
            Assert.Equal(68, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(81, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));


            // HasTypeDefinition references from objects, typesroot, devices 1-3, devices.data 1-3,
            // devices.trivial 1-3, and 4 variables
            // In the type hierarchy there are 3 on the variable type, 3 under complex type,
            // 8 under simpletype and nestedtype
            Assert.Equal(29, handler.Instances.Count(inst => inst.Value["type"]?["externalId"]?.ToString() ==
                ReferenceTypeIds.HasTypeDefinition.ToString()));

            // Every type mapped should be referenced through a "HasSubType, except for the root types
            // References, BaseObjectType, BaseVariableType, and BaseDataType.
            // 6 total object types (4 custom + folder type + base)
            // 4 total variable types (1 custom + property type + data variable type + base)
            // 11 total reference types
            // 7 (?) data types
            uint? GetNodeClass(JsonNode node)
            {
                return GetPropertyStruct<uint>(node, "NodeClass", "BaseNode");
            }

            foreach (var node in handler.Instances.Where(inst => GetNodeClass(inst.Value) == (uint)NodeClass.ReferenceType))
            {
                tester.Log.LogDebug("{V}", GetProperty<string>(node.Value, "DisplayName", "BaseNode"));
            }

            Assert.Equal(6, handler.Instances.Count(inst => GetNodeClass(inst.Value) == (uint)NodeClass.ObjectType));
            Assert.Equal(4, handler.Instances.Count(inst => GetNodeClass(inst.Value) == (uint)NodeClass.VariableType));
            Assert.Equal(11, handler.Instances.Count(inst => GetNodeClass(inst.Value) == (uint)NodeClass.ReferenceType));
            Assert.Equal(7, handler.Instances.Count(inst => GetNodeClass(inst.Value) == (uint)NodeClass.DataType));

            Assert.Equal(6 + 4 + 11 + 7 - 4, handler.Instances.Count(inst => inst.Value["type"]?["externalId"]?.ToString() ==
                ReferenceTypeIds.HasSubtype.ToString()));
        }
    }
}

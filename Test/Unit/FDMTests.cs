using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Server;
using System;
using System.Linq;
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
                    Space = "test",
                    ExcludeNonReferenced = true,
                }
            };
            tester.Config.Extraction.RootNode = new ProtoNodeId
            {
                NamespaceUri = "http://opcfoundation.org/UA/",
                NodeId = "i=84"
            };
            tester.Config.Extraction.NodeTypes.AsNodes = true;
            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.Relationships.CreateReferencedNodes = true;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
        }

        [Fact]
        public async Task TestMapCustomTypes()
        {
            tester.Config.Cognite.MetadataTargets.DataModels.ExcludeNonReferenced = true;
            tester.Config.Cognite.MetadataTargets.DataModels.TypesToMap = TypesToMap.Custom;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Single(handler.Spaces);
            // FolderType, BaseObjectType, BaseVariableType, BaseDataVariableType, PropertyType,
            // 4 custom object types, 1 custom variable types
            // BaseNode, BaseType, +4 type types, and ModellingRuleType

            foreach (var view in handler.Views)
            {
                tester.Log.LogDebug("{Key}", view.Key);
            }
            Assert.Equal(19, handler.Views.Count);
            // 8 base types, 2 custom object types, 1 custom variable type have container data
            Assert.Equal(13, handler.Containers.Count);
            /* foreach (var inst in handler.Instances)
            {
                tester.Log.LogDebug("{Id}: {Name}", inst.Key, inst.Value
                    ?["sources"]?.AsArray()?.FirstOrDefault(r => r["source"]["externalId"].GetValue<string>() == "BaseNode")
                        ?["properties"]?["DisplayName"]);
            } */
            Assert.Equal(80, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(105, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));
        }

        [Fact]
        public async Task TestMapReferencedTypes()
        {
            tester.Config.Cognite.MetadataTargets.DataModels.ExcludeNonReferenced = true;
            tester.Config.Cognite.MetadataTargets.DataModels.TypesToMap = TypesToMap.Referenced;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Single(handler.Spaces);
            // FolderType, BaseObjectType, BaseVariableType, BaseDataVariableType, PropertyType,
            // 4 custom object types, 1 custom variable type
            // BaseNode, BaseType, +4 type types, and ModellingRuleType
            /* foreach (var inst in handler.Instances)
            {
                tester.Log.LogDebug("{Id}: {Name}", inst.Key, inst.Value
                    ?["sources"]?.AsArray()?.FirstOrDefault(r => r["source"]["externalId"].GetValue<string>() == "BaseNode")
                        ?["properties"]?["DisplayName"]);
            } */
            foreach (var view in handler.Views)
            {
                tester.Log.LogDebug("{Key}", view.Key);
            }
            Assert.Equal(19, handler.Views.Count);
            // 8 base types, 2 custom object types, 1 custom variable type have container data
            Assert.Equal(13, handler.Containers.Count);
            Assert.Equal(80, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(105, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));
        }

        [Fact]
        public async Task TestMapReferencedTypesNoTrim()
        {
            tester.Config.Cognite.MetadataTargets.DataModels.ExcludeNonReferenced = false;
            tester.Config.Cognite.MetadataTargets.DataModels.TypesToMap = TypesToMap.Referenced;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Single(handler.Spaces);
            // FolderType, BaseObjectType, BaseVariableType, BaseDataVariableType, PropertyType,
            // 4 custom object types, 1 custom variable type
            // BaseNode, BaseType, +4 type types, ModellingRuleType, and DataTypeSystemType
            foreach (var view in handler.Views)
            {
                tester.Log.LogDebug("{Key}", view.Key);
            }
            Assert.Equal(21, handler.Views.Count);
            
            // 8 base types, 2 custom object types, 1 custom variable type have container data
            Assert.Equal(13, handler.Containers.Count);
            Assert.Equal(2570, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(6567, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));
        }

        [Fact]
        public async Task TestMapEverything()
        {
            tester.Config.Cognite.MetadataTargets.DataModels.ExcludeNonReferenced = false;
            tester.Config.Cognite.MetadataTargets.DataModels.TypesToMap = TypesToMap.All;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Single(handler.Spaces);
            Assert.Equal(214, handler.Views.Count);
            Assert.Equal(133, handler.Containers.Count);
            // Numbers are lower because more types are mapped, so more nodes are mapped as metadata.
            // This isn't always desired. You may want the entire type hierarchy without the types for all nodes.
            Assert.Equal(2287, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(4254, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));
        }
    }
}

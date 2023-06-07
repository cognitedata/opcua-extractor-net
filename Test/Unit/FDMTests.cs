using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            tester.Config.Cognite.FlexibleDataModels = new FdmDestinationConfig
            {
                Enabled = true,
                Space = "test",
                ExcludeNonReferenced = true,
            };
            tester.Config.Extraction.RootNode = new ProtoNodeId
            {
                NamespaceUri = "http://opcfoundation.org/UA/",
                NodeId = "i=86"
            };
            tester.Config.Extraction.NodeTypes.AsNodes = true;
            tester.Config.Extraction.Relationships.Enabled = true;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
        }

        [Fact]
        public async Task TestMapCustomTypes()
        {
            tester.Config.Cognite.FlexibleDataModels.ExcludeNonReferenced = true;
            tester.Config.Cognite.FlexibleDataModels.TypesToMap = TypesToMap.Custom;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Single(handler.Spaces);
            // FolderType, BaseObjectType, BaseVariableType, BaseDataVariableType, PropertyType,
            // 3 custom object types, 2 custom variable types
            // BaseNode, BaseType, +4 type types, and ModellingRuleType
            Assert.Equal(19, handler.Views.Count);
            // 8 base types, 2 custom object types, 1 custom variable type have container data
            Assert.Equal(13, handler.Containers.Count);
            foreach (var inst in handler.Instances)
            {
                tester.Log.LogInformation("{Key}", inst.Key);
            }
            Assert.Equal(63, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(71, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));
        }

        [Fact]
        public async Task TestMapReferencedTypes()
        {
            tester.Config.Cognite.FlexibleDataModels.ExcludeNonReferenced = true;
            tester.Config.Cognite.FlexibleDataModels.TypesToMap = TypesToMap.Referenced;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Single(handler.Spaces);
            // FolderType, BaseObjectType, BaseVariableType, BaseDataVariableType, PropertyType,
            // 3 custom object types, 1 custom variable type
            // BaseNode, BaseType, +4 type types, and ModellingRuleType
            Assert.Equal(18, handler.Views.Count);
            // 8 base types, 2 custom object types, 1 custom variable type have container data
            Assert.Equal(13, handler.Containers.Count);
            Assert.Equal(62, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(71, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));
        }

        [Fact]
        public async Task TestMapReferencedTypesNoTrim()
        {
            tester.Config.Cognite.FlexibleDataModels.ExcludeNonReferenced = false;
            tester.Config.Cognite.FlexibleDataModels.TypesToMap = TypesToMap.Referenced;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Single(handler.Spaces);
            // FolderType, BaseObjectType, BaseVariableType, BaseDataVariableType, PropertyType,
            // 3 custom object types, 1 custom variable type
            // BaseNode, BaseType, +4 type types, ModellingRuleType, and DataTypeSystemType
            Assert.Equal(19, handler.Views.Count);
            // 8 base types, 2 custom object types, 1 custom variable type have container data
            Assert.Equal(13, handler.Containers.Count);
            Assert.Equal(2155, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(6137, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));
        }

        [Fact]
        public async Task TestMapEverything()
        {
            tester.Config.Cognite.FlexibleDataModels.ExcludeNonReferenced = false;
            tester.Config.Cognite.FlexibleDataModels.TypesToMap = TypesToMap.All;

            var (handler, pusher) = tester.GetCDFPusher();
            using var extractor = tester.BuildExtractor(true, null, pusher);

            await extractor.RunExtractor(true);

            Assert.Single(handler.Spaces);
            Assert.Equal(214, handler.Views.Count);
            Assert.Equal(133, handler.Containers.Count);
            // Numbers are lower because more types are mapped, so more nodes are mapped as metadata.
            // This isn't always desired. You may want the entire type hierarchy without the types for all nodes.
            Assert.Equal(1874, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "node"));
            Assert.Equal(3824, handler.Instances.Count(inst => inst.Value["instanceType"].ToString() == "edge"));
        }
    }
}

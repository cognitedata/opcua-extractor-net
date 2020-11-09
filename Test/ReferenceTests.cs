using Cognite.OpcUa;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Test
{
    [Collection("Extractor tests")]
    public class ReferenceTests : MakeConsoleWork
    {
        // private readonly ILogger log = Log.Logger.ForContext(typeof(ReferenceTests));

        public ReferenceTests(ITestOutputHelper output) : base(output) { }

        private static async Task RunReferenceExtraction(ExtractorTester tester)
        {
            await tester.ClearPersistentData();

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;

            await tester.StartServer();
            tester.Config.Extraction.DataTypes.CustomNumericTypes = new[]
            {
                tester.IdToProtoDataType(tester.Server.Ids.Custom.MysteryType),
                tester.IdToProtoDataType(tester.Server.Ids.Custom.NumberType),
            };
            tester.Config.Extraction.DataTypes.IgnoreDataTypes = new[]
            {
                tester.IdToProto(tester.Server.Ids.Custom.IgnoreType)
            };

            tester.StartExtractor();
            await tester.TerminateRunTask(false);
        }

        [Fact]
        [Trait("Server", "array")]
        [Trait("Target", "References")]
        [Trait("Test", "hierarchicalreferences")]
        public async Task TestHierarchicalReferences()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array,
                QuitAfterMap = true,
                References = true
            });
            await tester.ClearPersistentData();

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.Extraction.Relationships.Hierarchical = true;
            tester.Config.Extraction.Relationships.InverseHierarchical = true;

            await tester.StartServer();
            tester.Server.PopulateArrayHistory();

            tester.Config.Extraction.DataTypes.CustomNumericTypes = new[]
            {
                tester.IdToProtoDataType(tester.Server.Ids.Custom.MysteryType),
                tester.IdToProtoDataType(tester.Server.Ids.Custom.NumberType),
            };
            tester.Config.Extraction.DataTypes.IgnoreDataTypes = new[]
            {
                tester.IdToProto(tester.Server.Ids.Custom.IgnoreType)
            };

            tester.StartExtractor();
            await tester.TerminateRunTask(false);

            var rels = tester.Handler.Relationships.Values;

            Assert.Equal(30, rels.Count);

            var assetRel = rels.First(rel => rel.externalId == "gp.Organizes;tl:i=1;tl:i=14");
            Assert.Equal("Asset", assetRel.sourceType);
            Assert.Equal("gp.tl:i=1", assetRel.sourceExternalId);
            Assert.Equal("gp.tl:i=14", assetRel.targetExternalId);
            var tsRel = rels.First(rel => rel.externalId == "gp.HasComponent;tl:i=1;tl:i=8");
            Assert.Equal("TimeSeries", tsRel.targetType);
            Assert.Equal("Asset", tsRel.sourceType);
            Assert.Equal("gp.tl:i=1", tsRel.sourceExternalId);
            Assert.Equal("gp.tl:i=8", tsRel.targetExternalId);
        }
        [Fact]
        [Trait("Server", "array")]
        [Trait("Target", "References")]
        [Trait("Test", "rawreferences")]
        public async Task TestRawReferences()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array,
                QuitAfterMap = true,
                References = true
            });

            tester.Config.Cognite.RawMetadata = new RawMetadataConfig
            {
                Database = "metadata",
                RelationshipsTable = "relationships"
            };
            await tester.ClearPersistentData();

            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;

            await tester.StartServer();
            tester.Server.PopulateArrayHistory();

            tester.Config.Extraction.DataTypes.CustomNumericTypes = new[]
            {
                tester.IdToProtoDataType(tester.Server.Ids.Custom.MysteryType),
                tester.IdToProtoDataType(tester.Server.Ids.Custom.NumberType),
            };
            tester.Config.Extraction.DataTypes.IgnoreDataTypes = new[]
            {
                tester.IdToProto(tester.Server.Ids.Custom.IgnoreType)
            };

            tester.StartExtractor();
            await tester.TerminateRunTask(false);

            var rels = tester.Handler.RelationshipsRaw.Values;
            TestRelationships(rels);
        }
        private static void TestRelationships(IEnumerable<RelationshipDummy> rels)
        {
            Assert.Equal(8, rels.Count());
            Assert.Equal(4, rels.Count(rel => rel.externalId.StartsWith("gp.HasSymmetricRelation", StringComparison.InvariantCulture)));
            Assert.Equal(2, rels.Count(rel => rel.externalId.StartsWith("gp.HasCustomRelation", StringComparison.InvariantCulture)));
            Assert.Equal(2, rels.Count(rel => rel.externalId.StartsWith("gp.IsCustomRelationOf", StringComparison.InvariantCulture)));

            var assetRel = rels.First(rel => rel.externalId == "gp.IsCustomRelationOf;tl:i=1;tl:i=2");
            Assert.Equal("Asset", assetRel.sourceType);
            Assert.Equal("gp.tl:i=1", assetRel.sourceExternalId);
            Assert.Equal("gp.tl:i=2", assetRel.targetExternalId);

            var tsRel = rels.First(rel => rel.externalId == "gp.HasSymmetricRelation;tl:i=10;tl:i=8");
            Assert.Equal("TimeSeries", tsRel.sourceType);
            Assert.Equal("gp.tl:i=10", tsRel.sourceExternalId);
            Assert.Equal("gp.tl:i=8", tsRel.targetExternalId);
        }

        [Fact]
        [Trait("Server", "array")]
        [Trait("Target", "References")]
        [Trait("Test", "basicreferences")]
        public async Task TestBasicReferences()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array,
                QuitAfterMap = true,
                References = true
            });
            await RunReferenceExtraction(tester);

            var rels = tester.Handler.Relationships.Values;
            TestRelationships(rels);
        }

        [Fact]
        [Trait("Server", "array")]
        [Trait("Target", "References")]
        [Trait("Test", "mqttreferences")]
        public async Task TestMqttReferences()
        {
            using var tester = new ExtractorTester(new ExtractorTestParameters
            {
                ServerName = ServerName.Array,
                QuitAfterMap = true,
                References = true,
                Pusher = "mqtt"
            });
            await RunReferenceExtraction(tester);

            var rels = tester.Handler.Relationships.Values;
            TestRelationships(rels);
        }
    }
}

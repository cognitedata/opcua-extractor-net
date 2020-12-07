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
        private readonly ILogger log = Log.Logger.ForContext(typeof(ReferenceTests));

        public ReferenceTests(ITestOutputHelper output) : base(output) { }


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

            var rels = tester.Handler.Relationships.Values;
            foreach (var rel in rels)
            {
                log.Information("Relationship: {id}, {source}, {target}, {type1}, {type2}",
                    rel.externalId, rel.sourceExternalId, rel.targetExternalId, rel.sourceType, rel.targetType);
            }
            Assert.Equal(8, rels.Count);
            Assert.Equal(4, rels.Count(rel => rel.externalId.StartsWith("gp.HasSymmetricRelation", StringComparison.InvariantCulture)));
            Assert.Equal(2, rels.Count(rel => rel.externalId.StartsWith("gp.HasCustomRelation", StringComparison.InvariantCulture)));
            Assert.Equal(2, rels.Count(rel => rel.externalId.StartsWith("gp.IsCustomRelationOf", StringComparison.InvariantCulture)));

            var assetRel = rels.First(rel => rel.externalId == "gp.IsCustomRelationOf;tl:i=1;tl:i=2");
            Assert.Equal("Asset", assetRel.sourceType);
            Assert.Equal("Asset", assetRel.targetType);
            Assert.Equal("gp.tl:i=1", assetRel.sourceExternalId);
            Assert.Equal("gp.tl:i=2", assetRel.targetExternalId);

            var tsRel = rels.First(rel => rel.externalId == "gp.HasSymmetricRelation;tl:i=10;tl:i=8");
            Assert.Equal("TimeSeries", tsRel.sourceType);
            Assert.Equal("TimeSeries", tsRel.targetType);
            Assert.Equal("gp.tl:i=10", tsRel.sourceExternalId);
            Assert.Equal("gp.tl:i=8", tsRel.targetExternalId);

            var arrayRel = rels.First(rel => rel.externalId == "gp.HasSymmetricRelation;tl:i=2;tl:i=3");
            Assert.Equal("Asset", arrayRel.sourceType);
            Assert.Equal("Asset", arrayRel.targetType);
        }
    }
}

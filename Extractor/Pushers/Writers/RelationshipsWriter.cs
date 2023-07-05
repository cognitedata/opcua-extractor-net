using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using CogniteSdk;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class RelationshipsWriter : IRelationshipsWriter
    {
        private readonly ILogger<RelationshipsWriter> log;
        private readonly FullConfig config;
        private readonly CogniteDestination destination;

        public RelationshipsWriter(
            ILogger<RelationshipsWriter> logger,
            CogniteDestination destination,
            FullConfig config
        )
        {
            this.log = logger;
            this.config = config;
            this.destination = destination;
        }

        public async Task<Result> PushReferences(IEnumerable<RelationshipCreate> relationships, CancellationToken token)
        {
            var result = new Result{ Created = 0, Updated = 0 };
            var counts = await Task.WhenAll(
                relationships.ChunkBy(1000).Select(chunk => PushReferencesChunk(chunk, token))
            );
            result.Created += counts.Sum();
            return result;
        }

        private async Task<int> PushReferencesChunk(IEnumerable<RelationshipCreate> relationships, CancellationToken token)
        {
            if (!relationships.Any())
                return 0;
            try
            {
                await destination.CogniteClient.Relationships.CreateAsync(relationships, token);
                return relationships.Count();
            }
            catch (ResponseException ex)
            {
                if (ex.Duplicated.Any())
                {
                    var existing = new HashSet<string>();
                    foreach (var dict in ex.Duplicated)
                    {
                        if (dict.TryGetValue("externalId", out var value))
                        {
                            if (value is MultiValue.String strValue)
                            {
                                existing.Add(strValue.Value);
                            }
                        }
                    }
                    if (!existing.Any())
                        throw;

                    relationships = relationships
                        .Where(rel => !existing.Contains(rel.ExternalId))
                        .ToList();
                    return await PushReferencesChunk(relationships, token);
                }
                else
                {
                    throw;
                }
            }
        }
    }
}

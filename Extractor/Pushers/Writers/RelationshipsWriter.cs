/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

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

        public RelationshipsWriter( ILogger<RelationshipsWriter> logger, CogniteDestination destination, FullConfig config)
        {
            this.log = logger;
            this.config = config;
            this.destination = destination;
        }

        /// <summary>
        /// Push all refernces to CDF relationship
        /// </summary>
        /// <param name="relationships">List of sanitized references</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A result reporting items created/updated</returns>
        public async Task<Result> PushReferences(IEnumerable<RelationshipCreate> relationships, CancellationToken token)
        {
            var result = new Result{ Created = 0, Updated = 0 };
            var counts = await Task.WhenAll(
                relationships.ChunkBy(1000).Select(chunk => PushReferencesChunk(chunk, token))
            );
            result.Created += counts.Sum();
            return result;
        }

        /// <summary>
        /// Push all references in chunks
        /// </summary>
        /// <param name="relationships">List of sanitized references</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
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

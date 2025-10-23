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

using Cognite.OpcUa.Utils;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Config
{
    public partial class UAServerExplorer : UAClient
    {
        private readonly ICollection<int> testAttributeChunkSizes = new[]
        {
            100000,
            10000,
            1000,
            100,
            10
        };
        /// <summary>
        /// Get AttributeChunk config value, by attempting to read for various chunk sizes.
        /// Uses a value decently proportional to the server size, only 10k if the server is large enough
        /// for that to make sense.
        /// Terminates as soon as a read succeeds.
        /// </summary>
        public async Task GetAttributeChunkSizes(CancellationToken token)
        {
            log.LogInformation("Reading variable chunk sizes to determine the AttributeChunk property");

            if (!Started)
            {
                await Run(token, 0);
                await LimitConfigValues(token);
            }

            await PopulateNodes(token);

            int oldArraySize = Config.Extraction.DataTypes.MaxArraySize;
            int expectedAttributeReads = nodeList.Sum(node => node.Attributes.GetAttributeSet(Config).Count());
            Config.History.Enabled = true;
            Config.Extraction.DataTypes.MaxArraySize = 10;

            var testChunks = testAttributeChunkSizes.Where(chunkSize =>
                chunkSize <= expectedAttributeReads || chunkSize <= 1000);

            if (expectedAttributeReads < 1000)
            {
                log.LogWarning("Reading less than 1000 attributes maximum. Most servers should support more, but" +
                            " this server only has enough nodes to read {Reads}", expectedAttributeReads);
                Summary.Attributes.LimitWarning = true;
                Summary.Attributes.KnownCount = expectedAttributeReads;
            }

            bool succeeded = false;

            foreach (int chunkSize in testChunks)
            {
                int count = 0;
                var toCheck = nodeList.TakeWhile(node =>
                {
                    count += node.Attributes.GetAttributeSet(Config).Count();
                    return count < chunkSize + 10;
                }).ToList();
                log.LogInformation("Total {Total}", nodeList.Count);
                log.LogInformation("Attempting to read attributes for {Cnt} nodes with ChunkSize {ChunkSize}", toCheck.Count, chunkSize);
                Config.Source.AttributesChunk = chunkSize;
                try
                {
                    await ToolUtil.RunWithTimeout(ReadNodeData(toCheck, token, "testing read tolerance"), 120);
                }
                catch (Exception e)
                {
                    log.LogInformation(e, "Failed to read node attributes");

                    if (e is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        throw new FatalException(
                            "Attribute read is unsupported, the extractor does not support servers which do not " +
                            "support the \"Read\" service");
                    }

                    continue;
                }

                log.LogInformation("Settled on AttributesChunk: {Size}", chunkSize);
                succeeded = true;
                baseConfig.Source.AttributesChunk = chunkSize;
                break;
            }

            Summary.Attributes.ChunkSize = baseConfig.Source.AttributesChunk;

            Config.Extraction.DataTypes.MaxArraySize = oldArraySize;

            if (!succeeded)
            {
                throw new FatalException("Failed to read node attributes for any chunk size");
            }
        }
    }
}

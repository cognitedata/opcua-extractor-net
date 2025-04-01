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

using Cognite.Extractor.Common;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
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
        private readonly ICollection<int> testHistoryChunkSizes = new[]
        {
            100,
            10,
            1
        };

        /// <summary>
        /// Attempts history read if possible, getting chunk sizes. It also determines granularity, 
        /// and sets backfill to true if it works and it estimates that there are a lot of points in some variables.
        /// </summary>
        public async Task GetHistoryReadConfig(CancellationToken token)
        {
            await PopulateNodes(token);
            await ReadNodeData(token);

            var historizingStates = nodeList.Where(node =>
                    (node is UAVariable variable) && !variable.IsProperty && variable.FullAttributes.ShouldReadHistory(Config))
                .Select(node => new VariableExtractionState(this, (UAVariable)node, true, true, true)).ToList();

            var stateMap = historizingStates.ToDictionary(state => state.SourceId);

            log.LogInformation("Read history to decide on decent history settings");

            if (!historizingStates.Any())
            {
                log.LogWarning("No historizing variables detected, unable analyze history");
                Summary.History.NoHistorizingNodes = true;
                return;
            }

            DateTime earliestTime = Config.History.StartTime?.Get() ?? CogniteTime.DateTimeEpoch;

            var details = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                EndTime = DateTime.UtcNow.AddDays(10),
                StartTime = earliestTime,
                NumValuesPerNode = (uint)Config.History.DataChunk
            };

            long largestEstimate = 0;

            long sumDistance = 0;
            int count = 0;

            HistoryReadNode? nodeWithData = null;

            bool failed = true;
            bool done = false;

            foreach (int chunkSize in testHistoryChunkSizes)
            {
                var chunk = historizingStates.Take(chunkSize);
                var historyParams = new HistoryReadParams(
                    chunk.Select(state => new HistoryReadNode(HistoryReadType.FrontfillData, state)).ToList(), details);
                try
                {
                    await ToolUtil.RunWithTimeout(DoHistoryRead(historyParams, token), 10);

                    foreach (var node in historyParams.Items)
                    {
                        if (node.LastResult == null) continue;
                        var data = ToolUtil.ReadResultToDataPoints(node.LastResult, stateMap[node.Id], this, log);
                        // If we want to do analysis of how best to read history, we need some number of datapoints
                        // If this number is too low, it typically means that there is no real history to read.
                        // Some servers write a single datapoint to history on startup, having a decently large number here
                        // means that we don't base our history analysis on those.
                        if (data.Length > 100 && nodeWithData == null)
                        {
                            nodeWithData = node;
                        }


                        if (data.Length < 2) continue;
                        count++;
                        long avgTicks = (data.Last().Timestamp.Ticks - data.First().Timestamp.Ticks) / (data.Length - 1);
                        sumDistance += avgTicks;

                        if (node.Completed) continue;
                        if (avgTicks == 0) continue;
                        long estimate = (DateTime.UtcNow.Ticks - data.First().Timestamp.Ticks) / avgTicks;
                        if (estimate > largestEstimate)
                        {
                            nodeWithData = node;
                            largestEstimate = estimate;
                        }
                    }


                    failed = false;
                    baseConfig.History.DataNodesChunk = chunkSize;
                    Config.History.DataNodesChunk = chunkSize;
                    done = true;
                }
                catch (Exception e)
                {
                    failed = true;
                    done = false;
                    log.LogWarning(e, "Failed to read history");
                    if (e is ServiceResultException exc && (
                            exc.StatusCode == StatusCodes.BadHistoryOperationUnsupported
                            || exc.StatusCode == StatusCodes.BadServiceUnsupported))
                    {
                        log.LogWarning("History read unsupported, despite Historizing being set to true. " +
                                    "The history config option must be set to false, or this will cause issues");
                        done = true;
                        break;
                    }
                }

                if (done) break;
            }


            if (failed)
            {
                log.LogWarning("Unable to read data history");
                return;
            }

            Summary.History.Enabled = true;
            baseConfig.History.Enabled = true;
            log.LogInformation("Settled on chunkSize: {Size}", baseConfig.History.DataNodesChunk);
            Summary.History.ChunkSize = baseConfig.History.DataNodesChunk;
            log.LogInformation("Largest estimated number of datapoints in a single nodes history is {LargestEstimate}, " +
                            "this is found by looking at the first datapoints, then assuming the average frequency holds until now", largestEstimate);

            if (nodeWithData == null)
            {
                log.LogWarning("No nodes found with more than 100 datapoints in history, further history analysis is not possible");
                return;
            }

            long totalAvgDistance = sumDistance / count;

            log.LogInformation("Average distance between timestamps across all nodes with history: {Distance}",
                TimeSpan.FromTicks(totalAvgDistance));
            var granularity = TimeSpan.FromTicks(totalAvgDistance * 10).Seconds + 1;

            log.LogInformation("Suggested granularity is: {Granularity} seconds", granularity);
            Config.History.Granularity = granularity.ToString();

            Summary.History.Granularity = TimeSpan.FromSeconds(granularity);

            bool backfillCapable = false;

            log.LogInformation("Read history backwards from {Time}", earliestTime);
            var backfillDetails = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                StartTime = DateTime.UtcNow,
                EndTime = earliestTime,
                NumValuesPerNode = (uint)Config.History.DataChunk
            };

            nodeWithData.ContinuationPoint = null;

            var backfillParams = new HistoryReadParams(new[] { nodeWithData }, backfillDetails);

            try
            {
                await ToolUtil.RunWithTimeout(DoHistoryRead(backfillParams, token), 10);

                var data = ToolUtil.ReadResultToDataPoints(nodeWithData.LastResult!, stateMap[nodeWithData.Id], this, log);

                log.LogInformation("Last ts: {TimeStamp}", data.First().Timestamp);

                var last = data.First();
                bool orderOk = true;
                foreach (var dp in data)
                {
                    if (dp.Timestamp > last.Timestamp)
                    {
                        orderOk = false;
                    }
                }

                if (!orderOk)
                {
                    log.LogWarning("Backfill does not result in properly ordered results");
                }
                else
                {
                    log.LogInformation("Backfill config results in properly ordered results");
                    backfillCapable = true;
                }
            }
            catch (Exception e)
            {
                log.LogInformation(e, "Failed to perform backfill");
            }

            Summary.History.BackfillRecommended = largestEstimate > 100000 && backfillCapable;

            if ((largestEstimate > 100000 || Config.History.Backfill) && backfillCapable)
            {
                log.LogInformation("Backfill is recommended or manually enabled, and the server is capable");
                baseConfig.History.Backfill = true;
            }
            else
            {
                log.LogInformation("Backfill is not recommended, or the server is incapable");
                baseConfig.History.Backfill = false;
            }

        }

    }
}

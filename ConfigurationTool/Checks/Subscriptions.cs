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

using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.Types;
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
        private readonly ICollection<int> testSubscriptionChunkSizes = new[]
        {
            10000,
            1000,
            100,
            10,
            1
        };

        /// <summary>
        /// Internal AllowTSMap, used to check whether a node should be mapped over or not,
        /// for use with subscriptions.
        /// </summary>
        /// <param name="node">Node to test</param>
        /// <returns>True if the config tool should keep the variable</returns>
        private bool AllowTSMap(UAVariable node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (node.ValueRank == ValueRanks.Scalar) return true;

            if (node.ArrayDimensions == null || node.ArrayDimensions.Length != 1) return false;

            int length = node.ArrayDimensions.First();

            return Config.Extraction.DataTypes.MaxArraySize < 0 || length > 0 && length <= Config.Extraction.DataTypes.MaxArraySize;

        }

        /// <summary>
        /// Attempts different chunk sizes for subscriptions. (number of created monitored items per attempt, 
        /// most servers should support at least one subscription).
        /// </summary>
        public async Task GetSubscriptionChunkSizes(CancellationToken token)
        {
            await PopulateNodes(token);
            await ReadNodeData(token);

            bool failed = true;
            var states = nodeList.Where(node =>
                    (node is UAVariable variable) && !variable.IsProperty
                    && AllowTSMap(variable))
                .Select(node => new VariableExtractionState(this, (node as UAVariable)!, false, false, true)).ToList();

            log.LogInformation("Get chunkSizes for subscribing to variables");

            if (states.Count == 0)
            {
                Summary.Subscriptions.NoSubscribable = true;
                log.LogWarning("There are no extractable states, subscriptions will not be tested");
                return;
            }

            var testChunks = testSubscriptionChunkSizes.Where(chunkSize =>
                chunkSize <= states.Count || chunkSize <= 1000);

            if (states.Count < 1000)
            {
                log.LogWarning("There are only {Count} extractable variables, so expected chunksizes may not be accurate. " +
                            "The default is 1000, which generally works.", states.Count);
                Summary.Subscriptions.LimitWarning = true;
                Summary.Subscriptions.KnownCount = states.Count;
            }

            var dps = new List<UADataPoint>();

            foreach (int chunkSize in testChunks)
            {
                Config.Source.SubscriptionChunk = chunkSize;
                try
                {
                    var task = new DataPointSubscriptionTask(
                        ToolUtil.GetSimpleListWriterHandler(dps, states.ToDictionary(state => state.SourceId), this, log, true),
                        states.Take(chunkSize),
                        Callbacks);

                    await ToolUtil.RunWithTimeout(task.Run(log, SessionManager, Config, SubscriptionManager!, token), 120);
                    baseConfig.Source.SubscriptionChunk = chunkSize;
                    failed = false;
                    break;
                }
                catch (Exception e)
                {
                    log.LogError(e, "Failed to subscribe to nodes, retrying with different chunkSize");
                    bool critical = false;
                    try
                    {
                        await ToolUtil.RunWithTimeout(() => Session!.RemoveSubscriptions(Session.Subscriptions.ToList()), 120);
                    }
                    catch (Exception ex)
                    {
                        critical = true;
                        Summary.Subscriptions.UnableToUnsubscribe = true;
                        log.LogWarning(ex, "Unable to remove subscriptions, further analysis is not possible");
                    }

                    if (e is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        critical = true;
                        Summary.Subscriptions.Unsupported = true;
                        log.LogWarning("CreateMonitoredItems or CreateSubscriptions services unsupported, the extractor " +
                                    "will not be able to properly read datapoints live from this server");
                    }

                    if (critical) break;
                }
            }

            if (failed)
            {
                log.LogWarning("Unable to subscribe to nodes");
                return;
            }

            Summary.Subscriptions.Enabled = true;
            log.LogInformation("Settled on chunkSize: {Size}", baseConfig.Source.SubscriptionChunk);
            log.LogInformation("Waiting for datapoints to arrive...");
            Summary.Subscriptions.ChunkSize = baseConfig.Source.SubscriptionChunk;

            for (int i = 0; i < 50; i++)
            {
                if (dps.Count > 0) break;
                await Task.Delay(100, token);
            }

            if (dps.Count > 0)
            {
                log.LogInformation("Datapoints arrived, subscriptions confirmed to be working properly");
            }
            else
            {
                log.LogWarning("No datapoints arrived, subscriptions may not be working properly, " +
                            "or there may be no updates on the server");
                Summary.Subscriptions.SilentWarning = true;
            }

            await Session!.RemoveSubscriptionsAsync(Session.Subscriptions.ToList());
        }
    }
}

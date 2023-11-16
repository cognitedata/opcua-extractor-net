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
using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.History
{
    public sealed class HistoryReader : IDisposable
    {
        private readonly UAClient uaClient;
        private readonly UAExtractor extractor;
        private readonly HistoryConfig config;
        private readonly TypeManager typeManager;
        private CancellationTokenSource source;
        // private ILogger log = Log.Logger.ForContext<HistoryReader>();
        private readonly TaskThrottler throttler;
        private readonly BlockingResourceCounter continuationPoints;

        private readonly OperationWaiter waiter;

        private readonly ILogger<HistoryReader> log;

        public HistoryReader(ILogger<HistoryReader> log,
            UAClient uaClient, UAExtractor extractor, TypeManager typeManager, HistoryConfig config, CancellationToken token)
        {
            this.log = log;
            this.config = config;
            this.uaClient = uaClient;
            this.extractor = extractor;
            this.typeManager = typeManager;
            var throttling = config.Throttling;
            throttler = new TaskThrottler(throttling.MaxParallelism, false, throttling.MaxPerMinute, TimeSpan.FromMinutes(1));
            source = CancellationTokenSource.CreateLinkedTokenSource(token);
            continuationPoints = new BlockingResourceCounter(
                throttling.MaxNodeParallelism > 0 ? throttling.MaxNodeParallelism : 1_000);
            waiter = new OperationWaiter();
        }

        private async Task Run(IEnumerable<UAHistoryExtractionState> states, HistoryReadType type)
        {
            using var scheduler = new HistoryScheduler(log, uaClient, extractor, typeManager, config, type,
                throttler, continuationPoints, states, source.Token);

            using var op = waiter.GetInstance();

            try
            {
                await scheduler.RunAsync();
            }
            catch (AggregateException aex)
            {
                throw new SmartAggregateException(aex);
            }
        }

        public void MaxNodeParallelismChanged()
        {
            continuationPoints.SetCapacity(config.Throttling.MaxNodeParallelism > 0 ? config.Throttling.MaxNodeParallelism : 1_000);
        }

        /// <summary>
        /// Frontfill data for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Nodes to be read</param>
        public async Task FrontfillData(IEnumerable<VariableExtractionState> states)
        {
            await Run(states, HistoryReadType.FrontfillData);
        }
        /// <summary>
        /// Backfill data for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Nodes to be read</param>
        public async Task BackfillData(IEnumerable<VariableExtractionState> states)
        {
            await Run(states, HistoryReadType.BackfillData);
        }
        /// <summary>
        /// Frontfill events for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Emitters to be read from</param>
        /// <param name="nodes">SourceNodes to read for</param>
        public async Task FrontfillEvents(IEnumerable<EventExtractionState> states)
        {
            await Run(states, HistoryReadType.FrontfillEvents);
        }
        /// <summary>
        /// Backfill events for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Emitters to be read from</param>
        /// <param name="nodes">SourceNodes to read for</param>
        public async Task BackfillEvents(IEnumerable<EventExtractionState> states)
        {
            await Run(states, HistoryReadType.BackfillEvents);
        }
        /// <summary>
        /// Request the history read terminate, then wait for all operations to finish before quitting.
        /// </summary>
        /// <param name="timeoutsec">Timeout in seconds</param>
        /// <returns>True if successfully aborted, false if waiting timed out</returns>
        public async Task<bool> Terminate(CancellationToken token, int timeoutsec = 30)
        {
            source.Cancel();
            bool timedOut = await waiter.Wait(timeoutsec * 1000, token);
            source.Dispose();
            source = CancellationTokenSource.CreateLinkedTokenSource(token);
            return timedOut;
        }

        public void Dispose()
        {
            source.Cancel();
            source.Dispose();
            waiter.Dispose();
            throttler.Dispose();
        }
    }
}

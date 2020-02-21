using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Prometheus.Client;
using Serilog;

namespace Cognite.OpcUa
{
    class HistoryReader
    {
        private static readonly Counter numFrontfillPoints = Metrics
            .CreateCounter("opcua_frontfill_data_points", "Number of datapoints retrieved through frontfill");
        private static readonly Counter numFrontfillData = Metrics
            .CreateCounter("opcua_frontfill_data_count", "Number of times frontfill has been run on datapoints");
        private static readonly Counter numBackfillPoints = Metrics
            .CreateCounter("opcua_backfill_data_points", "Number of datapoints retrieved through backfill");
        private static readonly Counter numBackfillData = Metrics
            .CreateCounter("opcua_backfill_data_count", "Number of times backfill has been run on datapoints");
        private static readonly Counter numFrontfillEvents = Metrics
            .CreateCounter("opcua_frontfill_events", "Number of events retrieved through frontfill");
        private static readonly Counter numFrontfillEvent = Metrics
            .CreateCounter("opcua_frontfill_events_count", "Number of times frontfill has been run on events");
        private static readonly Counter numBackfillEvents = Metrics
            .CreateCounter("opcua_backfill_events", "Number of events retrieved through backfill");
        private static readonly Counter numBackfillEvent = Metrics
            .CreateCounter("opcua_backfill_events_count", "Number of times backfill has been run on events");

        private readonly UAClient uaClient;
        private readonly Extractor extractor;
        private readonly HistoryConfig config;
        private readonly DateTime historyStartTime;
        private readonly IEnumerable<IPusher> pushers;
        private readonly TimeSpan historyGranularity;

        private bool aborting;
        private int running = 0;

        private static readonly ILogger log = Log.Logger.ForContext(typeof(HistoryReader));
        public HistoryReader(UAClient uaClient, Extractor extractor, IEnumerable<IPusher> pushers, HistoryConfig config)
        {
            this.config = config;
            this.uaClient = uaClient;
            this.extractor = extractor;
            this.pushers = pushers;
            historyStartTime = DateTimeOffset.FromUnixTimeMilliseconds(config.StartTime).DateTime;
            historyGranularity = config.Granularity <= 0 ? TimeSpan.Zero
                : TimeSpan.FromSeconds(config.Granularity);
        }
        /// <summary>
        /// Handle the result of a historyReadRaw. Takes information about the read, updates states and pushes datapoints.
        /// </summary>
        /// <param name="rawData">Data to be transformed into events</param>
        /// <param name="final">True if this is the last read</param>
        /// <param name="frontfill">True if this is frontfill, false for backfill</param>
        /// <param name="nodeid">NodeId being read</param>
        /// <param name="details">The HistoryReadDetails used</param>
        /// <returns></returns>
        private int HistoryDataHandler(IEncodeable rawData, bool final, bool frontfill, NodeId nodeid, HistoryReadDetails details)
        {
            if (rawData == null) return 0;
            if (!(rawData is HistoryData data))
            {
                log.Warning("Incorrect result type of history read data");
                return 0;
            }

            if (data.DataValues == null) return 0;
            var nodeState = extractor.GetNodeState(nodeid);

            string uniqueId = uaClient.GetUniqueId(nodeid);

            if (frontfill)
            {
                var last = data.DataValues.Any() ? data.DataValues.Max(dp => dp.SourceTimestamp) : DateTime.MinValue;
                nodeState.UpdateFromFrontfill(last, final);
                log.Debug("Frontfill of data for {id} at {ts}", uniqueId, last);
            }
            else
            {
                var first = data.DataValues.Any() ? data.DataValues.Min(dp => dp.SourceTimestamp) : DateTime.MaxValue;
                nodeState.UpdateFromBackfill(first, final);
                log.Debug("Backfill of data for {id} at {ts}", uniqueId, first);
            }

            int cnt = 0;
            foreach (var datapoint in data.DataValues)
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Extractor.BadDataPoints.Inc();
                    log.Debug("Bad history datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId,
                        datapoint.SourceTimestamp);
                    continue;
                }

                var buffDps = extractor.ToDataPoint(datapoint, nodeState, uniqueId);
                foreach (var buffDp in buffDps)
                {
                    log.Verbose("History DataPoint {dp}", buffDp.ToDebugDescription());
                    cnt++;
                }
                foreach (var buffDp in buffDps)
                {
                    extractor.DataPointQueue.Enqueue(buffDp);
                }
            }

            if (!final || !frontfill) return cnt;

            var buffered = nodeState.FlushBuffer();
            foreach (var dplist in buffered)
            {
                foreach (var dp in dplist)
                {
                    extractor.DataPointQueue.Enqueue(dp);
                }
            }

            return cnt;
        }

        /// <summary>
        /// Handler for HistoryRead of events. Simply pushes all events to the queue.
        /// </summary>
        /// <param name="rawEvts">Collection of events to be handled as IEncodable</param>
        /// <param name="final">True if this is the final call for this node, and the lock may be removed</param>
        /// <param name="frontfill">True if frontfill, false for backfill</param>
        /// <param name="nodeid">Id of the emitter in question.</param>
        /// <param name="details">History read details used to generate this HistoryRead result</param>
        private int HistoryEventHandler(IEncodeable rawEvts, bool final, bool frontfill, NodeId nodeid, HistoryReadDetails details)
        {
            if (rawEvts == null) return 0;
            if (!(rawEvts is HistoryEvent evts))
            {
                log.Warning("Incorrect return type of history read events");
                return 0;
            }
            if (!(details is ReadEventDetails eventDetails))
            {
                log.Warning("Incorrect details type of history read events");
                return 0;
            }
            var filter = eventDetails.Filter;
            if (filter == null)
            {
                log.Warning("No event filter, ignoring");
                return 0;
            }
            if (evts.Events == null) return 0;
            var emitterState = extractor.GetEmitterState(nodeid);
            int cnt = 0;

            var range = new TimeRange(DateTime.MaxValue, DateTime.MinValue);

            foreach (var evt in evts.Events)
            {
                var buffEvt = extractor.ConstructEvent(filter, evt.EventFields, nodeid);
                if (buffEvt == null) continue;

                if (buffEvt.Time < range.Start)
                {
                    range.Start = buffEvt.Time;
                }

                if (buffEvt.Time > range.End)
                {
                    range.End = buffEvt.Time;
                }

                extractor.EventQueue.Enqueue(buffEvt);
                cnt++;
            }

            if (frontfill)
            {
                emitterState.UpdateFromFrontfill(range.End, final);
                log.Debug("Frontfill of events for {id} at: {end}", nodeid, range.End);
            }
            else
            {
                emitterState.UpdateFromBackfill(range.Start, final);
                log.Debug("Backfill of events for {id} at: {end}", nodeid, range.Start);
            }

            if (!final || !frontfill) return cnt;
            var buffered = emitterState.FlushBuffer();
            foreach (var evt in buffered)
            { 
                extractor.EventQueue.Enqueue(evt);
            }
            return cnt;
        }
        /// <summary>
        /// Main history read loop. Reads for the given list of nodes until all are done, using the given HistoryReadDetails.
        /// </summary>
        /// <param name="details">HistoryReadDetails to be used</param>
        /// <param name="nodes">Nodes to be read</param>
        /// <param name="frontfill">True if frontfill, false for backfill, used for synching states</param>
        /// <param name="data">True if data is being read, false for events</param>
        /// <param name="handler">Callback to handle read results</param>
        private void BaseHistoryReadOp(HistoryReadDetails details,
            IEnumerable<NodeId> nodes,
            bool frontfill,
            bool data,
            Func<IEncodeable, bool, bool, NodeId, HistoryReadDetails, int> handler,
            CancellationToken token)
        {
            var readParams = new HistoryReadParams(nodes, details);
            while (readParams.Nodes.Any() && !token.IsCancellationRequested && !aborting)
            {
                int total = 0;
                int count = readParams.Nodes.Count();
                var results = uaClient.DoHistoryRead(readParams);
                if (aborting) break;
                foreach (var res in results)
                {
                    int cnt = handler(res.Item2, readParams.Completed[res.Item1], frontfill, res.Item1, details);

                    total += cnt;
                    log.Debug("{mode} {cnt} {type} for node {nodeId}", 
                        frontfill ? "Frontfill" : "Backfill", cnt, data ? "datapoints" : "events", res.Item1);
                }
                log.Information("{mode}ed {cnt} {type} for {nodeCount} states",
                    frontfill ? "Frontfill" : "Backfill", total, data ? "datapoints" : "events", count);

                if (data && frontfill)
                {
                    numFrontfillData.Inc();
                    numFrontfillPoints.Inc(total);
                }
                else if (data)
                {
                    numBackfillData.Inc();
                    numBackfillPoints.Inc(total);
                }
                else if (frontfill)
                {
                    numFrontfillEvent.Inc();
                    numFrontfillEvents.Inc(total);
                }
                else
                {
                    numBackfillEvent.Inc();
                    numBackfillEvents.Inc(total);
                }

                int termCount = readParams.Nodes.Count(id => readParams.Completed[id]);
                if (termCount > 0)
                {
                    log.Debug("Terminate {mode} of {type} for {count} states", frontfill ? "Frontfill" : "Backfill",
                        data ? "datapoints" : "events", termCount);
                }

                readParams.Nodes = readParams.Nodes.Where(id => !readParams.Completed[id]).ToList();
            }
        }
        /// <summary>
        /// Frontfill data for the given list of states
        /// </summary>
        /// <param name="nodes">Nodes to be read</param>
        private void FrontfillDataChunk(IEnumerable<NodeExtractionState> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = nodes.Select(node => node.SourceExtractedRange.End).Min();
            finalTimeStamp = finalTimeStamp < historyStartTime ? historyStartTime : finalTimeStamp;
            finalTimeStamp = finalTimeStamp > DateTime.UtcNow ? DateTime.UtcNow : finalTimeStamp;
            var details = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                EndTime = DateTime.UtcNow.AddDays(10),
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.DataChunk
            };
            log.Information("Frontfill data from {start} for {cnt} nodes", finalTimeStamp, nodes.Count());
            BaseHistoryReadOp(details, nodes.Select(node => node.Id), true, true, HistoryDataHandler, token);
        }
        /// <summary>
        /// Backfill data for the given list of states
        /// </summary>
        /// <param name="nodes">Nodes to be read</param>
        private void BackfillDataChunk(IEnumerable<NodeExtractionState> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = nodes.Select(node => node.SourceExtractedRange.Start).Max();
            if (finalTimeStamp <= historyStartTime) return;
            var details = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                // Reverse start/end time should result in backwards read according to the OPC-UA specification
                EndTime = historyStartTime,
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.DataChunk
            };
            log.Information("Backfill data from {start} for {cnt} nodes", finalTimeStamp, nodes.Count());

            BaseHistoryReadOp(details, nodes.Select(node => node.Id), false, true, HistoryDataHandler, token);
        }
        /// <summary>
        /// Frontfill events for the given list of states
        /// </summary>
        /// <param name="states">Emitters to be read from</param>
        /// <param name="nodes">SourceNodes to read for</param>
        private void FrontfillEventsChunk(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = states.Select(node => node.SourceExtractedRange.End).Min();
            finalTimeStamp = finalTimeStamp < historyStartTime ? historyStartTime : finalTimeStamp;
            finalTimeStamp = finalTimeStamp > DateTime.UtcNow ? DateTime.UtcNow : finalTimeStamp;
            var details = new ReadEventDetails
            {
                EndTime = DateTime.UtcNow.AddDays(10),
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.EventChunk,
                Filter = uaClient.BuildEventFilter(nodes)
            };
            log.Information("Frontfill events from {start} for {cnt} emitters", finalTimeStamp, states.Count());

            BaseHistoryReadOp(details, states.Select(node => node.Id), true, false, HistoryEventHandler, token); 

        }
        /// <summary>
        /// Backfill events for the given list of states
        /// </summary>
        /// <param name="states">Emitters to be read from</param>
        /// <param name="nodes">SourceNodes to read for</param>
        private void BackfillEventsChunk(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = states.Select(node => node.SourceExtractedRange.Start).Max();
            if (finalTimeStamp <= historyStartTime) return;
            var details = new ReadEventDetails
            {
                // Reverse start/end time should result in backwards read according to the OPC-UA specification
                EndTime = historyStartTime,
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.EventChunk,
                Filter = uaClient.BuildEventFilter(nodes)
            };
            log.Information("Backfill events from {start} for {cnt} emitters", finalTimeStamp, states.Count());

            BaseHistoryReadOp(details, states.Select(node => node.Id), false, false, HistoryEventHandler, token);
        }
        /// <summary>
        /// Frontfill data for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Nodes to be read</param>
        public async Task FrontfillData(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            try
            {
                Interlocked.Increment(ref running);
                var frontFillChunks = ExtractorUtils.GroupByTimeGranularity(
                    states.Select(state => (state, state.SourceExtractedRange.End)),
                    historyGranularity, config.DataNodesChunk);
                await Task.WhenAll(frontFillChunks.Select(chunk => Task.Run(() => FrontfillDataChunk(chunk, token))));
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }

        }
        /// <summary>
        /// Backfill data for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Nodes to be read</param>
        public async Task BackfillData(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            try
            {
                Interlocked.Increment(ref running);
                foreach (var state in states)
                {
                    if (state.SourceExtractedRange.Start < historyStartTime)
                    {
                        state.UpdateFromBackfill(DateTime.MinValue, true);
                    }
                }
                var backFillChunks = ExtractorUtils.GroupByTimeGranularity(
                    states.Where(state => state.SourceExtractedRange.Start > historyStartTime)
                        .Select(state => (state, state.SourceExtractedRange.Start)),
                    historyGranularity, config.DataNodesChunk);
                await Task.WhenAll(backFillChunks.Select(chunk => Task.Run(() => BackfillDataChunk(chunk, token))));
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }

        }
        /// <summary>
        /// Frontfill events for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Emitters to be read from</param>
        /// <param name="nodes">SourceNodes to read for</param>
        public async Task FrontfillEvents(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, CancellationToken token)
        {
            try
            {
                Interlocked.Increment(ref running);
                var frontFillChunks = ExtractorUtils.GroupByTimeGranularity(
                    states.Select(state => (state, state.SourceExtractedRange.End)),
                    historyGranularity, config.EventNodesChunk);
                await Task.WhenAll(frontFillChunks.Select(chunk =>
                    Task.Run(() => FrontfillEventsChunk(chunk, nodes, token))));
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }

        }
        /// <summary>
        /// Backfill events for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Emitters to be read from</param>
        /// <param name="nodes">SourceNodes to read for</param>
        public async Task BackfillEvents(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, CancellationToken token)
        {
            try
            {
                Interlocked.Increment(ref running);
                foreach (var state in states)
                {
                    if (state.SourceExtractedRange.Start < historyStartTime)
                    {
                        state.UpdateFromBackfill(DateTime.MinValue, true);
                    }
                }
                var backFillChunks = ExtractorUtils.GroupByTimeGranularity(
                    states.Where(state => state.SourceExtractedRange.Start > historyStartTime)
                        .Select(state => (state, state.SourceExtractedRange.Start)),
                    historyGranularity, config.EventNodesChunk);

                await Task.WhenAll(backFillChunks.Select(chunk =>
                    Task.Run(() => BackfillEventsChunk(chunk, nodes, token))));
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }
        }

        public async Task<bool> Terminate(CancellationToken token, int timeoutsec = 30)
        {
            log.Information("Attempting to abort history read");
            aborting = true;
            int timeout = timeoutsec * 10;
            int cycles = 0;
            while (running > 0 && cycles++ < timeout) await Task.Delay(100, token);
            aborting = false;
            if (running > 0)
            {
                Log.Warning("Failed to abort HistoryReader");
                return false;
            }
            log.Information("Aborted history read");

            return true;
        }
    }
}

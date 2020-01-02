using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Serilog;

namespace Cognite.OpcUa
{
    class HistoryReader
    {
        private readonly UAClient uaClient;
        private readonly Extractor extractor;
        private readonly HistoryConfig config;
        private readonly DateTime historyStartTime;
        private readonly IEnumerable<IPusher> pushers;
        private readonly TimeSpan historyGranularity;
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

        private int HistoryDataHandler(IEncodeable rawData, bool final, bool frontfill, NodeId nodeid, HistoryReadDetails details)
        {
            if (rawData == null) return 0;
            if (!(rawData is HistoryData data))
            {
                Log.Warning("Incorrect result type of history read data");
                return 0;
            }

            if (data.DataValues == null || !data.DataValues.Any()) return 0;
            var nodeState = extractor.GetNodeState(nodeid);

            string uniqueId = uaClient.GetUniqueId(nodeid);

            if (frontfill)
            {
                var last = data.DataValues.Max(dp => dp.SourceTimestamp);
                nodeState.UpdateFromFrontfill(last, final);
            }
            else
            {
                var first = data.DataValues.Min(dp => dp.SourceTimestamp);
                nodeState.UpdateFromBackfill(first, final);
            }

            int cnt = 0;
            foreach (var datapoint in data.DataValues)
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Extractor.BadDataPoints.Inc();
                    Log.Debug("Bad history datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId,
                        datapoint.SourceTimestamp);
                    continue;
                }

                var buffDps = extractor.ToDataPoint(datapoint, nodeState, uniqueId);
                foreach (var buffDp in buffDps)
                {
                    Log.Verbose("History DataPoint {dp}", buffDp.ToDebugDescription());
                    cnt++;
                }

                foreach (var pusher in pushers)
                {
                    foreach (var buffDp in buffDps)
                    {
                        pusher.BufferedDPQueue.Enqueue(buffDp);
                    }
                }
            }

            if (!final || frontfill) return cnt;

            var buffered = nodeState.FlushBuffer();
            foreach (var pusher in pushers)
            {
                foreach (var dplist in buffered)
                {
                    foreach (var dp in dplist)
                    {
                        pusher.BufferedDPQueue.Enqueue(dp);
                    }
                }
            }

            return cnt;
        }

        /// <summary>
        /// Callback for HistoryRead operations. Simply pushes all events to the queue.
        /// </summary>
        /// <param name="rawData">Collection of events to be handled as IEncodable</param>
        /// <param name="final">True if this is the final call for this node, and the lock may be removed</param>
        /// <param name="nodeid">Id of the emitter in question.</param>
        /// <param name="details">History read details used to generate this HistoryRead result</param>
        private int HistoryEventHandler(IEncodeable rawEvts, bool final, bool frontfill, NodeId nodeid, HistoryReadDetails details)
        {
            if (rawEvts == null) return 0;
            if (!(rawEvts is HistoryEvent evts))
            {
                Log.Warning("Incorrect return type of history read events");
                return 0;
            }
            if (!(details is ReadEventDetails eventDetails))
            {
                Log.Warning("Incorrect details type of history read events");
                return 0;
            }
            var filter = eventDetails.Filter;
            if (filter == null)
            {
                Log.Warning("No event filter, ignoring");
                return 0;
            }
            if (evts.Events == null) return 0;
            var emitterState = extractor.EventEmitterStates[nodeid];
            int cnt = 0;

            var range = new TimeRange(DateTime.MaxValue, DateTime.MinValue);

            foreach (var evt in evts.Events)
            {
                var buffEvt = extractor.ConstructEvent(filter, evt.EventFields);
                if (buffEvt == null) continue;

                if (buffEvt.Time < range.Start)
                {
                    range.Start = buffEvt.Time;
                }

                if (buffEvt.Time > range.End)
                {
                    range.End = buffEvt.Time;
                }

                foreach (var pusher in pushers)
                {
                    pusher.BufferedEventQueue.Enqueue(buffEvt);
                }
                cnt++;
            }

            if (frontfill)
            {
                emitterState.UpdateFromFrontfill(range.End, final);
            }
            else
            {
                emitterState.UpdateFromBackfill(range.Start, final);
            }

            if (!final || frontfill) return cnt;
            var buffered = emitterState.FlushBuffer();
            foreach (var pusher in pushers)
            {
                foreach (var evt in buffered)
                {
                    pusher.BufferedEventQueue.Enqueue(evt);
                }
            }
            return cnt;
        }

        private void BaseHistoryReadOp(HistoryReadDetails details,
            IEnumerable<NodeId> nodes,
            bool frontfill,
            string type,
            Func<IEncodeable, bool, bool, NodeId, HistoryReadDetails, int> handler,
            CancellationToken token)
        {
            var readParams = new UAClient.HistoryReadParams(nodes, details);
            int total = 0;
            while (readParams.Nodes.Any() && !token.IsCancellationRequested)
            {
                int count = readParams.Nodes.Count();
                var results = uaClient.DoHistoryRead(readParams);
                foreach (var res in results)
                {
                    int cnt = handler(res.Item2, readParams.Completed[res.Item1], frontfill, res.Item1, details);
                    total += cnt;
                    Log.Debug("{mode} {cnt} {type} for node {nodeId}", 
                        frontfill ? "Frontfill" : "Backfill", cnt, type, res.Item1);
                }
                Log.Information("{mode} {cnt} {type} for {nodeCount} states",
                    frontfill ? "Frontfill" : "Backfill", total, type, count);
                readParams.Nodes = readParams.Nodes.Where(id => !readParams.Completed[id]);
            }

        }

        private void FrontfillDataChunk(IEnumerable<NodeExtractionState> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = nodes.Select(node => node.ExtractedRange.End).Min();
            finalTimeStamp = finalTimeStamp < historyStartTime ? historyStartTime : finalTimeStamp;
            var details = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                EndTime = DateTime.UtcNow.AddDays(10),
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint) config.DataChunk
            };
            Log.Information("Frontfill data from {start} for {cnt} nodes", finalTimeStamp, nodes.Count());
            try
            {
                BaseHistoryReadOp(details, nodes.Select(node => node.Id), true, "datapoints", HistoryDataHandler,
                    token);
            }
            catch (ServiceResultException ex)
            {
                Utils.HandleServiceResult(ex, Utils.SourceOp.HistoryRead);
            }
        }

        private void BackfillDataChunk(IEnumerable<NodeExtractionState> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = nodes.Select(node => node.ExtractedRange.Start).Max();
            if (finalTimeStamp <= historyStartTime) return;
            var details = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                // Reverse start/end time should result in backwards read according to the OPC-UA specification
                EndTime = historyStartTime,
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.DataChunk
            };
            Log.Information("Backfill data from {start} for {cnt} nodes", finalTimeStamp, nodes.Count());

            try
            {
                BaseHistoryReadOp(details, nodes.Select(node => node.Id), false, "datapoints", HistoryDataHandler,
                    token);
            }
            catch (ServiceResultException ex)
            {
                Utils.HandleServiceResult(ex, Utils.SourceOp.HistoryRead);
            }
        }
        private void FrontfillEventsChunk(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = states.Select(node => node.ExtractedRange.End).Min();
            finalTimeStamp = finalTimeStamp == DateTime.MinValue
                ? finalTimeStamp
                : finalTimeStamp.Subtract(TimeSpan.FromMinutes(10));
            finalTimeStamp = finalTimeStamp < historyStartTime ? historyStartTime : finalTimeStamp;

            var details = new ReadEventDetails
            {
                EndTime = DateTime.UtcNow.AddDays(10),
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.DataChunk,
                Filter = uaClient.BuildEventFilter(nodes)
            };
            Log.Information("Frontfill events from {start} for {cnt} nodes", finalTimeStamp, nodes.Count());

            try
            {
                BaseHistoryReadOp(details, states.Select(node => node.Id), true, "events", HistoryEventHandler, token);
            }
            catch (ServiceResultException ex)
            {
                Utils.HandleServiceResult(ex, Utils.SourceOp.HistoryReadEvents);
            }
        }

        private void BackfillEventsChunk(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = states.Select(node => node.ExtractedRange.Start).Max();
            if (finalTimeStamp <= historyStartTime) return;
            var details = new ReadEventDetails
            {
                // Reverse start/end time should result in backwards read according to the OPC-UA specification
                EndTime = historyStartTime,
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.DataChunk,
                Filter = uaClient.BuildEventFilter(nodes)
            };
            Log.Information("Backfill events from {start} for {cnt} nodes", finalTimeStamp, nodes.Count());

            try
            {
                BaseHistoryReadOp(details, states.Select(node => node.Id), false, "events", HistoryEventHandler, token);
            }
            catch (ServiceResultException ex)
            {
                Utils.HandleServiceResult(ex, Utils.SourceOp.HistoryReadEvents);
            }
        }

        public async Task FrontfillData(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            var frontFillChunks = Utils.GroupByTimeGranularity(
                states.Select(state => (state, state.ExtractedRange.End)),
                historyGranularity, config.DataNodesChunk);
            await Task.WhenAll(frontFillChunks.Select(chunk => Task.Run(() => FrontfillDataChunk(chunk, token))));
        }

        public async Task BackfillData(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            var backFillChunks = Utils.GroupByTimeGranularity(
                states.Select(state => (state, state.ExtractedRange.Start)),
                historyGranularity, config.DataNodesChunk);
            await Task.WhenAll(backFillChunks.Select(chunk => Task.Run(() => BackfillDataChunk(chunk, token))));
        }
        public async Task FrontfillEvents(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, CancellationToken token)
        {
            var frontFillChunks = Utils.GroupByTimeGranularity(
                states.Select(state => (state, state.ExtractedRange.End)),
                historyGranularity, config.DataNodesChunk);
            await Task.WhenAll(frontFillChunks.Select(chunk => Task.Run(() => FrontfillEventsChunk(chunk, nodes, token))));
        }

        public async Task BackfillEvents(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, CancellationToken token)
        {
            var backFillChunks = Utils.GroupByTimeGranularity(
                states.Select(state => (state, state.ExtractedRange.Start)),
                historyGranularity, config.DataNodesChunk);

            await Task.WhenAll(backFillChunks.Select(chunk => Task.Run(() => BackfillEventsChunk(chunk, nodes, token))));
        }
    }
}

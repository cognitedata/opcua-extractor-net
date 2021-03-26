/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Prometheus;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Tool to read history from OPC-UA. Manages backfill and frontfill of datapoints and events.
    /// </summary>
    public class HistoryReader
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
        private readonly UAExtractor extractor;
        private readonly HistoryConfig config;
        private readonly DateTime historyStartTime;
        private readonly TimeSpan historyGranularity;

        private bool aborting;
        private int running;

        private readonly ILogger log = Log.Logger.ForContext(typeof(HistoryReader));
        /// <summary>
        /// Constructor, initialize from config
        /// </summary>
        /// <param name="uaClient">UAClient to use for history read</param>
        /// <param name="extractor">Parent extractor to enqueue points and events in</param>
        /// <param name="config">Configuration to use</param>
        public HistoryReader(UAClient uaClient, UAExtractor extractor, HistoryConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (uaClient == null) throw new ArgumentNullException(nameof(uaClient));
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            this.config = config;
            this.uaClient = uaClient;
            this.extractor = extractor;
            historyStartTime = CogniteTime.FromUnixTimeMilliseconds(config.StartTime);
            historyGranularity = config.Granularity <= 0
                ? TimeSpan.Zero
                : TimeSpan.FromSeconds(config.Granularity);
        }
        /// <summary>
        /// Handle the result of a historyReadRaw. Takes information about the read, updates states and pushes datapoints.
        /// </summary>
        /// <param name="rawData">Data to be transformed into events</param>
        /// <param name="node">Active HistoryReadNode</param>
        /// <param name="frontfill">True if this is frontfill, false for backfill</param>
        /// <returns>Number of points read</returns>
        private int HistoryDataHandler(IEncodeable rawData, HistoryReadNode node, bool frontfill, HistoryReadDetails _)
        {
            var data = rawData as HistoryData;

            if (node.State == null)
            {
                node.State = extractor.State.GetNodeState(node.Id);
            }

            if (node.State == null)
            {
                log.Warning("History data for unknown node received: {id}", node.Id);
                return 0;
            }

            List<DataValue> dataPoints = new List<DataValue>(data?.DataValues?.Count ?? 0);
            if (data?.DataValues != null)
            {
                foreach (var dp in data.DataValues)
                {
                    if (StatusCode.IsNotGood(dp.StatusCode))
                    {
                        UAExtractor.BadDataPoints.Inc();
                        log.Debug("Bad history datapoint: {BadDatapointExternalId} {SourceTimestamp}", node.State.Id, dp.SourceTimestamp);
                        continue;
                    }
                    dataPoints.Add(dp);
                }
            }

            var last = DateTime.MinValue;
            var first = DateTime.MaxValue;

            if (dataPoints.Any())
            {
                (first, last) = dataPoints.MinMax(dp => dp.SourceTimestamp);
            }

            if (config.IgnoreContinuationPoints)
            {
                node.Completed = !dataPoints.Any()
                    || frontfill && first == last && last == node.State.SourceExtractedRange.Last
                    || !frontfill && first == last && last == node.State.SourceExtractedRange.First;
            }

            if (frontfill)
            {
                node.State.UpdateFromFrontfill(last, node.Completed);
                log.Debug("Frontfill of data for {id} at {ts}", node.State.Id, last);
            }
            else
            {
                node.State.UpdateFromBackfill(first, node.Completed);
                log.Debug("Backfill of data for {id} at {ts}", node.State.Id, first);
            }

            int cnt = 0;

            var nodeState = node.State as VariableExtractionState;

            foreach (var datapoint in dataPoints)
            {
                var buffDps = extractor.Streamer.ToDataPoint(datapoint, nodeState);
                foreach (var buffDp in buffDps)
                {
                    log.Verbose("History DataPoint {dp}", buffDp.ToDebugDescription());
                    cnt++;
                }
                extractor.Streamer.Enqueue(buffDps);
            }

            if (!node.Completed || !frontfill) return cnt;

            var buffered = nodeState.FlushBuffer();
            nodeState.UpdateFromStream(buffered);
            extractor.Streamer.Enqueue(buffered);

            return cnt;
        }

        private DateTime? GetTimeAttribute(VariantCollection evt, EventFilter filter)
        {
            int index = filter.SelectClauses.FindIndex(atr =>
                atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                && atr.BrowsePath.Count == 1
                && atr.BrowsePath[0] == BrowseNames.Time);

            if (index < 0 || evt.Count <= index) return null;

            var raw = evt[index].Value;

            if (!(raw is DateTime dt)) return null;
            return dt;
        }

        /// <summary>
        /// Handler for HistoryRead of events. Simply pushes all events to the queue.
        /// </summary>
        /// <param name="rawEvts">Collection of events to be handled as IEncodable</param>
        /// <param name="node">Active HistoryReadNode</param>
        /// <param name="frontfill">True if frontfill, false for backfill</param>
        /// <param name="details">History read details used to generate this HistoryRead result</param>
        /// <returns>Number of events read</returns>
        private int HistoryEventHandler(IEncodeable rawEvts, HistoryReadNode node, bool frontfill, HistoryReadDetails details)
        {
            var evts = rawEvts as HistoryEvent;
            if (!(details is ReadEventDetails eventDetails))
            {
                log.Warning("Incorrect details type of history read events");
                return 0;
            }
            var filter = eventDetails.Filter;
            if (filter == null || filter.SelectClauses == null)
            {
                log.Warning("No event filter when reading from history, ignoring");
                return 0;
            }
            if (node.State == null)
            {
                node.State = extractor.State.GetEmitterState(node.Id);
            }

            if (node.State == null)
            {
                log.Warning("History events for unknown emitter received: {id}", node.Id);
                return 0;
            }

            var last = DateTime.MinValue;
            var first = DateTime.MaxValue;

            bool any = false;
            var createdEvents = new List<UAEvent>(evts?.Events?.Count ?? 0);
            if (evts?.Events != null)
            {
                foreach (var evt in evts.Events)
                {
                    var buffEvt = extractor.Streamer.ConstructEvent(filter, evt.EventFields, node.Id);
                    if (buffEvt == null)
                    {
                        var dt = GetTimeAttribute(evt.EventFields, filter);
                        if (dt != null)
                        {
                            // If the server somehow returns a full list of events that the extractor cannot parse,
                            // AND lacks a time attribute completely, then this may cause the extraction to end prematurely.
                            // That is probably unlikely, however, and at that point we can safely say that the server is
                            // not compliant enough for the extractor.
                            any = true;
                            if (dt > last) last = dt.Value;
                            if (dt < first) first = dt.Value;
                        }
                        UAExtractor.BadEvents.Inc();
                        continue;
                    }
                    else
                    {
                        if (buffEvt.Time > last) last = buffEvt.Time;
                        if (buffEvt.Time < first) first = buffEvt.Time;
                    }
                    any = true;
                    createdEvents.Add(buffEvt);
                }
            }

            if (config.IgnoreContinuationPoints)
            {
                // If all the returned events are at the end point, then we are receiving duplicates.
                node.Completed = !any
                    || frontfill && first == last && last == node.State.SourceExtractedRange.Last
                    || !frontfill && first == last && last == node.State.SourceExtractedRange.First;
            }

            if (frontfill)
            {
                node.State.UpdateFromFrontfill(last, node.Completed);
                log.Debug("Frontfill of events for {id} at: {end}", node.Id, last);
            }
            else
            {
                node.State.UpdateFromBackfill(first, node.Completed);
                log.Debug("Backfill of events for {id} at: {end}", node.Id, first);
            }

            extractor.Streamer.Enqueue(createdEvents);

            if (!node.Completed || !frontfill) return createdEvents.Count;

            var emitterState = node.State as EventExtractionState;

            var buffered = emitterState.FlushBuffer();
            if (buffered.Any())
            {
                var (smin, smax) = buffered.MinMax(dp => dp.Time);
                emitterState.UpdateFromStream(smin, smax);
            }
            log.Information("Read {cnt} events from buffer", buffered.Count());
            extractor.Streamer.Enqueue(buffered);

            return createdEvents.Count;
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
            Func<IEncodeable, HistoryReadNode, bool, HistoryReadDetails, int> handler,
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
                    int cnt = handler(res.RawData, res.Node, frontfill, details);

                    total += cnt;
                    log.Debug("{mode} {cnt} {type} for node {nodeId}",
                        frontfill ? "Frontfill" : "Backfill", cnt, data ? "datapoints" : "events", res.Node.Id);

                    if (config.IgnoreContinuationPoints)
                    {
                        res.Node.ContinuationPoint = null;
                    }
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

                int termCount = readParams.Nodes.Count(node => node.Completed);
                if (termCount > 0)
                {
                    log.Debug("Terminate {mode} of {type} for {count} states", frontfill ? "Frontfill" : "Backfill",
                        data ? "datapoints" : "events", termCount);
                }

                readParams.Nodes = readParams.Nodes.Where(node => !node.Completed).ToList();

                if (config.IgnoreContinuationPoints)
                {
                    DateTime newMin;
                    if (frontfill)
                    {
                        newMin = readParams.Nodes
                            .Where(node => node.State != null)
                            .Select(node => node.State.SourceExtractedRange.Last)
                            .Min();
                    }
                    else
                    {
                        newMin = readParams.Nodes
                            .Where(node => node.State != null)
                            .Select(node => node.State.SourceExtractedRange.First)
                            .Max();
                        if (newMin <= historyStartTime)
                        {
                            foreach (var node in readParams.Nodes)
                            {
                                node.Completed = true;
                                handler(null, node, frontfill, details);
                            }
                        }
                    }
                    if (data)
                    {
                        // Since datapoints all have distinct time, we can add a tick here to avoid reading the same again
                        // The same does not hold for events. Either way we also have a fallback check in the handlers 
                        // for cases when the server operates on a lower resolution than ticks.
                        (details as ReadRawModifiedDetails).StartTime = newMin.AddTicks(frontfill ? 1 : -1);
                    }
                    else
                    {
                        (details as ReadEventDetails).StartTime = newMin;
                    }
                }
            }
        }
        /// <summary>
        /// Frontfill data for the given list of states
        /// </summary>
        /// <param name="nodes">Nodes to be read</param>
        private void FrontfillDataChunk(IEnumerable<VariableExtractionState> nodes, CancellationToken token)
        {
            // Earliest latest timestamp in chunk.
            var finalTimeStamp = nodes.Min(node => node.SourceExtractedRange.Last);
            finalTimeStamp = finalTimeStamp < historyStartTime ? historyStartTime : finalTimeStamp;
            var details = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                EndTime = DateTime.MinValue,
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.DataChunk
            };
            log.Information("Frontfill data from {start} for {cnt} nodes", finalTimeStamp, nodes.Count());

            BaseHistoryReadOp(details, nodes.Select(node => node.SourceId), true, true, HistoryDataHandler, token);
        }
        /// <summary>
        /// Backfill data for the given list of states
        /// </summary>
        /// <param name="nodes">Nodes to be read</param>
        private void BackfillDataChunk(IEnumerable<VariableExtractionState> nodes, CancellationToken token)
        {
            var finalTimeStamp = nodes.Max(node => node.SourceExtractedRange.First);
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

            BaseHistoryReadOp(details, nodes.Select(node => node.SourceId), false, true, HistoryDataHandler, token);
        }
        /// <summary>
        /// Frontfill events for the given list of states
        /// </summary>
        /// <param name="states">Emitters to be read from</param>
        /// <param name="nodes">SourceNodes to read for</param>
        private void FrontfillEventsChunk(IEnumerable<EventExtractionState> states, CancellationToken token)
        {
            var finalTimeStamp = states.Min(node => node.SourceExtractedRange.Last);
            finalTimeStamp = finalTimeStamp < historyStartTime ? historyStartTime : finalTimeStamp;
            var details = new ReadEventDetails
            {
                EndTime = DateTime.MinValue,
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.EventChunk,
                Filter = uaClient.BuildEventFilter()
            };
            log.Information("Frontfill events from {start} for {cnt} emitters", finalTimeStamp, states.Count());

            BaseHistoryReadOp(details, states.Select(node => node.SourceId), true, false, HistoryEventHandler, token);

        }
        /// <summary>
        /// Backfill events for the given list of states
        /// </summary>
        /// <param name="states">Emitters to be read from</param>
        private void BackfillEventsChunk(IEnumerable<EventExtractionState> states, CancellationToken token)
        {
            var finalTimeStamp = states.Max(node => node.SourceExtractedRange.First);
            if (finalTimeStamp <= historyStartTime) return;
            var details = new ReadEventDetails
            {
                // Reverse start/end time should result in backwards read according to the OPC-UA specification
                EndTime = historyStartTime,
                StartTime = finalTimeStamp,
                NumValuesPerNode = (uint)config.EventChunk,
                Filter = uaClient.BuildEventFilter()
            };
            log.Information("Backfill events from {start} for {cnt} emitters", finalTimeStamp, states.Count());

            BaseHistoryReadOp(details, states.Select(node => node.SourceId), false, false, HistoryEventHandler, token);
        }
        /// <summary>
        /// Frontfill data for the given list of states. Chunks by time granularity and given chunksizes.
        /// </summary>
        /// <param name="states">Nodes to be read</param>
        public async Task FrontfillData(IEnumerable<VariableExtractionState> states, CancellationToken token)
        {
            if (states == null || !states.Any()) return;
            try
            {
                Interlocked.Increment(ref running);
                var frontfillChunks = states.GroupByTimeGranularity(historyGranularity,
                    state => state.SourceExtractedRange.Last, config.DataNodesChunk);
                await Task.WhenAll(frontfillChunks.Select(chunk => Task.Run(() => FrontfillDataChunk(chunk, token))));
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
        public async Task BackfillData(IEnumerable<VariableExtractionState> states, CancellationToken token)
        {
            if (states == null || !states.Any()) return;
            try
            {
                Interlocked.Increment(ref running);
                foreach (var state in states)
                {
                    if (state.SourceExtractedRange.First < historyStartTime)
                    {
                        state.UpdateFromBackfill(CogniteTime.DateTimeEpoch, true);
                    }
                }
                var backfillChunks = states.Where(state => state.SourceExtractedRange.First > historyStartTime)
                    .GroupByTimeGranularity(historyGranularity, state => state.SourceExtractedRange.First, config.DataNodesChunk);
                await Task.WhenAll(backfillChunks.Select(chunk => Task.Run(() => BackfillDataChunk(chunk, token))));
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
        public async Task FrontfillEvents(IEnumerable<EventExtractionState> states, CancellationToken token)
        {
            if (states == null || !states.Any()) return;
            try
            {
                Interlocked.Increment(ref running);
                var frontFillChunks = states.GroupByTimeGranularity(historyGranularity, state => state.SourceExtractedRange.Last, config.EventNodesChunk);
                await Task.WhenAll(frontFillChunks.Select(chunk =>
                    Task.Run(() => FrontfillEventsChunk(chunk, token))));
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
        public async Task BackfillEvents(IEnumerable<EventExtractionState> states, CancellationToken token)
        {
            if (states == null || !states.Any()) return;
            try
            {
                Interlocked.Increment(ref running);
                foreach (var state in states)
                {
                    if (state.SourceExtractedRange.First < historyStartTime)
                    {
                        state.UpdateFromBackfill(CogniteTime.DateTimeEpoch, true);
                    }
                }
                var backfillChunks = states.Where(state => state.SourceExtractedRange.First > historyStartTime)
                    .GroupByTimeGranularity(historyGranularity, state => state.SourceExtractedRange.First, config.EventNodesChunk);

                await Task.WhenAll(backfillChunks.Select(chunk =>
                    Task.Run(() => BackfillEventsChunk(chunk, token))));
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }
        }
        /// <summary>
        /// Request the history read terminate, then wait for all operations to finish before quitting.
        /// </summary>
        /// <param name="timeoutsec">Timeout in seconds</param>
        /// <returns>True if successfully aborted, false if waiting timed out</returns>
        public async Task<bool> Terminate(CancellationToken token, int timeoutsec = 30)
        {
            if (running == 0) return true;
            log.Debug("Attempting to abort history read");
            aborting = true;
            int timeout = timeoutsec * 10;
            int cycles = 0;
            while (running > 0 && cycles++ < timeout) await Task.Delay(100, token);
            aborting = false;
            if (running > 0)
            {
                log.Warning("Failed to abort HistoryReader");
                return false;
            }
            log.Debug("Aborted history read");

            return true;
        }
    }
}

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
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Prometheus;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public enum HistoryReadType
    {
        FrontfillData,
        BackfillData,
        FrontfillEvents,
        BackfillEvents
    }
    /// <summary>
    /// Parameter class containing the state of a single history read operation.
    /// </summary>
    public class HistoryReadParams
    {
        public HistoryReadDetails Details { get; }
        public IList<HistoryReadNode> Nodes { get; set; }
        public Exception? Exception { get; set; }

        public HistoryReadParams(IEnumerable<HistoryReadNode> nodes, HistoryReadDetails details)
        {
            Nodes = nodes.ToList();
            Details = details;
        }
    }
    public class HistoryReadNode
    {
        public HistoryReadNode(HistoryReadType type, UAHistoryExtractionState state)
        {
            Type = type;
            State = state;
            Id = state.SourceId;
            if (Id == null || Id.IsNullNodeId) throw new InvalidOperationException("NodeId may not be null");
        }
        /// <summary>
        /// Results in silently uninitilized State, unsafe.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        public HistoryReadNode(HistoryReadType type, NodeId id)
        {
            Type = type;
            Id = id;
            if (Id == null || Id.IsNullNodeId) throw new InvalidOperationException("NodeId may not be null");
        }
        public HistoryReadType Type { get; }
        [NotNull, AllowNull]
        public UAHistoryExtractionState? State { get; set; }
        public DateTime Time =>
            Type == HistoryReadType.BackfillData || Type == HistoryReadType.BackfillEvents
            ? State.SourceExtractedRange.First : State.SourceExtractedRange.Last;
        public NodeId Id { get; }
        public byte[]? ContinuationPoint { get; set; }
        public bool Completed { get; set; }
        public int LastRead { get; set; }
        public int TotalRead { get; set; }
    }

    public class HistoryScheduler : IDisposable
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
        private int numActiveNodes;
        private bool disposedValue;
        private readonly TaskThrottler throttler;

        private readonly HistoryReadType type;
        private readonly DateTime historyStartTime;
        private readonly TimeSpan historyGranularity;
        private readonly ContinuationPointThrottlingConfig throttling;
        private readonly ILogger log = Log.Logger.ForContext<HistoryScheduler>();

        private readonly BlockingCollection<HistoryReadParams> finishedReads = new BlockingCollection<HistoryReadParams>();

        private bool Frontfill => type == HistoryReadType.FrontfillData || type == HistoryReadType.FrontfillEvents;
        private bool Data => type == HistoryReadType.FrontfillData || type == HistoryReadType.BackfillData;
        public HistoryScheduler(UAClient uaClient, UAExtractor extractor, HistoryConfig config, HistoryReadType type)
        {
            throttling = config.Throttling;
            this.extractor = extractor;
            this.uaClient = uaClient;
            this.config = config;
            this.type = type;
            throttler = new TaskThrottler(throttling.MaxParallelism, false, throttling.MaxPerMinute, TimeSpan.FromMinutes(1));

            historyStartTime = CogniteTime.FromUnixTimeMilliseconds(config.StartTime);
            historyGranularity = config.Granularity <= 0
                ? TimeSpan.Zero
                : TimeSpan.FromSeconds(config.Granularity);
        }

        public async Task Run(IEnumerable<UAHistoryExtractionState> states, CancellationToken token)
        {
            await Task.Run(() => SchedulingLoop(states, token), CancellationToken.None);
        }

        private IEnumerable<HistoryReadNode> GetHistoryChunk(List<HistoryReadNode> nodes, int idx)
        {
            if (idx >= nodes.Count) yield break;
            int chunkSize = Data
                ? config.DataNodesChunk
                : config.EventNodesChunk;
            DateTime initial = nodes[idx].Time;
            for (int i = idx; i < nodes.Count; i++)
            {
                if (chunkSize > 0 && i - idx + 1 > chunkSize) yield break;
                if (config.Granularity >= 0 && nodes[i].Time > initial + historyGranularity) yield break;
                if (throttling.MaxNodeParallelism > 0 && i - idx + numActiveNodes + 1 > throttling.MaxNodeParallelism) yield break;
                yield return nodes[i];
            }
        }

        private string GetResourceName()
        {
            switch (type)
            {
                case HistoryReadType.BackfillData:
                case HistoryReadType.FrontfillData:
                    return "datapoints";
                case HistoryReadType.FrontfillEvents:
                case HistoryReadType.BackfillEvents:
                    return "events";
            }
            throw new InvalidOperationException();
        }

        private void LogHistoryTermination(List<HistoryReadNode> toTerminate, int totalRead, int totalNodes)
        {
            if (!toTerminate.Any()) return;
            string name = GetResourceName();
            log.Information("Finish reading {type} for {cnt} nodes: {totalRead}/{totalNodes}",
                type, toTerminate.Count, totalRead + toTerminate.Count, totalNodes);
            var builder = new StringBuilder();
            builder.AppendFormat("Finish reading {0}. Retrieved:", type);
            foreach (var node in toTerminate)
            {
                builder.AppendFormat("\n    {0} {1} total for {2}. End is now at {3}",
                    node.TotalRead,
                    name,
                    node.State.Id,
                    Frontfill ? node.State.SourceExtractedRange.Last : node.State.SourceExtractedRange.First);
            }
            log.Debug(builder.ToString());
        }

        private void LogHistoryChunk(IList<HistoryReadNode> nodes, int total)
        {
            if (!nodes.Any()) return;
            string name = GetResourceName();
            log.Information("HistoryRead {type} for {nodes} nodes. Retrieved {cnt} {name}.", type, nodes.Count, total, name);
            var builder = new StringBuilder();
            builder.AppendFormat("HistoryRead {0}. Retrieved:", type);
            foreach (var node in nodes)
            {
                builder.AppendFormat("\n    {0} {1} for {2}. End is now at {3}",
                    node.LastRead,
                    name,
                    node.State.Id,
                    Frontfill ? node.State.SourceExtractedRange.Last : node.State.SourceExtractedRange.First);
            }
            log.Debug(builder.ToString());
        }

        private HistoryReadDetails GetReadDetails(IEnumerable<HistoryReadNode> nodes)
        {
            switch (type)
            {
                case HistoryReadType.FrontfillData:
                    return new ReadRawModifiedDetails
                    {
                        IsReadModified = false,
                        EndTime = DateTime.MinValue,
                        StartTime = nodes.First().Time,
                        NumValuesPerNode = (uint)config.DataChunk
                    };
                case HistoryReadType.BackfillData:
                    return new ReadRawModifiedDetails
                    {
                        IsReadModified = false,
                        // Reverse start/end time should result in backwards read according to the OPC-UA specification
                        EndTime = historyStartTime,
                        StartTime = nodes.Last().Time,
                        NumValuesPerNode = (uint)config.DataChunk
                    };
                case HistoryReadType.FrontfillEvents:
                    return new ReadEventDetails
                    {
                        EndTime = DateTime.MinValue,
                        StartTime = nodes.First().Time,
                        NumValuesPerNode = (uint)config.EventChunk,
                        Filter = uaClient.BuildEventFilter()
                    };
                case HistoryReadType.BackfillEvents:
                    return new ReadEventDetails
                    {
                        EndTime = historyStartTime,
                        StartTime = nodes.Last().Time,
                        NumValuesPerNode = (uint)config.EventChunk,
                        Filter = uaClient.BuildEventFilter()
                    };
            }
            throw new InvalidOperationException();
        }

        private List<HistoryReadParams> GetNextChunks(List<HistoryReadNode> nodes, int index, out int newIndex)
        {
            int lIndex = index;
            List<HistoryReadParams> chunks = new List<HistoryReadParams>();
            List<HistoryReadNode> chunk;
            do
            {
                chunk = GetHistoryChunk(nodes, lIndex).ToList();
                numActiveNodes += chunk.Count;
                lIndex += chunk.Count;
                if (chunk.Any())
                {
                    var details = GetReadDetails(chunk);
                    chunks.Add(new HistoryReadParams(chunk, details));
                }
            } while (chunk.Any());
            newIndex = lIndex;
            return chunks;
        }

        private void LogReadFailure(HistoryReadParams finishedRead)
        {
            string msg = $"HistoryRead {type} failed for nodes" +
                $" {string.Join(',', finishedRead.Nodes.Select(node => node.State.Id))}: {finishedRead.Exception?.Message ?? ""}";
            log.Error(msg);
            ExtractorUtils.LogException(log, finishedRead.Exception, "Critical failure in HistoryRead", "Failure in HistoryRead");
        }

        private void IncrementMetrics(int total)
        {
            switch (type)
            {
                case HistoryReadType.FrontfillData:
                    numFrontfillData.Inc();
                    numFrontfillPoints.Inc(total);
                    break;
                case HistoryReadType.BackfillData:
                    numBackfillData.Inc();
                    numBackfillPoints.Inc(total);
                    break;
                case HistoryReadType.FrontfillEvents:
                    numFrontfillEvent.Inc();
                    numFrontfillEvents.Inc(total);
                    break;
                case HistoryReadType.BackfillEvents:
                    numBackfillEvent.Inc();
                    numBackfillEvents.Inc(total);
                    break;
            }
        }

        private int HandleFinishedRead(
            HistoryReadParams read,
            List<HistoryReadParams> chunks,
            int totalRead,
            int totalNodes,
            Action<HistoryReadNode> terminateCb)
        {
            if (read.Exception != null)
            {
                LogReadFailure(read);
                return read.Nodes.Count;
            }
            int total = read.Nodes.Sum(node => node.LastRead);
            IncrementMetrics(total);
            LogHistoryChunk(read.Nodes, total);

            if (config.IgnoreContinuationPoints && !Frontfill)
            {
                var newMin = read.Nodes.Select(node => node.Time).Max();
                if (newMin <= historyStartTime)
                {
                    foreach (var node in read.Nodes)
                    {
                        node.Completed = true;
                        terminateCb(node);
                    }
                }
            }

            var toTerminate = read.Nodes.Where(node => node.Completed).ToList();
            LogHistoryTermination(toTerminate, totalRead, totalNodes);
            read.Nodes = read.Nodes.Where(node => !node.Completed).ToList();

            if (read.Nodes.Any())
            {
                chunks.Add(read);
            }

            return toTerminate.Count;
        }

        private List<HistoryReadParams> RecalculateChunks(List<HistoryReadParams> reads)
        {
            var result = new List<HistoryReadParams>();
            int total = reads.Sum(read => read.Nodes.Count);
            if (total == 0) return result;
            // Effectively remove then re-add all nodes to the schedule.
            numActiveNodes -= total;
            var nodes = reads.SelectMany(read => read.Nodes)
                .OrderBy(node => node.Time)
                .ToList();
            return GetNextChunks(nodes, 0, out _);
        }

        private void SchedulingLoop(IEnumerable<UAHistoryExtractionState> states, CancellationToken token)
        {
            if (!states.Any()) return;

            using var source = CancellationTokenSource.CreateLinkedTokenSource(token);

            log.Information("Start reading history of type {type} for {cnt} nodes", type, states.Count());

            var nodes = states
                .Select(state => new HistoryReadNode(type, state))
                .OrderBy(state => state.Time)
                .ToList();
            int totalRead = 0;
            int totalNodes = nodes.Count;

            int index = 0;

            if (type == HistoryReadType.BackfillData || type == HistoryReadType.BackfillEvents)
            {
                var toTerminate = nodes.TakeWhile(node => node.State.SourceExtractedRange.Last <= historyStartTime).ToList();
                index += toTerminate.Count;
                totalRead += toTerminate.Count;
                foreach (var node in toTerminate)
                {
                    node.State.UpdateFromBackfill(CogniteTime.DateTimeEpoch, true);
                }
                LogHistoryTermination(toTerminate, totalRead, totalNodes);
            }


            var chunks = GetNextChunks(nodes, index, out index);

            var cb = Data
                ? (Action<IEncodeable?, HistoryReadNode, HistoryReadDetails>)HistoryDataHandler
                : HistoryEventHandler;

            while (numActiveNodes > 0 || chunks.Any())
            {
                if (token.IsCancellationRequested) break;
                var generators = chunks
                    .Select<HistoryReadParams, Func<Task>>(
                        chunk => () =>
                        {
                            return Task.Run(() => BaseHistoryReadOp(chunk, cb, finishedReads, source.Token));
                        })
                    .ToList();
                chunks.Clear();
                foreach (var generator in generators)
                {
                    throttler.EnqueueTask(generator);
                }
                var finished = new List<HistoryReadParams>();
                try
                {
                    finished.Add(finishedReads.Take(source.Token));
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                while (finishedReads.TryTake(out var finishedRead))
                {
                    finished.Add(finishedRead);
                }
                foreach (var read in finished)
                {
                    int terminated = HandleFinishedRead(read, chunks, totalRead, totalNodes, node => cb(null, node, read.Details));
                    totalRead += terminated;
                    numActiveNodes -= terminated;
                }
                if (config.IgnoreContinuationPoints)
                {
                    chunks = RecalculateChunks(chunks);
                    if (Data)
                    {
                        // Since datapoints all have distinct time, we can add a tick here to avoid reading the same again
                        // The same does not hold for events. Either way we also have a fallback check in the handlers 
                        // for cases when the server operates on a lower resolution than ticks.
                        foreach (var chunk in chunks)
                        {
                            if (!(chunk.Details is ReadRawModifiedDetails details)) continue;
                            details.StartTime = details.StartTime.AddTicks(Frontfill ? 1 : -1);
                        }
                    }
                }
                chunks.AddRange(GetNextChunks(nodes, index, out index));
            }

            log.Information("Finish history of type {type} for {cnt} nodes", type, nodes.Count);
            source.Cancel();
        }



        private void BaseHistoryReadOp(
            HistoryReadParams readParams,
            Action<IEncodeable, HistoryReadNode, HistoryReadDetails> handler,
            BlockingCollection<HistoryReadParams> finishedReads,
            CancellationToken token)
        {
            try
            {
                if (token.IsCancellationRequested) return;
                var results = uaClient.DoHistoryRead(readParams);

                foreach (var res in results)
                {
                    handler(res.RawData, res.Node, readParams.Details);

                    if (config.IgnoreContinuationPoints)
                    {
                        res.Node.ContinuationPoint = null;
                    }
                }
            }
            catch (Exception ex)
            {
                readParams.Exception = ex;
                throw;
            }
            finally
            {
                finishedReads.Add(readParams, token);
            }
        }

        /// <summary>
        /// Handle the result of a historyReadRaw. Takes information about the read, updates states and sends datapoints to the streamer.
        /// </summary>
        /// <param name="rawData">Data to be transformed into events</param>
        /// <param name="node">Active HistoryReadNode</param>
        /// <returns>Number of points read</returns>
        private void HistoryDataHandler(IEncodeable? rawData, HistoryReadNode node, HistoryReadDetails _)
        {
            var data = rawData as HistoryData;

            if (node.State == null)
            {
                node.State = extractor.State.GetNodeState(node.Id);
            }

            if (node.State == null)
            {
                log.Warning("History data for unknown node received: {id}", node.Id);
                return;
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
                    || Frontfill && first == last && last == node.State.SourceExtractedRange.Last
                    || !Frontfill && first == last && last == node.State.SourceExtractedRange.First;
            }

            if (Frontfill)
            {
                node.State.UpdateFromFrontfill(last, node.Completed);
            }
            else
            {
                node.State.UpdateFromBackfill(first, node.Completed);
            }

            int cnt = 0;

            var nodeState = node.State as VariableExtractionState;

            if (nodeState == null) return;

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

            node.LastRead = cnt;
            node.TotalRead += cnt;

            if (!node.Completed || !Frontfill) return;

            var buffered = nodeState.FlushBuffer();
            if (buffered.Any())
            {
                log.Debug("Read {cnt} datapoints from buffer of state {id}", buffered.Count(), node.State.Id);
                nodeState.UpdateFromStream(buffered);
                extractor.Streamer.Enqueue(buffered);
            }
        }

        private static DateTime? GetTimeAttribute(VariantCollection evt, EventFilter filter)
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
        /// <param name="details">History read details used to generate this HistoryRead result</param>
        /// <returns>Number of events read</returns>
        private void HistoryEventHandler(IEncodeable? rawEvts, HistoryReadNode node, HistoryReadDetails details)
        {
            var evts = rawEvts as HistoryEvent;
            if (!(details is ReadEventDetails eventDetails))
            {
                log.Warning("Incorrect details type of history read events");
                return;
            }
            var filter = eventDetails.Filter;
            if (filter == null || filter.SelectClauses == null)
            {
                log.Warning("No event filter when reading from history, ignoring");
                return;
            }
            if (node.State == null)
            {
                node.State = extractor.State.GetEmitterState(node.Id);
            }

            if (node.State == null)
            {
                log.Warning("History events for unknown emitter received: {id}", node.Id);
                return;
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
                    || Frontfill && first == last && last == node.State.SourceExtractedRange.Last
                    || !Frontfill && first == last && last == node.State.SourceExtractedRange.First;
            }

            if (Frontfill)
            {
                node.State.UpdateFromFrontfill(last, node.Completed);
            }
            else
            {
                node.State.UpdateFromBackfill(first, node.Completed);
            }

            extractor.Streamer.Enqueue(createdEvents);

            node.LastRead = createdEvents.Count;
            node.TotalRead += createdEvents.Count;

            if (!node.Completed || !Frontfill) return;

            var emitterState = node.State as EventExtractionState;

            if (emitterState == null) return;

            var buffered = emitterState.FlushBuffer();
            if (buffered.Any())
            {
                var (smin, smax) = buffered.MinMax(dp => dp.Time);
                emitterState.UpdateFromStream(smin, smax);
                log.Debug("Read {cnt} events from buffer of state {id}", buffered.Count(), node.State.Id);
                extractor.Streamer.Enqueue(buffered);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    throttler.Dispose();
                    finishedReads.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }

    public class HistoryReader
    {
        private readonly UAClient uaClient;
        private readonly UAExtractor extractor;
        private readonly HistoryConfig config;
        private CancellationTokenSource source;
        private int running;
        private ILogger log = Log.Logger.ForContext<HistoryReader>();
        public HistoryReader(UAClient uaClient, UAExtractor extractor, HistoryConfig config, CancellationToken token)
        {
            this.config = config;
            this.uaClient = uaClient;
            this.extractor = extractor;
            source = CancellationTokenSource.CreateLinkedTokenSource(token);
        }

        private async Task Run(IEnumerable<UAHistoryExtractionState> states, HistoryReadType type)
        {
            var scheduler = new HistoryScheduler(uaClient, extractor, config, type);
            try
            {
                Interlocked.Increment(ref running);
                await scheduler.Run(states, source.Token);
            }
            finally
            {
                scheduler.Dispose();
                Interlocked.Decrement(ref running);
            }
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
            if (running == 0) return true;
            log.Debug("Attempting to abort history read");
            source.Cancel();
            int timeout = timeoutsec * 10;
            int cycles = 0;
            while (running > 0 && cycles++ < timeout) await Task.Delay(100, token);
            source.Dispose();
            source = CancellationTokenSource.CreateLinkedTokenSource(token);
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

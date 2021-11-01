﻿/* Cognite Extractor for OPC-UA
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
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.History
{
    public class HistoryMetrics
    {
        public Counter NumItems { get; }
        public Counter NumInstances { get; }

        public HistoryMetrics(HistoryReadType type)
        {
            switch (type)
            {
                case HistoryReadType.FrontfillData:
                    NumItems = Metrics.CreateCounter("opcua_frontfill_data_points", "Number of datapoints retrieved through frontfill");
                    NumInstances = Metrics.CreateCounter("opcua_frontfill_data_count", "Number of times frontfill has been run on datapoints");
                    break;
                case HistoryReadType.BackfillData:
                    NumItems = Metrics.CreateCounter("opcua_backfill_data_points", "Number of datapoints retrieved through backfill");
                    NumInstances = Metrics.CreateCounter("opcua_backfill_data_count", "Number of times backfill has been run on datapoints");
                    break;
                case HistoryReadType.FrontfillEvents:
                    NumItems = Metrics.CreateCounter("opcua_frontfill_events", "Number of events retrieved through frontfill");
                    NumInstances = Metrics.CreateCounter("opcua_frontfill_events_count", "Number of times frontfill has been run on events");
                    break;
                case HistoryReadType.BackfillEvents:
                    NumItems = Metrics.CreateCounter("opcua_backfill_events", "Number of events retrieved through backfill");
                    NumInstances = Metrics.CreateCounter("opcua_backfill_events_count", "Number of times backfill has been run on events");
                    break;
                default: throw new InvalidOperationException("Invalid type");
            }
        }
    }


    public class HistoryScheduler : SharedResourceScheduler<HistoryReadNode>
    {
        private readonly UAClient uaClient;
        private readonly UAExtractor extractor;
        private readonly HistoryConfig config;

        private readonly HistoryReadType type;
        private readonly DateTime historyStartTime;
        private readonly DateTime? historyEndTime;
        private readonly TimeSpan historyGranularity;
        private readonly ILogger log;

        private bool Frontfill => type == HistoryReadType.FrontfillData || type == HistoryReadType.FrontfillEvents;
        private bool Data => type == HistoryReadType.FrontfillData || type == HistoryReadType.BackfillData;

        private int numReads;

        private readonly HistoryMetrics metrics;
        private readonly int chunkSize;
        private readonly int nodeCount;

        private readonly TimeSpan? maxReadLength;

        private readonly List<Exception> exceptions = new List<Exception>();

        public HistoryScheduler(
            ILogger log,
            UAClient uaClient,
            UAExtractor extractor,
            HistoryConfig config,
            HistoryReadType type,
            TaskThrottler throttler,
            IResourceCounter resource,
            IEnumerable<UAHistoryExtractionState> states,
            CancellationToken token)
            : base(
                  GetNodes(states, log, type, config.StartTime, out var count),
                  throttler,
                  (type == HistoryReadType.FrontfillData || type == HistoryReadType.BackfillData)
                    ? config.DataNodesChunk
                    : config.EventNodesChunk,
                  resource,
                  token)
        {
            this.log = log;
            this.uaClient = uaClient;
            this.extractor = extractor;
            this.config = config;
            this.type = type;
            chunkSize = Data ? config.DataNodesChunk : config.EventNodesChunk;

            maxReadLength = config.MaxReadLengthValue.Value;
            if (maxReadLength == TimeSpan.Zero || maxReadLength == Timeout.InfiniteTimeSpan) maxReadLength = null;

            nodeCount = count;

            historyStartTime = GetStartTime(config.StartTime);
            if (!string.IsNullOrWhiteSpace(config.EndTime)) historyEndTime = CogniteTime.ParseTimestampString(config.EndTime)!;

            historyGranularity = config.GranularityValue.Value;

            metrics = new HistoryMetrics(type);
        }

        private static DateTime GetStartTime(string? start)
        {
            if (string.IsNullOrWhiteSpace(start)) return CogniteTime.DateTimeEpoch;
            var parsed = CogniteTime.ParseTimestampString(start);
            if (parsed == null) throw new ArgumentException($"Invalid history start time: {start}");
            return parsed!.Value;
        }

        private static IEnumerable<HistoryReadNode> GetNodes(
            IEnumerable<UAHistoryExtractionState> states,
            ILogger log,
            HistoryReadType type,
            string? historyStart,
            out int count)
        {
            var nodes = states.Select(state => new HistoryReadNode(type, state)).ToList();

            var startTime = GetStartTime(historyStart);

            if (type == HistoryReadType.BackfillData || type == HistoryReadType.BackfillEvents)
            {
                var toTerminate = nodes.Where(node => node.Time <= startTime).ToList();
                nodes = nodes.Where(node => node.Time > startTime).ToList();
                foreach (var node in toTerminate)
                {
                    node.State.UpdateFromBackfill(CogniteTime.DateTimeEpoch, true);
                }
                LogHistoryTermination(log, toTerminate, type);
            }
            count = nodes.Count;
            return nodes;
        }


        protected override void AbortChunk(IChunk<HistoryReadNode> chunk, CancellationToken token)
        {
            var readChunk = (HistoryReadParams)chunk;
            uaClient.AbortHistoryRead(readChunk, CancellationToken.None).Wait(CancellationToken.None);
        }

        private static DateTime Max(DateTime t1, DateTime t2)
        {
            return t1 > t2 ? t1 : t2;
        }

        private (DateTime min, DateTime max) GetReadRange(IEnumerable<HistoryReadNode> nodes)
        {
            DateTime min, max;
            if (Frontfill)
            {
                min = Max(nodes.First().Time, historyStartTime);
                if (maxReadLength == null) max = DateTime.MinValue;
                else
                {
                    max = min + maxReadLength.Value;
                    if (max > (historyEndTime ?? DateTime.UtcNow)) max = historyEndTime ?? DateTime.MinValue;
                }
            }
            else
            {
                min = Max(nodes.Last().Time, historyStartTime);
                if (maxReadLength == null) max = historyStartTime;
                else max = Max(min - maxReadLength.Value, historyStartTime);
            }
            return (min, max);
        }

        private (HistoryReadDetails, DateTime, DateTime) GetReadDetails(IEnumerable<HistoryReadNode> nodes)
        {
            HistoryReadDetails details;
            var (min, max) = GetReadRange(nodes);
            switch (type)
            {
                case HistoryReadType.FrontfillData:
                    details = new ReadRawModifiedDetails
                    {
                        IsReadModified = false,
                        StartTime = min,
                        EndTime = max,
                        NumValuesPerNode = (uint)config.DataChunk
                    };
                    break;
                case HistoryReadType.BackfillData:
                    details = new ReadRawModifiedDetails
                    {
                        IsReadModified = false,
                        StartTime = min,
                        EndTime = max,
                        NumValuesPerNode = (uint)config.DataChunk
                    };
                    break;
                case HistoryReadType.FrontfillEvents:
                    details = new ReadEventDetails
                    {
                        StartTime = min,
                        EndTime = max,
                        NumValuesPerNode = (uint)config.EventChunk,
                        Filter = uaClient.BuildEventFilter()
                    };
                    break;
                case HistoryReadType.BackfillEvents:
                    details = new ReadEventDetails
                    {
                        StartTime = min,
                        EndTime = max,
                        NumValuesPerNode = (uint)config.EventChunk,
                        Filter = uaClient.BuildEventFilter()
                    };
                    break;
                default:
                    throw new InvalidOperationException();
            }
            return (details, min, max);
        }

        protected override async Task ConsumeChunk(IChunk<HistoryReadNode> chunk, CancellationToken token)
        {
            if (token.IsCancellationRequested) return;
            numReads++;
            var readChunk = (HistoryReadParams)chunk;
            await uaClient.DoHistoryRead(readChunk, token);
        }

        protected override IChunk<HistoryReadNode> GetChunk(IEnumerable<HistoryReadNode> items)
        {
            var (details, startTime, endTime) = GetReadDetails(items);

            if (maxReadLength != null)
            {
                foreach (var node in items)
                {
                    if (node.ContinuationPoint != null) continue;
                    node.StartTime = startTime;
                    node.EndTime = endTime;
                }
            }
            return new HistoryReadParams(items, details);
        }

        protected override IEnumerable<HistoryReadNode> GetNextChunk(
            IEnumerable<HistoryReadNode> items,
            int capacity,
            out IEnumerable<HistoryReadNode> newItems)
        {
            newItems = items;
            if (!items.Any()) return Enumerable.Empty<HistoryReadNode>();

            int toTake = chunkSize > 0 ? Math.Min(capacity, chunkSize) : capacity;
            var chunk = new List<HistoryReadNode>();

            bool hasCps = items.First().ContinuationPoint != null;

            DateTime? start = null;
            foreach (var item in items)
            {
                if (chunk.Count >= toTake) break;
                if (item.ContinuationPoint == null)
                {
                    if (start == null) start = item.Time;
                    if (item.Time - start > historyGranularity) break;
                    // Do not mix nodes with and without continuation-points
                    if (hasCps) break;
                }

                chunk.Add(item);
            }

            newItems = items.Skip(chunk.Count);
            return chunk;
        }

        protected override IEnumerable<IChunk<HistoryReadNode>> GetNextChunks(
            IEnumerable<HistoryReadNode> items,
            int capacity,
            out IEnumerable<HistoryReadNode> newItems)
        {
            items = items.OrderBy(nd => nd.ContinuationPoint == null).ThenBy(nd => nd.Time).ToList();
            return base.GetNextChunks(items, capacity, out newItems);
        }

        public new async Task RunAsync()
        {
            log.LogInformation("Begin reading history of type {Type} for {Count} nodes", type, nodeCount);
            await base.RunAsync();
            log.LogInformation("Finish reading history of type {Type} for {Count} nodes. " +
                "Took a total of {NumOps} operations", type, nodeCount, numReads);
            if (exceptions.Any())
            {
                throw new AggregateException(exceptions);
            }
        }

        #region results

        private void LogReadFailure(IChunk<HistoryReadNode> finishedRead)
        {
            log.LogError("HistoryRead {Type} failed for nodes {Nodes}: {ErrorMessage}",
                type, string.Join(", ", finishedRead.Items.Select(node => node.State.Id)), finishedRead.Exception?.Message);

            ExtractorUtils.LogException(log, finishedRead.Exception, "Critical failure in HistoryRead", "Failure in HistoryRead");
        }

        private static string GetResourceName(HistoryReadType type)
        {
            switch (type)
            {
                case HistoryReadType.BackfillData:
                case HistoryReadType.FrontfillData:
                    return "datapoints";
                case HistoryReadType.FrontfillEvents:
                case HistoryReadType.BackfillEvents:
                    return "events";
                default:
                    break;
            }
            throw new InvalidOperationException();
        }

        private static void LogHistoryTermination(ILogger log, List<HistoryReadNode> toTerminate, HistoryReadType type)
        {
            if (!toTerminate.Any()) return;
            string name = GetResourceName(type);
            var builder = new StringBuilder();
            bool frontfill = type == HistoryReadType.FrontfillData || type == HistoryReadType.FrontfillEvents;
            foreach (var node in toTerminate)
            {
                builder.AppendFormat("\n    {0} {1} total for {2}. End is now at {3}",
                    node.TotalRead,
                    name,
                    node.State.Id,
                    frontfill ? node.State.SourceExtractedRange.Last : node.State.SourceExtractedRange.First);
            }
            log.LogDebug("Finish reading {Type}. Retrieved: {Data}", name, builder);
        }

        protected override IEnumerable<HistoryReadNode> HandleTaskResult(IChunk<HistoryReadNode> chunk, CancellationToken token)
        {
            var readChunk = (HistoryReadParams)chunk;

            metrics.NumInstances.Inc();
            numReads++;

            if (chunk.Exception != null)
            {
                LogReadFailure(chunk);
                exceptions.Add(chunk.Exception);
                return Enumerable.Empty<HistoryReadNode>();
            }

            foreach (var node in chunk.Items)
            {
                if (Data)
                {
                    HistoryDataHandler(node);
                }
                else
                {
                    HistoryEventHandler(node, readChunk.Details);
                }

                if (config.IgnoreContinuationPoints)
                {
                    node.ContinuationPoint = null;
                }

                metrics.NumItems.Inc(node.LastRead);
            }

            var toTerminate = chunk.Items.Where(node => node.Completed).ToList();
            LogHistoryTermination(log, toTerminate, type);

            return Enumerable.Empty<HistoryReadNode>();
        }

        protected override void OnIteration(int pending, int operations, int finished, int total)
        {
            log.LogDebug("Read history of type {Type}: {Pending} pending, {Op} total operations. {Finished}/{Total}",
                type, pending, operations, finished, total);
        }


        /// <summary>
        /// Handle the result of a historyReadRaw. Takes information about the read, updates states and sends datapoints to the streamer.
        /// </summary>
        /// <param name="node">Active HistoryReadNode</param>
        /// <returns>Number of points read</returns>
        private void HistoryDataHandler(HistoryReadNode node)
        {
            var data = node.LastResult as HistoryData;
            node.LastResult = null;

            if (node.State == null)
            {
                node.State = extractor.State.GetNodeState(node.Id);
            }

            if (node.State == null)
            {
                log.LogWarning("History data for unknown node received: {Id}", node.Id);
                return;
            }

            List<DataValue> dataPoints = new List<DataValue>(data?.DataValues?.Count ?? 0);
            if (data?.DataValues != null)
            {
                int badDps = 0;
                foreach (var dp in data.DataValues)
                {
                    if (StatusCode.IsNotGood(dp.StatusCode))
                    {
                        UAExtractor.BadDataPoints.Inc();

                        badDps++;
                        if (config.LogBadValues)
                        {
                            log.LogTrace("Bad history datapoint: {BadDatapointExternalId} {SourceTimestamp}. Value: {Value}, Status: {Status}",
                                node.State.Id, dp.SourceTimestamp, dp.Value, ExtractorUtils.GetStatusCodeName((uint)dp.StatusCode));
                        }
                        continue;
                    }
                    dataPoints.Add(dp);
                }
                if (badDps > 0 && config.LogBadValues)
                {
                    log.LogDebug("Received {Count} bad history datapoints for {BadDatapointExternalId}",
                        badDps, node.State.Id);
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
                    || !Frontfill && first == last && last == node.State.SourceExtractedRange.First
                    || !Frontfill && last <= historyStartTime;
            }

            if (maxReadLength != null)
            {
                if (Frontfill)
                {
                    node.Completed &= historyEndTime != null && node.EndTime >= historyEndTime || node.EndTime == DateTime.MinValue;
                }
                else
                {
                    node.Completed &= node.EndTime <= historyStartTime;
                }
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


            if (node.State is not VariableExtractionState nodeState) return;

            foreach (var datapoint in dataPoints)
            {
                var buffDps = extractor.Streamer.ToDataPoint(datapoint, nodeState);
                foreach (var buffDp in buffDps)
                {
                    log.LogTrace("History DataPoint {DataPoint}", buffDp);
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
                log.LogDebug("Read {Count} datapoints from buffer of state {Id}", buffered.Count(), node.State.Id);
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

            if (raw is not DateTime dt) return null;
            return dt;
        }

        /// <summary>
        /// Handler for HistoryRead of events. Simply pushes all events to the queue.
        /// </summary>
        /// <param name="node">Active HistoryReadNode</param>
        /// <param name="details">History read details used to generate this HistoryRead result</param>
        /// <returns>Number of events read</returns>
        private void HistoryEventHandler(HistoryReadNode node, HistoryReadDetails details)
        {
            var evts = node.LastResult as HistoryEvent;
            node.LastResult = null;

            if (details is not ReadEventDetails eventDetails)
            {
                log.LogWarning("Incorrect details type of history read events");
                return;
            }
            var filter = eventDetails.Filter;
            if (filter == null || filter.SelectClauses == null)
            {
                log.LogWarning("No event filter when reading from history, ignoring");
                return;
            }
            if (node.State == null)
            {
                node.State = extractor.State.GetEmitterState(node.Id);
            }

            if (node.State == null)
            {
                log.LogWarning("History events for unknown emitter received: {Id}", node.Id);
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
                    || !Frontfill && first == last && last == node.State.SourceExtractedRange.First
                    || !Frontfill && last <= historyStartTime;
            }

            if (maxReadLength != null)
            {
                if (Frontfill)
                {
                    node.Completed &= historyEndTime != null && node.EndTime >= historyEndTime || node.EndTime == DateTime.MinValue;
                }
                else
                {
                    node.Completed &= node.EndTime <= historyStartTime;
                }
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

            if (node.State is not EventExtractionState emitterState) return;

            var buffered = emitterState.FlushBuffer();
            if (buffered.Any())
            {
                var (smin, smax) = buffered.MinMax(dp => dp.Time);
                emitterState.UpdateFromStream(smin, smax);
                log.LogDebug("Read {Count} events from buffer of state {Id}", buffered.Count(), node.State.Id);
                extractor.Streamer.Enqueue(buffered);
            }
        }
        #endregion
    }
}

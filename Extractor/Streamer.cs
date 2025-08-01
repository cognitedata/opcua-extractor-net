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
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Nito.AsyncEx;
using System.Threading.Tasks;
using Cognite.OpcUa.Utils;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Handles pushing of events and datapoints to destinations.
    /// Buffers up to a point.
    /// </summary>
    public class Streamer
    {
        private readonly UAExtractor extractor;
        private readonly FullConfig config;

        private readonly AsyncBlockingQueue<UADataPoint> dataPointQueue;
        private readonly AsyncBlockingQueue<UAEvent> eventQueue;

        private const int maxEventCount = 100_000;
        private const int maxDpCount = 1_000_000;

        private readonly ILogger<Streamer> log;

        private static readonly Counter missedArrayPoints = Metrics
            .CreateCounter("opcua_array_points_missed", "Points missed due to incorrect ArrayDimensions");
        private static readonly Summary timeToExtractorDps = Metrics.CreateSummary("opcua_streaming_delay_datapoints",
            "Time difference between datapoint SourceTimestamp to local time when they reach the extractor from subscriptions, in seconds");
        private static readonly Summary timeToExtractorEvents = Metrics.CreateSummary("opcua_streaming_delay_events",
            "Time difference between event Time to local time when they reach the extractor from subscriptions, in seconds");

        public bool AllowEvents { get; set; }
        public bool AllowData { get; set; }

        public Streamer(ILogger<Streamer> log, UAExtractor extractor, FullConfig config)
        {
            this.log = log;
            this.extractor = extractor;
            this.config = config;

            dataPointQueue = new AsyncBlockingQueue<UADataPoint>(maxDpCount, "Datapoints", log);
            eventQueue = new AsyncBlockingQueue<UAEvent>(maxEventCount, "Events", log);

            dataPointQueue.OnQueueOverflow += OnQueueOverflow;
            eventQueue.OnQueueOverflow += OnQueueOverflow;
        }

        private void OnQueueOverflow(object sender, EventArgs e)
        {
            extractor.Looper.Scheduler.TryTriggerTask("Pushers");
        }

        /// <summary>
        /// Enqueue a datapoint, pushes if this exceeds the maximum.
        /// </summary>
        /// <param name="dp">Datapoint to enqueue.</param>
        public void Enqueue(UADataPoint dp)
        {
            dataPointQueue.Enqueue(dp);
        }
        /// <summary>
        /// Enqueue a list of datapoints, pushes if this exceeds the maximum.
        /// </summary>
        /// <param name="dps">Datapoints to enqueue.</param>
        public void Enqueue(IEnumerable<UADataPoint> dps)
        {
            if (dps == null) return;
            dataPointQueue.Enqueue(dps);
        }
        /// <summary>
        /// Enqueues an event, pushes if this exceeds the maximum.
        /// </summary>
        /// <param name="evt">Event to enqueue.</param>
        public void Enqueue(UAEvent evt)
        {
            eventQueue.Enqueue(evt);
        }
        /// <summary>
        /// Enqueues a list of events, pushes if this exceeds the maximum.
        /// </summary>
        /// <param name="events">Events to enqueue.</param>
        public void Enqueue(IEnumerable<UAEvent> events)
        {
            if (events == null) return;
            eventQueue.Enqueue(events);
        }


        /// <summary>
        /// Enqueue a datapoint, pushes if this exceeds the maximum.
        /// </summary>
        /// <param name="dp">Datapoint to enqueue.</param>
        public async Task EnqueueAsync(UADataPoint dp)
        {
            await dataPointQueue.EnqueueAsync(dp);
        }
        /// <summary>
        /// Enqueue a list of datapoints, pushes if this exceeds the maximum.
        /// </summary>
        /// <param name="dps">Datapoints to enqueue.</param>
        public async Task EnqueueAsync(IEnumerable<UADataPoint> dps)
        {
            if (dps == null) return;
            await dataPointQueue.EnqueueAsync(dps);
        }
        /// <summary>
        /// Enqueues an event, pushes if this exceeds the maximum.
        /// </summary>
        /// <param name="evt">Event to enqueue.</param>
        public async Task EnqueueAsync(UAEvent evt)
        {
            await eventQueue.EnqueueAsync(evt);
        }
        /// <summary>
        /// Enqueues a list of events, pushes if this exceeds the maximum.
        /// </summary>
        /// <param name="events">Events to enqueue.</param>
        public async Task EnqueueAsync(IEnumerable<UAEvent> events)
        {
            if (events == null) return;
            await eventQueue.EnqueueAsync(events);
        }
        /// <summary>
        /// Push data points to destinations
        /// </summary>
        /// <param name="passingPushers">Succeeding pushers, data will be pushed to these.</param>
        /// <param name="failingPushers">Failing pushers, data will not be pushed to these.</param>
        /// <returns>True if history should be restarted after this</returns>
        public async Task PushDataPoints(IEnumerable<IPusher> passingPushers,
            IEnumerable<IPusher> failingPushers, CancellationToken token)
        {
            if (!AllowData) return;

            var dataPointList = new List<UADataPoint>();
            var pointRanges = new Dictionary<string, TimeRange>();

            await foreach (var dp in dataPointQueue.DrainAsync(token))
            {
                dataPointList.Add(dp);
                if (!pointRanges.TryGetValue(dp.Id, out var range))
                {
                    pointRanges[dp.Id] = new TimeRange(dp.Timestamp, dp.Timestamp);
                    continue;
                }
                pointRanges[dp.Id] = range.Extend(dp.Timestamp, dp.Timestamp);
            }

            var results = await Task.WhenAll(passingPushers.Select(pusher => pusher.PushDataPoints(dataPointList, token)));

            bool anyFailed = results.Any(status => status == false);

            if (anyFailed || failingPushers.Any())
            {
                List<IPusher> failedPushers = new List<IPusher>();
                if (anyFailed)
                {
                    var failed = results.Select((res, idx) => (result: res, Index: idx)).Where(x => x.result == false).ToList();
                    foreach (var pair in failed)
                    {
                        var pusher = passingPushers.ElementAt(pair.Index);
                        pusher.DataFailing = true;
                        failedPushers.Add(pusher);
                    }
                    log.LogWarning("Pushers of types {Types} failed while pushing datapoints",
                        string.Concat(failedPushers.Select(pusher => pusher.GetType().ToString())));
                    extractor.OnDataPushFailure();
                }
                if (config.FailureBuffer.Enabled && extractor.FailureBuffer != null)
                {
                    await extractor.FailureBuffer.WriteDatapoints(dataPointList, pointRanges, token);
                }

                return;
            }
            var reconnectedPushers = passingPushers.Where(pusher => pusher.DataFailing).ToList();
            if (reconnectedPushers.Count != 0)
            {
                log.LogInformation("{Count} failing pushers were able to push data, reconnecting", reconnectedPushers.Count);
                extractor.OnDataPushRecovery();

                foreach (var pusher in reconnectedPushers)
                {
                    pusher.DataFailing = false;
                }
            }

            if (config.FailureBuffer.Enabled && extractor.FailureBuffer != null && extractor.FailureBuffer.AnyPoints)
            {
                await extractor.FailureBuffer.ReadDatapoints(passingPushers, token);
            }
            foreach ((string id, var range) in pointRanges)
            {
                var state = extractor.State.GetNodeState(id);
                if (state != null && (extractor.AllowUpdateState || !state.FrontfillEnabled && !state.BackfillEnabled)) state.UpdateDestinationRange(range.First, range.Last);
            }
        }
        /// <summary>
        /// Push events to destinations
        /// </summary>
        /// <param name="passingPushers">Succeeding pushers, events will be pushed to these.</param>
        /// <param name="failingPushers">Failing pushers, events will not be pushed to these.</param>
        /// <returns>True if history should be restarted after this</returns>
        public async Task PushEvents(IEnumerable<IPusher> passingPushers,
            IEnumerable<IPusher> failingPushers, CancellationToken token)
        {
            if (!AllowEvents) return;

            var eventList = new List<UAEvent>();
            var eventRanges = new Dictionary<NodeId, TimeRange>();

            await foreach (var evt in eventQueue.DrainAsync(token))
            {
                eventList.Add(evt);
                if (!eventRanges.TryGetValue(evt.EmittingNode, out var range))
                {
                    eventRanges[evt.EmittingNode] = new TimeRange(evt.Time, evt.Time);
                    continue;
                }

                eventRanges[evt.EmittingNode] = range.Extend(evt.Time, evt.Time);
            }

            var results = await Task.WhenAll(passingPushers.Select(pusher => pusher.PushEvents(eventList, token)));

            var anyFailed = results.Any(status => status == false);

            if (anyFailed || failingPushers.Any())
            {
                var failedPushers = new List<IPusher>();
                if (anyFailed)
                {
                    var failed = results.Select((res, idx) => (result: res, Index: idx)).Where(x => x.result == false).ToList();
                    foreach (var pair in failed)
                    {
                        var pusher = passingPushers.ElementAt(pair.Index);
                        pusher.EventsFailing = true;
                        failedPushers.Add(pusher);
                    }
                    log.LogWarning("Pushers of types {Types} failed while pushing events",
                        failedPushers.Select(pusher => pusher.GetType().ToString()).Aggregate((src, val) => src + ", " + val));
                    extractor.OnEventsPushFailure();
                }

                if (config.FailureBuffer.Enabled && extractor.FailureBuffer != null)
                {
                    await extractor.FailureBuffer.WriteEvents(eventList, token);
                }

                return;
            }
            var reconnectedPushers = passingPushers.Where(pusher => pusher.EventsFailing).ToList();
            if (reconnectedPushers.Count != 0)
            {
                log.LogInformation("{Count} failing pushers were able to push events, reconnecting", reconnectedPushers.Count);
                extractor.OnEventsPushRecovery();

                foreach (var pusher in reconnectedPushers)
                {
                    pusher.EventsFailing = false;
                }
            }
            if (config.FailureBuffer.Enabled && extractor.FailureBuffer != null && extractor.FailureBuffer.AnyEvents)
            {
                await extractor.FailureBuffer.ReadEvents(passingPushers, token);
            }
            foreach (var (id, range) in eventRanges)
            {
                var state = extractor.State.GetEmitterState(id);
                if (state != null && (extractor.AllowUpdateState || !state.FrontfillEnabled && !state.BackfillEnabled)) state?.UpdateDestinationRange(range.First, range.Last);
            }
        }
        /// <summary>
        /// Handles notifications on subscribed items, pushes all new datapoints to the queue.
        /// </summary>
        /// <param name="item">Modified item</param>
        public void DataSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs args)
        {
            if (config.Logger.UaSessionTracing)
            {
                log.LogDump("Streamed datapoint monitored item", item);
                log.LogDump("Streamed datapoint event args", args);
            }
            if (item == null) return;
            var node = extractor.State.GetNodeState(item.ResolvedNodeId);
            if (node == null)
            {
                log.LogWarning("Subscription to unknown node: {Id}", item.ResolvedNodeId);
                return;
            }

            foreach (var datapoint in item.DequeueValues())
            {
                HandleStreamedDatapoint(datapoint, node);
            }
        }


        public void HandleStreamedDatapoint(DataValue datapoint, VariableExtractionState node)
        {
            if (StatusCode.IsNotGood(datapoint.StatusCode))
            {
                UAExtractor.BadDataPoints.Inc();

                if (config.Subscriptions.LogBadValues)
                {
                    log.LogDebug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}. Value: {Value}, Status: {Status}",
                        node.Id, datapoint.SourceTimestamp, datapoint.Value, ExtractorUtils.GetStatusCodeName((uint)datapoint.StatusCode));
                }

                switch (config.Extraction.StatusCodes.StatusCodesToIngest)
                {
                    case StatusCodeMode.All:
                        break;
                    case StatusCodeMode.Uncertain:
                        if (!StatusCode.IsUncertain(datapoint.StatusCode))
                        {
                            return;
                        }
                        break;
                    case StatusCodeMode.GoodOnly:
                        return;
                }
            }

            timeToExtractorDps.Observe((DateTime.UtcNow - datapoint.SourceTimestamp).TotalSeconds);

            if (node.AsEvents)
            {
                var evt = DpAsEvent(datapoint, node);
                log.LogTrace("Subscription DataPoint treated as event {Event}", node);
                node.UpdateFromStream(DateTime.MaxValue, datapoint.SourceTimestamp);
                Enqueue(evt);
                return;
            }

            var buffDps = ToDataPoint(datapoint, node);
            if (StatusCode.IsGood(datapoint.StatusCode))
            {
                node.UpdateFromStream(buffDps);
            }

            if ((extractor.StateStorage == null || config.StateStorage.IntervalValue.Value == Timeout.InfiniteTimeSpan)
                 && (node.IsFrontfilling && datapoint.SourceTimestamp > node.SourceExtractedRange.Last
                    || node.IsBackfilling && datapoint.SourceTimestamp < node.SourceExtractedRange.First)) return;

            foreach (var buffDp in buffDps)
            {
                Enqueue(buffDp);
            }
        }


        private static string GetArrayUniqueId(string baseId, int index)
        {
            if (index < 0) return baseId;
            string idxStr = $"[{index}]";
            if (baseId.Length + idxStr.Length < 255) return baseId + idxStr;
            return baseId.Substring(0, 255 - idxStr.Length) + idxStr;
        }

        private UAEvent DpAsEvent(DataValue datapoint, VariableExtractionState node)
        {
            var value = extractor.TypeConverter.ConvertToString(datapoint.WrappedValue);
            var evt = new UAEvent
            {
                EmittingNode = node.SourceId,
                EventId = $"{node.Id}-{datapoint.SourceTimestamp.Ticks}",
                Message = value,
                SourceNode = node.SourceId,
                Time = datapoint.SourceTimestamp,
            };
            evt.SetMetadata(extractor.TypeConverter, new[] {
                new EventFieldValue(new RawTypeField(new QualifiedName("Status")), new Variant(datapoint.StatusCode))
            }, log);
            return evt;
        }

        /// <summary>
        /// Transform a given DataValue into a datapoint or a list of datapoints if the variable in question has array type.
        /// </summary>
        /// <param name="value">DataValue to be transformed</param>
        /// <param name="variable">NodeExtractionState for variable the datavalue belongs to</param>
        /// <returns>List of converted datapoints</returns>
        public IEnumerable<UADataPoint> ToDataPoint(DataValue value, VariableExtractionState variable)
        {
            string uniqueId = variable.Id;

            if (value.Value is Array)
            {
                var variantArray = extractor.TypeConverter.ExtractVariantArray(value.WrappedValue);
                int dim = 1;
                if (variantArray.Length == 0) return Enumerable.Empty<UADataPoint>();
                if (!variable.IsArray)
                {
                    log.LogDebug("Array values returned for scalar variable {Id}", variable.Id);
                    if (variantArray.Length > 1)
                    {
                        missedArrayPoints.Inc(variantArray.Length - 1);
                    }
                }
                else if (variable.ArrayDimensions[0] >= variantArray.Length)
                {
                    dim = variantArray.Length;
                }
                else
                {
                    dim = variable.ArrayDimensions[0];
                    log.LogDebug("Missing {Count} points for variable {Id} due to too small ArrayDimensions", variantArray.Length - dim, variable.Id);
                    missedArrayPoints.Inc(variantArray.Length - dim);
                }
                var ret = new List<UADataPoint>();
                for (int i = 0; i < dim; i++)
                {
                    var id = variable.IsArray ? GetArrayUniqueId(uniqueId, i) : uniqueId;
                    var entry = variantArray.GetValueOrDefault(i, Variant.Null);
                    ret.Add(variable.DataType.ToDataPoint(extractor, entry, value.SourceTimestamp, id, value.StatusCode));
                }
                return ret;
            }
            if (variable.IsArray)
            {
                uniqueId = GetArrayUniqueId(uniqueId, 0);
            }

            var sdp = variable.DataType.ToDataPoint(extractor, value.WrappedValue, value.SourceTimestamp, uniqueId, value.StatusCode);
            return new[] { sdp };
        }

        /// <summary>
        /// Handle subscription callback for events
        /// </summary>
        public void EventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs args)
        {
            if (config.Logger.UaSessionTracing)
            {
                log.LogDump("Streamed event monitored item", item);
                log.LogDump("Streamed event event args", args);
            }
            if (item == null) return;
            if (item.Filter is not EventFilter filter)
            {
                log.LogWarning("Triggered event without filter");
                return;
            }
            var eventState = extractor.State.GetEmitterState(item.ResolvedNodeId);
            if (eventState == null)
            {
                log.LogWarning("Event triggered from unknown node: {Id}", item.ResolvedNodeId);
                return;
            }

            foreach (var eventFields in item.DequeueEvents())
            {
                if (eventFields == null || eventFields.EventFields == null) continue;
                var buffEvent = ConstructEvent(filter, eventFields.EventFields, item.ResolvedNodeId);
                if (buffEvent == null)
                {
                    UAExtractor.BadEvents.Inc();
                    continue;
                }

                timeToExtractorEvents.Observe((DateTime.UtcNow - buffEvent.Time).TotalSeconds);

                // The event buffer is deprecated and will be removed soon.
                // Until it is, it only does anything useful if writing to influxdb is enabled.
                eventState.UpdateFromStream(buffEvent, config.Influx != null);

                // Either backfill/frontfill is done, or we are not outside of each respective bound
                if ((extractor.StateStorage == null || config.StateStorage.IntervalValue.Value == Timeout.InfiniteTimeSpan)
                    && config.Influx != null
                    && (eventState.IsFrontfilling && buffEvent.Time > eventState.SourceExtractedRange.Last
                        || eventState.IsBackfilling && buffEvent.Time < eventState.SourceExtractedRange.First)) continue;

                Enqueue(buffEvent);
            }
        }

        /// <summary>
        /// Construct event from filter and collection of event fields
        /// </summary>
        /// <param name="filter">Filter that resulted in this event</param>
        /// <param name="eventFields">Fields for a single event</param>
        public UAEvent? ConstructEvent(EventFilter filter, VariantCollection eventFields, NodeId emitter)
        {
            int eventTypeIndex = filter.SelectClauses.FindIndex(atr =>
                atr.BrowsePath.Count == 1
                && atr.BrowsePath[0] == BrowseNames.EventType);

            if (eventTypeIndex < 0 || eventFields.Count <= eventTypeIndex)
            {
                log.LogWarning("Triggered event has no type, ignoring.");
                return null;
            }

            var typeId = eventFields[eventTypeIndex].Value as NodeId;
            // Many servers don't handle filtering on history data.
            if (typeId == null || !extractor.State.ActiveEvents.TryGetValue(typeId, out var eventType))
            {
                log.LogTrace("Invalid event type: {EventType}", typeId);
                return null;
            }

            var extractedProperties = new Dictionary<string, EventFieldValue>();

            for (int i = 0; i < filter.SelectClauses.Count; i++)
            {
                var clause = filter.SelectClauses[i];
                var field = new RawTypeField(clause.BrowsePath);
                if (!eventType.CollectedFields.Contains(field)) continue;

                string name = string.Join('_', clause.BrowsePath.Select(name => name.Name));
                if (name != "EventId" && name != "SourceNode" && name != "EventType" && config.Events.DestinationNameMap.TryGetValue(name, out var mapped))
                {
                    field = new RawTypeField(new QualifiedNameCollection(clause.BrowsePath.Take(clause.BrowsePath.Count - 1).Append(mapped)));
                    name = mapped;
                }
                if (!extractedProperties.TryGetValue(name, out var extracted) || extracted.Value == Variant.Null)
                {
                    extractedProperties[name] = new EventFieldValue(field, eventFields[i]);
                }
            }

            if (!extractedProperties.TryGetValue("EventId", out var rawEventId) || rawEventId.Value.Value is not byte[] byteEventId)
            {
                log.LogTrace("Event of type {Type} lacks id", typeId);
                return null;
            }

            string eventId = Convert.ToBase64String(byteEventId);
            if (!extractedProperties.TryGetValue("SourceNode", out var rawSourceNode) || rawSourceNode.Value.Value is not NodeId sourceNode)
            {
                sourceNode = NodeId.Null;
            }

            if (!extractedProperties.TryGetValue("Time", out var rawTime) || rawTime.Value.Value is not DateTime time)
            {
                log.LogTrace("Event lacks specified time, type: {Type}", typeId);
                return null;
            }

            var finalProperties = extractedProperties.Select(kvp => kvp.Value);
            var buffEvent = new UAEvent
            {
                Message = extractor.TypeConverter.ConvertToString(extractedProperties.GetValueOrDefault("Message")?.Value ?? Variant.Null),
                EventId = config.Extraction.IdPrefix + eventId,
                SourceNode = sourceNode,
                Time = time,
                EventType = eventType,
                EmittingNode = emitter
            };
            buffEvent.SetMetadata(extractor.TypeConverter, finalProperties, log);
            return buffEvent;
        }
    }
}

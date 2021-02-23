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
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using Serilog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public class Streamer
    {
        private readonly UAExtractor extractor;
        private readonly FullConfig config;

        private readonly Queue<UADataPoint> dataPointQueue = new Queue<UADataPoint>();
        private readonly Queue<UAEvent> eventQueue = new Queue<UAEvent>();

        private const int maxEventCount = 100_000;
        private const int maxDpCount = 1_000_000;

        private readonly object dataPointMutex = new object();
        private readonly object eventMutex = new object();

        private readonly ILogger log = Log.Logger.ForContext(typeof(Streamer));

        private static readonly Counter missedArrayPoints = Metrics
            .CreateCounter("opcua_array_points_missed", "Points missed due to incorrect ArrayDimensions");

        public bool AllowEvents { get; set; }
        public bool AllowData { get; set; }

        public Streamer(UAExtractor extractor, FullConfig config)
        {
            this.extractor = extractor;
            this.config = config;
        }

        public void Enqueue(UADataPoint dp)
        {
            lock (dataPointMutex)
            {
                dataPointQueue.Enqueue(dp);
                if (dataPointQueue.Count >= maxDpCount) extractor.Looper.TriggerPush();
            }
        }
        public void Enqueue(IEnumerable<UADataPoint> dps)
        {
            if (dps == null) return;
            lock (dataPointMutex)
            {
                foreach (var dp in dps) dataPointQueue.Enqueue(dp);
                if (dataPointQueue.Count >= maxDpCount) extractor.Looper.TriggerPush();

            }
        }
        public void Enqueue(UAEvent evt)
        {
            lock (eventMutex)
            {
                eventQueue.Enqueue(evt);
                if (eventQueue.Count >= maxEventCount) extractor.Looper.TriggerPush();
            }
        }
        public void Enqueue(IEnumerable<UAEvent> events)
        {
            if (events == null) return;
            lock (eventMutex)
            {
                foreach (var evt in events) eventQueue.Enqueue(evt);
                if (eventQueue.Count >= maxEventCount) extractor.Looper.TriggerPush();
            }
        }
        /// <summary>
        /// Push data points to destinations
        /// </summary>
        /// <param name="passingPushers">Succeeding pushers, data will be pushed to these.</param>
        /// <param name="failingPushers">Failing pushers, data will not be pushed to these.</param>
        /// <returns>True if history should be restarted after this</returns>
        public async Task<bool> PushDataPoints(IEnumerable<IPusher> passingPushers,
            IEnumerable<IPusher> failingPushers, CancellationToken token)
        {
            if (!AllowData) return false;

            bool restartHistory = false;

            var dataPointList = new List<UADataPoint>();
            var pointRanges = new Dictionary<string, TimeRange>();

            lock (dataPointMutex)
            {
                while (dataPointQueue.TryDequeue(out UADataPoint dp))
                {
                    dataPointList.Add(dp);
                    if (!pointRanges.TryGetValue(dp.Id, out var range))
                    {
                        pointRanges[dp.Id] = new TimeRange(dp.Timestamp, dp.Timestamp);
                        continue;
                    }
                    pointRanges[dp.Id] = range.Extend(dp.Timestamp, dp.Timestamp);
                }
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
                    log.Warning("Pushers of types {types} failed while pushing datapoints",
                        string.Concat(failedPushers.Select(pusher => pusher.GetType().ToString())));
                }
                if (config.FailureBuffer.Enabled)
                {
                    await extractor.FailureBuffer.WriteDatapoints(dataPointList, pointRanges, token);
                }

                return false;
            }
            var reconnectedPushers = passingPushers.Where(pusher => pusher.DataFailing).ToList();
            if (reconnectedPushers.Any())
            {
                log.Information("{cnt} failing pushers were able to push data, reconnecting", reconnectedPushers.Count);

                if (config.History.Enabled && extractor.State.NodeStates.Any(state => state.FrontfillEnabled))
                {
                    log.Information("Restarting history for {cnt} states", extractor.State.NodeStates.Count(state => state.FrontfillEnabled));
                    bool success = await extractor.TerminateHistory(30);
                    if (!success) throw new ExtractorFailureException("Failed to terminate history reader");
                    foreach (var state in extractor.State.NodeStates.Where(state => state.FrontfillEnabled))
                    {
                        state.RestartHistory();
                    }
                    restartHistory = true;
                }

                foreach (var pusher in reconnectedPushers)
                {
                    pusher.DataFailing = false;
                }
            }

            if (config.FailureBuffer.Enabled && extractor.FailureBuffer.AnyPoints)
            {
                await extractor.FailureBuffer.ReadDatapoints(passingPushers, token);
            }
            foreach ((string id, var range) in pointRanges)
            {
                var state = extractor.State.GetNodeState(id);
                state.UpdateDestinationRange(range.First, range.Last);
            }
            return restartHistory;
        }
        /// <summary>
        /// Push events to destinations
        /// </summary>
        /// <param name="passingPushers">Succeeding pushers, events will be pushed to these.</param>
        /// <param name="failingPushers">Failing pushers, events will not be pushed to these.</param>
        /// <returns>True if history should be restarted after this</returns>
        public async Task<bool> PushEvents(IEnumerable<IPusher> passingPushers,
            IEnumerable<IPusher> failingPushers, CancellationToken token)
        {
            if (!AllowEvents) return false;

            var eventList = new List<UAEvent>();
            var eventRanges = new Dictionary<NodeId, TimeRange>();

            bool restartHistory = false;

            lock (eventMutex)
            {
                while (eventQueue.TryDequeue(out UAEvent evt))
                {
                    eventList.Add(evt);
                    if (!eventRanges.TryGetValue(evt.EmittingNode, out var range))
                    {
                        eventRanges[evt.EmittingNode] = new TimeRange(evt.Time, evt.Time);
                        continue;
                    }

                    eventRanges[evt.EmittingNode] = range.Extend(evt.Time, evt.Time);
                }
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
                    log.Warning("Pushers of types {types} failed while pushing events",
                        failedPushers.Select(pusher => pusher.GetType().ToString()).Aggregate((src, val) => src + ", " + val));
                }

                if (config.FailureBuffer.Enabled)
                {
                    await extractor.FailureBuffer.WriteEvents(eventList, token);
                }

                return false;
            }
            var reconnectedPushers = passingPushers.Where(pusher => pusher.EventsFailing).ToList();
            if (reconnectedPushers.Any())
            {
                log.Information("{cnt} failing pushers were able to push events, reconnecting", reconnectedPushers.Count);

                if (config.Events.History && extractor.State.EmitterStates.Any(state => state.FrontfillEnabled))
                {
                    log.Information("Restarting event history for {cnt} states", extractor.State.EmitterStates.Count(state => state.FrontfillEnabled));
                    bool success = await extractor.TerminateHistory(30);
                    if (!success) throw new ExtractorFailureException("Failed to terminate history reader");
                    foreach (var state in extractor.State.EmitterStates.Where(state => state.FrontfillEnabled))
                    {
                        state.RestartHistory();
                    }
                    restartHistory = true;
                }

                foreach (var pusher in reconnectedPushers)
                {
                    pusher.EventsFailing = false;
                }
            }
            if (config.FailureBuffer.Enabled && extractor.FailureBuffer.AnyEvents)
            {
                await extractor.FailureBuffer.ReadEvents(passingPushers, token);
            }
            foreach (var (id, range) in eventRanges)
            {
                var state = extractor.State.GetEmitterState(id);
                state.UpdateDestinationRange(range.First, range.Last);
            }


            return restartHistory;
        }
        /// <summary>
        /// Handles notifications on subscribed items, pushes all new datapoints to the queue.
        /// </summary>
        /// <param name="item">Modified item</param>
        public void DataSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs _)
        {
            if (item == null) return;
            var node = extractor.State.GetNodeState(item.ResolvedNodeId);
            if (node == null)
            {
                log.Warning("Subscription to unknown node: {id}", item.ResolvedNodeId);
                return;
            }

            foreach (var datapoint in item.DequeueValues())
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    UAExtractor.BadDataPoints.Inc();
                    log.Debug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}", node.Id, datapoint.SourceTimestamp);
                    continue;
                }
                var buffDps = ToDataPoint(datapoint, node);
                node.UpdateFromStream(buffDps);

                if ((extractor.StateStorage == null || config.StateStorage.Interval <= 0)
                    && (node.IsFrontfilling && datapoint.SourceTimestamp > node.SourceExtractedRange.Last
                        || node.IsBackfilling && datapoint.SourceTimestamp < node.SourceExtractedRange.First)) continue;
                foreach (var buffDp in buffDps)
                {
                    log.Verbose("Subscription DataPoint {dp}", buffDp.ToDebugDescription());
                    Enqueue(buffDp);
                }
            }
        }
        private static string GetArrayUniqueId(string baseId, int index)
        {
            if (index < 0) return baseId;
            string idxStr = $"[{index}]";
            if (baseId.Length + idxStr.Length < 255) return baseId + idxStr;
            return baseId.Substring(0, 255 - idxStr.Length) + idxStr;
        }

        /// <summary>
        /// Transform a given DataValue into a datapoint or a list of datapoints if the variable in question has array type.
        /// </summary>
        /// <param name="value">DataValue to be transformed</param>
        /// <param name="variable">NodeExtractionState for variable the datavalue belongs to</param>
        /// <param name="uniqueId"></param>
        /// <returns>UniqueId to be used, for efficiency</returns>
        public IEnumerable<UADataPoint> ToDataPoint(DataValue value, VariableExtractionState variable)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (variable == null) throw new ArgumentNullException(nameof(value));

            string uniqueId = variable.Id;

            if (value.Value is Array values)
            {
                int dim = 1;
                if (values.Length == 0) return Enumerable.Empty<UADataPoint>();
                if (!variable.IsArray)
                {
                    log.Debug("Array values returned for scalar variable {id}", variable.Id);
                    if (values.Length > 1)
                    {
                        missedArrayPoints.Inc(values.Length - 1);
                    }
                }
                else if (variable.ArrayDimensions[0] >= values.Length)
                {
                    dim = values.Length;
                }
                else
                {
                    dim = variable.ArrayDimensions[0];
                    log.Debug("Missing {cnt} points for variable {id} due to too small ArrayDimensions", values.Length - dim, variable.Id);
                    missedArrayPoints.Inc(values.Length - dim);
                }
                var ret = new List<UADataPoint>();
                for (int i = 0; i < dim; i++)
                {
                    var id = variable.IsArray ? GetArrayUniqueId(uniqueId, i) : uniqueId;
                    ret.Add(variable.DataType.ToDataPoint(extractor, values.GetValue(i), value.SourceTimestamp, id));
                }
                return ret;
            }
            if (variable.IsArray)
            {
                uniqueId = GetArrayUniqueId(uniqueId, 0);
            }

            var sdp = variable.DataType.ToDataPoint(extractor, value.WrappedValue, value.SourceTimestamp, uniqueId);
            return new[] { sdp };
        }
        /// <summary>
        /// Handle subscription callback for events
        /// </summary>
        public void EventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs _)
        {
            if (item == null) return;
            if (!(item.Filter is EventFilter filter))
            {
                log.Warning("Triggered event without filter");
                return;
            }
            var eventState = extractor.State.GetEmitterState(item.ResolvedNodeId);
            if (eventState == null)
            {
                log.Warning("Event triggered from unknown node: {id}", item.ResolvedNodeId);
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
                eventState.UpdateFromStream(buffEvent);

                // Either backfill/frontfill is done, or we are not outside of each respective bound
                if ((extractor.StateStorage == null || config.StateStorage.Interval <= 0)
                    && (eventState.IsFrontfilling && buffEvent.Time > eventState.SourceExtractedRange.Last
                        || eventState.IsBackfilling && buffEvent.Time < eventState.SourceExtractedRange.First)) continue;

                log.Verbose(buffEvent.ToString());
                Enqueue(buffEvent);
            }
        }
        /// <summary>
        /// Construct event from filter and collection of event fields
        /// </summary>
        /// <param name="filter">Filter that resulted in this event</param>
        /// <param name="eventFields">Fields for a single event</param>
        public UAEvent ConstructEvent(EventFilter filter, VariantCollection eventFields, NodeId emitter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            if (eventFields == null) throw new ArgumentNullException(nameof(eventFields));

            int eventTypeIndex = filter.SelectClauses.FindIndex(atr =>
                atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                && atr.BrowsePath.Count == 1
                && atr.BrowsePath[0] == BrowseNames.EventType);

            if (eventTypeIndex < 0 || eventFields.Count <= eventTypeIndex)
            {
                log.Warning("Triggered event has no type, ignoring.");
                return null;
            }

            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            // Many servers don't handle filtering on history data.
            if (eventType == null || !extractor.State.ActiveEvents.TryGetValue(eventType, out var targetEventFields))
            {
                log.Verbose("Invalid event type: {eventType}", eventType);
                return null;
            }

            var extractedProperties = new Dictionary<string, object>();

            for (int i = 0; i < filter.SelectClauses.Count; i++)
            {
                var clause = filter.SelectClauses[i];
                if (clause.BrowsePath.Count != 1
                    || !targetEventFields.Contains(new EventField(clause.TypeDefinitionId, clause.BrowsePath[0]))) continue;

                string name = clause.BrowsePath[0].Name;
                if (name != "EventId" && name != "SourceNode" && name != "EventType" && config.Events.DestinationNameMap.TryGetValue(name, out var mapped))
                {
                    name = mapped;
                }
                if (!extractedProperties.TryGetValue(name, out var extracted) || extracted == null)
                {
                    extractedProperties[name] = eventFields[i];
                }
            }

            if (!(extractedProperties.GetValueOrDefault("EventId") is byte[] rawEventId))
            {
                log.Verbose("Event of type {type} lacks id", eventType);
                return null;
            }

            string eventId = Convert.ToBase64String(rawEventId);
            var sourceNode = extractedProperties.GetValueOrDefault("SourceNode") as NodeId;

            if (!(extractedProperties.GetValueOrDefault("Time") is DateTime time))
            {
                log.Verbose("Event lacks specified time, type: {type}", eventType);
                return null;
            }
            var buffEvent = new UAEvent
            {
                Message = extractor.ConvertToString(extractedProperties.GetValueOrDefault("Message")),
                EventId = config.Extraction.IdPrefix + eventId,
                SourceNode = sourceNode,
                Time = time,
                EventType = eventType,
                MetaData = extractedProperties
                    .Where(kvp => kvp.Key != "Message" && kvp.Key != "EventId" && kvp.Key != "SourceNode"
                                  && kvp.Key != "Time" && kvp.Key != "EventType")
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                EmittingNode = emitter
            };
            return buffEvent;
        }
    }
}

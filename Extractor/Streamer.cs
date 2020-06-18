using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Opc.Ua;
using Opc.Ua.Client;
using Serilog;

namespace Cognite.OpcUa
{
    public class Streamer
    {
        private readonly Extractor extractor;
        private readonly FullConfig config;

        public ConcurrentQueue<BufferedDataPoint> DataPointQueue { get; }
            = new ConcurrentQueue<BufferedDataPoint>();

        public ConcurrentQueue<BufferedEvent> EventQueue { get; }
            = new ConcurrentQueue<BufferedEvent>();

        private readonly ILogger log = Log.Logger.ForContext(typeof(Streamer));


        public bool AllowEvents { get; set; }
        public bool AllowData { get; set; }

        public Streamer(Extractor extractor, FullConfig config)
        {
            this.extractor = extractor;
            this.config = config;
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

            var dataPointList = new List<BufferedDataPoint>();
            var pointRanges = new Dictionary<string, TimeRange>();

            while (DataPointQueue.TryDequeue(out BufferedDataPoint dp))
            {
                dataPointList.Add(dp);
                if (!pointRanges.ContainsKey(dp.Id))
                {
                    pointRanges[dp.Id] = new TimeRange(dp.Timestamp, dp.Timestamp);
                    continue;
                }
                pointRanges[dp.Id] = pointRanges[dp.Id].Extend(dp.Timestamp, dp.Timestamp);
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
                    log.Warning("Pushers with indices {idx} failed while pushing datapoints",
                        failed.Select(pair => pair.Index.ToString(CultureInfo.InvariantCulture)).Aggregate((src, val) => src + ", " + val));
                    foreach (var state in extractor.State.NodeStates)
                    {
                        state.RestartHistory();
                    }
                }
                if (config.FailureBuffer.Enabled)
                {
                    await extractor.FailureBuffer.WriteDatapoints(dataPointList, pointRanges, failingPushers.Concat(failedPushers), token);
                }

                return false;
            }
            var reconnectedPushers = passingPushers.Where(pusher => pusher.DataFailing).ToList();
            if (reconnectedPushers.Any())
            {
                log.Information("{cnt} failing pushers were able to push data, reconnecting", reconnectedPushers.Count);
                // Try to push any non-historizing points
                var nonHistorizing = pointRanges.Keys.Where(key => !extractor.State.GetNodeState(key).FrontfillEnabled).ToHashSet();
                var pointsToPush = dataPointList.Where(point => nonHistorizing.Contains(point.Id)).ToList();
                var pushResults = await Task.WhenAll(reconnectedPushers.Select(pusher => pusher.PushDataPoints(pointsToPush, token)));

                // Here we are fine with "null" result. Not ideal, but it is what it is.
                // In theory we might end up in an expensive loop, but the connection tests should be designed so that
                // success there implies success otherwise.
                if (!pushResults.All(res => res == null || res == true)) return false;

                if (config.History.Enabled)
                {
                    bool success = await extractor.TerminateHistory(30, token);
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
            if (config.FailureBuffer.Enabled && extractor.FailureBuffer.Any)
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
            var eventList = new List<BufferedEvent>();
            var eventRanges = new Dictionary<NodeId, TimeRange>();

            bool restartHistory = false;

            while (EventQueue.TryDequeue(out BufferedEvent evt))
            {
                eventList.Add(evt);
                if (!eventRanges.ContainsKey(evt.EmittingNode))
                {
                    eventRanges[evt.EmittingNode] = new TimeRange(evt.Time, evt.Time);
                    continue;
                }

                eventRanges[evt.EmittingNode] = eventRanges[evt.EmittingNode].Extend(evt.Time, evt.Time);
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
                    log.Warning("Pushers with indices {idx} failed while pushing events", 
                        failed.Select(pair => pair.Index.ToString(CultureInfo.InvariantCulture)).Aggregate((src, val) => src + ", " + val));
                    foreach (var state in extractor.State.EmitterStates)
                    {
                        state.RestartHistory();
                    }
                }

                if (config.FailureBuffer.Enabled)
                {
                    await extractor.FailureBuffer.WriteEvents(eventList, failedPushers.Concat(failingPushers), token);
                }

                return false;
            }
            var reconnectedPushers = passingPushers.Where(pusher => pusher.EventsFailing).ToList();
            if (reconnectedPushers.Any())
            {
                // Try to push any non-historizing points
                var nonHistorizing = eventRanges.Keys.Where(key => !extractor.State.GetEmitterState(key).IsFrontfilling).ToHashSet();
                var eventsToPush = eventList.Where(point => nonHistorizing.Contains(point.EmittingNode)).ToList();
                var pushResults = await Task.WhenAll(reconnectedPushers.Select(pusher => pusher.PushEvents(eventsToPush, token)));

                // Here we are fine with "null" result. Not ideal, but it is what it is.
                // In theory we might end up in an expensive loop, but the connection tests should be designed so that
                // success there implies success otherwise.
                if (!pushResults.All(res => res == null || res == true)) return false;

                if (config.Events.HistorizingEmitterIds != null && config.Events.HistorizingEmitterIds.Any())
                {
                    bool success = await extractor.TerminateHistory(30, token);
                    if (!success) throw new ExtractorFailureException("Failed to terminate history reader");
                    foreach (var state in extractor.State.EmitterStates.Where(state => state.IsFrontfilling))
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
        public void DataSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (item == null || eventArgs == null) return;
            string uniqueId = extractor.GetUniqueId(item.ResolvedNodeId);
            var node = extractor.State.GetNodeState(item.ResolvedNodeId);

            foreach (var datapoint in item.DequeueValues())
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Extractor.BadDataPoints.Inc();
                    log.Debug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId, datapoint.SourceTimestamp);
                    continue;
                }
                var buffDps = ToDataPoint(datapoint, node, uniqueId);
                node.UpdateFromStream(buffDps);
                if (node.IsFrontfilling && (extractor.StateStorage == null || config.StateStorage.Interval <= 0)) return;
                foreach (var buffDp in buffDps)
                {
                    log.Verbose("Subscription DataPoint {dp}", buffDp.ToDebugDescription());
                }
                foreach (var buffDp in buffDps)
                {
                    DataPointQueue.Enqueue(buffDp);
                }
            }
        }
        /// <summary>
        /// Transform a given DataValue into a datapoint or a list of datapoints if the variable in question has array type.
        /// </summary>
        /// <param name="value">DataValue to be transformed</param>
        /// <param name="variable">NodeExtractionState for variable the datavalue belongs to</param>
        /// <param name="uniqueId"></param>
        /// <returns>UniqueId to be used, for efficiency</returns>
        public IEnumerable<BufferedDataPoint> ToDataPoint(DataValue value, NodeExtractionState variable, string uniqueId)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (variable == null) throw new ArgumentNullException(nameof(value));

            if (variable.ArrayDimensions != null && variable.ArrayDimensions.Count > 0 && variable.ArrayDimensions[0] > 0)
            {
                var ret = new List<BufferedDataPoint>();
                if (!(value.Value is Array))
                {
                    Extractor.BadDataPoints.Inc();
                    log.Debug("Bad array datapoint: {BadPointName} {BadPointValue}", uniqueId, value.Value.ToString());
                    return Enumerable.Empty<BufferedDataPoint>();
                }
                var values = (Array)value.Value;
                for (int i = 0; i < Math.Min(variable.ArrayDimensions[0], values.Length); i++)
                {
                    var dp = variable.DataType.IsString
                        ? new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            extractor.ConvertToString(values.GetValue(i)))
                        : new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            UAClient.ConvertToDouble(values.GetValue(i)));
                    ret.Add(dp);
                }
                return ret;
            }
            var sdp = variable.DataType.IsString
                ? new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    extractor.ConvertToString(value.Value))
                : new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToDouble(value.Value));
            return new[] { sdp };
        }
        /// <summary>
        /// Handle subscription callback for events
        /// </summary>
        public void EventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (eventArgs == null || item == null) return;
            if (!(eventArgs.NotificationValue is EventFieldList triggeredEvent))
            {
                log.Warning("No event in event subscription notification: {}", item.StartNodeId);
                return;
            }
            var eventFields = triggeredEvent.EventFields;
            if (!(item.Filter is EventFilter filter))
            {
                log.Warning("Triggered event without filter");
                return;
            }
            var buffEvent = ConstructEvent(filter, eventFields, item.ResolvedNodeId);
            if (buffEvent == null) return;
            var eventState = extractor.State.GetEmitterState(item.ResolvedNodeId);
            eventState.UpdateFromStream(buffEvent);

            // Either backfill/frontfill is done, or we are not outside of each respective bound
            if ((extractor.StateStorage == null || config.StateStorage.Interval <= 0)
                && (eventState.IsFrontfilling && buffEvent.Time > eventState.SourceExtractedRange.Last
                    || eventState.IsBackfilling && buffEvent.Time < eventState.SourceExtractedRange.First)) return;

            log.Verbose(buffEvent.ToDebugDescription());
            EventQueue.Enqueue(buffEvent);
        }
        /// <summary>
        /// Construct event from filter and collection of event fields
        /// </summary>
        /// <param name="filter">Filter that resulted in this event</param>
        /// <param name="eventFields">Fields for a single event</param>
        public BufferedEvent ConstructEvent(EventFilter filter, VariantCollection eventFields, NodeId emitter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            if (eventFields == null) throw new ArgumentNullException(nameof(eventFields));

            int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                       && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                log.Warning("Triggered event has no type, ignoring.");
                return null;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            // Many servers don't handle filtering on history data.
            if (eventType == null || !extractor.State.ActiveEvents.ContainsKey(eventType))
            {
                log.Verbose("Invalid event type: {eventType}", eventType);
                return null;
            }
            var targetEventFields = extractor.State.ActiveEvents[eventType];

            var extractedProperties = new Dictionary<string, object>();

            for (int i = 0; i < filter.SelectClauses.Count; i++)
            {
                var clause = filter.SelectClauses[i];
                if (!targetEventFields.Any(field =>
                    field.Root == clause.TypeDefinitionId
                    && field.BrowseName == clause.BrowsePath[0]
                    && clause.BrowsePath.Count == 1)) continue;

                string name = clause.BrowsePath[0].Name;
                if (config.Events.ExcludeProperties.Contains(name) || config.Events.BaseExcludeProperties.Contains(name)) continue;
                if (config.Events.DestinationNameMap.ContainsKey(name) && name != "EventId" && name != "SourceNode" && name != "EventType")
                {
                    name = config.Events.DestinationNameMap[name];
                }
                if (!extractedProperties.ContainsKey(name) || extractedProperties[name] == null)
                {
                    extractedProperties[name] = eventFields[i].Value;
                }
            }

            if (!(extractedProperties.GetValueOrDefault("EventId") is byte[] rawEventId))
            {
                log.Verbose("Event of type {type} lacks id", eventType);
                return null;
            }

            string eventId = Convert.ToBase64String(rawEventId);
            var sourceNode = extractedProperties.GetValueOrDefault("SourceNode");
            if (sourceNode == null || !extractor.State.IsMappedNode(sourceNode as NodeId))
            {
                log.Verbose("Invalid source node, type: {type}, id: {id}", eventType, eventId);
                return null;
            }

            var time = extractedProperties.GetValueOrDefault("Time");
            if (time == null)
            {
                log.Verbose("Event lacks specified time, type: {type}", eventType, eventId);
                return null;
            }
            var buffEvent = new BufferedEvent
            {
                Message = extractor.ConvertToString(extractedProperties.GetValueOrDefault("Message")),
                EventId = config.Extraction.IdPrefix + eventId,
                SourceNode = extractedProperties.GetValueOrDefault("SourceNode") as NodeId,
                Time = (DateTime)time,
                EventType = eventType,
                MetaData = extractedProperties
                    .Where(kvp => kvp.Key != "Message" && kvp.Key != "EventId" && kvp.Key != "SourceNode"
                                  && kvp.Key != "Time" && kvp.Key != "EventType")
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                EmittingNode = emitter,
                ReceivedTime = DateTime.UtcNow,
            };
            return buffEvent;
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using LiteDB;
using LiteQueue;
using Opc.Ua;
using Prometheus;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Manages a litedb database for persisting state and buffer.
    /// </summary>
    public sealed class StateStorage : IDisposable
    {
        public const string StringDPQueue = "string_dp_queue";
        public const string DoubleDPQueue = "double_dp_queue";
        public const string EventQueue = "event_queue";

        public const string VariableStates = "variable_states";
        public const string EmitterStates = "emitter_states";
        public const string InfluxVariableStates = "influx_variable_states";
        public const string InfluxEventStates = "influx_event_states";

        private const int DataBatchSize = 100000;
        private const int EventBatchSize = 1000;

        private readonly Extractor extractor;
        private readonly LiteDatabase db;

        private readonly ILogger log = Log.Logger.ForContext(typeof(StateStorage));

        private readonly LiteQueue<StringDataPointPoco> stringDataQueue;
        private readonly LiteQueue<DoubleDataPointPoco> doubleDataQueue;
        private readonly LiteQueue<EventPoco> eventQueue;
        public bool AnyPoints { get; set; }
        public bool AnyEvents { get; set; }

        private static readonly Counter statesStoredCounter = Metrics.CreateCounter(
            "opcua_num_state_stored", "The number of ranges stored in state storage");

        private static readonly Counter stateReadOperations = Metrics.CreateCounter(
            "opcua_state_read_count", "The number of times ranges have been read from state storage");

        private static readonly Gauge numDoublePointsInQueue = Metrics.CreateGauge(
            "opcua_queue_num_points_double", "The number of double-valued datapoints in the local buffer queue");

        private static readonly Gauge numStringPointsInQueue = Metrics.CreateGauge(
            "opcua_queue_num_points_string", "The number of string-valued datapoints in the local buffer queue");

        private static readonly Gauge numEventsInQueue = Metrics.CreateGauge(
            "opcua_queue_num_events", "The number of events in the local buffer queue");

        /// <summary>
        /// Constructor, initializes queues if necessary, and tests whether they contain any points.
        /// </summary>
        /// <param name="extractor">Extractor parent</param>
        /// <param name="config">Active configuration</param>
        public StateStorage(Extractor extractor, FullConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            this.extractor = extractor ?? throw new ArgumentNullException(nameof(extractor));

            string connection = $"Filename={config.StateStorage.Location};upgrade=true";

            db = new LiteDatabase(connection);

            if (!config.FailureBuffer.Enabled || !config.FailureBuffer.LocalQueue) return;

            stringDataQueue = new LiteQueue<StringDataPointPoco>(db, StringDPQueue);
            stringDataQueue.ResetOrphans();
            doubleDataQueue = new LiteQueue<DoubleDataPointPoco>(db, DoubleDPQueue);
            doubleDataQueue.ResetOrphans();
            AnyPoints = QueueAny(stringDataQueue).Result || QueueAny(doubleDataQueue).Result;
            eventQueue = new LiteQueue<EventPoco>(db, EventQueue);
            eventQueue.ResetOrphans();
            AnyEvents = QueueAny(eventQueue).Result;
            numDoublePointsInQueue.Set(doubleDataQueue.Count());
            numStringPointsInQueue.Set(stringDataQueue.Count());
            numEventsInQueue.Set(eventQueue.Count());
        }

        static StateStorage()
        {
            // Change the global bson mapper - only used in this class for now.
            // Don't trim white space in externalIds - need exact match with CDF.
            BsonMapper.Global.TrimWhitespace = false;

            BsonMapper.Global.ResolveMember += (type, memberInfo, member) =>
            {
                if (member.DataType == typeof(DateTime))
                {
                    member.Deserialize = (bson, m) => DateTime.FromBinary(bson.AsInt64);
                    member.Serialize = (dt, m) => ((DateTime)dt).ToBinary();
                }
            };

        }

        /// <summary>
        /// Store destination ranges to file. We use destination ranges as those always represent a conservative estimate
        /// of the data in destination systems. When adding new pushers, just allow them to do their own initialization or reset the
        /// database. This operates on the same principle as pushers:
        /// The most conservative option out of any pusher with timestamp initialization enabled
        /// is used, in addition to what is stored in the StateStorage.
        /// </summary>
        /// <param name="states">States to persist, if dirty</param>
        /// <param name="name">Name of the collection to persist to</param>
        public async Task StoreExtractionState(IEnumerable<BaseExtractionState> states, string name, CancellationToken token)
        {
            var toStore = states.Where(s => s.IsDirty).ToList();
            var pocos = toStore
                .Select(state => new ExtractionStatePoco()
                {
                    Id = extractor.GetUniqueId(state.Id),
                    FirstTimestamp = state.DestinationExtractedRange.Start,
                    LastTimestamp = state.DestinationExtractedRange.End
                }).ToList();

            if (!toStore.Any()) return;

            try
            {
                foreach (var chunk in ExtractorUtils.ChunkBy(pocos, 100))
                {
                    await Task.Run(() =>
                    {
                        var col = db.GetCollection<ExtractionStatePoco>(name);
                        col.Upsert(chunk);
                    }, token);
                    statesStoredCounter.Inc(chunk.Count());
                    foreach (var poco in chunk)
                    {
                        log.Debug("Saved {name}: ({start}, {end})",
                            poco.Id, poco.FirstTimestamp, poco.LastTimestamp);
                    }
                }

                log.Debug("Saved {Stored} out of {TotalNumber} historizing extraction states to store {name}", 
                    toStore.Count, states.Count(state => state.Historizing), name);

                foreach (var state in toStore)
                {
                    state.IsDirty = false;
                    state.StatePersisted = true;
                }
            }
            catch (LiteException e)
            {
                log.Warning(e, "Failed to store extraction state: {Message}", e.Message);
            }
        }
        /// <summary>
        /// Reads all states for convenience. Instead of doing nlog(n) searches, we just read m entries. So long as
        /// we're not reading a fraction of all known states smaller than 1/log(n), this is more efficient. With how it is used
        /// this is almost certainly better. For when it isn't, a search parameter is provided.
        /// </summary>
        /// <param name="states">States to read into</param>
        /// <param name="name">Name of collection to read</param>
        public async Task<bool> ReadExtractionStates(IEnumerable<BaseExtractionState> states, string name,
            bool search, CancellationToken token)
        {
            if (!states.Any()) return true;
            var stateMap = states.ToDictionary(state => extractor.GetUniqueId(state.Id));

            try
            {
                IEnumerable<ExtractionStatePoco> pocos;
                if (search)
                {
                    pocos = await Task.Run(() =>
                    {
                        var col = db.GetCollection<ExtractionStatePoco>(name);
                        var ret = new List<ExtractionStatePoco>();
                        foreach (var kvp in stateMap)
                        {
                            if (token.IsCancellationRequested) break;
                            var poco = col.FindById(kvp.Key);
                            if (poco == null) continue;
                            stateReadOperations.Inc();
                            ret.Add(poco);
                        }

                        return ret;
                    }, token);
                }
                else
                {
                    pocos = await Task.Run(() =>
                    {
                        var col = db.GetCollection<ExtractionStatePoco>(name);
                        stateReadOperations.Inc();
                        return col.FindAll();
                    }, token);
                }
                int count = 0;
                foreach (var poco in pocos)
                {
                    if (stateMap.ContainsKey(poco.Id))
                    {
                        count++;
                        log.Debug("Initialized {name} to ({start}, {end})",
                            poco.Id, poco.FirstTimestamp, poco.LastTimestamp);
                        stateMap[poco.Id].InitExtractedRange(poco.FirstTimestamp,
                            poco.LastTimestamp);
                        stateMap[poco.Id].StatePersisted = true;
                    }
                }
                log.Information("Initialized extracted ranges from statestore {name} for {cnt} nodes", 
                    name, count);
            }
            catch (LiteException e)
            {
                log.Warning(e, "Failed to restore extraction state: {Message}", e.Message);
                return false;
            }

            return true;
        }
        /// <summary>
        /// Return true if given queue contains any elements
        /// </summary>
        /// <param name="queue">Queue to test</param>
        /// <returns>True if queue contains any elements</returns>
        private static Task<bool> QueueAny<T>(LiteQueue<T> queue)
        {
            return Task.Run(() =>
            {
                var head = queue.Dequeue();
                if (head == null) return false;
                queue.Abort(head);
                return true;
            });
        }
        /// <summary>
        /// Write given list of datapoints to buffer queue
        /// </summary>
        /// <param name="points">Points to write</param>
        /// <returns>True on success</returns>
        public async Task<bool> WritePointsToQueue(IEnumerable<BufferedDataPoint> points, CancellationToken token)
        {
            if (points == null) return true;
            var stringPoints = new List<StringDataPointPoco>();
            var doublePoints = new List<DoubleDataPointPoco>();

            foreach (var point in points)
            {
                if (point.IsString)
                {
                    stringPoints.Add(new StringDataPointPoco(point));
                }
                else
                {
                    doublePoints.Add(new DoubleDataPointPoco(point));
                }
            }


            log.Information("Write {cnt} points to queue", stringPoints.Count + doublePoints.Count);
            try
            {
                await Task.WhenAll(
                    Task.Run(() => { stringDataQueue.Enqueue(stringPoints); }, token),
                    Task.Run(() => { doubleDataQueue.Enqueue(doublePoints); }, token));
            }
            catch (LiteException e)
            {
                log.Error(e, "Failed to insert datapoints into litedb queue");
                return false;
            }
            numStringPointsInQueue.Inc(stringPoints.Count);
            numDoublePointsInQueue.Inc(doublePoints.Count);
            AnyPoints = true;
            return true;
        }
        /// <summary>
        /// Read datapoints from buffer queue
        /// </summary>
        /// <param name="pushers">Pushers to write to</param>
        /// <returns>True on success</returns>
        public async Task<bool> ReadPointsFromQueue(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (pushers == null) return true;
            bool failed = false;
            await Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    var stringRecords = stringDataQueue.Dequeue(DataBatchSize);
                    var doubleRecords = doubleDataQueue.Dequeue(DataBatchSize);
                    var points = stringRecords.Select(record => record.Payload.ToDataPoint()).ToList();
                    int stringPoints = points.Count;
                    points.AddRange(doubleRecords.Select(record => record.Payload.ToDataPoint()));
                    int doublePoints = points.Count - stringPoints;
                    log.Information("Read {cnt} points from litedb queue", points.Count);


                    var results = await Task.WhenAll(pushers.Select(pusher => pusher.PushDataPoints(points, token)));

                    if (results.Any(res => res == false))
                    {
                        stringDataQueue.Abort(stringRecords);
                        doubleDataQueue.Abort(doubleRecords);
                        failed = true;
                        return;
                    }
                    // At this point, it should be safe to write points to destination ranges, even if
                    // not all pushers are being pushed to here. Those that are not should already have the points.
                    // If the extractor has been down we push to all just to be safe
                    var ranges = new Dictionary<string, TimeRange>();

                    foreach (var point in points)
                    {
                        if (!ranges.ContainsKey(point.Id))
                        {
                            ranges[point.Id] = new TimeRange(point.Timestamp, point.Timestamp);
                            continue;
                        }

                        var range = ranges[point.Id];
                        if (range.Start > point.Timestamp)
                        {
                            range.Start = point.Timestamp;
                        }
                        else if (range.End < point.Timestamp)
                        {
                            range.End = point.Timestamp;
                        }
                    }

                    foreach (var kvp in ranges)
                    {
                        var state = extractor.State.GetNodeState(kvp.Key);
                        state.UpdateDestinationRange(kvp.Value);
                    }

                    stringDataQueue.Commit(stringRecords);
                    numStringPointsInQueue.Dec(stringPoints);
                    doubleDataQueue.Commit(doubleRecords);
                    numDoublePointsInQueue.Dec(doublePoints);
                    if (stringRecords.Count < DataBatchSize && doubleRecords.Count < DataBatchSize) break;
                }
                if (numStringPointsInQueue.Value < 0)
                {
                    numStringPointsInQueue.Set(stringDataQueue.Count());
                }
                if (numDoublePointsInQueue.Value < 0)
                {
                    numDoublePointsInQueue.Set(doubleDataQueue.Count());
                }
                AnyPoints = false;
            }, token);

            return failed;
        }
        /// <summary>
        /// Write given list of events to buffer queue
        /// </summary>
        /// <param name="events">Events to write</param>
        /// <returns>True on success</returns>
        public async Task<bool> WriteEventsToQueue(IEnumerable<BufferedEvent> events, CancellationToken token)
        {
            if (events == null) return true;
            var eventPocos = events.Select(evt => new EventPoco(evt, extractor)).ToList();

            log.Information("Write {cnt} events to queue", eventPocos.Count);

            try
            {
                await Task.Run(() => eventQueue.Enqueue(eventPocos), token);
            }
            catch (LiteException e)
            {
                log.Error(e, "Failed to insert events into litedb queue");
                return false;
            }
            numEventsInQueue.Inc(eventPocos.Count);

            AnyEvents = true;
            return true;
        }
        /// <summary>
        /// Read events from buffer queue
        /// </summary>
        /// <param name="pushers">Pushers to write to</param>
        /// <returns>True on success</returns>
        public async Task<bool> ReadEventsFromQueue(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (pushers == null) return true;
            bool failed = false;
            await Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    var records = eventQueue.Dequeue(EventBatchSize);
                    var events = records.Select(record => record.Payload.ToBufferedEvent(extractor)).ToList();

                    log.Information("Read {cnt} events from litedb queue", events.Count);

                    var results = await Task.WhenAll(pushers.Select(pusher => pusher.PushEvents(events, token)));

                    if (results.Any(res => res == false))
                    {
                        eventQueue.Abort(records);
                        failed = true;
                        return;
                    }

                    var ranges = new Dictionary<NodeId, TimeRange>();

                    foreach (var evt in events)
                    {
                        if (!ranges.ContainsKey(evt.EmittingNode))
                        {
                            ranges[evt.EmittingNode] = new TimeRange(evt.Time, evt.Time);
                            continue;
                        }

                        var range = ranges[evt.EmittingNode];
                        if (range.Start > evt.Time)
                        {
                            range.Start = evt.Time;
                        }
                        else if (range.End < evt.Time)
                        {
                            range.End = evt.Time;
                        }
                    }

                    foreach (var kvp in ranges)
                    {
                        var state = extractor.State.GetEmitterState(kvp.Key);
                        state.UpdateDestinationRange(kvp.Value);
                    }
                    numEventsInQueue.Dec(events.Count);
                    eventQueue.Commit(records);
                    if (records.Count < EventBatchSize) break;
                }

                if (numEventsInQueue.Value < 0)
                {
                    numEventsInQueue.Set(eventQueue.Count());
                }
                AnyEvents = false;
            }, token);

            return failed;
        }
        /// <summary>
        /// Read MQTT states from buffer.
        /// </summary>
        /// <param name="name">Name of collection in litedb</param>
        /// <param name="invalidateTreshold">Only return states created after this</param>
        /// <returns>List of states</returns>
        public async Task<IEnumerable<string>> ReadMqttStates(string name, DateTime 
            invalidateTreshold, CancellationToken token)
        {
            try
            {
                var pocos = await Task.Run(() =>
                {
                    var col = db.GetCollection<MqttStatePoco>(name);
                    return col.FindAll();
                }, token);
                return pocos.Where(poco => poco.CreatedAt < invalidateTreshold || invalidateTreshold < new DateTime(1971, 1, 1))
                    .Select(poco => poco.Id);
            }
            catch (LiteException e)
            {
                log.Warning(e, "Failed to read mqtt states: {Message}", e.Message);
                return Array.Empty<string>();
            }
        }
        /// <summary>
        /// Write MQTT states to buffer
        /// </summary>
        /// <param name="name">Name of collection in litedb</param>
        /// <param name="ids">Ids to write</param>
        public async Task StoreMqttStates(string name, IEnumerable<string> ids, CancellationToken token)
        {
            var time = DateTime.UtcNow;
            var pocos = ids.Select(id => new MqttStatePoco {Id = id, CreatedAt = time}).ToList();
            if (!ids.Any()) return;
            try
            {
                foreach (var chunk in ExtractorUtils.ChunkBy(pocos, 100))
                {
                    await Task.Run(() =>
                    {
                        var col = db.GetCollection<MqttStatePoco>(name);
                        col.Upsert(chunk);
                    }, token);
                    statesStoredCounter.Inc(chunk.Count());
                }

                log.Debug("Saved {Stored} MQTT states to {name}", pocos.Count, name);
            }
            catch (LiteException e)
            {
                log.Warning(e, "Failed to store extraction state: {Message}", e.Message);
            }
        }
        public void Dispose()
        {
            db?.Dispose();
        }

        private class MqttStatePoco
        {
            [BsonId] public string Id { get; set; }
            [BsonField("created")] public DateTime CreatedAt { get; set; }
        }
        private class ExtractionStatePoco
        {
            [BsonId] public string Id { get; set; }
            [BsonField("first")] public DateTime FirstTimestamp { get; set; }
            [BsonField("last")] public DateTime LastTimestamp { get; set; }
        }

        private class StringDataPointPoco
        {
            [BsonId] public ObjectId Id { get; set; }
            [BsonField("ts")] public DateTime Timestamp { get; set; }
            [BsonField("value")] public string Value { get; set; }
            [BsonField("externalId")] public string ExternalId { get; set; }
            public StringDataPointPoco(BufferedDataPoint dp)
            {
                Id = ObjectId.NewObjectId();
                Timestamp = dp.Timestamp;
                Value = dp.StringValue;
                ExternalId = dp.Id;
            }

            public StringDataPointPoco()
            {
            }
            public BufferedDataPoint ToDataPoint()
            {
                return new BufferedDataPoint(Timestamp, ExternalId, Value);
            }
        }
        private class DoubleDataPointPoco
        {
            [BsonId] public ObjectId Id { get; set; }
            [BsonField("ts")] public DateTime Timestamp { get; set; }
            [BsonField("value")] public double Value { get; set; }
            [BsonField("externalId")] public string ExternalId { get; set; }
            public DoubleDataPointPoco(BufferedDataPoint dp)
            {
                Id = ObjectId.NewObjectId();
                Timestamp = dp.Timestamp;
                Value = dp.DoubleValue;
                ExternalId = dp.Id;
            }
            public DoubleDataPointPoco()
            {
            }
            public BufferedDataPoint ToDataPoint()
            {
                return new BufferedDataPoint(Timestamp, ExternalId, Value);
            }
        }
        private class EventPoco
        {
            [BsonId] public string Id { get; set; }
            [BsonField("msg")] public string Message { get; set; }
            [BsonField("emitter")] public string Emitter { get; set; }
            [BsonField("time")] public DateTime Time { get; set; }
            [BsonField("source")] public string SourceNode { get; set; }
            [BsonField("type")] public string EventType { get; set; }
            [BsonField("metadata")] public Dictionary<string, string> MetaData { get; set; }

            public EventPoco(BufferedEvent evt, Extractor extractor)
            {
                Id = evt.EventId;
                Message = evt.Message;
                Emitter = extractor.GetUniqueId(evt.EmittingNode);
                Time = evt.Time;
                SourceNode = extractor.GetUniqueId(evt.SourceNode);
                EventType = extractor.GetUniqueId(evt.EventType);
                MetaData = new Dictionary<string, string>();
                foreach (var dt in evt.MetaData)
                {
                    MetaData[dt.Key] = extractor.ConvertToString(dt.Value);
                }
            }

            public EventPoco() {}

            public BufferedEvent ToBufferedEvent(Extractor extractor)
            {
                var evt = new BufferedEvent
                {
                    EventId = Id,
                    EmittingNode = extractor.State.GetEmitterState(Emitter).Id,
                    Time = Time,
                    SourceNode = extractor.State.GetNodeId(SourceNode),
                    Message = Message,
                    ReceivedTime = DateTime.UtcNow,
                    MetaData = MetaData == null ? new Dictionary<string, object>()
                        : MetaData.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value)
                };
                if (!evt.MetaData.ContainsKey("Type"))
                {
                    evt.MetaData["Type"] = EventType;
                }

                return evt;
            }

        }
    }

}

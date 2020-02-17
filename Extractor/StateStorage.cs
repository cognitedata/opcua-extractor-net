﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using LiteDB;
using LiteQueue;

namespace Cognite.OpcUa
{
    public sealed class StateStorage : IDisposable
    {
        public const string StringDPQueue = "string_dp_queue";
        public const string DoubleDPQueue = "double_dp_queue";
        public const string VariableStates = "variable_states";
        public const string EmitterStates = "emitter_states";
        public const string EventQueue = "event_queue";

        public const int dataBatchSize = 100000;
        public const int eventBatchSize = 1000;

        private readonly Extractor extractor;
        private readonly LiteDatabase db;

        private readonly ILogger log = Log.Logger.ForContext<StateStorage>();

        private readonly LiteQueue<StringDataPointPoco> stringDataQueue;
        private readonly LiteQueue<DoubleDataPointPoco> doubleDataQueue;
        private readonly LiteQueue<EventPoco> eventQueue;
        public bool AnyPoints { get; set; }
        public bool AnyEvents { get; set; }
        public StateStorage(Extractor extractor, FullConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            this.extractor = extractor ?? throw new ArgumentNullException(nameof(extractor));

            string connection = $"Filename={config.StateStorage.Location};upgrade=true";

            db = new LiteDatabase(connection);

            if (config.FailureBuffer.Enabled && config.FailureBuffer.LocalQueue)
            {
                stringDataQueue = new LiteQueue<StringDataPointPoco>(db, StringDPQueue);
                stringDataQueue.ResetOrphans();
                doubleDataQueue = new LiteQueue<DoubleDataPointPoco>(db, DoubleDPQueue);
                doubleDataQueue.ResetOrphans();
                AnyPoints = stringDataQueue.Dequeue() != null || doubleDataQueue.Dequeue() != null;
                eventQueue = new LiteQueue<EventPoco>(db, EventQueue);
                eventQueue.ResetOrphans();
                AnyEvents = eventQueue.Dequeue() != null;
            }
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
        public async Task StoreExtractionState(IEnumerable<IExtractionState> states, string name, CancellationToken token)
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
                await Task.Run(() =>
                {
                    var col = db.GetCollection<ExtractionStatePoco>(name);
                    col.Upsert(pocos);
                });
                log.Debug("Saved {Stored} out of {TotalNumber} extraction states to store", 
                    toStore.Count, states.Count());
                foreach (var state in toStore)
                {
                    state.IsDirty = false;
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
        /// this is almost certainly better.
        /// </summary>
        /// <param name="states">States to read into</param>
        /// <param name="name">Name of collection to read</param>
        public async Task<bool> ReadExtractionStates(IEnumerable<IExtractionState> states, string name,
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
                            ret.Add(poco);
                        }

                        return ret;
                    });
                }
                else
                {
                    pocos = await Task.Run(() =>
                    {
                        var col = db.GetCollection<ExtractionStatePoco>(name);
                        return col.FindAll();
                    }, token);
                }
                log.Debug("Found {cnt} states in the state storage", pocos.Count());
                int count = 0;
                foreach (var poco in pocos)
                {
                    if (stateMap.ContainsKey(poco.Id))
                    {
                        count++;
                        stateMap[poco.Id].InitExtractedRange(poco.FirstTimestamp,
                            poco.LastTimestamp);
                        log.Information("Initialized {id} to ({start}, {end}), new: ({start}, {end})",
                            poco.Id,
                            poco.FirstTimestamp,
                            poco.LastTimestamp,
                            stateMap[poco.Id].SourceExtractedRange.Start.ToUniversalTime(),
                            stateMap[poco.Id].SourceExtractedRange.End.ToUniversalTime());
                    }
                }
                log.Information("Initialized extracted ranges from statestore for {cnt} nodes", count);
            }
            catch (LiteException e)
            {
                log.Warning(e, "Failed to restore extraction state: {Message}", e.Message);
                return false;
            }

            return true;
        }

        public async Task WritePointsToQueue(IEnumerable<BufferedDataPoint> points, CancellationToken token)
        {
            if (points == null) return;
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

            try
            {
                await Task.WhenAll(
                    Task.Run(() => { stringDataQueue.Enqueue(stringPoints); }, token),
                    Task.Run(() => { doubleDataQueue.Enqueue(doublePoints); }, token));
            }
            catch (LiteException e)
            {
                log.Error(e, "Failed to insert datapoints into litedb queue");
            }
        }
        public async Task<bool> ReadPointsFromQueue(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (pushers == null) return true;
            bool failed = false;
            await Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    var stringRecords = stringDataQueue.Dequeue(dataBatchSize);
                    var doubleRecords = doubleDataQueue.Dequeue(dataBatchSize);
                    var points = stringRecords.Select(record => record.Payload.ToDataPoint());
                    points = points.Concat(doubleRecords.Select(record => record.Payload.ToDataPoint())).ToList();
                    var results = await Task.WhenAll(pushers.Select(pusher => pusher.PushDataPoints(points, token)));
                    if (results.Any(res => res == false))
                    {
                        stringDataQueue.Abort(stringRecords);
                        doubleDataQueue.Abort(doubleRecords);
                        failed = true;
                        return;
                    }
                    stringDataQueue.Commit(stringRecords);
                    doubleDataQueue.Commit(doubleRecords);
                    if (stringRecords.Count < dataBatchSize && doubleRecords.Count < dataBatchSize) break;
                }
            }, token);
            if (!failed)
            {
                foreach (var pusher in pushers)
                {
                    pusher.DataFailing = false;
                }
            }
            return failed;
        }

        public async Task WriteEventsToQueue(IEnumerable<BufferedEvent> events, CancellationToken token)
        {
            if (events == null) return;
            var eventPocos = events.Select(evt => new EventPoco(evt, extractor)).ToList();

            try
            {
                await Task.Run(() => eventQueue.Enqueue(eventPocos), token);
            }
            catch (LiteException e)
            {
                log.Error(e, "Failed to insert datapoints into litedb queue");
            }
        }

        public async Task<bool> ReadEventsFromQueue(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (pushers == null) return true;
            bool failed = false;
            await Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    var records = eventQueue.Dequeue(eventBatchSize);
                    var events = records.Select(record => record.Payload.ToBufferedEvent(extractor));
                    var results = await Task.WhenAll(pushers.Select(pusher => pusher.PushEvents(events, token)));

                    if (results.Any(res => res == false))
                    {
                        eventQueue.Abort(records);
                        failed = true;
                        return;
                    }
                    eventQueue.Commit(records);
                    if (records.Count < eventBatchSize) break;
                }
            }, token);
            if (!failed)
            {
                foreach (var pusher in pushers)
                {
                    pusher.EventsFailing = false;
                }
            }
            return failed;
        }
        public void Dispose()
        {
            db?.Dispose();
        }
        private class ExtractionStatePoco
        {
            [BsonId]
            public string Id { get; set; }

            [BsonField("first")]
            public DateTime FirstTimestamp { get; set; }

            [BsonField("last")]
            public DateTime LastTimestamp { get; set; }
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

            public BufferedEvent ToBufferedEvent(Extractor extractor)
            {
                var evt = new BufferedEvent
                {
                    EventId = Id,
                    EmittingNode = extractor.GetEmitterState(Emitter).Id,
                    Time = Time,
                    SourceNode = extractor.GetNodeState(SourceNode).Id,
                    Message = Message,
                    ReceivedTime = DateTime.UtcNow,
                    MetaData = MetaData.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value)
                };
                if (!evt.MetaData.ContainsKey("EventType"))
                {
                    evt.MetaData["EventType"] = EventType;
                }

                return evt;
            }

        }
    }

}

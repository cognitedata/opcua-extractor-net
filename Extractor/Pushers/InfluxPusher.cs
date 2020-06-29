﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdysTech.InfluxDB.Client.Net;
using Cognite.Extractor.Common;
using Opc.Ua;
using Prometheus;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Pusher for InfluxDb. Currently supports only double-valued datapoints
    /// </summary>
    public sealed class InfluxPusher : IPusher
    {
        public UAExtractor Extractor { set; get; }
        public IPusherConfig BaseConfig { get; }
        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }


        private readonly InfluxPusherConfig config;
        private InfluxDBClient client;

        private static readonly Counter dataPointsCounter = Metrics
            .CreateCounter("opcua_datapoints_pushed_influx", "Number of datapoints pushed to influxdb");
        private static readonly Counter dataPointPushes = Metrics
            .CreateCounter("opcua_datapoint_pushes_influx", "Number of times datapoints have been pushed to influxdb");
        private static readonly Counter dataPointPushFailures = Metrics
            .CreateCounter("opcua_datapoint_push_failures_influx", "Number of completely failed pushes of datapoints to influxdb");
        private static readonly Counter skippedDatapoints = Metrics
            .CreateCounter("opcua_skipped_datapoints_influx", "Number of datapoints skipped by influxdb pusher");
        private static readonly Counter eventsCounter = Metrics
            .CreateCounter("opcua_events_pushed_influx", "Number of events pushed to influxdb");
        private static readonly Counter eventsPushes = Metrics
            .CreateCounter("opcua_event_pushes_influx", "Number of times events have been pushed to influxdb");
        private static readonly Counter eventPushFailures = Metrics
            .CreateCounter("opcua_event_push_failures_influx", "Number of completely failed pushes of events to influxdb");
        private static readonly Counter skippedEvents = Metrics
            .CreateCounter("opcua_skipped_events_influx", "Number of events skipped by influxdb pusher");

        private readonly ILogger log = Log.Logger.ForContext(typeof(InfluxPusher));

        public InfluxPusher(InfluxPusherConfig config)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            BaseConfig = config;
            client = new InfluxDBClient(config.Host, config.Username, config.Password);
        }
        /// <summary>
        /// Push each datapoint to influxdb. The datapoint Id, which corresponds to timeseries externalId in CDF, is used as MeasurementName
        /// </summary>
        public async Task<bool?> PushDataPoints(IEnumerable<BufferedDataPoint> points, CancellationToken token)
        {
            if (points == null) return null;
            var dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            foreach (var lBuffer in points)
            {
                var buffer = lBuffer;
                if (buffer.Timestamp <= DateTime.UnixEpoch)
                {
                    skippedDatapoints.Inc();
                    continue;
                }

                if (!buffer.IsString && !double.IsFinite(buffer.DoubleValue))
                {
                    if (config.NonFiniteReplacement != null)
                    {
                        buffer = new BufferedDataPoint(buffer, config.NonFiniteReplacement.Value);
                    }
                    else
                    {
                        skippedDatapoints.Inc();
                        continue;
                    }
                }
                count++;
                dataPointList.Add(buffer);
            }

            if (count == 0) return null;

            var groups = dataPointList.GroupBy(point => point.Id);

            var ipoints = new List<IInfluxDatapoint>();

            foreach (var group in groups)
            {
                var ts = Extractor.State.GetNodeState(group.Key);
                if (ts == null) continue;
                ipoints.AddRange(group.Select(dp => BufferedDPToInflux(ts, dp)));
            }

            if (config.Debug) return null;

            try
            {
                await client.PostPointsAsync(config.Database, ipoints, config.PointChunkSize);
                log.Debug("Successfully pushed {cnt} points to influxdb", count);
                dataPointsCounter.Inc(count);
            }
            catch (Exception e)
            {
                if (e is InfluxDBException iex)
                {
                    log.Debug("Failed to insert datapoints into influxdb: {line}, {reason}", 
                        iex.FailedLine, iex.Reason);
                }
                dataPointPushFailures.Inc();
                log.Error("Failed to insert {count} datapoints into influxdb: {msg}", count, e.Message);
                log.Debug(e, "Failed to insert datapoints into influxdb");
                return false;
            }
            dataPointPushes.Inc();
            return true;
        }
        /// <summary>
        /// Push events to influxdb. Events are stored such that each event type on a given node has its own measurement,
        /// on the form "events.[SourceNode uniqueId]:[Type uniqueId]"
        /// </summary>
        /// <param name="events">Events to push</param>
        /// <returns>True on success, null if none were pushed</returns>

        public async Task<bool?> PushEvents(IEnumerable<BufferedEvent> events, CancellationToken token)
        {
            if (events == null) return null;
            var evts = new List<BufferedEvent>();
            int count = 0;
            foreach (var evt in events)
            {
                if (evt.Time < DateTime.UnixEpoch)
                {
                    skippedEvents.Inc();
                    continue;
                }

                count++;
                evts.Add(evt);
            }

            if (count == 0) return null;

            var points = evts.Select(BufferedEventToInflux);
            if (config.Debug) return true;
            try
            {
                await client.PostPointsAsync(config.Database, points, config.PointChunkSize);
                eventsCounter.Inc(count);
                log.Debug("Successfully pushed {cnt} events to influxdb", count);
            }
            catch (Exception ex)
            {
                log.Warning("Failed to push {cnt} events to influxdb", count);
                log.Debug(ex, "Failed to push events to influxdb");
                eventPushFailures.Inc();
                return false;
            }
            eventsPushes.Inc();
            return true;
        }
        /// <summary>
        /// Reads the first and last datapoint from influx for each timeseries, sending the timestamps to each passed state
        /// </summary>
        /// <param name="states">List of historizing nodes</param>
        /// <param name="backfillEnabled">True if backfill is enabled, in which case the first timestamp will be read</param>
        /// <returns>True on success</returns>
        public async Task<bool> InitExtractedRanges(
            IEnumerable<NodeExtractionState> states,
            bool backfillEnabled,
            bool initMissing,
            CancellationToken token)
        {
            if (!states.Any() || config.Debug || !config.ReadExtractedRanges) return true;
            var ranges = new ConcurrentDictionary<string, TimeRange>();
            var getRangeTasks = states.Select(async state =>
            {
                var id = Extractor.GetUniqueId(state.SourceId,
                    state.ArrayDimensions != null && state.ArrayDimensions.Count > 0 && state.ArrayDimensions[0] > 0 ? 0 : -1);
                var last = await client.QueryMultiSeriesAsync(config.Database,
                    $"SELECT last(value) FROM \"{id}\"");

                if (last.Any() && last.First().HasEntries)
                {
                    DateTime ts = last.First().Entries[0].Time;
                    ranges[id] = new TimeRange(ts, ts);
                }

                if (backfillEnabled && last.Any() && last.First().HasEntries)
                {
                    var first = await client.QueryMultiSeriesAsync(config.Database,
                        $"SELECT first(value) FROM \"{id}\"");
                    if (first.Any() && first.First().HasEntries)
                    {
                        DateTime ts = first.First().Entries[0].Time;
                        ranges[id] = new TimeRange(ts, ranges[id].Last);
                    }
                }
                if (ranges.ContainsKey(id))
                {
                    state.InitExtractedRange(ranges[id].First, ranges[id].Last);
                }
                else if (initMissing)
                {
                    state.InitToEmpty();
                }
            });
            try
            {
                await Task.WhenAll(getRangeTasks);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to get timestamps from influxdb");
                return false;
            }
            return true;
        }
        /// <summary>
        /// Read range of events for the given emitter, and the given nodes, for the given list of series.
        /// The actual series to be read will be constructed from the series with any matching node in the given
        /// list of nodes.
        /// </summary>
        /// <param name="state">State to read range for</param>
        /// <param name="nodes">SourceNodes to use</param>
        /// <param name="backfillEnabled">True to also read start</param>
        /// <param name="seriesNames">List of all series to read for</param>
        private async Task InitExtractedEventRange(EventExtractionState state,
            IEnumerable<NodeId> nodes,
            bool backfillEnabled,
            IEnumerable<string> seriesNames,
            bool initMissing,
            CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            var mutex = new object();
            var bestRange = TimeRange.Empty;
            string emitterId = state.Id;

            var ids = seriesNames.Where(name => nodes.Any(node =>
                name.StartsWith("events." + Extractor.GetUniqueId(node), StringComparison.InvariantCulture)));

            var tasks = ids.Select(async id =>
            {
                var last = await client.QueryMultiSeriesAsync(config.Database,
                    $"SELECT last(value) FROM \"{id}\" WHERE Emitter='{emitterId}'");

                if (last.Any() && last.First().HasEntries)
                {
                    DateTime ts = last.First().Entries[0].Time;
                    lock (mutex)
                    {
                        bestRange = bestRange.Extend(null, ts);
                    }
                }

                if (backfillEnabled)
                {
                    if (!last.Any()) return;
                    var first = await client.QueryMultiSeriesAsync(config.Database,
                        $"SELECT first(value) FROM \"{id}\" WHERE Emitter='{emitterId}'");
                    if (first.Any() && first.First().HasEntries)
                    {
                        DateTime ts = first.First().Entries[0].Time;
                        lock (mutex)
                        {
                            bestRange = bestRange.Extend(ts, null);
                        }
                    }
                }
            });
            await Task.WhenAll(tasks);
            token.ThrowIfCancellationRequested();
            if (bestRange.Last == CogniteTime.DateTimeEpoch && backfillEnabled)
            {
                bestRange = new TimeRange(bestRange.First, DateTime.UtcNow);
            }

            if (bestRange.First == DateTime.MaxValue)
            {
                bestRange = new TimeRange(bestRange.Last, bestRange.Last);
            }

            if (initMissing && bestRange == TimeRange.Empty)
            {
                state.InitToEmpty();
            }
            else
            {
                state.InitExtractedRange(bestRange.First, bestRange.Last);
            }
        }
        /// <summary>
        /// Reads the first and last datapoint from influx for each emitter, sending the timestamps to each passed state
        /// </summary>
        /// <param name="states">List of historizing emitters</param>
        /// <param name="nodes">Relevant nodes to consider events for (sourcenodes)</param>
        /// <param name="backfillEnabled">True if backfill is enabled, in which case the first timestamp will be read</param>
        /// <returns>True on success</returns>
        public async Task<bool> InitExtractedEventRanges(IEnumerable<EventExtractionState> states,
            IEnumerable<NodeId> nodes,
            bool backfillEnabled,
            bool initMissing,
            CancellationToken token)
        {
            if (!states.Any() || config.Debug || !config.ReadExtractedRanges) return true;
            IEnumerable<string> eventSeries;
            try
            {
                var measurements = await client.QueryMultiSeriesAsync(config.Database,
                    "SHOW MEASUREMENTS");
                eventSeries = measurements.SelectMany(series => series.Entries.Select(entry => entry.Name as string));
                eventSeries = eventSeries.Where(series => series.StartsWith("events.", StringComparison.InvariantCulture));

            }
            catch (Exception e)
            {
                log.Error(e, "Failed to list measurements in influxdb");
                return false;
            }

            var getRangeTasks = states.Select(state => InitExtractedEventRange(state, nodes, backfillEnabled, eventSeries, initMissing, token));
            try
            {
                await Task.WhenAll(getRangeTasks);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to get timestamps from influxdb");
                return false;
            }
            return true;
        }
        /// <summary>
        /// Test if the database is available
        /// </summary>
        /// <returns>True on success</returns>
        public async Task<bool?> TestConnection(FullConfig fullConfig, CancellationToken token)
        {
            if (config.Debug) return true;
            IEnumerable<string> dbs;
            try
            {
                dbs = await client.GetInfluxDBNamesAsync();
            }
            catch (Exception ex)
            {
                log.Error("Failed to get db names from influx server: {host}, this is most likely due to a faulty connection or" +
                          " wrong credentials");
                log.Debug(ex, "Failed to get db names from influx server: {host}", config.Host);
                return false;
            }
            if (dbs == null || !dbs.Contains(config.Database))
            {
                log.Warning("Database {db} does not exist in influxDb: {host}, attempting to create", config.Database, config.Host);
                try
                {
                    if (await client.CreateDatabaseAsync(config.Database)) return true;
                }
                catch (Exception ex)
                {
                    log.Error(ex, "Failed to create database {db} in influxdb: {message}", config.Database, ex.Message);
                    return false;
                }
                log.Error("Database not successfully created");
                return false;
            }
            return true;
        }

        private static IInfluxDatapoint BufferedDPToInflux(NodeExtractionState state, BufferedDataPoint dp)
        {

            if (state.DataType.IsString)
            {
                var idp = new InfluxDatapoint<string>
                {
                    UtcTimestamp = dp.Timestamp,
                    MeasurementName = dp.Id
                };
                idp.Fields.Add("value", dp.StringValue ?? "");
                return idp;
            }
            if (state.DataType.Identifier == DataTypes.Boolean)
            {
                var idp = new InfluxDatapoint<bool>
                {
                    UtcTimestamp = dp.Timestamp,
                    MeasurementName = dp.Id
                };
                idp.Fields.Add("value", Math.Abs(dp.DoubleValue) < 0.1);
                return idp;
            }
            if (state.DataType.Identifier < DataTypes.Float
                     || state.DataType.Identifier == DataTypes.Integer
                     || state.DataType.Identifier == DataTypes.UInteger)
            {
                var idp = new InfluxDatapoint<long>
                {
                    UtcTimestamp = dp.Timestamp,
                    MeasurementName = dp.Id
                };
                idp.Fields.Add("value", (long)dp.DoubleValue);
                return idp;
            }
            else
            {
                var idp = new InfluxDatapoint<double>
                {
                    UtcTimestamp = dp.Timestamp,
                    MeasurementName = dp.Id
                };
                idp.Fields.Add("value", dp.DoubleValue);
                return idp;
            }
        }

        private string SourceNodeToString(NodeId sourceNode)
        {
            if (sourceNode == null || sourceNode.IsNullNodeId)
            {
                return "none";
            }
            var state = Extractor.State.GetActiveNode(sourceNode);
            if (state == null) return "none";
            return Extractor.GetUniqueId(sourceNode);
        }

        private IInfluxDatapoint BufferedEventToInflux(BufferedEvent evt)
        {
            var idp = new InfluxDatapoint<string>
            {
                UtcTimestamp = evt.Time,
                MeasurementName = "events." + SourceNodeToString(evt.SourceNode) + ":"
                                  + (evt.MetaData.ContainsKey("Type") ? evt.MetaData["Type"] : Extractor.GetUniqueId(evt.EventType))
            };
            idp.Fields.Add("value", evt.Message);
            idp.Fields.Add("id", evt.EventId);
            idp.Tags["Emitter"] = Extractor.GetUniqueId(evt.EmittingNode);
            foreach (var kvp in evt.MetaData)
            {
                if (kvp.Key == "SourceNode" || kvp.Key == "Type") continue;
                var str = Extractor.ConvertToString(kvp.Value);
                idp.Tags[kvp.Key] = string.IsNullOrEmpty(str) ? "null" : str;
            }

            return idp;
        }
        /// <summary>
        /// Read datapoints from influxdb for the given list of influxBufferStates
        /// </summary>
        /// <param name="states">InfluxBufferStates to read from</param>
        /// <returns>List of datapoints</returns>
        public async Task<IEnumerable<BufferedDataPoint>> ReadDataPoints(
            IDictionary<string, InfluxBufferState> states,
            CancellationToken token)
        {
            if (config.Debug) return Array.Empty<BufferedDataPoint>();
            token.ThrowIfCancellationRequested();
            if (states == null) throw new ArgumentNullException(nameof(states));

            var fetchTasks = states.Select(state => client.QueryMultiSeriesAsync(config.Database,
                    $"SELECT * FROM \"{state.Key}\""
                    + GetWhereClause(state.Value))
            ).ToList();

            var results = await Task.WhenAll(fetchTasks);
            token.ThrowIfCancellationRequested();

            var finalPoints = new List<BufferedDataPoint>();

            foreach (var series in results)
            {
                if (!series.Any()) continue;
                var current = series.First();
                string id = current.SeriesName;
                if (!states.ContainsKey(id)) continue;
                bool isString = states[id].Type == InfluxBufferType.StringType;
                finalPoints.AddRange(current.Entries.Select(dp =>
                {
                    if (isString)
                    {
                        return new BufferedDataPoint(dp.Time, id, (string) dp.Value);
                    }

                    double convVal;
                    if (dp.Value == "true" || dp.Value == "false")
                    {
                        convVal = dp.Value == "true" ? 1 : 0;
                    }
                    else
                    {
                        convVal = Convert.ToDouble(dp.Value);
                    }

                    return new BufferedDataPoint(dp.Time, id, convVal);
                }));
            }

            return finalPoints;
        }
        private static string GetWhereClause(InfluxBufferState state)
        {
            if (state.DestinationExtractedRange == TimeRange.Complete) return "";
            string ret = " WHERE";
            bool first = false;
            if (state.DestinationExtractedRange.First > CogniteTime.DateTimeEpoch)
            {
                first = true;
                ret += $" time >= {(state.DestinationExtractedRange.First - CogniteTime.DateTimeEpoch).Ticks * 100}";
            }
            if (state.DestinationExtractedRange.Last < DateTime.MaxValue)
            {
                if (first) ret += " AND";
                ret += $" time <= {(state.DestinationExtractedRange.Last - CogniteTime.DateTimeEpoch).Ticks * 100}";
            }

            return ret;
        }


        /// <summary>
        /// Read events from influxdb back into BufferedEvents
        /// </summary>
        /// <param name="startTime">Time to read from, reading forwards</param>
        /// <param name="measurements">Nodes to read events from</param>
        /// <returns>A list of read events</returns>
        public async Task<IEnumerable<BufferedEvent>> ReadEvents(
            IDictionary<string, InfluxBufferState> states,
            CancellationToken token)
        {
            if (config.Debug) return Array.Empty<BufferedEvent>();
            token.ThrowIfCancellationRequested();

            var fetchTasks = states.Select(state => client.QueryMultiSeriesAsync(config.Database,
                $"SELECT * FROM /events.{state.Key.Replace("/", "\\/", StringComparison.InvariantCulture)}:.*/"
                + GetWhereClause(state.Value))
            ).ToList();

            var results = await Task.WhenAll(fetchTasks);
            token.ThrowIfCancellationRequested();
            var finalEvents = new List<BufferedEvent>();

            foreach (var series in results.SelectMany(res => res).DistinctBy(series => series.SeriesName))
            {
                if (!series.Entries.Any()) continue;

                var name = series.SeriesName.Substring(7);

                NodeId sourceNode = null;
                string baseKey = null;
                if (name.StartsWith("none", StringComparison.InvariantCulture))
                {
                    baseKey = "none";
                }
                else
                {
                    baseKey = Extractor.State.AllActiveExternalIds.FirstOrDefault(key =>
                    name.StartsWith(key, StringComparison.InvariantCulture));

                    if (baseKey == null) continue;

                    sourceNode = Extractor.State.GetNodeId(baseKey);
                    if (sourceNode == null) continue;
                }

                
                finalEvents.AddRange(series.Entries.Select(res =>
                {
                    // The client uses ExpandoObject as dynamic, which implements IDictionary
                    if (!(res is IDictionary<string, object> values)) return null;
                    var emitter = Extractor.State.GetEmitterState((string)values["Emitter"]);
                    if (emitter == null) return null;
                    var evt = new BufferedEvent
                    {
                        Time = (DateTime)values["Time"],
                        EventId = (string)values["Id"],
                        Message = (string)values["Value"],
                        EmittingNode = emitter.Id,
                        SourceNode = sourceNode,
                        MetaData = new Dictionary<string, object>()
                    };
                    evt.MetaData["Type"] = name.Substring(baseKey.Length + 1);
                    foreach (var kvp in values)
                    {
                        if (kvp.Key == "Time" || kvp.Key == "Id" || kvp.Key == "Value"
                            || kvp.Key == "Emitter" || kvp.Key == "Type" || string.IsNullOrEmpty(kvp.Value as string)) continue;
                        evt.MetaData.Add(kvp.Key, kvp.Value);
                    }
                    log.Verbose(evt.ToDebugDescription());

                    return evt;
                }).Where(evt => evt != null));
            }

            return finalEvents;
        }
        /// <summary>
        /// Recreate the influxdbclient with new configuration.
        /// </summary>
        public void Reconfigure()
        {
            log.Information("Reconfiguring influxPusher with: {host}", config.Host);
            client = new InfluxDBClient(config.Host, config.Username, config.Password);
        }

        public void Reset()
        {
        }

        public void Dispose()
        {
            client?.Dispose();
        }
    }
}

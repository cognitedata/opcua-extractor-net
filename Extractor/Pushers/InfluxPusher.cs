﻿/* Cognite Extractor for OPC-UA
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

using AdysTech.InfluxDB.Client.Net;
using Cognite.Extractor.Common;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Prometheus;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Pusher for InfluxDb, supports datapoints and events.
    /// </summary>
    public sealed class InfluxPusher : IPusher
    {
        public UAExtractor Extractor { set; get; }
        public IPusherConfig BaseConfig { get; }
        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }
        public List<UANode> PendingNodes { get; } = new List<UANode>();
        public List<UAReference> PendingReferences { get; } = new List<UAReference>();

        private readonly DateTime minTs = DateTime.Parse("1677-09-21T00:12:43.145224194Z");
        private readonly DateTime maxTs = DateTime.Parse("2262-04-11T23:47:16.854775806Z");


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
        public async Task<bool?> PushDataPoints(IEnumerable<UADataPoint> points, CancellationToken token)
        {
            if (points == null) return null;
            var dataPointList = new List<UADataPoint>();

            int count = 0;
            foreach (var lBuffer in points)
            {
                var buffer = lBuffer;
                if (buffer.Timestamp < minTs || buffer.Timestamp > maxTs)
                {
                    skippedDatapoints.Inc();
                    continue;
                }

                if (!buffer.IsString && !double.IsFinite(buffer.DoubleValue.Value))
                {
                    if (config.NonFiniteReplacement != null)
                    {
                        buffer = new UADataPoint(buffer, config.NonFiniteReplacement.Value);
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
                ipoints.AddRange(group.Select(dp => UADataPointToInflux(ts, dp)));
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
                dataPointPushFailures.Inc();
                if (e is InfluxDBException iex)
                {
                    log.Error("Failed to insert datapoints into influxdb: {line}, {reason}. Message: {msg}",
                        iex.FailedLine, iex.Reason, iex.Message);
                    if (iex.Reason.StartsWith("partial write"))
                    {
                        dataPointPushes.Inc();
                        int droppedIdx = iex.Message.LastIndexOf("dropped=");
                        if (droppedIdx != -1)
                        {
                            string droppedRaw = iex.Message.Substring(droppedIdx + 8).Trim();
                            if (int.TryParse(droppedRaw, out int num) && num > 0)
                            {
                                skippedDatapoints.Inc(num);
                            }
                        }
                        return true;
                    }
                    return false;
                }
                log.Error("Failed to insert {count} datapoints into influxdb: {msg}", count, e.Message);
                return false;
            }
            dataPointPushes.Inc();
            return true;
        }
        /// <summary>
        /// Push events to influxdb. Events are stored such that each event type on a given node has its own measurement,
        /// on the form "events.[Emitter uniqueId]:[Type uniqueId]"
        /// </summary>
        /// <param name="events">Events to push</param>
        /// <returns>True on success, null if none were pushed, false on failure</returns>
        public async Task<bool?> PushEvents(IEnumerable<UAEvent> events, CancellationToken token)
        {
            if (events == null) return null;
            var evts = new List<UAEvent>();
            int count = 0;
            foreach (var evt in events)
            {
                if (evt.Time < minTs || evt.Time > maxTs)
                {
                    skippedEvents.Inc();
                    continue;
                }

                count++;
                evts.Add(evt);
            }

            if (count == 0) return null;

            var points = evts.Select(UAEventToInflux);
            if (config.Debug) return null;
            try
            {
                await client.PostPointsAsync(config.Database, points, config.PointChunkSize);
                eventsCounter.Inc(count);
                log.Debug("Successfully pushed {cnt} events to influxdb", count);
            }
            catch (Exception ex)
            {
                log.Warning(ex, "Failed to push {cnt} events to influxdb: {msg}", count, ex.Message);
                eventPushFailures.Inc();
                return false;
            }
            eventsPushes.Inc();
            return true;
        }
        /// <summary>
        /// Reads the first and optionally last datapoint from influx for each timeseries, sending the timestamps to each passed state
        /// </summary>
        /// <param name="states">List of historizing nodes</param>
        /// <param name="backfillEnabled">True if backfill is enabled, in which case the first timestamp will be read</param>
        /// <returns>True on success</returns>
        public async Task<bool> InitExtractedRanges(
            IEnumerable<VariableExtractionState> states,
            bool backfillEnabled,
            CancellationToken token)
        {
            if (!states.Any() || config.Debug || !config.ReadExtractedRanges) return true;
            var ranges = new ConcurrentDictionary<string, TimeRange>();

            var ids = new List<string>();
            foreach (var state in states)
            {
                if (state.IsArray)
                {
                    for (int i = 0; i < state.ArrayDimensions[0]; i++)
                    {
                        ids.Add(Extractor.GetUniqueId(state.SourceId, i));
                    }
                }
                else
                {
                    ids.Add(state.Id);
                }
            }

            var getRangeTasks = ids.Select(async id =>
            {
                ranges[id] = TimeRange.Empty;

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

            foreach (var state in states)
            {
                if (state.IsArray)
                {
                    for (int i = 0; i < state.ArrayDimensions[0]; i++)
                    {
                        if (ranges.TryGetValue(Extractor.GetUniqueId(state.SourceId, i), out var range))
                        {
                            if (range == TimeRange.Empty)
                            {
                                state.InitToEmpty();
                            }
                            else
                            {
                                state.InitExtractedRange(range.First, range.Last);
                            }
                        }
                    }
                }
                else
                {
                    if (ranges.TryGetValue(state.Id, out var range))
                    {
                        if (range == TimeRange.Empty)
                        {
                            state.InitToEmpty();
                        }
                        else
                        {
                            state.InitExtractedRange(range.First, range.Last);
                        }
                    }
                }
            }

            return true;
        }
        /// <summary>
        /// Read range of events for the given emitter in the given list of series.
        /// Since each emitter can create an arbitrary number of series (one per type),
        /// we must retrieve the series names first.
        /// </summary>
        /// <param name="state">State to read range for</param>
        /// <param name="backfillEnabled">True to also read first timestamp</param>
        /// <param name="seriesNames">List of all series to read for</param>
        private async Task InitExtractedEventRange(EventExtractionState state,
            bool backfillEnabled,
            IEnumerable<string> seriesNames,
            CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            var mutex = new object();
            var bestRange = TimeRange.Empty;

            var ids = seriesNames
                .Where(name => name.StartsWith("events." + state.Id + ":", StringComparison.InvariantCulture))
                .Distinct().ToList();

            var tasks = ids.Select(async id =>
            {
                var last = await client.QueryMultiSeriesAsync(config.Database,
                    $"SELECT last(value) FROM \"{id}\"");

                if (last.Any() && last.First().HasEntries)
                {
                    DateTime ts = last.First().Entries[0].Time;
                    lock (mutex)
                    {
                        bestRange = bestRange.Extend(ts, ts);
                    }
                }

                if (backfillEnabled)
                {
                    if (!last.Any()) return;
                    var first = await client.QueryMultiSeriesAsync(config.Database,
                        $"SELECT first(value) FROM \"{id}\"");
                    if (first.Any() && first.First().HasEntries)
                    {
                        DateTime ts = first.First().Entries[0].Time;
                        lock (mutex)
                        {
                            bestRange = bestRange.Extend(ts, ts);
                        }
                    }
                }
            });
            await Task.WhenAll(tasks);
            token.ThrowIfCancellationRequested();

            if (bestRange == TimeRange.Empty)
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
        /// <param name="backfillEnabled">True if backfill is enabled, in which case the first timestamp will be read</param>
        /// <returns>True on success</returns>
        public async Task<bool> InitExtractedEventRanges(IEnumerable<EventExtractionState> states,
            bool backfillEnabled,
            CancellationToken token)
        {
            if (!states.Any() || config.Debug || !config.ReadExtractedEventRanges) return true;
            IEnumerable<string> eventSeries;
            try
            {
                var measurements = await client.QueryMultiSeriesAsync(config.Database, "SHOW MEASUREMENTS");
                eventSeries = measurements.SelectMany(series => series.Entries.Select(entry => entry.Name as string));
                eventSeries = eventSeries.Where(series => series.StartsWith("events.", StringComparison.InvariantCulture));

            }
            catch (Exception e)
            {
                log.Error("Failed to list measurements in influxdb: {msg}", e.Message);
                return false;
            }

            log.Information("Initializing extracted event ranges for {cnt} emitters", states.Count());

            var getRangeTasks = states.Select(state => InitExtractedEventRange(state, backfillEnabled, eventSeries, token));
            try
            {
                await Task.WhenAll(getRangeTasks);
            }
            catch (Exception e)
            {
                log.Error("Failed to get timestamps from influxdb: {msg}", e.Message);
                return false;
            }
            foreach (var state in states)
            {
                log.Information("State: {id} initialized to {start}, {end}", state.Id, state.DestinationExtractedRange.First, state.DestinationExtractedRange.Last);
            }
            return true;
        }
        /// <summary>
        /// Test if the database is available, create it if it does not exist.
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
                          " wrong credentials: {msg}", config.Host, ex.Message);
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
                    log.Error("Failed to create database {db} in influxdb: {message}", config.Database, ex.Message);
                    return false;
                }
                log.Error("Database not successfully created");
                return false;
            }
            return true;
        }
        /// <summary>
        /// True if the passed datatype should be treated as integer.
        /// </summary>
        /// <param name="dataType"></param>
        /// <returns></returns>
        private static bool IsInteger(uint dataType)
        {
            return dataType >= DataTypes.SByte && dataType <= DataTypes.UInt64
                     || dataType == DataTypes.Integer
                     || dataType == DataTypes.UInteger;
        }
        /// <summary>
        /// Create an influx datapoint from the given <see cref="UADataPoint"/> for the given
        /// <see cref="VariableExtractionState"/>.
        /// </summary>
        /// <param name="state">State the datapoint belongs to</param>
        /// <param name="dp">Datapoint to convert</param>
        /// <returns>Converted datapoint</returns>
        private static IInfluxDatapoint UADataPointToInflux(VariableExtractionState state, UADataPoint dp)
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
                idp.Fields.Add("value", Math.Abs(dp.DoubleValue.Value) < 0.1);
                return idp;
            }
            if (state.DataType.IsStep || IsInteger(state.DataType.Identifier))
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
                idp.Fields.Add("value", dp.DoubleValue.Value);
                return idp;
            }
        }
        /// <summary>
        /// Convert the given <see cref="UAEvent"/> to an influx datapoint.
        /// </summary>
        /// <param name="evt">Event to convert</param>
        /// <returns>Converted event</returns>
        private IInfluxDatapoint UAEventToInflux(UAEvent evt)
        {
            var idp = new InfluxDatapoint<string>
            {
                UtcTimestamp = evt.Time
            };
            string name = "events." + Extractor.GetUniqueId(evt.EmittingNode) + ":";
            if (evt.MetaData != null && evt.MetaData.TryGetValue("Type", out var rawType))
            {
                name += rawType;
            }
            else
            {
                name += Extractor.GetUniqueId(evt.EventType);
            }
            idp.MeasurementName = name;

            idp.Fields.Add("value", evt.Message ?? "");
            idp.Fields.Add("id", evt.EventId ?? "");
            string sourceNode;
            if (evt.MetaData != null && evt.MetaData.TryGetValue("SourceNode", out var rawSourceNode))
            {
                sourceNode = Extractor.ConvertToString(rawSourceNode);
            }
            else
            {
                sourceNode = Extractor.GetUniqueId(evt.SourceNode);
            }
            idp.Tags.Add("source", sourceNode ?? "null");
            if (evt.MetaData != null)
            {
                foreach (var kvp in evt.MetaData)
                {
                    if (kvp.Key == "Emitter" || kvp.Key == "Type" || kvp.Key == "SourceNode") continue;
                    var str = Extractor.ConvertToString(kvp.Value);
                    idp.Tags[kvp.Key] = string.IsNullOrEmpty(str) ? "null" : str;
                }
            }
            return idp;
        }
        /// <summary>
        /// Read datapoints from influxdb for the given list of influxBufferStates
        /// </summary>
        /// <param name="states">InfluxBufferStates to read from</param>
        /// <returns>List of datapoints</returns>
        public async Task<IEnumerable<UADataPoint>> ReadDataPoints(
            IDictionary<string, InfluxBufferState> states,
            CancellationToken token)
        {
            if (config.Debug) return Array.Empty<UADataPoint>();
            token.ThrowIfCancellationRequested();
            if (states == null) throw new ArgumentNullException(nameof(states));

            var fetchTasks = states.Select(state => client.QueryMultiSeriesAsync(config.Database,
                    $"SELECT * FROM \"{state.Key}\""
                    + GetWhereClause(state.Value))
            ).ToList();

            var results = await Task.WhenAll(fetchTasks);
            token.ThrowIfCancellationRequested();

            var finalPoints = new List<UADataPoint>();

            foreach (var series in results)
            {
                if (!series.Any()) continue;
                var current = series.First();
                string id = current.SeriesName;
                if (!states.TryGetValue(id, out var state)) continue;
                bool isString = state.Type == InfluxBufferType.StringType;
                finalPoints.AddRange(current.Entries.Select(dp =>
                {
                    if (isString)
                    {
                        return new UADataPoint(dp.Time, id, (string)dp.Value);
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

                    return new UADataPoint(dp.Time, id, convVal);
                }));
            }

            return finalPoints;
        }
        /// <summary>
        /// Build where clause for queries for reading datapoints.
        /// </summary>
        /// <param name="state">State to read from</param>
        /// <returns>String where clause that can be appended to query.</returns>
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

        private readonly HashSet<string> excludeEventTags = new HashSet<string>
        {
            "id", "value", "source", "time", "type"
        };

        /// <summary>
        /// Read events from influxdb back into <see cref="UAEvent"/>s
        /// </summary>
        /// <param name="states">Nodes to read events from</param>
        /// <returns>A list of read events</returns>
        public async Task<IEnumerable<UAEvent>> ReadEvents(
            IDictionary<string, InfluxBufferState> states,
            CancellationToken token)
        {
            if (config.Debug || states == null) return Array.Empty<UAEvent>();
            token.ThrowIfCancellationRequested();

            var fetchTasks = states.Select(state => client.QueryMultiSeriesAsync(config.Database,
                $"SELECT * FROM /events.{state.Key.Replace("/", "\\/", StringComparison.InvariantCulture)}:.*/"
                + GetWhereClause(state.Value))
            ).ToList();

            var results = await Task.WhenAll(fetchTasks);
            token.ThrowIfCancellationRequested();
            var finalEvents = new List<UAEvent>();

            foreach (var series in results.SelectMany(res => res).DistinctBy(series => series.SeriesName))
            {
                if (!series.Entries.Any()) continue;

                var name = series.SeriesName.Substring(7);

                var state = states.Values.FirstOrDefault(state => name.StartsWith(state.Id, StringComparison.InvariantCulture));
                if (state == null) continue;


                finalEvents.AddRange(series.Entries.Select(res =>
                {
                    // The client uses ExpandoObject as dynamic, which implements IDictionary
                    if (!(res is IDictionary<string, object> values)) return null;
                    var sourceNode = Extractor.State.GetNodeId((string)values["Source"]);
                    var type = Extractor.State.GetNodeId(name.Substring(state.Id.Length + 1));
                    var evt = new UAEvent
                    {
                        Time = (DateTime)values["Time"],
                        EventId = (string)values["Id"],
                        Message = (string)values["Value"],
                        EmittingNode = state.SourceId,
                        SourceNode = sourceNode,
                        EventType = type,
                        MetaData = new Dictionary<string, object>()
                    };
                    foreach (var kvp in values)
                    {
                        if (string.IsNullOrEmpty(kvp.Value as string) || excludeEventTags.Contains(kvp.Key.ToLower())) continue;
                        evt.MetaData.Add(kvp.Key, kvp.Value);
                    }
                    log.Verbose(evt.ToString());

                    return evt;
                }).Where(evt => evt != null));
            }

            return finalEvents;
        }
        /// <summary>
        /// Recreate the influxdbclient with new configuration. Used for testing.
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

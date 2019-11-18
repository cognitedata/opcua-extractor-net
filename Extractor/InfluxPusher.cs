using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdysTech.InfluxDB.Client.Net;
using Opc.Ua;
using Prometheus.Client;
using Serilog;
using Exception = System.Exception;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Pusher for InfluxDb. Currently supports only double-valued datapoints
    /// </summary>
    public class InfluxPusher : IPusher
    {
        public Extractor Extractor { set; private get; }
        public int Index { get; set; }
        public PusherConfig BaseConfig { get; }
        public bool Failing;

        public ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; } = new ConcurrentQueue<BufferedDataPoint>();
        public ConcurrentQueue<BufferedEvent> BufferedEventQueue { get; } = new ConcurrentQueue<BufferedEvent>();
        private readonly InfluxClientConfig config;
        private readonly Dictionary<string, DateTime> latestTimestamp = new Dictionary<string, DateTime>();
        private InfluxDBClient client;

        private static readonly Counter numInfluxPusher = Metrics
            .CreateCounter("opcua_influx_pusher_count", "Number of active influxdb pushers");
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

        public InfluxPusher(InfluxClientConfig config)
        {
            this.config = config;
            BaseConfig = config;
            client = new InfluxDBClient(config.Host, config.Username, config.Password);
            numInfluxPusher.Inc();
        }
        /// <summary>
        /// Push each datapoint to influxdb. The datapoint Id, which corresponds to timeseries externalId in CDF, is used as MeasurementName
        /// </summary>
        public async Task<IEnumerable<BufferedDataPoint>> PushDataPoints(CancellationToken token)
        {
            var dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            while (BufferedDPQueue.TryDequeue(out BufferedDataPoint buffer))
            {
                if (buffer.Timestamp <= DateTime.UnixEpoch)
                {
                    skippedDatapoints.Inc();
                    continue;
                }

                if (buffer.Timestamp < latestTimestamp.GetValueOrDefault(buffer.Id)) continue;

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

            if (count == 0)
            {
                Log.Verbose("Push 0 datapoints to influxdb");
                return null;
            }
            var groups = dataPointList.GroupBy(point => point.Id);

            var points = new List<IInfluxDatapoint>();

            foreach (var group in groups)
            {
                var ts = Extractor.GetNodeState(group.Key);
                if (ts == null) continue;
                dataPointsCounter.Inc(group.Count());
                points.AddRange(group.Select(dp => BufferedDPToInflux(ts, dp)));
            }
            Log.Debug("Push {cnt} datapoints to influxdb {db}", points.Count, config.Database);
            try
            {
                await client.PostPointsAsync(config.Database, points, config.PointChunkSize);
            }
            catch (Exception e)
            {
                dataPointPushFailures.Inc();
                Log.Error(e, "Failed to insert datapoints into influxdb");
                return dataPointList;
            }
            dataPointPushes.Inc();
            return Array.Empty<BufferedDataPoint>();
        }

        public async Task PushEvents(CancellationToken token)
        {
            var evts = new List<BufferedEvent>();
            int count = 0;
            while (BufferedEventQueue.TryDequeue(out BufferedEvent evt))
            {
                if (evt.Time < DateTime.UnixEpoch)
                {
                    skippedEvents.Inc();
                    continue;
                }

                count++;
                evts.Add(evt);
            }

            if (count == 0)
            {
                Log.Verbose("Push 0 events to influxdb");
                return;
            }

            Log.Debug("Push {cnt} events to influxdb", count);
            var points = evts.Select(BufferedEventToInflux);
            try
            {
                await client.PostPointsAsync(config.Database, points, config.PointChunkSize);
                eventsCounter.Inc(count);
            }
            catch (Exception)
            {
                eventPushFailures.Inc();
                throw;
            }
            eventsPushes.Inc();

        }
        /// <summary>
        /// Reads the last datapoint from influx for each timeseries, sending the timestamp to each passed state
        /// </summary>
        /// <param name="states">List of historizing nodes</param>
        /// <returns>True on success</returns>
        public async Task<bool> InitLatestTimestamps(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            var getLastTasks = states.Select(async state =>
            {
                var id = Extractor.GetUniqueId(state.Id,
                    state.ArrayDimensions != null && state.ArrayDimensions.Length > 0 && state.ArrayDimensions[0] > 0 ? 0 : -1);
                var values = await client.QueryMultiSeriesAsync(config.Database,
                    $"SELECT last(value) FROM \"{id}\"");
                if (values.Any() && values.First().HasEntries)
                {
                    DateTime timestamp = values.First().Entries[0].Time;
                    state.InitTimestamp(timestamp);
                    latestTimestamp[id] = timestamp;
                }
                else
                {
                    state.InitTimestamp(DateTime.UnixEpoch);
                    latestTimestamp[id] = DateTime.UnixEpoch;
                }
            });
            try
            {
                await Task.WhenAll(getLastTasks);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to get timestamps from influxdb");
                return false;
            }
            return true;
        }

        public async Task<bool> TestConnection(CancellationToken token)
        {
            IEnumerable<string> dbs;
            try
            {
                dbs = await client.GetInfluxDBNamesAsync();
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to get db names from influx server: {host}", config.Host);
                return false;
            }
            if (dbs == null || !dbs.Contains(config.Database))
            {
                Log.Error("Database {db} does not exist in influxDb: {host}", config.Database, config.Host);
                return false;
            }
            return true;
        }

        private IInfluxDatapoint BufferedDPToInflux(NodeExtractionState state, BufferedDataPoint dp)
        {
            if (state.DataType.IsString)
            {
                var idp = new InfluxDatapoint<string>
                {
                    UtcTimestamp = dp.Timestamp,
                    MeasurementName = dp.Id
                };
                idp.Fields.Add("value", dp.StringValue);
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

        private IInfluxDatapoint BufferedEventToInflux(BufferedEvent evt)
        {
            var idp = new InfluxDatapoint<string>
            {
                UtcTimestamp = evt.Time,
                MeasurementName = "events." + Extractor.GetUniqueId(evt.SourceNode)
            };
            idp.Fields.Add("Value", evt.Message);
            idp.Fields.Add("Id", evt.EventId);
            idp.Tags["Type"] = Extractor.GetUniqueId(evt.EventType);
            foreach (var kvp in evt.MetaData)
            {
                idp.Tags[kvp.Key] = Extractor.ConvertToString(kvp.Value);
            }

            return idp;
        }

        public async Task<IEnumerable<BufferedDataPoint>> ReadDataPoints(DateTime startTime,
            IDictionary<string, bool> measurements,
            CancellationToken token)
        {
            var timestamp = startTime - DateTime.UnixEpoch;
            var fetchTasks = measurements.Keys.Select(measurement =>
                client.QueryMultiSeriesAsync(config.Database, 
                    $"SELECT * FROM \"{measurement}\" WHERE time >= {timestamp.Ticks}")).ToList();

            var results = await Task.WhenAll(fetchTasks);
            var finalPoints = new List<BufferedDataPoint>();
            foreach (var series in results)
            {
                if (!series.Any()) continue;
                var current = series.First();
                string id = current.SeriesName;
                if (!measurements.ContainsKey(id)) continue;
                bool isString = measurements[id];
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

        public void Reconfigure()
        {
            client = new InfluxDBClient(config.Host, config.Username, config.Password);
        }
    }
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdysTech.InfluxDB.Client.Net;
using Opc.Ua;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Pusher for InfluxDb. Currently supports only double-valued datapoints
    /// </summary>
    public class InfluxPusher : IPusher
    {
        public Extractor Extractor { set; private get; }
        public UAClient UAClient { set; private get; }
        public PusherConfig BaseConfig { get; }
        public bool Failing;

        public ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; } = new ConcurrentQueue<BufferedDataPoint>();
        public ConcurrentQueue<BufferedEvent> BufferedEventQueue { get; } = new ConcurrentQueue<BufferedEvent>();
        private readonly InfluxClientConfig config;
        private readonly InfluxDBClient client;
        public InfluxPusher(InfluxClientConfig config)
        {
            this.config = config;
            BaseConfig = config;
            client = new InfluxDBClient(config.Host, config.Username, config.Password);
        }
        /// <summary>
        /// Push each datapoint to influxdb. The datapoint Id, which corresponds to timeseries externalId in CDF, is used as MeasurementName
        /// </summary>
        public async Task PushDataPoints(CancellationToken token)
        {
            var dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            while (BufferedDPQueue.TryDequeue(out BufferedDataPoint buffer))
            {
                if (buffer.Timestamp <= DateTime.MinValue) continue;
                count++;
                dataPointList.Add(buffer);
            }

            if (count == 0)
            {
                Log.Verbose("Push 0 datapoints to influxdb");
                return;
            }
            var groups = dataPointList.GroupBy(point => point.Id);

            var points = new List<IInfluxDatapoint>();

            foreach (var group in groups)
            {
                var ts = Extractor.GetNodeState(group.Key);
                if (ts == null) continue;
                foreach (var dp in group)
                {
                    if (ts.DataType.IsString)
                    {
                        var idp = new InfluxDatapoint<string>
                        {
                            UtcTimestamp = dp.Timestamp,
                            MeasurementName = dp.Id
                        };
                        idp.Fields.Add("value", dp.StringValue);
                        points.Add(idp);
                    }
                    else if (ts.DataType.Identifier == DataTypes.Boolean)
                    {
                        var idp = new InfluxDatapoint<bool>
                        {
                            UtcTimestamp = dp.Timestamp,
                            MeasurementName = dp.Id
                        };
                        idp.Fields.Add("value", Math.Abs(dp.DoubleValue) < 0.1);
                        points.Add(idp);

                    }
                    else if (ts.DataType.Identifier < DataTypes.Float
                             || ts.DataType.Identifier == DataTypes.Integer
                             || ts.DataType.Identifier == DataTypes.UInteger)
                    {
                        var idp = new InfluxDatapoint<long>
                        {
                            UtcTimestamp = dp.Timestamp,
                            MeasurementName = dp.Id
                        };
                        idp.Fields.Add("value", (long)dp.DoubleValue);
                        points.Add(idp);
                    }
                    else
                    {
                        var idp = new InfluxDatapoint<double>
                        {
                            UtcTimestamp = dp.Timestamp,
                            MeasurementName = dp.Id
                        };
                        idp.Fields.Add("value", dp.DoubleValue);
                        points.Add(idp);
                    }
                }
            }
            Log.Debug("Push {cnt} datapoints to influxdb", points.Count);
            await client.PostPointsAsync(config.Database, points, config.PointChunkSize);
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
                var values = await client.QueryMultiSeriesAsync(config.Database,
                    $"SELECT last(value) FROM \"{UAClient.GetUniqueId(state.Id, state.ArrayDimensions != null && state.ArrayDimensions.Length > 0 && state.ArrayDimensions[0] > 0 ? 0 : -1)}\"");
                if (values.Any() && values.First().HasEntries)
                {
                    DateTime timestamp = values.First().Entries[0].Time;
                    state.InitTimestamp(timestamp);
                }
                else
                {
                    state.InitTimestamp(Utils.Epoch);
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
    }
}

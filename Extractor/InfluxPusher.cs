using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdysTech.InfluxDB.Client.Net;
using Serilog;

namespace Cognite.OpcUa
{
    public class InfluxPusher : IPusher
    {
        public Extractor Extractor { set; private get; }
        public UAClient UAClient { set; private get; }
        public PusherConfig BaseConfig { get; private set; }
        public bool failing;

        public ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; } = new ConcurrentQueue<BufferedDataPoint>();
        private readonly InfluxClientConfig config;
        private readonly InfluxDBClient client;
        public InfluxPusher(InfluxClientConfig config)
        {
            this.config = config;
            BaseConfig = config;
            client = new InfluxDBClient(config.Host, config.Username, config.Password);
        }
        public async Task PushDataPoints(CancellationToken token)
        {
            var dataPointList = new List<BufferedDataPoint>();

            int count = 0;
            while (BufferedDPQueue.TryDequeue(out BufferedDataPoint buffer) && count++ < 100000)
            {
                if (buffer.timestamp > DateTime.MinValue)
                {
                    dataPointList.Add(buffer);
                }
            }
            var tasks = Utils.ChunkBy(dataPointList, config.PointChunkSize).Select(async points =>
            {
                var influxPoints = dataPointList.Select(point =>
                {
                    var dp = new InfluxDatapoint<double>
                    {
                        UtcTimestamp = point.timestamp,
                        MeasurementName = point.Id,
                    };
                    dp.Fields.Add("value", point.doubleValue);
                    return dp;
                });
                Log.Information("Push {NumInfluxPointsToPush} points to InfluxDB", points.Count());
                await client.PostPointsAsync(config.Database, influxPoints, 10000);
            });
            await Task.WhenAll(tasks);
        }

        public async Task<bool> PushNodes(IEnumerable<BufferedNode> nodes, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            var historizingVariables = variables.Where(variable => variable.Historizing);
            if (!historizingVariables.Any()) return true;
            var getLastTasks = variables.Select(async variable =>
            {
                var values = await client.QueryMultiSeriesAsync(config.Database, $"SELECT last(value) FROM \"{UAClient.GetUniqueId(variable.Id)}\"");
                if (values.Any() && values.First().HasEntries)
                {
                    DateTime timestamp = values.First().Entries[0].Time;
                    variable.LatestTimestamp = timestamp;
                }
                else
                {
                    variable.LatestTimestamp = new DateTime(1970, 1, 1);
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

        public void Reset()
        {
        }
    }
}

﻿using System;
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
        public ConcurrentQueue<BufferedEvent> BufferedEventQueue { get; } = new ConcurrentQueue<BufferedEvent>();
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
        public Task PushEvents(CancellationToken token)
        {
            BufferedEventQueue.Clear();
            return Task.CompletedTask;
        }

        public Task<bool> PushNodes(IEnumerable<BufferedNode> nodes, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            var historizingVariables = variables.Where(variable => variable.Historizing);
            return Task.FromResult(true);
        }
        public async Task<bool> InitLatestTimestamps(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            var getLastTasks = states.Select(async state =>
            {
                var values = await client.QueryMultiSeriesAsync(config.Database,
                    $"SELECT last(value) FROM \"{UAClient.GetUniqueId(state.Id, state.ArrayDimensions != null && state.ArrayDimensions[0] > 0 ? 0 : -1)}\"");
                if (values.Any() && values.First().HasEntries)
                {
                    DateTime timestamp = values.First().Entries[0].Time;
                    state.InitTimestamp(timestamp);
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

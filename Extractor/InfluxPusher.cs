using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB.LineProtocol.Client;
using InfluxDB.LineProtocol.Payload;

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
        private readonly LineProtocolClient client;
        public InfluxPusher(InfluxClientConfig config)
        {
            this.config = config;
            BaseConfig = config;
            client = new LineProtocolClient(new Uri(config.Host), config.Database, config.Username, config.Password);
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
                var linePoints = dataPointList.Select(point =>
                    new LineProtocolPoint(
                        point.Id,
                        new Dictionary<string, object> { { "value", point.isString ? point.stringValue : (object)point.doubleValue } },
                        null,
                        point.timestamp
                    ));
                var payload = new LineProtocolPayload();
                foreach (var point in linePoints)
                {
                    payload.Add(point);
                }
                Logger.LogInfo($"Push {points.Count()} points to InfluxDB");
                if (!config.Debug)
                {
                    var result = await client.WriteAsync(payload, token);
                    if (!result.Success)
                    {
                        failing = true;
                        throw new Exception(result.ErrorMessage);
                    }
                    failing = false;
                }
            });
            await Task.WhenAll(tasks);
        }

        public async Task<bool> PushNodes(IEnumerable<BufferedNode> nodes, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            return true;
        }

        public void Reset()
        {
        }
    }
}

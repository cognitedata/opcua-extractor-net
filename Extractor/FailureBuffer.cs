using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Serilog;

namespace Cognite.OpcUa
{
    public sealed class FailureBuffer : IDisposable
    {
        private readonly InfluxPusher influxPusher;
        private readonly FailureBufferConfig config;
        private readonly Dictionary<string, bool> managedPoints;
        private readonly Dictionary<int, DateTime> startTimes;
        private readonly Dictionary<int, DateTime> eventStartTimes;
        private readonly HashSet<NodeId> managedEvents = new HashSet<NodeId>();
        private readonly Extractor extractor;

        private readonly bool useLocalQueue;

        public bool Any => any || useLocalQueue && extractor.StateStorage.AnyPoints;
        private bool any;
        public bool AnyEvents => anyEvents || useLocalQueue && extractor.StateStorage.AnyEvents;
        private bool anyEvents;

        private static readonly ILogger log = Log.Logger.ForContext(typeof(FailureBuffer));
        public FailureBuffer(FailureBufferConfig config, Extractor extractor)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            managedPoints = new Dictionary<string, bool>();
            startTimes = new Dictionary<int, DateTime>();
            eventStartTimes = new Dictionary<int, DateTime>();
            if (config.Influx?.Database == null) return;

            influxPusher = new InfluxPusher(new InfluxClientConfig
            {
                Database = config.Influx.Database,
                Host = config.Influx.Host,
                Password = config.Influx.Password,
                PointChunkSize = config.Influx.PointChunkSize
            })
            {
                Extractor = extractor
            };
            this.extractor = extractor;
            useLocalQueue = extractor.StateStorage != null && config.LocalQueue;
            var connTest = influxPusher.TestConnection(CancellationToken.None);
            connTest.Wait();
            if (connTest.Result == null || !connTest.Result.Value)
            {
                throw new ExtractorFailureException("Failed to connect to buffer influxdb");
            }
        }
        public async Task WriteDatapoints(IEnumerable<BufferedDataPoint> points, IEnumerable<int> indices, CancellationToken token)
        {
            if (points == null || !points.Any() || indices == null || !indices.Any()) return;
            bool success = false;
            if (config.Influx != null && config.Influx.Write && influxPusher != null)
            {
                try
                {
                    var result = await influxPusher.PushDataPoints(points, token);
                    log.Information("Inserted {cnt} points into influxdb failure buffer", points.Count());
                    success = result.GetValueOrDefault();
                }
                catch (Exception e)
                {
                    log.Error(e, "Failed to insert into influxdb buffer");
                }
            }

            if (!success && useLocalQueue)
            {
                success = await extractor.StateStorage.WritePointsToQueue(points, token);
            }


            if (success)
            {
                var min = points.Select(dp => dp.Timestamp).Min();
                foreach (int index in indices)
                {
                    if (!startTimes.ContainsKey(index))
                    {
                        startTimes[index] = min;
                    }
                }

                foreach (var dp in points)
                {
                    managedPoints[dp.Id] = dp.IsString;
                }

                any = true;
            }
        }
        public async Task<IEnumerable<BufferedDataPoint>> ReadDatapoints(int index, CancellationToken token)
        {
            if (!startTimes.ContainsKey(index)) return Array.Empty<BufferedDataPoint>();
            bool success = false;
            IEnumerable<BufferedDataPoint> ret = new List<BufferedDataPoint>();

            if (config.Influx != null && config.Influx.Write && influxPusher != null)
            {
                ret = ret.Concat(await influxPusher.ReadDataPoints(startTimes[index], managedPoints, token));
                log.Information("Read {cnt} points from influxdb failure buffer", ret.Count());
                success = true;
            }

            ret = ret.DistinctBy(dp => new {dp.Id, dp.Timestamp}).ToList();
            if (success)
            {
                startTimes.Remove(index);
                if (!startTimes.Any())
                {
                    managedPoints.Clear();
                    any = false;
                }
            }
            return ret;
        }

        public async Task WriteEvents(IEnumerable<BufferedEvent> events, int index, CancellationToken token)
        {
            if (events == null || !events.Any()) return;

            bool success = false;
            if (config.Influx != null && config.Influx.Write && influxPusher != null)
            {
                var result = await influxPusher.PushEvents(events, token);
                success = result.GetValueOrDefault();
                log.Information("Inserted {cnt} events into influxdb failure buffer", events.Count());
            }

            if (success)
            {
                var mints = events.Select(evt => evt.Time).Min();
                if (!eventStartTimes.ContainsKey(index) || mints < eventStartTimes[index])
                {
                    eventStartTimes[index] = mints;
                }
                foreach (var evt in events)
                {
                    managedEvents.Add(evt.SourceNode);
                }

                anyEvents = true;
            }
        }

        public async Task<IEnumerable<BufferedEvent>> ReadEvents(int index, CancellationToken token)
        {
            if (!eventStartTimes.ContainsKey(index)) return Array.Empty<BufferedEvent>();
            bool success = false;
            IEnumerable<BufferedEvent> ret = new List<BufferedEvent>();
            if (config.Influx != null && config.Influx.Write && influxPusher != null)
            {
                try
                {
                    ret = ret.Concat(await influxPusher.ReadEvents(eventStartTimes[index], managedEvents, token));
                    log.Information("Read {cnt} events from influxdb failure buffer", ret.Count());
                    success = true;
                }
                catch (Exception e)
                {
                    log.Error(e, "Failed to read events from influxdb buffer");
                }
            }

            ret = ret.DistinctBy(evt => new { evt.EventId, evt.Time }).ToList();
            if (success)
            {
                eventStartTimes.Remove(index);
                if (!eventStartTimes.Any())
                {
                    managedEvents.Clear();
                    anyEvents = false;
                }
            }

            return ret;
        }
        public void Dispose()
        {
            influxPusher?.Dispose();
        }
    }
}

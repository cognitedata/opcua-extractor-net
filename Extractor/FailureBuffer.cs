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
        public bool Any { get; private set; }
        public bool AnyEvents { get; private set; }

        private bool bufferFileEmpty;
        private readonly object fileLock = new object();
        public FailureBuffer(FailureBufferConfig config, Extractor extractor)
        {
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
            var connTest = influxPusher.TestConnection(CancellationToken.None);
            connTest.Wait();
            if (!connTest.Result)
            {
                throw new Exception("Failed to connect to buffer influxdb");
            }
        }
        public async Task WriteDatapoints(IEnumerable<BufferedDataPoint> points, int index,
            double? nonFiniteReplacement, CancellationToken token)
        {
            if (points == null || !points.Any()) return;
            bool success = false;
            if (config.Influx != null && config.Influx.Write && influxPusher != null)
            {
                influxPusher.BaseConfig.NonFiniteReplacement = nonFiniteReplacement;
                foreach (var dp in points)
                {
                    influxPusher.BufferedDPQueue.Enqueue(dp);
                }

                try
                {
                    await influxPusher.PushDataPoints(token);
                    Log.Information("Inserted {cnt} points into influxdb failure buffer", points.Count());
                    success = true;
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to insert into influxdb buffer");
                }
            }

            if (!success && config.FilePath != null)
            {
                WriteBufferToFile(points, Path.Join(config.FilePath, "buffer.bin"), token);
                success = true;
            }

            if (success)
            {
                if (!startTimes.ContainsKey(index))
                {
                    startTimes[index] = points.Select(dp => dp.Timestamp).Min();
                }
                foreach (var dp in points)
                {
                    managedPoints[dp.Id] = dp.IsString;
                }

                Any = true;
            }
        }
        public async Task<IEnumerable<BufferedDataPoint>> ReadDatapoints(int index, CancellationToken token)
        {
            if (!startTimes.ContainsKey(index)) return Array.Empty<BufferedDataPoint>();
            bool success = false;
            IEnumerable<BufferedDataPoint> ret = new List<BufferedDataPoint>();
            if (config.Influx != null && config.Influx.Write && influxPusher != null)
            {
                try
                {
                    ret = ret.Concat(await influxPusher.ReadDataPoints(startTimes[index], managedPoints, token));
                    Log.Information("Read {cnt} points from influxdb failure buffer", ret.Count());
                    success = true;
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to read from influxdb buffer");
                }
            }

            if (config.FilePath != null && (!success || !bufferFileEmpty))
            {
                ret = ret.Concat(ReadBufferFromFile(Path.Join(config.FilePath, "buffer.bin"), token));
                success = true;
            }

            ret = ret.DistinctBy(dp => new {dp.Id, dp.Timestamp}).ToList();
            if (success)
            {
                startTimes.Remove(index);
                if (!startTimes.Any())
                {
                    managedPoints.Clear();
                    Any = false;
                    lock (fileLock)
                    {
                        File.Create(Path.Join(config.FilePath, "buffer.bin")).Close();
                    }
                    bufferFileEmpty = false;
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
                foreach (var evt in events)
                {
                    influxPusher.BufferedEventQueue.Enqueue(evt);
                }

                try
                {
                    await influxPusher.PushEvents(token);
                    Log.Information("Inserted {cnt} events into influxdb failure buffer", events.Count());
                    success = true;
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to insert into influxdb buffer");
                }
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

                AnyEvents = true;
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
                    Log.Information("Read {cnt} events from influxdb failure buffer", ret.Count());
                    success = true;
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to read events from influxdb buffer");
                }
            }

            ret = ret.DistinctBy(evt => new { evt.EventId, evt.Time }).ToList();
            if (success)
            {
                eventStartTimes.Remove(index);
                if (!eventStartTimes.Any())
                {
                    managedEvents.Clear();
                    AnyEvents = false;
                }
            }

            return ret;
        }
        /// <summary>
        /// Write a list of datapoints to buffer file.
        /// </summary>
        /// <param name="dataPoints">List of points to be buffered</param>
        private void WriteBufferToFile(IEnumerable<BufferedDataPoint> dataPoints,
            string path,
            CancellationToken token)
        {
            lock (fileLock)
            {
                using FileStream fs = new FileStream(path, FileMode.Append, FileAccess.Write);
                int count = 0;
                foreach (var dp in dataPoints)
                {
                    if (token.IsCancellationRequested) return;
                    count++;
                    byte[] bytes = dp.ToStorableBytes();
                    fs.Write(bytes, 0, bytes.Length);
                }
                if (count > 0)
                {
                    bufferFileEmpty = false;
                    Log.Debug("Write {NumDatapointsToPersist} datapoints to file", count);
                }
                else
                {
                    Log.Verbose("Write 0 datapoints to file");
                }
            }
        }

        /// <summary>
        /// Reads buffer from file
        /// </summary>
        private IEnumerable<BufferedDataPoint> ReadBufferFromFile(string path, CancellationToken token)
        {
            var result = new List<BufferedDataPoint>();
            lock (fileLock)
            {
                int count = 0;
                using (FileStream fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read))
                {
                    byte[] sizeBytes = new byte[sizeof(ushort)];
                    while (!token.IsCancellationRequested)
                    {
                        int read = fs.Read(sizeBytes, 0, sizeBytes.Length);
                        if (read < sizeBytes.Length) break;
                        ushort size = BitConverter.ToUInt16(sizeBytes, 0);
                        byte[] dataBytes = new byte[size];
                        int dRead = fs.Read(dataBytes, 0, size);
                        if (dRead < size) break;
                        var buffDp = new BufferedDataPoint(dataBytes);
                        if (buffDp.Id == null)
                        {
                            Log.Warning($"Invalid datapoint in buffer file {path}: {buffDp.Id}");
                            continue;
                        }
                        count++;
                        Log.Debug(buffDp.ToDebugDescription());
                        result.Add(buffDp);
                    }
                }

                if (count == 0)
                {
                    Log.Verbose("Read 0 point from file");
                }
                Log.Debug("Read {NumDatapointsToRead} points from file", count);
            }

            return result;
        }

        public void Dispose()
        {
            influxPusher?.Dispose();
        }
    }
}

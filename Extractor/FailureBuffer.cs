using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace Cognite.OpcUa
{
    public class FailureBuffer
    {
        private readonly InfluxPusher influxPusher;
        private readonly FailureBufferConfig config;
        private readonly Dictionary<string, bool> managedPoints;
        private readonly Dictionary<int, DateTime> startTimes;
        private readonly Extractor extractor;
        public bool Any { get; private set; }

        private bool bufferFileEmpty;
        private readonly object fileLock = new object();
        public FailureBuffer(FailureBufferConfig config, Extractor extractor)
        {
            this.config = config;
            this.extractor = extractor;
            managedPoints = new Dictionary<string, bool>();
            startTimes = new Dictionary<int, DateTime>();
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
            points = points.Where(point => !extractor.GetNodeState(point.Id).Historizing).ToList();
            if (!points.Any()) return;
            bool success = false;
            bool useBackup = true;
            if (config.Influx != null && config.Influx.Write && influxPusher != null)
            {
                influxPusher.BaseConfig.NonFiniteReplacement = nonFiniteReplacement;
                foreach (var dp in points)
                {
                    influxPusher.BufferedDPQueue.Enqueue(dp);
                }

                useBackup = false;
                try
                {
                    await influxPusher.PushDataPoints(token);
                    Log.Information("Inserted {cnt} points into influxdb failure buffer", points.Count());
                    success = true;
                }
                catch (Exception e)
                {
                    useBackup = true;
                    Log.Error(e, "Failed to insert into influxdb buffer");
                }
            }

            if (useBackup && config.FilePath != null)
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
            bool useBackup = true;
            if (config.Influx != null && config.Influx.Write && influxPusher != null)
            {
                useBackup = false;
                try
                {
                    ret = ret.Concat(await influxPusher.ReadDataPoints(startTimes[index], managedPoints, token));
                    Log.Information("Read {cnt} points from influxdb failure buffer", ret.Count());
                    success = true;
                }
                catch (Exception e)
                {
                    useBackup = true;
                    Log.Error(e, "Failed to read from influxdb buffer");
                }
            }

            if (config.FilePath != null && (useBackup || !bufferFileEmpty))
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
        /// <summary>
        /// Write a list of datapoints to buffer file. Only writes non-historizing datapoints.
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
        /// Reads buffer from file into the datapoint queue
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
    }
}

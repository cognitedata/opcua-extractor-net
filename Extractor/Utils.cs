/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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

using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using YamlDotNet.Serialization;

namespace Cognite.OpcUa
{
    public static class Utils
    {
        public static readonly DateTime Epoch = DateTimeOffset.FromUnixTimeMilliseconds(0).DateTime;
        public static bool BufferFileEmpty { get; set; }
        private static readonly object fileLock = new object();
        private static readonly object dateFileLock = new object();
        /// <summary>
        /// Write a list of datapoints to buffer file. Only writes non-historizing datapoints.
        /// </summary>
        /// <param name="dataPoints">List of points to be buffered</param>
        public static void WriteBufferToFile(IEnumerable<BufferedDataPoint> dataPoints,
            CogniteClientConfig config,
            CancellationToken token,
            IDictionary<string, bool> nodeIsHistorizing = null)
        {
            lock (fileLock)
            {
                using FileStream fs = new FileStream(config.BufferFile, FileMode.Append, FileAccess.Write);
                int count = 0;
                foreach (var dp in dataPoints)
                {
                    if (token.IsCancellationRequested) return;
                    if (nodeIsHistorizing?[dp.Id] ?? dp.IsString) continue;
                    count++;
                    byte[] bytes = dp.ToStorableBytes();
                    fs.Write(bytes, 0, bytes.Length);
                }
                if (count > 0)
                {
                    BufferFileEmpty = false;
                    Log.Information("Write {NumDatapointsToPersist} datapoints to file", count);
                }
            }
        }
        /// <summary>
        /// Reads buffer from file into the datapoint queue
        /// </summary>
        public static void ReadBufferFromFile(ConcurrentQueue<BufferedDataPoint> bufferedDPQueue,
            CogniteClientConfig config,
            CancellationToken token,
            IDictionary<string, bool> nodeIsHistorizing = null)
        {
            lock (fileLock)
            {
                int count = 0;
                using (FileStream fs = new FileStream(config.BufferFile, FileMode.OpenOrCreate, FileAccess.Read))
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
                        if (buffDp.Id == null || (!nodeIsHistorizing?.ContainsKey(buffDp.Id) ?? false))
                        {
                            Log.Warning($"Invalid datapoint in buffer file {config.BufferFile}: {buffDp.Id}");
                            continue;
                        }
                        count++;
                        Log.Debug(buffDp.ToDebugDescription());
                        bufferedDPQueue.Enqueue(buffDp);
                    }
                }
                Log.Information("Read {NumDatapointsToRead} points from file", count);
                File.Create(config.BufferFile).Close();
                BufferFileEmpty |= count > 0 && new FileInfo(config.BufferFile).Length == 0;
            }
        }
        /// <summary>
        /// Write given latest event timestamp to file
        /// </summary>
        /// <param name="date">Date to be written</param>
        public static void WriteLastEventTimestamp(DateTime date)
        {
            lock (dateFileLock)
            {
                using FileStream fs = new FileStream("latestEvent.bin", FileMode.OpenOrCreate, FileAccess.Write);
                fs.Write(BitConverter.GetBytes(date.ToBinary()));
            }
        }
        public static IEnumerable<IDictionary<TKey, IEnumerable<TVal>>> ChunkDictOfLists<TKey, TVal>(
            IDictionary<TKey, List<TVal>> points, int maxPerList, int maxKeys)
        {
            var ret = new List<Dictionary<TKey, IEnumerable<TVal>>>();
            var current = new Dictionary<TKey, IEnumerable<TVal>>();
            int count = 0;
            int keyCount = 0;

            foreach (var (key, value) in points)
            {
                if (!value.Any())
                    continue;

                if (keyCount >= maxKeys)
                {
                    ret.Add(current);
                    current = new Dictionary<TKey, IEnumerable<TVal>>();
                    count = 0;
                    keyCount = 0;
                }

                int pcount = value.Count;
                if (count + pcount <= maxPerList)
                {
                    current[key] = value;
                    count += pcount;
                    keyCount++;
                    continue;
                }

                // fill up the current batch to max_datapoints data points and keep the remaining data points in current.
                var inCurrent = value.Take(Math.Min(maxPerList - count, pcount)).ToList();
                if (inCurrent.Count > 0)
                {
                    current[key] = inCurrent;
                }
                ret.Add(current);

                // inNext can have too many datapoints
                var inNext = value.Skip(inCurrent.Count);
                if (inNext.Any())
                {
                    var chunks = ChunkBy(inNext, maxPerList).Select(chunk => new Dictionary<TKey, IEnumerable<TVal>> { { key, chunk } });
                    if (chunks.Count() > 1)
                    {
                        ret.AddRange(chunks.Take(chunks.Count() - 1));
                    }
                    current = chunks.Last();
                    keyCount = 1;
                    count = current[key].Count();
                }
                else
                {
                    current = new Dictionary<TKey, IEnumerable<TVal>>();
                    count = 0;
                    keyCount = 0;
                }
            }

            if (current.Any())
            {
                ret.Add(current);
            }

            return ret;
        }
        /// <summary>
        /// Read latest event timestamp from file.
        /// </summary>
        /// <returns>Retrieved date or DateTime.MinValue</returns>
        public static DateTime ReadLastEventTimestamp()
        {
            lock (dateFileLock)
            {
                using FileStream fs = new FileStream("latestEvent.bin", FileMode.OpenOrCreate, FileAccess.Read);
                byte[] rawRead = new byte[sizeof(long)];
                int read = fs.Read(rawRead, 0, sizeof(long));
                if (read < sizeof(long)) return DateTime.MinValue;
                return DateTime.FromBinary(BitConverter.ToInt64(rawRead));
            }
        }
        /// <summary>
        /// Map yaml config to the FullConfig object
        /// </summary>
        /// <param name="configPath">Path to config file</param>
        /// <returns>A <see cref="FullConfig"/> object representing the entire config file</returns>
        public static FullConfig GetConfig(string configPath)
        {
            FullConfig fullConfig;
            using (var rawConfig = new StringReader(File.ReadAllText(configPath)))
            {
                var deserializer = new DeserializerBuilder()
                    .WithTagMapping("!cdf", typeof(CogniteClientConfig))
                    .WithTagMapping("!influx", typeof(InfluxClientConfig))
                    .Build();
                fullConfig = deserializer.Deserialize<FullConfig>(rawConfig);
            }
			string envLogdir = Environment.GetEnvironmentVariable("OPCUA_LOGGER_DIR");
            if (!string.IsNullOrWhiteSpace(envLogdir))
			{
				fullConfig.Logging.LogFolder = envLogdir;
			}
            return fullConfig;
        }
        /// <summary>
        /// Divide input into a number of size limited chunks
        /// </summary>
        /// <typeparam name="T">Type in input enumerable</typeparam>
        /// <param name="input">Input enumerable of any size</param>
        /// <param name="maxSize">Maximum size of return enumerables</param>
        /// <returns>A number of enumerables smaller or equal to maxSize</returns>
        public static IEnumerable<IEnumerable<T>> ChunkBy<T>(IEnumerable<T> input, int maxSize)
        {
            return input
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / maxSize)
                .Select(x => x.Select(v => v.Value));
        }
        /// <summary>
        /// Reduce the length of given string to maxLength, if it is longer.
        /// </summary>
        /// <param name="str">String to be shortened</param>
        /// <param name="maxLength">Maximum length of final string</param>
        /// <returns>String which contains the first `maxLength` characters of the passed string.</returns>
        public static string Truncate(string str, int maxLength)
        {
            if (string.IsNullOrEmpty(str) || str.Length <= maxLength) return str;
            return str.Substring(0, maxLength);
        }
    }
}

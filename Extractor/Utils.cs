using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Sdk;
using YamlDotNet.RepresentationModel;
using YamlDotNet.Serialization;

namespace Cognite.OpcUa
{
    public static class Utils
    {
        private static readonly int retryCount = 3;
        public static bool BufferFileEmpty { get; set; }
        private static readonly object fileLock = new object();
        /// <summary>
        /// Retry the given asynchronous action a fixed number of times, logging each failure, and delaying with exponential backoff.
        /// </summary>
        /// <typeparam name="T">Expected return type</typeparam>
        /// <param name="action">Asynchronous action to be performed</param>
        /// <param name="failureMessage">Message to log on failure, in addition to attempt number</param>
        /// <param name="expectResponseException">If true, expect a <see cref="ResponseException"/> and throw it immediately if found</param>
        /// <returns>The expected return type, result of the asynchronous call</returns>
        public static async Task<T> RetryAsync<T>(Func<Task<T>> action, string failureMessage, bool expectResponseException = false)
        {
            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    T result = await action();
                    return result;
                }
                catch (Exception e)
                {
                    if (e.GetType() == typeof(ResponseException))
                    {
                        var re = (ResponseException)e;
                        if (i == retryCount - 1 || expectResponseException)
                        {
                            throw re;
                        }
                    }
                    Logger.LogWarning(failureMessage + ", " + e.Message + ": attempt " + (i + 1) + "/" + retryCount);
                    // Logger.LogException(e);
                }
                await Task.Delay(500 * (1 << i));
            }
            return default(T);
        }
        /// <summary>
        /// Write a list of datapoints to buffer file. Only writes non-historizing datapoints.
        /// </summary>
        /// <param name="dataPoints">List of points to be buffered</param>
        public static void WriteBufferToFile(IEnumerable<BufferedDataPoint> dataPoints,
            CogniteClientConfig config,
            IDictionary<string, bool> nodeIsHistorizing = null)
        {
            lock (fileLock)
            {
                using (FileStream fs = new FileStream(config.BufferFile, FileMode.Append, FileAccess.Write))
                {
                    int count = 0;
                    foreach (var dp in dataPoints)
                    {
                        if (nodeIsHistorizing?[dp.Id] ?? false) continue;
                        count++;
                        BufferFileEmpty = false;
                        byte[] bytes = dp.ToStorableBytes();
                        fs.Write(bytes, 0, bytes.Length);
                    }
                    if (count > 0)
                    {
                        Logger.LogInfo("Write " + count + " datapoints to file");
                    }
                }
            }
        }
        /// <summary>
        /// Reads buffer from file into the datapoint queue
        /// </summary>
        public static void ReadBufferFromFile(ConcurrentQueue<BufferedDataPoint> bufferedDPQueue,
            CogniteClientConfig config,
            IDictionary<string, bool> nodeIsHistorizing = null)
        {
            lock (fileLock)
            {
                using (FileStream fs = new FileStream(config.BufferFile, FileMode.OpenOrCreate, FileAccess.Read))
                {
                    byte[] sizeBytes = new byte[sizeof(ushort)];
                    while (true)
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
                            Logger.LogWarning("Bad datapoint in file");
                            continue;
                        }
                        Logger.LogData(buffDp);
                        bufferedDPQueue.Enqueue(buffDp);
                    }
                }
            }
            File.Create(config.BufferFile).Close();
            BufferFileEmpty = true;
        }
        /// <summary>
        /// Map yaml config to the FullConfig object
        /// </summary>
        /// <param name="configPath">Path to config file</param>
        /// <returns>A <see cref="FullConfig"/> object representing the entire config file</returns>
        public static FullConfig GetConfig(string configPath)
        {
            FullConfig fullConfig = null;
            try
            {
                using (var rawConfig = new StringReader(File.ReadAllText(configPath)))
                {
                    fullConfig = new Deserializer().Deserialize<FullConfig>(rawConfig);
                }
                string envKey = Environment.GetEnvironmentVariable("COGNITE_API_KEY");
                if (string.IsNullOrWhiteSpace(fullConfig.CogniteConfig.ApiKey) && !string.IsNullOrWhiteSpace(envKey))
                {
                    fullConfig.CogniteConfig.ApiKey = envKey;
                }
                string envProject = Environment.GetEnvironmentVariable("COGNITE_API_PROJECT");
                if (string.IsNullOrWhiteSpace(fullConfig.CogniteConfig.Project) && !string.IsNullOrWhiteSpace(envProject))
                {
                    fullConfig.CogniteConfig.Project = envProject;
                }
                foreach (var kvp in fullConfig.NSMaps)
                {
                    Console.WriteLine(kvp.Key + ", " + kvp.Value);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to load config");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
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
    }
}

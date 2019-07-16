using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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
            var config = ReadConfig(configPath);
            FullConfig fullConfig = null;
            try
            {
                var clientCfg = config.Children[new YamlScalarNode("client")];
                var nsmaps = (YamlMappingNode)config.Children[new YamlScalarNode("nsmaps")];
                var cogniteConfig = config.Children[new YamlScalarNode("cognite")];
                var loggerConfig = config.Children[new YamlScalarNode("logging")];
                var metricsConfig = config.Children[new YamlScalarNode("metrics")];
                var bulkSizes = config.Children[new YamlScalarNode("bulksizes")];
                fullConfig = new FullConfig
                {
                    NSMaps = nsmaps,
                    UAConfig = DeserializeNode<UAClientConfig>(clientCfg),
                    CogniteConfig = DeserializeNode<CogniteClientConfig>(cogniteConfig),
                    LoggerConfig = DeserializeNode<LoggerConfig>(loggerConfig),
                    MetricsConfig = DeserializeNode<MetricsConfig>(metricsConfig),
                    BulkSizes = DeserializeNode<BulkSizes>(bulkSizes)
                };
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
        /// Reads config from file, then maps to a YamlDotNet tree
        /// </summary>
        /// <param name="configPath">Path to the config file</param>
        /// <returns>The root <see cref="YamlMappingNode"/></returns>
        public static YamlMappingNode ReadConfig(string configPath)
        {
            if (!File.Exists(configPath))
            {
                Console.WriteLine("Failed to open config file " + configPath);
            }
            string document = File.ReadAllText(configPath);
            StringReader input = new StringReader(document);
            YamlStream stream = new YamlStream();
            stream.Load(input);

            return (YamlMappingNode)stream.Documents[0].RootNode;
        }
        /// <summary>
        /// Generic implementation of a small hack to use the YamlDotNet deserializer on individual nodes
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="node">The root node for the target object</param>
        /// <returns>An instantiated instance of the target type</returns>
        public static T DeserializeNode<T>(YamlNode node)
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var reader = new StreamReader(stream))
            {
                new YamlStream(new YamlDocument[] { new YamlDocument(node) }).Save(writer);
                writer.Flush();
                stream.Position = 0;
                try
                {
                    return new Deserializer().Deserialize<T>(reader);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to load config: " + node);
                    throw e;
                }
            }
        }
    }
}

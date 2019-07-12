using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Sdk;

namespace Cognite.OpcUa
{
    static class Utils
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
                    Console.WriteLine("Try action");
                    return await action();
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
                    Logger.LogException(e);
                }
                Thread.Sleep(500 * (2 << i));
            }
            return default(T);
        }
        /// <summary>
        /// Write a list of datapoints to buffer file. Only writes non-historizing datapoints.
        /// </summary>
        /// <param name="dataPoints">List of points to be buffered</param>
        public static void WriteBufferToFile(IEnumerable<BufferedDataPoint> dataPoints,
            CogniteClientConfig config,
            IDictionary<string, bool> nodeIsHistorizing)
        {
            lock (fileLock)
            {
                using (FileStream fs = new FileStream(config.BufferFile, FileMode.Append, FileAccess.Write))
                {
                    int count = 0;
                    foreach (var dp in dataPoints)
                    {
                        if (nodeIsHistorizing[dp.Id]) continue;
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
            IDictionary<string, bool> nodeIsHistorizing)
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
                        if (buffDp.Id == null || !nodeIsHistorizing.ContainsKey(buffDp.Id))
                        {
                            Logger.LogWarning("Bad datapoint in file");
                            continue;
                        }
                        bufferedDPQueue.Enqueue(buffDp);
                    }
                }
            }
            File.Create(config.BufferFile).Close();
            BufferFileEmpty = true;
        }
    }
}

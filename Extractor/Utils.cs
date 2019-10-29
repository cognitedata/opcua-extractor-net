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
using System.Threading.Tasks;
using CogniteSdk;
using CogniteSdk.Login;
using CogniteSdk.TimeSeries;
using Opc.Ua;
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

                if (count == 0)
                {
                    Log.Verbose("Read 0 point from file");
                }
                Log.Debug("Read {NumDatapointsToRead} points from file", count);
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

        public enum SourceOp
        {
            SelectEndpoint, CreateSession, Browse, BrowseNext,
            CreateSubscription, CreateMonitoredItems, ReadAttributes, HistoryRead,
            HistoryReadEvents, ReadRootNode, DefaultOperation
        }

        public static SilentServiceException GetRootSilentException(AggregateException aex)
        {
            if (aex.InnerException is SilentServiceException silent)
            {
                return silent;
            }
            if (aex.InnerException is AggregateException aex2)
            {
                return GetRootSilentException(aex2);
            }

            return null;
        }
        public static void LogException(Exception e, string message, string silentMessage)
        {
            if (e is AggregateException aex)
            {
                var silent = GetRootSilentException(aex);
                if (silent != null)
                {
                    Log.Debug(silent, silentMessage);
                    return;
                }
            } 
            else if (e is SilentServiceException silent)
            {
                Log.Debug(silent, silentMessage);
                return;
            }
            Log.Error(e, message);
        }
        public static Exception HandleServiceResult(ServiceResultException ex, SourceOp op)
        {
            uint code = ex.StatusCode;
            string symId = StatusCode.LookupSymbolicId(code);
            switch (code)
            {
                // Handle common errors
                case StatusCodes.BadDecodingError:
                case StatusCodes.BadUnknownResponse:
                    // This really shouldn't happen, it is either some freak communication error or an issue with the server
                    Log.Error("Server responded with bad data: {code}, at operation {op}", symId, op.ToString());
                    Log.Error("This is unlikely to be an issue with the extractor");
                    Log.Error("If it repeats, it is most likely a bug in the server");
                    return new SilentServiceException("Server responded with bad data", ex, op);
                case StatusCodes.BadCertificateChainIncomplete:
                case StatusCodes.BadCertificateHostNameInvalid:
                case StatusCodes.BadCertificateInvalid:
                case StatusCodes.BadCertificateIssuerRevocationUnknown:
                case StatusCodes.BadCertificateIssuerRevoked:
                case StatusCodes.BadCertificateIssuerTimeInvalid:
                case StatusCodes.BadCertificateIssuerUseNotAllowed:
                case StatusCodes.BadCertificatePolicyCheckFailed:
                case StatusCodes.BadCertificateRevocationUnknown:
                case StatusCodes.BadCertificateRevoked:
                    Log.Error("There was an issue with the certificate: {code} at operation {op}", symId, op.ToString());
                    return new SilentServiceException("There was an issue with the certificate", ex, op);
                case StatusCodes.BadNothingToDo:
                    Log.Error("Server had nothing to do, this is likely an issue with the extractor: {code} at operation {op}", 
                        symId, op.ToString());
                    return new SilentServiceException("Server had nothing to do", ex, op);
                case StatusCodes.BadSessionClosed:
                    // This sometimes occurs if the client is closed during an operation, it is expected
                    Log.Error("Service failed due to closed session: {code} at operation {op}", symId, op.ToString());
                    return new SilentServiceException("Service failed due to closed session", ex, op);
                case StatusCodes.BadServerNotConnected:
                    Log.Error("The client attempted a connection without being connected to the server: {code} at operation {op}", 
                        symId, op.ToString());
                    Log.Error("This is most likely an issue with the extractor");
                    return new SilentServiceException("Attempted call to unconnected server", ex, op);
                case StatusCodes.BadServerHalted:
                    Log.Error("Server halted unexpectedly: {code} at operation {op}", symId, op.ToString());
                    return new SilentServiceException("Server stopped unexpectedly", ex, op);
                default:
                    switch (op)
                    {
                        case SourceOp.SelectEndpoint:
                            if (code == StatusCodes.BadNotConnected || code == StatusCodes.BadSecureChannelClosed)
                            {
                                // The most common error, generally happens if the server cannot be found
                                Log.Error("Unable to connect to discovery server: {code} at operation {op}", 
                                    symId, op.ToString());
                                Log.Error("Check the EndpointURL, and make sure that the server is accessible");
                                return new SilentServiceException("Unable to connect to discovery server", ex, op);
                            }
                            break;
                        case SourceOp.CreateSession:
                            switch (code)
                            {
                                case StatusCodes.BadIdentityTokenInvalid:
                                    Log.Error("Invalid identity token, most likely a configuration issue: {code} at operation {op}", 
                                        symId, op.ToString());
                                    Log.Error("Make sure that the username and password given are valid");
                                    return new SilentServiceException("Invalid identity token", ex, op);
                                case StatusCodes.BadIdentityTokenRejected:
                                    Log.Error("Identity token rejected, most likely incorrect username or password: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("Identity token rejected", ex, op);
                                case StatusCodes.BadCertificateUntrusted:
                                    Log.Error("Certificate not trusted by server: {code} at operation {op}", symId, op.ToString());
                                    Log.Error("This can be fixed by moving trusting the certificate on the server");
                                    return new SilentServiceException("Certificate untrusted", ex, op);
                            }
                            break;
                        case SourceOp.ReadRootNode:
                            if (code == StatusCodes.BadNodeIdInvalid || code == StatusCodes.BadNodeIdUnknown)
                            {
                                Log.Error("Root node not found, check configuration: {code} at operation {op}", 
                                    symId, op.ToString());
                                return new SilentServiceException("Root node not found", ex, op);
                            }
                            goto case SourceOp.ReadAttributes;
                        case SourceOp.Browse:
                            switch (code)
                            {
                                case StatusCodes.BadNodeIdInvalid:
                                case StatusCodes.BadNodeIdUnknown:
                                case StatusCodes.BadReferenceTypeIdInvalid:
                                case StatusCodes.BadBrowseDirectionInvalid:
                                    Log.Error("Error during browse, this is most likely a limitation of the server: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("Unexpected error during Browse", ex, op);
                            }
                            goto case SourceOp.DefaultOperation;
                        case SourceOp.BrowseNext:
                            if (code == StatusCodes.BadServiceUnsupported)
                            {
                                Log.Error("BrowseNext not supported by server: {code} at operation {op}", symId, op.ToString());
                                Log.Error("This is a required service, but it may be possible to increase browse chunk sizes to avoid the issue");
                                return new SilentServiceException("BrowseNext unspported", ex, op);
                            }
                            goto case SourceOp.Browse;
                        case SourceOp.ReadAttributes:
                            switch (code)
                            {
                                case StatusCodes.BadNodeIdInvalid:
                                case StatusCodes.BadNodeIdUnknown:
                                case StatusCodes.BadAttributeIdInvalid:
                                case StatusCodes.BadNotReadable:
                                    Log.Error("Failure during read, this is most likely a limitation of the server: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("Unexpected error during Read", ex, op);
                                case StatusCodes.BadUserAccessDenied:
                                    Log.Error("Failed to read attributes due to insufficient access rights: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("User access denied during Read", ex, op);
                                case StatusCodes.BadSecurityModeInsufficient:
                                    Log.Error("Failed to read attributes due to insufficient security level: {code} at operation {op}",
                                        symId, op.ToString());
                                    Log.Error("This generally means that reading of specific attributes/nodes requires a secure connection" +
                                              ", and the current connection is not sufficiently secure");
                                    return new SilentServiceException("Insufficient security during Read", ex, op);
                            }
                            goto case SourceOp.DefaultOperation;
                        case SourceOp.CreateSubscription:
                            switch (code)
                            {
                                case StatusCodes.BadTooManySubscriptions:
                                    Log.Error("Too many subscriptions on server: {code} at operation {op}", symId, op.ToString());
                                    Log.Error("The extractor creates a maximum of three subscriptions, one for data, one for events, one for auditing");
                                    Log.Error("If this happens after multiple reconnects, it may be due to poor reconnect handling somewhere, " +
                                              "in that case, it may help to turn on ForceRestart in order to clean up subscriptions between each reconnect");
                                    return new SilentServiceException("Too many subscriptions", ex, op);
                                case StatusCodes.BadServiceUnsupported:
                                    Log.Error("Create subscription unsupported by server: {code} at operation {op}", symId, op.ToString());
                                    Log.Error("This may be an issue with the extractor, or more likely a server limitation");
                                    return new SilentServiceException("CreateSubscription unsupported", ex, op);
                            }
                            // Creating a subscription in the SDK also involves a call to the CreateMonitoredItems service, usually
                            goto case SourceOp.CreateMonitoredItems;
                        case SourceOp.CreateMonitoredItems:
                            switch (code)
                            {
                                case StatusCodes.BadSubscriptionIdInvalid:
                                    Log.Error("Subscription not found on server", symId, op.ToString());
                                    Log.Error("This is generally caused by a desync between the server and the client");
                                    Log.Error("A solution may be to turn on ForceRestart, to clean up subscriptions between each connect");
                                    return new SilentServiceException("Subscription id invalid", ex, op);
                                case StatusCodes.BadFilterNotAllowed:
                                case StatusCodes.BadFilterOperatorUnsupported:
                                case StatusCodes.BadFilterOperandInvalid:
                                case StatusCodes.BadFilterLiteralInvalid:
                                case StatusCodes.BadEventFilterInvalid:
                                    Log.Error("Event filter invalid: {code} at operation {op}", symId, op.ToString());
                                    Log.Error("This may be an issue with the extractor, or the server may not fully support event filtering");
                                    return new SilentServiceException("Filter related error", ex, op);
                                case StatusCodes.BadTooManyMonitoredItems:
                                    Log.Error("Server has reached limit of monitored items", symId, op.ToString());
                                    Log.Error("The extractor requires one monitored item per data variable, and one per configured event emitter node");
                                    Log.Error("If this happens after multiple reconnects it may be due to poor reconnect handling somewhere, " +
                                              "in that case, it may help to turn on ForceRestarts in order to clean up subscriptions between each reconnect");
                                    return new SilentServiceException("Too many monitoredItems", ex, op);
                            }
                            goto case SourceOp.DefaultOperation;
                        case SourceOp.HistoryRead:
                            switch (code)
                            {
                                case StatusCodes.BadNodeIdInvalid:
                                case StatusCodes.BadNodeIdUnknown:
                                case StatusCodes.BadDataEncodingInvalid:
                                case StatusCodes.BadDataEncodingUnsupported:
                                    Log.Error("Failure during HistoryRead, this may be caused by a server limitation: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("Unexpected error in HistoryRead", ex, op);
                                case StatusCodes.BadUserAccessDenied:
                                    Log.Error("Failed to read History due to insufficient access rights: {code} at operation {op}",
                                        symId, op.ToString());
                                    return new SilentServiceException("User access denied during HistoryRead", ex, op);
                                case StatusCodes.BadTooManyOperations:
                                    Log.Error("Failed to read History due to too many operations: {code} at operation {op}",
                                        symId, op.ToString());
                                    Log.Error("This may be due to too large chunk sizes, try to lower chunk sizes for {op}", op.ToString());
                                    return new SilentServiceException("Too many operations during HistoryRead", ex, op);
                                case StatusCodes.BadHistoryOperationUnsupported:
                                case StatusCodes.BadHistoryOperationInvalid:
                                    Log.Error("HistoryRead operation unsupported by server: {code} at operation {op}");
                                    Log.Error("The extractor uses HistoryReadRaw for data and HistoryReadEvents for events");
                                    Log.Error("If the server does not support one, they may be disabled individually");
                                    return new SilentServiceException("HistoryRead operation unspported", ex, op);
                            }

                            break;
                        case SourceOp.HistoryReadEvents:
                            switch (code)
                            {
                                case StatusCodes.BadFilterNotAllowed:
                                case StatusCodes.BadFilterOperatorUnsupported:
                                case StatusCodes.BadFilterOperandInvalid:
                                case StatusCodes.BadFilterLiteralInvalid:
                                case StatusCodes.BadEventFilterInvalid:
                                    Log.Error("Event filter invalid: {code} at operation {op}", symId, op.ToString());
                                    Log.Error("This may be an issue with the extractor, or the server may not fully support event filtering");
                                    return new SilentServiceException("Filter related error", ex, op);
                            }
                            goto case SourceOp.HistoryRead;
                        case SourceOp.DefaultOperation:
                            switch (code)
                            {
                                case StatusCodes.BadServiceUnsupported:
                                    Log.Error("Base requirement \"{op}\" unspported by server: {code}", op.ToString(), symId);
                                    Log.Error("This is a required service, if the server does not support it the extractor may not be used");
                                    return new SilentServiceException($"{op.ToString()} unsupported", ex, op);
                                case StatusCodes.BadNoContinuationPoints:
                                    Log.Error("Server is out of continuationPoints, this may be the " +
                                              "result of poor configuration of the extractor: {code} at operation {op}", 
                                        symId, op.ToString());
                                    Log.Error("If the chunk sizes for {op} are set very low, that may be the cause", op.ToString());
                                    return new SilentServiceException($"Too many continuationPoints for {op.ToString()}", ex, op);
                                case StatusCodes.BadTooManyOperations:
                                    Log.Error("Too many operations, this is most likely due to chunkSize being set too high: {code} at operation {op}",
                                        symId, op.ToString());
                                    Log.Error("Try lowering the chunk sizes for {op}", op.ToString());
                                    return new SilentServiceException($"Too many operations for {op.ToString()}", ex, op);
                            }
                            break;
                    }
                    return new Exception("Unhandled ServiceResultException", ex);
            }
        }
    }

    public class SilentServiceException : Exception
    {
        public readonly Utils.SourceOp Operation;
        public readonly uint StatusCode;

        public SilentServiceException(string msg, ServiceResultException ex, Utils.SourceOp op) : base(msg, ex)
        {
            Operation = op;
            StatusCode = ex.StatusCode;
        }
    }
}

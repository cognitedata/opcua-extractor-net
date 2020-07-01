using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Opc.Ua;
using Prometheus;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Utility for various ways of storing datapoints and events if the connection to a destination goes down.
    /// </summary>
    public sealed class FailureBuffer : IDisposable
    {
        private readonly InfluxPusher influxPusher;
        private readonly FailureBufferConfig config;
        private readonly FullConfig fullConfig;
        private readonly UAExtractor extractor;

        private readonly Dictionary<string, InfluxBufferState> nodeBufferStates;
        private readonly Dictionary<string, InfluxBufferState> eventBufferStates;

        public bool Any => anyPoints || fileAnyPoints;
        private bool fileAnyPoints;
        private bool anyPoints;
        public bool AnyEvents => anyEvents || fileAnyEvents;
        private bool fileAnyEvents;
        private bool anyEvents;

        private static readonly Gauge numPointsInBuffer = Metrics.CreateGauge(
            "opcua_buffer_num_points", "The number of datapoints in the local buffer file");

        private static readonly Gauge numEventsInBuffer = Metrics.CreateGauge(
            "opcua_buffer_num_events", "The number of events in the local buffer file");

        private static readonly ILogger log = Log.Logger.ForContext(typeof(FailureBuffer));
        /// <summary>
        /// Constructor. This checks whether any points or events exists in the buffer files
        /// and creates files if they do not exist.
        /// </summary>
        /// <param name="fullConfig"></param>
        /// <param name="extractor"></param>
        public FailureBuffer(FullConfig fullConfig, UAExtractor extractor, InfluxPusher influxPusher)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            if (fullConfig == null) throw new ArgumentNullException(nameof(fullConfig));
            config = fullConfig.FailureBuffer;
            this.fullConfig = fullConfig;

            this.extractor = extractor;

            if (!string.IsNullOrEmpty(config.DatapointPath))
            {
                if (!File.Exists(config.DatapointPath))
                {
                    File.Create(config.DatapointPath).Close();
                }

                fileAnyPoints |= new FileInfo(config.DatapointPath).Length > 0;
            }

            if (!string.IsNullOrEmpty(config.EventPath))
            {
                if (!File.Exists(config.EventPath))
                {
                    File.Create(config.EventPath).Close();
                }

                fileAnyEvents |= new FileInfo(config.EventPath).Length > 0;
            }

            if (!config.Influx) return;
            if (influxPusher == null) throw new ConfigurationException("FailureBuffer expecting influxpusher, but none is registered");
            this.influxPusher = influxPusher;

            nodeBufferStates = new Dictionary<string, InfluxBufferState>();
            eventBufferStates = new Dictionary<string, InfluxBufferState>();
        }
        /// <summary>
        /// Load buffer states from state storage if influxdb buffering and state storage is enabled.
        /// </summary>
        /// <param name="states">States to read into</param>
        /// <param name="nodeIds">Nodes to read for</param>
        public async Task InitializeBufferStates(IEnumerable<NodeExtractionState> states,
            IEnumerable<EventExtractionState> evtStates, CancellationToken token)
        {
            if (!config.Influx || influxPusher == null || !config.InfluxStateStore) return;
            var variableStates = states
                .Where(state => !state.FrontfillEnabled)
                .Select(state => new InfluxBufferState(state))
                .ToList();

            await extractor.StateStorage.RestoreExtractionState(
                variableStates.ToDictionary(state => state.Id),
                fullConfig.StateStorage.InfluxVariableStore,
                false,
                token);

            foreach (var state in variableStates)
            {
                if (state.DestinationExtractedRange == TimeRange.Empty) continue;
                nodeBufferStates[state.Id] = state;
                if (state.DestinationExtractedRange.First <= state.DestinationExtractedRange.Last)
                {
                    anyPoints = true;
                }
            }

            var eventStates = evtStates.Select(state => new InfluxBufferState(state)).ToList();

            await extractor.StateStorage.RestoreExtractionState(
                eventStates.ToDictionary(state => state.Id),
                fullConfig.StateStorage.InfluxEventStore,
                false,
                token);

            foreach (var state in eventStates)
            {
                if (state.DestinationExtractedRange == TimeRange.Empty) continue;
                eventBufferStates[state.Id] = state;
                if (state.DestinationExtractedRange.First < state.DestinationExtractedRange.Last)
                {
                    anyEvents = true;
                }
            }
        }
        /// <summary>
        /// Write datapoints to enabled buffer locations.
        /// </summary>
        /// <param name="points">Datapoints to write</param>
        /// <param name="pointRanges">Ranges for given data variables, to simplify storage and state</param>
        /// <param name="pushers">Active pushers</param>
        /// <returns>True on success</returns>
        public async Task<bool> WriteDatapoints(IEnumerable<BufferedDataPoint> points, IDictionary<string, TimeRange> pointRanges,
            IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (points == null || !points.Any() || pushers == null || !pushers.Any() || pointRanges == null
                || !pointRanges.Any()) return true;

            points = points.GroupBy(pt => pt.Id)
                .Where(group => !extractor.State.GetNodeState(group.Key).FrontfillEnabled)
                .SelectMany(group => group)
                .ToList();

            log.Information("Push {cnt} points to failurebuffer", points.Count());

            if (!points.Any()) return true;

            bool success = true;

            if (config.Influx && influxPusher != null)
            {
                try
                {
                    if (success && !influxPusher.DataFailing)
                    {
                        bool any = false;
                        foreach ((string key, var value) in pointRanges)
                        {
                            if (!nodeBufferStates.ContainsKey(key))
                            {
                                var state = extractor.State.GetNodeState(key);
                                if (state.FrontfillEnabled) continue;
                                nodeBufferStates[key] = new InfluxBufferState(state);
                                nodeBufferStates[key].InitExtractedRange(TimeRange.Empty.First, TimeRange.Empty.Last);
                            }
                            nodeBufferStates[key].UpdateDestinationRange(value.First, value.Last);
                            any |= value.First <= value.Last;
                        }
                        if (config.InfluxStateStore)
                        {
                            await extractor.StateStorage.StoreExtractionState(nodeBufferStates.Values,
                                fullConfig.StateStorage.InfluxVariableStore, token).ConfigureAwait(false);
                        }

                        anyPoints |= any;
                    }
                }
                catch (Exception e)
                {
                    success = false;
                    log.Error(e, "Failed to insert into influxdb buffer");
                }
            }

            if (!string.IsNullOrEmpty(config.DatapointPath))
            {
                try
                {
                    await Task.Run(() => WriteDatapointsToFile(config.DatapointPath, points, token));
                    fileAnyPoints |= points.Any();
                }
                catch (Exception ex)
                {
                    log.Error(ex, "Failed to write datapoints to file");
                    success = false;
                }
            }

            return success;
        }
        /// <summary>
        /// Read datapoints from storage locations into given list of pushers
        /// </summary>
        /// <param name="pushers">Pushers to write to</param>
        /// <returns>True on success</returns>
        public async Task<bool> ReadDatapoints(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (pushers == null) throw new ArgumentNullException(nameof(pushers));
            bool success = true;

            if (config.Influx && influxPusher != null)
            {
                var activeStates = nodeBufferStates.Where(kvp =>
                        !kvp.Value.Historizing
                        && kvp.Value.DestinationExtractedRange.First >= kvp.Value.DestinationExtractedRange.Last)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                if (activeStates.Any())
                {
                    try
                    {
                        var dps = await influxPusher.ReadDataPoints(activeStates, token);
                        log.Information("Read {cnt} points from influxdb failure buffer", dps.Count());
                        await Task.WhenAll(pushers
                            .Where(pusher => !(pusher is InfluxPusher))
                            .Select(pusher => pusher.PushDataPoints(dps, token)));

                        foreach (var state in activeStates)
                        {
                            state.Value.ClearRanges();
                        }

                        if (config.InfluxStateStore)
                        {
                            await extractor.StateStorage.StoreExtractionState(activeStates.Values,
                                fullConfig.StateStorage.InfluxVariableStore, token).ConfigureAwait(false);
                        }

                        anyPoints = false;
                    }
                    catch (Exception e)
                    {
                        success = false;
                        Log.Error(e, "Failed to read points from influxdb");
                    }
                }
                else if (anyPoints)
                {
                    log.Warning("All ranges are empty, but anyPoints is set to true");
                }
            }

            if (!string.IsNullOrEmpty(config.DatapointPath))
            {
                success &= await ReadDatapointsFromFile(pushers, token);
            }

            return success;
        }
        /// <summary>
        /// Write events to storage locations
        /// </summary>
        /// <param name="events">Events to write</param>
        /// <param name="pushers">Active pushers</param>
        /// <returns>True on success</returns>
        public async Task<bool> WriteEvents(IEnumerable<BufferedEvent> events,
            IEnumerable<IPusher> pushers,
            CancellationToken token)
        {
            if (events == null || !events.Any() || pushers == null || !pushers.Any()) return true;

            events = events.GroupBy(evt => evt.EmittingNode)
                .Where(group => !extractor.State.GetEmitterState(group.Key).FrontfillEnabled)
                .SelectMany(group => group)
                .ToList();

            log.Information("Push {cnt} events to failurebuffer", events.Count());

            if (!events.Any()) return true;

            bool success = true;

            if (config.Influx)
            {
                if (!influxPusher.EventsFailing)
                {
                    var eventRanges = new Dictionary<string, TimeRange>();
                    bool any = false;
                    foreach (var evt in events)
                    {
                        var emitterId = extractor.GetUniqueId(evt.EmittingNode);
                        if (!eventRanges.ContainsKey(emitterId))
                        {
                            eventRanges[emitterId] = new TimeRange(evt.Time, evt.Time);
                            continue;
                        }
                        any = true;
                        eventRanges[emitterId] = eventRanges[emitterId].Extend(evt.Time, evt.Time);
                    }

                    foreach ((string emitterId, var range) in eventRanges)
                    {
                        if (!eventBufferStates.ContainsKey(emitterId))
                        {
                            eventBufferStates[emitterId] = new InfluxBufferState(extractor.State.GetEmitterState(emitterId));
                            eventBufferStates[emitterId].InitExtractedRange(TimeRange.Empty.First, TimeRange.Empty.Last);
                        }
                        eventBufferStates[emitterId].UpdateDestinationRange(range.First, range.Last);
                    }

                    if (config.InfluxStateStore)
                    {
                        await extractor.StateStorage.StoreExtractionState(eventBufferStates.Values,
                            fullConfig.StateStorage.InfluxEventStore, token).ConfigureAwait(false);
                    }
                    if (any)
                    {
                        anyEvents = true;
                    }
                }
                else
                {
                    log.Warning("Influxpusher is failing, events will not be buffered in influxdb");
                }
            }

            if (!string.IsNullOrEmpty(config.EventPath))
            {
                try
                {
                    await Task.Run(() => WriteEventsToFile(config.EventPath, events, extractor, token));
                    fileAnyEvents |= events.Any();
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to write events to file");
                    success = false;
                }
            }

            return success;
        }
        /// <summary>
        /// Read events from storage locations into given list of pushers
        /// </summary>
        /// <param name="pushers">Pushers to write to</param>
        /// <returns>True on success</returns>
        public async Task<bool> ReadEvents(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (pushers == null) throw new ArgumentNullException(nameof(pushers));
            bool success = true;

            if (config.Influx && influxPusher != null)
            {
                var activeStates = eventBufferStates.Where(kvp =>
                        !kvp.Value.Historizing
                        && kvp.Value.DestinationExtractedRange.First <= kvp.Value.DestinationExtractedRange.Last)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                if (activeStates.Any())
                {
                    try
                    {
                        var events = await influxPusher.ReadEvents(activeStates, token);

                        log.Information("Read {cnt} events from influxdb failure buffer", events.Count());
                        await Task.WhenAll(pushers
                            .Where(pusher => !(pusher is InfluxPusher))
                            .Select(pusher => pusher.PushEvents(events, token)));

                        foreach (var state in activeStates)
                        {
                            state.Value.ClearRanges();
                        }

                        if (config.InfluxStateStore)
                        {
                            await extractor.StateStorage.StoreExtractionState(activeStates.Values,
                                fullConfig.StateStorage.InfluxEventStore, token).ConfigureAwait(false);
                        }
                        anyEvents = false;
                    }
                    catch (Exception e)
                    {
                        success = false;
                        Log.Error(e, "Failed to read events from influxdb");
                    }
                }
                else if (anyEvents)
                {
                    log.Warning("No active event states, but anyEvents is set to true");
                    //anyEvents = false;
                }
            }

            if (!string.IsNullOrEmpty(config.EventPath))
            {
                success &= await ReadEventsFromFile(pushers, token);
            }

            return success;

        }
        /// <summary>
        /// Read datapoints from binary file into given list of pushers
        /// </summary>
        /// <param name="pushers">Pushers to write to</param>
        /// <returns>True on success</returns>
        private async Task<bool> ReadDatapointsFromFile(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            long nextPos = 0;
            bool success = true;

            do
            {
                IEnumerable<BufferedDataPoint> points;
                try
                {
                    (points, nextPos) =
                        await Task.Run(() => ReadDatapointsFromFile(config.DatapointPath, nextPos, 100000, token));
                    foreach (var pusher in pushers)
                    {
                        success &= await pusher.PushDataPoints(points, token) ?? true;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to read datapoints from file");
                    success = false;
                    break;
                }

                if (!success) break;
                var ranges = new Dictionary<string, TimeRange>();

                foreach (var point in points)
                {
                    if (!ranges.ContainsKey(point.Id))
                    {
                        ranges[point.Id] = new TimeRange(point.Timestamp, point.Timestamp);
                        continue;
                    }

                    ranges[point.Id] = ranges[point.Id].Extend(point.Timestamp, point.Timestamp);
                }

                foreach (var kvp in ranges)
                {
                    var state = extractor.State.GetNodeState(kvp.Key);
                    state.UpdateDestinationRange(kvp.Value.First, kvp.Value.Last);
                }

            } while (nextPos > 0);

            if (!success) return false;

            log.Information("Wipe datapoint buffer file");
            File.Create(config.DatapointPath).Close();
            fileAnyPoints = false;
            numPointsInBuffer.Set(0);

            return true;
        }
        /// <summary>
        /// Read events from binary file into given list of pushers
        /// </summary>
        /// <param name="pushers">Pushers to write to</param>
        /// <returns>True on success</returns>
        private async Task<bool> ReadEventsFromFile(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            long nextPos = 0;
            bool success = true;

            do
            {
                IEnumerable<BufferedEvent> events;
                try
                {
                    (events, nextPos) = await Task.Run(() => 
                        ReadEventsFromFile(config.EventPath, extractor, nextPos, 10000, token));

                    foreach (var pusher in pushers)
                    {
                        success &= await pusher.PushEvents(events, token) ?? true;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to read events from file");
                    success = false;
                    break;
                }

                if (!success) break;
                var ranges = new Dictionary<NodeId, TimeRange>();

                foreach (var evt in events)
                {
                    if (!ranges.ContainsKey(evt.EmittingNode))
                    {
                        ranges[evt.EmittingNode] = new TimeRange(evt.Time, evt.Time);
                        continue;
                    }

                    ranges[evt.EmittingNode] = ranges[evt.EmittingNode].Extend(evt.Time, evt.Time);
                }

                foreach (var kvp in ranges)
                {
                    var state = extractor.State.GetEmitterState(kvp.Key);
                    state.UpdateDestinationRange(kvp.Value.First, kvp.Value.Last);
                }

            } while (nextPos > 0);

            if (!success) return false;

            log.Information("Wipe event buffer file");
            File.Create(config.EventPath).Close();
            fileAnyEvents = false;
            numEventsInBuffer.Set(0);

            return true;
        }
        /// <summary>
        /// Write datapoints to a binary file
        /// </summary>
        /// <param name="file">File to write to</param>
        /// <param name="dps">Datapoints to write</param>
        public static void WriteDatapointsToFile(string file, IEnumerable<BufferedDataPoint> dps, CancellationToken token)
        {
            if (dps == null) throw new ArgumentNullException(nameof(dps));
            if (file == null) throw new ArgumentNullException(nameof(file));
            using var fs = new FileStream(file, FileMode.Append, FileAccess.Write, FileShare.None);

            int count = 0;

            foreach (var dp in dps)
            {
                if (token.IsCancellationRequested) break;
                var bytes = dp.ToStorableBytes();
                fs.Write(bytes, 0, bytes.Length);
                count++;
            }
            if (count > 0)
            {
                log.Debug("Write {cnt} points to file", count);
                numPointsInBuffer.Inc(count);
            }
            fs.Flush();
        }
        /// <summary>
        /// Write events to a binary file
        /// </summary>
        /// <param name="file">File to write to</param>
        /// <param name="evts">Events to write</param>
        /// <param name="extractor">Extractor, used to map NodeIds</param>
        public static void WriteEventsToFile(string file, IEnumerable<BufferedEvent> evts, UAExtractor extractor, CancellationToken token)
        {
            if (evts == null) throw new ArgumentNullException(nameof(evts));
            if (file == null) throw new ArgumentNullException(nameof(file));
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            using var fs = new FileStream(file, FileMode.Append, FileAccess.Write, FileShare.None);

            int count = 0;

            foreach (var evt in evts)
            {
                if (token.IsCancellationRequested) break;
                var bytes = evt.ToStorableBytes(extractor);
                fs.Write(bytes, 0, bytes.Length);
                count++;
            }

            if (count > 0)
            {
                log.Debug("Write {cnt} events to file", count);
                numEventsInBuffer.Inc();
            }
            fs.Flush();
        }
        /// <summary>
        /// Read datapoints from a binary file. Reads straight from file stream into datapoints
        /// </summary>
        /// <param name="file">File to read from</param>
        /// <param name="startPos">Start position in file</param>
        /// <param name="limit">Maximum number of datapoints to read</param>
        /// <returns>List of datapoints and new position in file</returns>
        public static (IEnumerable<BufferedDataPoint> dps, long pos) ReadDatapointsFromFile(string file, long startPos, int limit,
            CancellationToken token)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));
            var dps = new List<BufferedDataPoint>();
            int count = 0;
            long pos;
            bool final;
            using (var fs = new FileStream(file, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None))
            {
                var sizeBytes = new byte[sizeof(ushort)];
                fs.Seek(startPos, SeekOrigin.Begin);
                while (!token.IsCancellationRequested && count < limit)
                {
                    int read = fs.Read(sizeBytes, 0, sizeBytes.Length);
                    if (read < sizeBytes.Length) break;
                    ushort size = BitConverter.ToUInt16(sizeBytes, 0);
                    var dataBytes = new byte[size];
                    int dRead = fs.Read(dataBytes, 0, size);
                    if (dRead < size) break;
                    var (buffDp, _) = BufferedDataPoint.FromStorableBytes(dataBytes, 0);
                    if (buffDp.Id == null)
                    {
                        log.Warning("Invalid datapoint in buffer file");
                        continue;
                    }

                    count++;
                    log.Verbose(buffDp.ToDebugDescription());
                    dps.Add(buffDp);
                }

                pos = fs.Position;
                final = pos == fs.Length;
                if (count == 0)
                {
                    log.Verbose("Read 0 point from file");
                }
                else
                {
                    log.Debug("Read {NumDatapointsToRead} points from file", count);
                }
                fs.Flush();
            }

            if (final || dps.Count < limit)
            {
                pos = 0;
            }


            return (dps, pos);
        }
        /// <summary>
        /// Read events from binary file
        /// </summary>
        /// <param name="file">File to read from</param>
        /// <param name="extractor">Extractor, used for NodeId transformations</param>
        /// <param name="startPos">Position to start reading from</param>
        /// <param name="limit">Maximum number of events to read</param>
        /// <returns>List of events and new position in file</returns>
        public static (IEnumerable<BufferedEvent> events, long pos) ReadEventsFromFile(string file,
            UAExtractor extractor, long startPos, int limit, CancellationToken token)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));
            var evts = new List<BufferedEvent>();
            int count = 0;
            long pos;
            bool final;
            using (FileStream fs = new FileStream(file, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None))
            {
                fs.Seek(startPos, SeekOrigin.Begin);
                byte[] sizeBytes = new byte[sizeof(ushort)];
                while (!token.IsCancellationRequested && count < limit)
                {
                    int read = fs.Read(sizeBytes, 0, sizeBytes.Length);
                    if (read < sizeBytes.Length) break;
                    ushort size = BitConverter.ToUInt16(sizeBytes, 0);
                    byte[] dataBytes = new byte[size];
                    int dRead = fs.Read(dataBytes, 0, size);
                    if (dRead < size) break;
                    var (evt, _) = BufferedEvent.FromStorableBytes(dataBytes, extractor, 0);
                    if (evt.EventId == null || evt.SourceNode == null)
                    {
                        log.Warning("Invalid event in buffer file");
                        continue;
                    }

                    count++;
                    log.Verbose(evt.ToDebugDescription());
                    evts.Add(evt);
                }
                if (count > 0)
                {
                    log.Debug("Read {NumEventsToRead} events from file", count);
                }
                pos = fs.Position;
                final = pos == fs.Length;
                fs.Flush();
            }

            if (final || count < limit)
            {
                pos = 0;
            }

            return (evts, pos);
        }
        public void Dispose()
        {
            influxPusher?.Dispose();
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Prometheus.Client;
using Serilog;

namespace Cognite.OpcUa
{
    public sealed class FailureBuffer : IDisposable
    {
        private readonly InfluxPusher influxPusher;
        private readonly FailureBufferConfig config;
        private readonly Extractor extractor;

        private readonly Dictionary<string, InfluxBufferState> nodeBufferStates;
        private readonly Dictionary<string, InfluxBufferState> eventBufferStates;

        private readonly bool useLocalQueue;

        public bool Any => anyPoints || fileAnyPoints || useLocalQueue && extractor.StateStorage.AnyPoints;
        private bool fileAnyPoints;
        private bool anyPoints;
        public bool AnyEvents => anyEvents || fileAnyEvents || useLocalQueue && extractor.StateStorage.AnyEvents;
        private bool fileAnyEvents;
        private bool anyEvents;

        private static readonly Gauge numPointsInBuffer = Metrics.CreateGauge(
            "opcua_buffer_num_points", "The number of datapoints in the local buffer file");

        private static readonly Gauge numEventsInBuffer = Metrics.CreateGauge(
            "opcua_buffer_num_events", "The number of events in the local buffer file");

        private static readonly ILogger log = Log.Logger.ForContext(typeof(FailureBuffer));
        public FailureBuffer(FullConfig fullConfig, Extractor extractor)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            if (fullConfig == null) throw new ArgumentNullException(nameof(fullConfig));
            config = fullConfig.FailureBuffer;

            useLocalQueue = extractor.StateStorage != null && config.LocalQueue;
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

            if (config.Influx?.Database == null) return;

            nodeBufferStates = new Dictionary<string, InfluxBufferState>();
            eventBufferStates = new Dictionary<string, InfluxBufferState>();

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
            var connTest = influxPusher.TestConnection(fullConfig, CancellationToken.None);
            connTest.Wait();
            if (connTest.Result == null || !connTest.Result.Value)
            {
                throw new ExtractorFailureException("Failed to connect to buffer influxdb");
            }
        }

        public async Task InitializeBufferStates(IEnumerable<NodeExtractionState> states,
            IEnumerable<NodeId> nodeIds, CancellationToken token)
        {
            if (config.Influx != null && influxPusher != null && config.Influx.StateStorage)
            {
                var variableStates = states
                    .Where(state => !state.Historizing)
                    .Select(state => new InfluxBufferState(state, false))
                    .ToList();

                foreach (var state in variableStates)
                {
                    state.DestinationExtractedRange.Start = DateTime.MinValue;
                    state.DestinationExtractedRange.End = DateTime.MaxValue;
                }

                await extractor.StateStorage.ReadExtractionStates(variableStates, StateStorage.InfluxVariableStates,
                    false, token);

                foreach (var state in variableStates)
                {
                    if (!state.StatePersisted) continue;
                    nodeBufferStates[extractor.GetUniqueId(state.Id)] = state;
                    if (state.DestinationExtractedRange.Start <= state.DestinationExtractedRange.End)
                    {
                        anyPoints = true;
                    }
                }

                var eventStates = nodeIds.Select(id => new InfluxBufferState(id)).ToList();

                await extractor.StateStorage.ReadExtractionStates(eventStates,
                    StateStorage.InfluxEventStates, false,
                    token);

                foreach (var state in eventStates)
                {
                    state.DestinationExtractedRange.Start = DateTime.MinValue;
                    state.DestinationExtractedRange.End = DateTime.MaxValue;
                }

                foreach (var state in eventStates)
                {
                    if (state.StatePersisted)
                    {
                        eventBufferStates[extractor.GetUniqueId(state.Id)] = state;
                        if (state.DestinationExtractedRange.Start < state.DestinationExtractedRange.End)
                        {
                            anyEvents = true;
                        }
                    }
                }
            }
        }

        public async Task<bool> WriteDatapoints(IEnumerable<BufferedDataPoint> points, IDictionary<string, TimeRange> pointRanges,
            IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (points == null || !points.Any() || pushers == null || !pushers.Any() || pointRanges == null
                || !pointRanges.Any()) return true;

            points = points.GroupBy(pt => pt.Id)
                .Where(group => !extractor.GetNodeState(group.Key).Historizing)
                .SelectMany(group => group)
                .ToList();

            bool success = true;

            if (config.Influx != null && influxPusher != null)
            {
                try
                {
                    if (config.Influx.Write)
                    {
                        var result = await influxPusher.PushDataPoints(points, token);
                        success = result.GetValueOrDefault();
                        if (success)
                        {
                            log.Information("Inserted {cnt} points into influxdb failure buffer", points.Count());
                        }
                    }
                    if (success && !influxPusher.DataFailing)
                    {
                        foreach ((string key, var value) in pointRanges)
                        {
                            if (!nodeBufferStates.ContainsKey(key))
                            {
                                var state = extractor.GetNodeState(key);
                                if (state.Historizing) continue;
                                nodeBufferStates[key] = new InfluxBufferState(state, false);
                            }
                            nodeBufferStates[key].UpdateDestinationRange(value);
                        }
                        if (config.Influx.StateStorage)
                        {
                            await extractor.StateStorage.StoreExtractionState(nodeBufferStates.Values,
                                StateStorage.InfluxVariableStates, token).ConfigureAwait(false);
                        }

                        anyPoints = true;
                    }
                }
                catch (Exception e)
                {
                    success = false;
                    log.Error(e, "Failed to insert into influxdb buffer");
                }
            }

            if (useLocalQueue)
            {
                success &= await extractor.StateStorage.WritePointsToQueue(points, token);
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
                    Log.Error(ex, "Failed to write datapoints to file");
                    success = false;
                }
            }

            return success;
        }
        public async Task<bool> ReadDatapoints(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (pushers == null) throw new ArgumentNullException(nameof(pushers));
            bool success = true;

            if (config.Influx != null && influxPusher != null)
            {
                var activeStates = nodeBufferStates.Where(kvp =>
                        !kvp.Value.Historizing
                        && kvp.Value.DestinationExtractedRange.End >= kvp.Value.DestinationExtractedRange.Start)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                if (activeStates.Any())
                {
                    try
                    {
                        var dps = await influxPusher.ReadDataPoints(activeStates, token);
                        log.Information("Read {cnt} points from influxdb failure buffer", dps.Count());
                        await Task.WhenAll(pushers
                            .Where(pusher =>
                                !(pusher.BaseConfig is InfluxClientConfig ifc)
                                || ifc.Host != config.Influx.Host
                                || ifc.Database != config.Influx.Database)
                            .Select(pusher => pusher.PushDataPoints(dps, token)));

                        foreach (var state in activeStates)
                        {
                            state.Value.ClearRanges();
                        }

                        if (config.Influx.StateStorage)
                        {
                            await extractor.StateStorage.StoreExtractionState(activeStates.Values,
                                StateStorage.InfluxVariableStates, token).ConfigureAwait(false);
                        }

                        anyPoints = false;
                    }
                    catch (Exception e)
                    {
                        success = false;
                        Log.Error(e, "Failed to read points from influxdb");
                    }
                }
            }

            if (useLocalQueue && extractor.StateStorage.AnyPoints)
            {
                success &= await extractor.StateStorage.ReadPointsFromQueue(pushers, token);
            }

            if (!string.IsNullOrEmpty(config.DatapointPath))
            {
                success &= await ReadDatapointsFromFile(pushers, token);
            }

            return success;
        }

        public async Task<bool> WriteEvents(IEnumerable<BufferedEvent> events,
            IEnumerable<IPusher> pushers,
            CancellationToken token)
        {
            if (events == null || !events.Any() || pushers == null || !pushers.Any()) return true;

            events = events.GroupBy(evt => evt.EmittingNode)
                .Where(group => !extractor.GetEmitterState(group.Key).Historizing)
                .SelectMany(group => group)
                .ToList();

            bool success = true;

            if (config.Influx != null && influxPusher != null)
            {
                try
                {
                    if (config.Influx.Write)
                    {
                        var result = await influxPusher.PushEvents(events, token);
                        success = result.GetValueOrDefault();
                        if (success)
                        {
                            log.Information("Inserted {cnt} events into influxdb failure buffer", events.Count());
                        }
                    }

                    if (success && !influxPusher.EventsFailing)
                    {
                        var eventRanges = new Dictionary<string, TimeRange>();
                        foreach (var evt in events)
                        {
                            var sourceId = extractor.GetUniqueId(evt.SourceNode);
                            if (!eventRanges.ContainsKey(sourceId))
                            {
                                eventRanges[sourceId] = new TimeRange(evt.Time, evt.Time);
                                continue;
                            }

                            var range = eventRanges[sourceId];
                            if (evt.Time > range.End)
                            {
                                range.End = evt.Time;
                            }
                            else if (evt.Time < range.Start)
                            {
                                range.Start = evt.Time;
                            }
                        }

                        foreach ((string sourceId, var range) in eventRanges)
                        {
                            if (!eventBufferStates.ContainsKey(sourceId))
                            {
                                eventBufferStates[sourceId] = new InfluxBufferState(extractor.ExternalToNodeId[sourceId]);
                            }
                            eventBufferStates[sourceId].UpdateDestinationRange(range);
                        }

                        if (config.Influx.StateStorage)
                        {
                            await extractor.StateStorage.StoreExtractionState(eventBufferStates.Values,
                                StateStorage.InfluxEventStates, token).ConfigureAwait(false);
                        }

                        anyEvents = true;
                    }
                }
                catch (Exception e)
                {
                    success = false;
                    log.Error(e, "Failed to insert events into influxdb buffer");
                }
            }

            if (useLocalQueue)
            {
                success &= await extractor.StateStorage.WriteEventsToQueue(events, token);
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

        public async Task<bool> ReadEvents(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            if (pushers == null) throw new ArgumentNullException(nameof(pushers));
            bool success = true;

            if (config.Influx != null && influxPusher != null)
            {
                var activeStates = eventBufferStates.Where(kvp =>
                        !kvp.Value.Historizing
                        && kvp.Value.DestinationExtractedRange.End >= kvp.Value.DestinationExtractedRange.Start)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                if (activeStates.Any())
                {
                    try
                    {
                        var events = await influxPusher.ReadEvents(activeStates, token);
                        log.Information("Read {cnt} events from influxdb failure buffer", events.Count());
                        await Task.WhenAll(pushers
                            .Where(pusher =>
                                !(pusher.BaseConfig is InfluxClientConfig ifc)
                                || ifc.Host != config.Influx.Host
                                || ifc.Database != config.Influx.Database)
                            .Select(pusher => pusher.PushEvents(events, token)));

                        foreach (var state in activeStates)
                        {
                            state.Value.ClearRanges();
                        }

                        if (config.Influx.StateStorage)
                        {
                            await extractor.StateStorage.StoreExtractionState(activeStates.Values,
                                StateStorage.InfluxEventStates, token).ConfigureAwait(false);
                        }
                        anyEvents = false;
                    }
                    catch (Exception e)
                    {
                        success = false;
                        Log.Error(e, "Failed to read events from influxdb");
                    }
                }
            }

            if (useLocalQueue && extractor.StateStorage.AnyEvents)
            {
                success &= await extractor.StateStorage.ReadEventsFromQueue(pushers, token);
            }

            if (!string.IsNullOrEmpty(config.EventPath))
            {
                success &= await ReadEventsFromFile(pushers, token);
            }

            return success;

        }

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

                    var range = ranges[point.Id];
                    if (range.Start > point.Timestamp)
                    {
                        range.Start = point.Timestamp;
                    }
                    else if (range.End < point.Timestamp)
                    {
                        range.End = point.Timestamp;
                    }
                }

                foreach (var kvp in ranges)
                {
                    var state = extractor.GetNodeState(kvp.Key);
                    state.UpdateDestinationRange(kvp.Value);
                }

            } while (nextPos > 0);

            if (success)
            {
                log.Information("Wipe datapoint buffer file");
                File.Create(config.DatapointPath).Close();
                fileAnyPoints = false;
                numPointsInBuffer.Set(0);
            }

            return success;
        }

        private async Task<bool> ReadEventsFromFile(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            long nextPos = 0;
            bool success = true;

            do
            {
                IEnumerable<BufferedEvent> events;
                try
                {
                    (events, nextPos) =
                        await Task.Run(() => ReadEventsFromFile(config.EventPath, extractor, nextPos, 10000, token));
                    foreach (var pusher in pushers)
                    {
                        success &= await pusher.PushEvents(events, token) ?? true;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to read datapoints from file");
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

                    var range = ranges[evt.EmittingNode];
                    if (range.Start > evt.Time)
                    {
                        range.Start = evt.Time;
                    }
                    else if (range.End < evt.Time)
                    {
                        range.End = evt.Time;
                    }
                }

                foreach (var kvp in ranges)
                {
                    var state = extractor.GetEmitterState(kvp.Key);
                    state.UpdateDestinationRange(kvp.Value);
                }

            } while (nextPos > 0);

            if (success)
            {
                log.Information("Wipe event buffer file");
                File.Create(config.EventPath).Close();
                fileAnyEvents = false;
                numEventsInBuffer.Set(0);
            }

            return success;
        }

        public static void WriteDatapointsToFile(string file, IEnumerable<BufferedDataPoint> dps, CancellationToken token)
        {
            if (dps == null) throw new ArgumentNullException(nameof(dps));
            if (file == null) throw new ArgumentNullException(nameof(file));
            using FileStream fs = new FileStream(file, FileMode.Append, FileAccess.Write, FileShare.None);

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

        public static void WriteEventsToFile(string file, IEnumerable<BufferedEvent> evts, Extractor extractor, CancellationToken token)
        {
            if (evts == null) throw new ArgumentNullException(nameof(evts));
            if (file == null) throw new ArgumentNullException(nameof(file));
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            using FileStream fs = new FileStream(file, FileMode.Append, FileAccess.Write, FileShare.None);

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

        public static (IEnumerable<BufferedDataPoint>, long) ReadDatapointsFromFile(string file, long startPos, int limit,
            CancellationToken token)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));
            var dps = new List<BufferedDataPoint>();
            int count = 0;
            long pos;
            bool final = false;
            using (FileStream fs = new FileStream(file, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None))
            {
                byte[] sizeBytes = new byte[sizeof(ushort)];
                fs.Seek(startPos, SeekOrigin.Begin);
                while (!token.IsCancellationRequested && count < limit)
                {
                    int read = fs.Read(sizeBytes, 0, sizeBytes.Length);
                    if (read < sizeBytes.Length) break;
                    ushort size = BitConverter.ToUInt16(sizeBytes, 0);
                    byte[] dataBytes = new byte[size];
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

        public static (IEnumerable<BufferedEvent>, long) ReadEventsFromFile(string file,
            Extractor extractor, long startPos, int limit, CancellationToken token)
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
                        log.Warning("Invalid datapoint in buffer file");
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

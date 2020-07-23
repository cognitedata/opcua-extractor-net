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

            if (!points.Any()) return true;

            log.Information("Push {cnt} points to failurebuffer", points.Count());

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
                            if (!nodeBufferStates.TryGetValue(key, out var bufferState))
                            {
                                var state = extractor.State.GetNodeState(key);
                                if (state.FrontfillEnabled) continue;
                                nodeBufferStates[key] = bufferState = new InfluxBufferState(state);
                                bufferState.InitExtractedRange(TimeRange.Empty.First, TimeRange.Empty.Last);
                            }
                            bufferState.UpdateDestinationRange(value.First, value.Last);
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

            if (!events.Any()) return true;

            log.Information("Push {cnt} events to failurebuffer", events.Count());

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
                        any = true;
                        if (!eventRanges.TryGetValue(emitterId, out var range))
                        {
                            eventRanges[emitterId] = new TimeRange(evt.Time, evt.Time);
                            continue;
                        }
                        eventRanges[emitterId] = range.Extend(evt.Time, evt.Time);
                    }

                    foreach ((string emitterId, var range) in eventRanges)
                    {
                        if (!eventBufferStates.TryGetValue(emitterId, out var bufferState))
                        {
                            eventBufferStates[emitterId] = bufferState = new InfluxBufferState(extractor.State.GetEmitterState(emitterId));
                            bufferState.InitExtractedRange(TimeRange.Empty.First, TimeRange.Empty.Last);
                        }
                        bufferState.UpdateDestinationRange(range.First, range.Last);
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
            bool success = true;
            bool final = false;

            using (var stream = new FileStream(config.DatapointPath, FileMode.OpenOrCreate, FileAccess.Read))
            {
                do
                {
                    try
                    {
                        var points = new List<BufferedDataPoint>();

                        int count = 0;
                        while (!token.IsCancellationRequested && count < 1_000_000)
                        {
                            var dp = BufferedDataPoint.FromStream(stream);
                            if (dp == null)
                            {
                                final = true;
                                break;
                            }
                            points.Add(dp);
                        }
                        log.Information("Read {cnt} datapoints from file", points.Count);
                        if (!points.Any()) break;

                        var results = await Task.WhenAll(pushers.Select(pusher => pusher.PushDataPoints(points, token)));

                        success &= results.All(result => result ?? true);
                        if (!success) break;

                        var ranges = new Dictionary<string, TimeRange>();

                        foreach (var point in points)
                        {
                            if (!ranges.TryGetValue(point.Id, out var range))
                            {
                                ranges[point.Id] = new TimeRange(point.Timestamp, point.Timestamp);
                                continue;
                            }

                            ranges[point.Id] = range.Extend(point.Timestamp, point.Timestamp);
                        }

                        foreach (var kvp in ranges)
                        {
                            var state = extractor.State.GetNodeState(kvp.Key);
                            state.UpdateDestinationRange(kvp.Value.First, kvp.Value.Last);
                        }
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, "Failed to read datapoints from file");
                        success = false;
                        break;
                    }
                } while (!final && !token.IsCancellationRequested);
            }

            if (token.IsCancellationRequested) return true;

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
            bool success = true;
            bool final = false;

            using (var stream = new FileStream(config.EventPath, FileMode.OpenOrCreate, FileAccess.Read))
            {
                do
                {
                    try
                    {
                        var events = new List<BufferedEvent>();

                        int count = 0;
                        while (!token.IsCancellationRequested && count < 10_000)
                        {
                            var evt = BufferedEvent.FromStream(stream, extractor);
                            if (evt == null)
                            {
                                final = true;
                                break;
                            }
                            events.Add(evt);
                        }
                        log.Information("Read {cnt} events from file", events.Count);

                        var results = await Task.WhenAll(pushers.Select(pusher => pusher.PushEvents(events, token)));

                        success &= results.All(result => result ?? true);
                        if (!success) break;

                        var ranges = new Dictionary<NodeId, TimeRange>();

                        foreach (var evt in events)
                        {
                            if (!ranges.TryGetValue(evt.EmittingNode, out var range))
                            {
                                ranges[evt.EmittingNode] = new TimeRange(evt.Time, evt.Time);
                                continue;
                            }

                            ranges[evt.EmittingNode] = range.Extend(evt.Time, evt.Time);
                        }

                        foreach (var kvp in ranges)
                        {
                            var state = extractor.State.GetEmitterState(kvp.Key);
                            state.UpdateDestinationRange(kvp.Value.First, kvp.Value.Last);
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Failed to read events from file");
                        success = false;
                        break;
                    }
                } while (!final && !token.IsCancellationRequested);
            }

            if (token.IsCancellationRequested) return true;

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
        public void Dispose()
        {
            influxPusher?.Dispose();
        }
    }
}

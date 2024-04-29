/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

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

using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Utility for various ways of storing datapoints and events if the connection to a destination goes down.
    /// </summary>
    public sealed class FailureBuffer
    {
        private readonly FailureBufferConfig config;
        private readonly FullConfig fullConfig;
        private readonly UAExtractor extractor;

        public bool AnyPoints => fileAnyPoints;
        private bool fileAnyPoints;
        public bool AnyEvents => fileAnyEvents;
        private bool fileAnyEvents;

        private static readonly Gauge numPointsInBuffer = Metrics.CreateGauge(
            "opcua_buffer_num_points", "The number of datapoints in the local buffer file");

        private static readonly Gauge numEventsInBuffer = Metrics.CreateGauge(
            "opcua_buffer_num_events", "The number of events in the local buffer file");

        private readonly ILogger<FailureBuffer> log;
        /// <summary>
        /// Constructor. This checks whether any points or events exists in the buffer files
        /// and creates files if they do not exist.
        /// </summary>
        /// <param name="fullConfig"></param>
        /// <param name="extractor"></param>
        public FailureBuffer(ILogger<FailureBuffer> log, FullConfig fullConfig, UAExtractor extractor)
        {
            this.log = log;
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
        }

        /// <summary>
        /// Write datapoints to enabled buffer locations.
        /// </summary>
        /// <param name="points">Datapoints to write</param>
        /// <param name="pointRanges">Ranges for given data variables, to simplify storage and state</param>
        /// <returns>True on success</returns>
        public async Task<bool> WriteDatapoints(IEnumerable<UADataPoint> points, IDictionary<string, TimeRange> pointRanges, CancellationToken token)
        {
            if (points == null || !points.Any() || pointRanges == null || !pointRanges.Any() || fullConfig.DryRun) return true;

            points = points.GroupBy(pt => pt.Id)
                .Where(group => !extractor.State.GetNodeState(group.Key)?.FrontfillEnabled ?? false)
                .SelectMany(group => group)
                .ToList();

            if (!points.Any()) return true;

            log.LogInformation("Push {Count} points to failurebuffer", points.Count());

            if (!string.IsNullOrEmpty(config.DatapointPath))
            {
                try
                {
                    await Task.Run(() => WriteDatapointsToFile(points, token), CancellationToken.None);
                    fileAnyPoints |= points.Any();
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to write datapoints to file");
                    return false;
                }
            }

            return true;
        }
        /// <summary>
        /// Read datapoints from storage locations into given list of pushers
        /// </summary>
        /// <param name="pushers">Pushers to write to</param>
        /// <returns>True on success</returns>
        public async Task<bool> ReadDatapoints(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            bool success = true;

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
        /// <returns>True on success</returns>
        public async Task<bool> WriteEvents(IEnumerable<UAEvent> events, CancellationToken token)
        {
            if (events == null || !events.Any() || fullConfig.DryRun) return true;

            events = events.GroupBy(evt => evt.EmittingNode)
                .Where(group => !extractor.State.GetEmitterState(group.Key)?.FrontfillEnabled ?? false)
                .SelectMany(group => group)
                .ToList();

            if (!events.Any()) return true;

            log.LogInformation("Push {Count} events to failurebuffer", events.Count());

            if (!string.IsNullOrEmpty(config.EventPath))
            {
                try
                {
                    await Task.Run(() => WriteEventsToFile(events, token), CancellationToken.None);
                    fileAnyEvents |= events.Any();
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to write events to file");
                    return false;
                }
            }

            return true;
        }
        /// <summary>
        /// Read events from storage locations into given list of pushers
        /// </summary>
        /// <param name="pushers">Pushers to write to</param>
        /// <returns>True on success</returns>
        public async Task<bool> ReadEvents(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            bool success = true;

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
            bool final = false;

            try
            {
                using var stream = new FileStream(config.DatapointPath, FileMode.OpenOrCreate, FileAccess.Read);
                do
                {
                    var points = new List<UADataPoint>();

                    int count = 0;
                    while (!token.IsCancellationRequested && count < 1_000_000)
                    {
                        var dp = UADataPoint.FromStream(stream);
                        if (dp == null)
                        {
                            final = true;
                            break;
                        }
                        points.Add(dp);
                    }
                    points = points
                        .GroupBy(point => point.Id)
                        .Where(group => extractor.State.GetNodeState(group.Key) != null)
                        .SelectMany(group => group).ToList();

                    log.LogInformation("Read {Count} datapoints from file", points.Count);
                    if (points.Count == 0 && final) break;

                    var results = await Task.WhenAll(pushers.Select(pusher => pusher.PushDataPoints(points, token)));

                    if (!results.All(result => result ?? true)) return false;

                    var ranges = points
                        .GroupBy(point => point.Id)
                        .Select(group => (Id: group.Key, Range: group.MinMax(pt => pt.Timestamp)));

                    foreach (var group in ranges)
                    {
                        var state = extractor.State.GetNodeState(group.Id);
                        if (state == null) continue;
                        if (extractor.AllowUpdateState) state.UpdateDestinationRange(group.Range.Min, group.Range.Max);
                    }

                } while (!final && !token.IsCancellationRequested);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to read datapoints from file");
                return false;
            }

            if (token.IsCancellationRequested) return true;

            log.LogInformation("Wipe datapoint buffer file");
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
            bool final = false;

            try
            {
                using var stream = new FileStream(config.EventPath, FileMode.OpenOrCreate, FileAccess.Read);
                do
                {
                    var events = new List<UAEvent>();

                    int count = 0;
                    while (!token.IsCancellationRequested && count < 10_000)
                    {
                        var evt = UAEvent.FromStream(stream, extractor);
                        if (evt == null)
                        {
                            final = true;
                            break;
                        }
                        events.Add(evt);
                    }

                    log.LogInformation("Read {Count} raw events", events.Count);

                    events = events
                        .Where(evt => evt.EmittingNode != null && !evt.EmittingNode.IsNullNodeId)
                        .ToList();

                    log.LogInformation("Read {Count} events from file", events.Count);
                    if (events.Count == 0 && final) break;

                    var results = await Task.WhenAll(pushers.Select(pusher => pusher.PushEvents(events, token)));

                    if (!results.All(result => result ?? true)) return false;

                    var ranges = events
                        .GroupBy(evt => evt.EmittingNode)
                        .Select(group => (Id: group.Key, Range: group.MinMax(evt => evt.Time)));

                    foreach (var group in ranges)
                    {
                        var state = extractor.State.GetEmitterState(group.Id);
                        if (state == null) continue;
                        if (extractor.AllowUpdateState) state.UpdateDestinationRange(group.Range.Min, group.Range.Max);
                    }

                } while (!final && !token.IsCancellationRequested);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to read events from file");
                return false;
            }


            if (token.IsCancellationRequested) return true;

            log.LogInformation("Wipe event buffer file");
            File.Create(config.EventPath).Close();
            fileAnyEvents = false;
            numEventsInBuffer.Set(0);

            return true;
        }
        /// <summary>
        /// Write datapoints to a binary file
        /// </summary>
        /// <param name="dps">Datapoints to write</param>
        private void WriteDatapointsToFile(IEnumerable<UADataPoint> dps, CancellationToken token)
        {
            int count = 0;

            long currentSize = 0;
            if (config.MaxBufferSize > 0)
            {
                currentSize = new FileInfo(config.DatapointPath).Length;
            }

            using (var fs = new FileStream(config.DatapointPath, FileMode.Append, FileAccess.Write, FileShare.None))
            {
                foreach (var dp in dps)
                {
                    if (token.IsCancellationRequested) break;
                    var bytes = dp.ToStorableBytes();
                    if (config.MaxBufferSize > 0 && (currentSize + bytes.Length > config.MaxBufferSize))
                    {
                        log.LogWarning("Not writing datapoints to buffer due to file size at limit");
                        break;
                    }
                    currentSize += bytes.Length;
                    fs.Write(bytes, 0, bytes.Length);
                    count++;
                }
                fs.Flush();
            }

            if (count > 0)
            {
                log.LogDebug("Write {Count} points to file", count);
                numPointsInBuffer.Inc(count);
            }
        }
        /// <summary>
        /// Write events to a binary file
        /// </summary>
        /// <param name="evts">Events to write</param>
        private void WriteEventsToFile(IEnumerable<UAEvent> evts, CancellationToken token)
        {
            int count = 0;

            long currentSize = 0;
            if (config.MaxBufferSize > 0)
            {
                currentSize = new FileInfo(config.EventPath).Length;
            }

            using (var fs = new FileStream(config.EventPath, FileMode.Append, FileAccess.Write, FileShare.None))
            {
                foreach (var evt in evts)
                {
                    if (token.IsCancellationRequested) break;
                    var bytes = evt.ToStorableBytes(extractor);
                    if (config.MaxBufferSize > 0 && (currentSize + bytes.Length > config.MaxBufferSize))
                    {
                        log.LogWarning("Not writing events to buffer due to file size at limit");
                        break;
                    }
                    currentSize += bytes.Length;
                    fs.Write(bytes, 0, bytes.Length);
                    count++;
                }
                fs.Flush();
            }

            if (count > 0)
            {
                log.LogDebug("Write {Count} events to file", count);
                numEventsInBuffer.Inc();
            }
        }
    }
}

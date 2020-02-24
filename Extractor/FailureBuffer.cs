using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
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

        public bool Any => any || useLocalQueue && extractor.StateStorage.AnyPoints;
        private bool any;
        public bool AnyEvents => anyEvents || useLocalQueue && extractor.StateStorage.AnyEvents;
        private bool anyEvents;

        private static readonly ILogger log = Log.Logger.ForContext(typeof(FailureBuffer));
        public FailureBuffer(FailureBufferConfig config, Extractor extractor)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            this.config = config ?? throw new ArgumentNullException(nameof(config));

            useLocalQueue = extractor.StateStorage != null && config.LocalQueue;
            this.extractor = extractor;

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
            var connTest = influxPusher.TestConnection(CancellationToken.None);
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
                    .Select(state => new InfluxBufferState(state, false));

                await extractor.StateStorage.ReadExtractionStates(variableStates, StateStorage.InfluxVariableStates,
                    false, token);
                foreach (var state in variableStates)
                {
                    if (state.StatePersisted)
                    {
                        nodeBufferStates[extractor.GetUniqueId(state.Id)] = state;
                    }
                }

                var eventStates = nodeIds.Select(id => new InfluxBufferState(id));

                await extractor.StateStorage.ReadExtractionStates(eventStates,
                    StateStorage.InfluxEventStates, false,
                    token);

                foreach (var state in eventStates)
                {
                    if (state.StatePersisted)
                    {
                        eventBufferStates[extractor.GetUniqueId(state.Id)] = state;
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

                    if (config.Influx.StateStorage && success && !influxPusher.DataFailing)
                    {
                        foreach ((string key, var value) in pointRanges)
                        {
                            if (!nodeBufferStates.ContainsKey(key))
                            {
                                nodeBufferStates[key] = new InfluxBufferState(extractor.GetNodeState(key), false);
                            }
                            nodeBufferStates[key].UpdateDestinationRange(value);
                        }

                        await extractor.StateStorage.StoreExtractionState(nodeBufferStates.Values,
                            StateStorage.InfluxVariableStates, token).ConfigureAwait(false);

                        any = true;
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

            return success;
        }
        public async Task<bool> ReadDatapoints(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            bool success = true;

            if (config.Influx != null && influxPusher != null)
            {
                var activeStates = nodeBufferStates.Where(kvp =>
                        !kvp.Value.Historizing
                        && kvp.Value.DestinationExtractedRange.End > kvp.Value.DestinationExtractedRange.Start)
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

                        any = false;
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

                    if (config.Influx.StateStorage && success && !influxPusher.EventsFailing)
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
                        await extractor.StateStorage.StoreExtractionState(eventBufferStates.Values,
                            StateStorage.InfluxEventStates, token).ConfigureAwait(false);

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

            return success;
        }

        public async Task<bool> ReadEvents(IEnumerable<IPusher> pushers, CancellationToken token)
        {
            bool success = true;

            if (config.Influx != null && influxPusher != null)
            {
                var activeStates = eventBufferStates.Where(kvp =>
                        !kvp.Value.Historizing
                        && kvp.Value.DestinationExtractedRange.End > kvp.Value.DestinationExtractedRange.Start)
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

            return success;

        }
        public void Dispose()
        {
            influxPusher?.Dispose();
        }
    }
}

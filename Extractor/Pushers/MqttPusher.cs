/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using Prometheus;
using Serilog;
using Cognite.Extensions;
using Opc.Ua;
using Cognite.OpcUa.Types;

namespace Cognite.OpcUa.Pushers
{
    public sealed class MQTTPusher : IPusher
    {
        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }
        public List<UANode> PendingNodes { get; } = new List<UANode>();
        public List<UAReference> PendingReferences { get; } = new List<UAReference>();
        public UAExtractor Extractor { get; set; }
        public IPusherConfig BaseConfig => config;
        private readonly MqttPusherConfig config;
        private readonly IMqttClient client;
        private readonly IMqttClientOptions options;

        private readonly ILogger log = Log.Logger.ForContext(typeof(MQTTPusher));

        private readonly MqttApplicationMessageBuilder baseBuilder;

        private readonly DateTime minDateTime = new DateTime(1971, 1, 1);

        private bool closed;

        private HashSet<string> existingNodes = new HashSet<string>();

        private Dictionary<NodeId, string> eventParents = new Dictionary<NodeId, string>();

        private static readonly Counter createdAssets = Metrics
            .CreateCounter("opcua_created_assets_mqtt", "Number of assets pushed over mqtt");
        private static readonly Counter createdTimeseries = Metrics
            .CreateCounter("opcua_created_timeseries_mqtt", "Number of timeseries pushed over mqtt");
        private static readonly Counter skippedDatapoints = Metrics
            .CreateCounter("opcua_skipped_datapoints_mqtt", "Number of datapoints skipped by MQTT pusher");
        private static readonly Counter skippedEvents = Metrics
            .CreateCounter("opcua_skipped_events_mqtt", "Number of events skipped by MQTT pusher");
        private static readonly Counter dataPointsCounter = Metrics
            .CreateCounter("opcua_datapoints_pushed_mqtt", "Number of datapoints pushed to MQTT");
        private static readonly Counter dataPointPushes = Metrics
            .CreateCounter("opcua_datapoint_pushes_mqtt", "Number of times datapoints have been pushed to MQTT");
        private static readonly Counter eventCounter = Metrics
            .CreateCounter("opcua_events_pushed_mqtt", "Number of events pushed to MQTT");
        private static readonly Counter eventPushCounter = Metrics
            .CreateCounter("opcua_event_pushes_mqtt", "Number of times events have been pushed to MQTT");
        public MQTTPusher(MqttPusherConfig config)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            var builder = new MqttClientOptionsBuilder()
                .WithClientId(config.ClientId)
                .WithTcpServer(config.Host, config.Port)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(15))
                .WithCommunicationTimeout(TimeSpan.FromSeconds(10))
                .WithCleanSession();

            if (config.UseTls)
            {
                builder = builder.WithTls();
                if (!string.IsNullOrEmpty(config.Username) && !string.IsNullOrEmpty(config.Host))
                {
                    builder = builder.WithCredentials(config.Username, config.Password);
                }
            }

            options = builder.Build();
            client = new MqttFactory().CreateMqttClient();
            baseBuilder = new MqttApplicationMessageBuilder()
                .WithAtLeastOnceQoS();
            if (config.Debug) return;

            client.UseDisconnectedHandler(async e =>
            {
                log.Warning("MQTT Client disconnected");
                log.Debug(e.Exception, "MQTT client disconnected");
                await Task.Delay(1000);
                if (closed) return;
                try
                {
                    await client.ConnectAsync(options, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    log.Warning("Failed to reconnect to broker: {msg}", ex.Message);
                }
            });
            client.UseConnectedHandler(_ =>
            {
                log.Information("MQTT client connected");
            });
            client.ConnectAsync(options, CancellationToken.None).Wait();
        }
        #region interface
        public async Task<bool?> PushDataPoints(IEnumerable<UADataPoint> points, CancellationToken token)
        {
            if (points == null) return null;
            if (!client.IsConnected)
            {
                log.Warning("Client is not connected");
                return false;
            }
            int count = 0;
            var dataPointList = new Dictionary<string, List<UADataPoint>>();

            foreach (var ldp in points)
            {
                var dp = ldp;
                if (dp.Timestamp < minDateTime)
                {
                    skippedDatapoints.Inc();
                    continue;
                }

                if (!dp.IsString && (!double.IsFinite(dp.DoubleValue.Value)
                    || dp.DoubleValue >= CogniteUtils.NumericValueMax
                    || dp.DoubleValue <= CogniteUtils.NumericValueMin))
                {
                    if (config.NonFiniteReplacement != null)
                    {
                        dp = new UADataPoint(dp, config.NonFiniteReplacement.Value);
                    }
                    else
                    {
                        skippedDatapoints.Inc();
                        continue;
                    }
                }

                if (dp.IsString && dp.StringValue == null)
                {
                    dp = new UADataPoint(dp, "");
                }

                count++;
                if (!dataPointList.ContainsKey(dp.Id))
                {
                    dataPointList[dp.Id] = new List<UADataPoint>();
                }
                dataPointList[dp.Id].Add(dp);
            }

            if (count == 0 || config.Debug) return null;

            var dpChunks = dataPointList.Select(kvp => (kvp.Key, (IEnumerable<UADataPoint>)kvp.Value)).ChunkBy(100000, 10000).ToArray();
            var pushTasks = dpChunks.Select(chunk => PushDataPointsChunk(chunk.ToDictionary(pair => pair.Key, pair => pair.Values), token)).ToList();
            var results = await Task.WhenAll(pushTasks);

            if (!results.All(res => res))
            {
                log.Debug("Failed to push {cnt} points to CDF over MQTT", count);
                return false;
            }

            log.Debug("Successfully pushed {cnt} points to CDF over MQTT", count);

            return true;
        }
        public async Task<bool?> TestConnection(FullConfig config, CancellationToken token)
        {
            if (client.IsConnected) return true;
            closed = false;
            try
            {
                await client.ConnectAsync(options, token);
            }
            catch (Exception e)
            {
                log.Warning("Failed to connect to MQTT broker: {msg}", e.Message);
				return false;
            }
            log.Information("Connected to MQTT broker");
            return client.IsConnected;
        }

        public async Task<bool> PushNodes(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> variables,
            UpdateConfig update,
            CancellationToken token)
        {
            if (!client.IsConnected) return false;
            if (variables == null) throw new ArgumentNullException(nameof(variables));
            if (objects == null) throw new ArgumentNullException(nameof(objects));
            if (update == null) throw new ArgumentNullException(nameof(update));

            if (!string.IsNullOrEmpty(config.LocalState) && Extractor.StateStorage != null && existingNodes == null)
            {
                Dictionary<string, ExistingState> states;
                if (config.SkipMetadata)
                {
                    states = variables
                        .Select(node => Extractor.GetUniqueId(node.Id))
                        .Select(id => new ExistingState { Id = id })
                        .ToDictionary(state => state.Id);
                }
                else
                {
                    states = objects
                       .Select(node => Extractor.GetUniqueId(node.Id))
                       .Concat(variables.Select(variable => Extractor.GetUniqueId(variable.Id, variable.Index)))
                       .Select(id => new ExistingState { Id = id })
                       .ToDictionary(state => state.Id);
                }

                await Extractor.StateStorage.RestoreExtractionState<MqttState, ExistingState>(
                    states,
                    config.LocalState,
                    (state, poco) => state.Existing = true,
                    token);

                foreach (var node in states)
                {
                    if (node.Value.Existing)
                    {
                        existingNodes.Add(node.Key);
                    }
                }
            }

            if (existingNodes.Any())
            {
                if (!update.Objects.AnyUpdate)
                {
                    objects = objects
                        .Where(obj => !existingNodes.Contains(Extractor.GetUniqueId(obj.Id))).ToList();
                }
                else
                {
                    foreach (var obj in objects) obj.Changed = existingNodes.Contains(Extractor.GetUniqueId(obj.Id));
                }
                if (!update.Variables.AnyUpdate)
                {
                    variables = variables
                        .Where(variable => !existingNodes.Contains(Extractor.GetUniqueId(variable.Id, variable.Index))).ToList();
                }
                else
                {
                    foreach (var node in variables) node.Changed = existingNodes.Contains(Extractor.GetUniqueId(node.Id, node.Index));
                }
            }

            if (!objects.Any() && !variables.Any()) return true;
            if (!config.SkipMetadata)
            {
                await Extractor.ReadProperties(objects.Concat(variables));
            }

            log.Information("Pushing {cnt} assets and {cnt2} timeseries over MQTT", objects.Count(), variables.Count());

            if (config.Debug) return true;

            if (objects.Any() && !config.SkipMetadata)
            {
                var results = await Task.WhenAll(objects.ChunkBy(1000).Select(chunk => PushAssets(chunk, update.Objects, token)));
                if (!results.All(res => res)) return false;
            }

            if (variables.Any())
            {
                var results = await Task.WhenAll(variables.ChunkBy(1000).Select(chunk => PushTimeseries(chunk, update.Variables, token)));
                if (!results.All(res => res)) return false;
                foreach (var ts in variables)
                {
                    if (ts.Index == -1) continue;
                    eventParents[ts.Id] = Extractor.GetUniqueId(ts.ParentId);
                }
            }

            var newStates = objects
                    .Select(node => Extractor.GetUniqueId(node.Id))
                    .Concat(variables.Select(variable => Extractor.GetUniqueId(variable.Id, variable.Index)))
                    .Select(id => new ExistingState { Id = id, Existing = true, LastTimeModified = DateTime.UtcNow })
                    .ToList();

            foreach (var state in newStates)
            {
                existingNodes.Add(state.Id);
            }

            if (!string.IsNullOrEmpty(config.LocalState) && Extractor.StateStorage != null)
            {
                await Extractor.StateStorage.StoreExtractionState(
                    newStates,
                    config.LocalState,
                    state => new MqttState { Id = state.Id, CreatedAt = DateTime.UtcNow },
                    token);
            }

            return true;
        }
        public async Task<bool?> PushEvents(IEnumerable<UAEvent> events, CancellationToken token)
        {
            if (events == null) return null;
            var eventList = new List<UAEvent>();
            int count = 0;
            foreach (var buffEvent in events)
            {
                if (buffEvent.Time < PusherUtils.CogniteMinTime || buffEvent.Time > PusherUtils.CogniteMaxTime)
                {
                    skippedEvents.Inc();
                    continue;
                }
                eventList.Add(buffEvent);
                count++;
            }
            if (count == 0) return null;
            if (config.Debug) return null;

            var results = await Task.WhenAll(eventList.ChunkBy(1000).Select(chunk => PushEventsChunk(chunk, token)));
            if (!results.All(result => result))
            {
                log.Debug("Failed to push {cnt} events to CDF over MQTT", count);
                return false;
            }

            log.Debug("Successfully pushed {cnt} events to CDF over MQTT", count);

            return true;
        }

        public async Task<bool> PushReferences(IEnumerable<UAReference> references, CancellationToken token)
        {
            if (config.SkipMetadata) return true;

            var relationships = references.Select(rel => rel.ToRelationship(config.DataSetId, Extractor));

            if (!string.IsNullOrEmpty(config.LocalState))
            {
                var states = relationships
                    .Select(rel => rel.ExternalId)
                    .Select(id => new ExistingState { Id = id })
                    .ToDictionary(state => state.Id);

                await Extractor.StateStorage.RestoreExtractionState<MqttState, ExistingState>(
                    states,
                    config.LocalState,
                    (state, poco) => state.Existing = true,
                    token);

                
                foreach (var node in states)
                {
                    if (node.Value.Existing)
                    {
                        existingNodes.Add(node.Key);
                    }
                }
            }

            if (existingNodes.Any())
            {
                relationships = relationships.Where(rel => !existingNodes.Contains(rel.ExternalId)).ToList();
            }

            if (!relationships.Any()) return true;

            log.Information("Pushing {cnt} relationships to CDF over MQTT", relationships.Count());

            var tasks = relationships.ChunkBy(1000).Select(chunk => PushReferencesChunk(chunk, token));
            var results = await Task.WhenAll(tasks);

            if (!results.All(res => res)) return false;

            var newStates = relationships
                .Select(rel => rel.ExternalId)
                .Select(id => new ExistingState { Id = id, Existing = true, LastTimeModified = DateTime.UtcNow })
                .ToList();

            foreach (var state in newStates)
            {
                existingNodes.Add(state.Id);
            }

            if (!string.IsNullOrEmpty(config.LocalState))
            {
                await Extractor.StateStorage.StoreExtractionState(
                    newStates,
                    config.LocalState,
                    state => new MqttState { Id = state.Id, CreatedAt = DateTime.UtcNow },
                    token);
            }
            return true;
        }

        /// <summary>
        /// Reset the pusher, preparing it to be restarted
        /// </summary>
        public void Reset()
        {
            existingNodes = new HashSet<string>();
        }

        #endregion
        #region pushing
        private async Task<bool> PushDataPointsChunk(IDictionary<string, IEnumerable<UADataPoint>> dataPointList, CancellationToken token)
        {
            int count = 0;
            var inserts = dataPointList.Select(kvp =>
            {
                (string externalId, var values) = kvp;
                var item = new DataPointInsertionItem
                {
                    ExternalId = externalId
                };
                if (values.First().IsString)
                {
                    item.StringDatapoints = new StringDatapoints();
                    item.StringDatapoints.Datapoints.AddRange(values.Select(ipoint =>
                        new StringDatapoint
                        {
                            Timestamp = new DateTimeOffset(ipoint.Timestamp).ToUnixTimeMilliseconds(),
                            Value = ipoint.StringValue
                        }));
                }
                else
                {
                    item.NumericDatapoints = new NumericDatapoints();
                    item.NumericDatapoints.Datapoints.AddRange(values.Select(ipoint =>
                        new NumericDatapoint
                        {
                            Timestamp = new DateTimeOffset(ipoint.Timestamp).ToUnixTimeMilliseconds(),
                            Value = ipoint.DoubleValue.Value
                        }));
                }

                count += values.Count();
                return item;
            });

            var req = new DataPointInsertionRequest();
            req.Items.AddRange(inserts);
            if (!req.Items.Any()) return true;


            var data = req.ToByteArray();
            var msg = baseBuilder
                .WithPayload(data)
                .WithTopic(config.DatapointTopic)
                .Build();

            try
            {
                await client.PublishAsync(msg, token);
            }
            catch (Exception e)
            {
                log.Error("Failed to write to MQTT: {msg}", e.Message);
                return false;
            }

            dataPointPushes.Inc();
            dataPointsCounter.Inc(count);

            return true;
        }

        private async Task<bool> PushAssets(IEnumerable<UANode> objects, TypeUpdateConfig update, CancellationToken token)
        {
            bool useRawStore = config.RawMetadata != null && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.AssetsTable);
            var assets = ConvertNodes(objects, update);

            if (useRawStore)
            {
                var rawObj = new RawRequestWrapper<AssetCreate>
                {
                    Database = config.RawMetadata.Database,
                    Table = config.RawMetadata.AssetsTable,
                    Rows = assets.Select(asset => new RawRowCreateDto<AssetCreate> { Key = asset.ExternalId, Columns = asset })
                };
                var rawData = JsonSerializer.SerializeToUtf8Bytes(rawObj, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
                var rawMsg = baseBuilder
                    .WithTopic(config.RawTopic)
                    .WithPayload(rawData)
                    .Build();

                try
                {
                    await client.PublishAsync(rawMsg, token);
                    createdAssets.Inc(assets.Count());
                }
                catch (Exception ex)
                {
                    log.Error("Failed to write assets to raw over MQTT: {msg}", ex.Message);
                }

                return true;
            }

            var data = JsonSerializer.SerializeToUtf8Bytes(assets, null);
            
            var msg = baseBuilder
                .WithTopic(config.AssetTopic)
                .WithPayload(data)
                .Build();

            try
            {
                await client.PublishAsync(msg, token);
                createdAssets.Inc(assets.Count());
            }
            catch (Exception e)
            {
                log.Error("Failed to write assets to MQTT: {msg}", e.Message);
                return false;
            }

            return true;
        }

        private IEnumerable<AssetCreate> ConvertNodes(IEnumerable<UANode> nodes, TypeUpdateConfig update)
        {
            foreach (var node in nodes)
            {
                var create = node.ToCDFAsset(Extractor, config.DataSetId, config.MetadataMapping?.Assets);
                if (create == null) continue;
                if (!node.Changed)
                {
                    yield return create;
                    continue;
                }
                if (!update.Context) create.ParentExternalId = null;
                if (!update.Description) create.Description = null;
                if (!update.Metadata) create.Metadata = null;
                if (!update.Name) create.Name = null;
                yield return create;
            }
        }

        private IEnumerable<StatelessTimeSeriesCreate> ConvertVariables(IEnumerable<UAVariable> variables, TypeUpdateConfig update)
        {
            foreach (var variable in variables)
            {
                var create = variable.ToStatelessTimeSeries(Extractor, config.DataSetId, config.MetadataMapping?.Timeseries);
                if (create == null) continue;
                if (!variable.Changed)
                {
                    yield return create;
                    continue;
                }
                if (!update.Context) create.AssetExternalId = null;
                if (!update.Description) create.Description = null;
                if (!update.Metadata) create.Metadata = null;
                if (!update.Name) create.Name = null;
                yield return create;
            }
        }

        private async Task<bool> PushTimeseries(IEnumerable<UAVariable> variables, TypeUpdateConfig update, CancellationToken token)
        {
            bool useRawStore = config.RawMetadata != null && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.TimeseriesTable);

            var timeseries = ConvertVariables(variables, update);

            bool useMinimalTs = useRawStore || config.SkipMetadata;

            if (useMinimalTs)
            {
                var minimalTimeseries = variables
                    .Where(variable => !update.AnyUpdate || !variable.Changed)
                    .Select(variable => variable.ToTimeseries(Extractor, config.DataSetId, null, null, true))
                    .Where(variable => variable != null)
                    .ToList();

                if (minimalTimeseries.Any())
                {
                    var minimalData = JsonSerializer.SerializeToUtf8Bytes(minimalTimeseries, null);

                    var minimalMsg = baseBuilder
                        .WithPayload(minimalData)
                        .WithTopic(config.TsTopic)
                        .Build();

                    try
                    {
                        await client.PublishAsync(minimalMsg, token);
                        createdTimeseries.Inc(timeseries.Count());
                    }
                    catch (Exception e)
                    {
                        log.Error("Failed to write minimal timeseries to MQTT: {msg}", e.Message);
                        return false;
                    }
                }
            }

            if (config.SkipMetadata) return true;

            if (useRawStore)
            {
                var rawObj = new RawRequestWrapper<StatelessTimeSeriesCreate>
                {
                    Database = config.RawMetadata.Database,
                    Table = config.RawMetadata.TimeseriesTable,
                    Rows = timeseries.Select(ts => new RawRowCreateDto<StatelessTimeSeriesCreate> { Key = ts.ExternalId, Columns = ts })
                };

                var rawData = JsonSerializer.SerializeToUtf8Bytes(rawObj, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
                var rawMsg = baseBuilder
                    .WithTopic(config.RawTopic)
                    .WithPayload(rawData)
                    .Build();

                try
                {
                    await client.PublishAsync(rawMsg, token);
                }
                catch (Exception e)
                {
                    log.Error("Failed to write timeseries to raw over MQTT: {msg}", e.Message);
                    return false;
                }

                return true;
            }


            var data = JsonSerializer.SerializeToUtf8Bytes(timeseries, null);
            var msg = baseBuilder
                .WithPayload(data)
                .WithTopic(config.TsTopic)
                .Build();

            try
            {
                await client.PublishAsync(msg, token);
                createdTimeseries.Inc(timeseries.Count());
            }
            catch (Exception e)
            {
                log.Error("Failed to write timeseries to MQTT: {msg}", e.Message);
                return false;
            }

            return true;
        }

        private async Task<bool> PushEventsChunk(IEnumerable<UAEvent> evts, CancellationToken token)
        {
            if (config.Debug) return true;
            var events = evts
                .Select(evt => evt.ToStatelessCDFEvent(Extractor, config.DataSetId, eventParents))
                .Where(evt => evt != null);

            var data = JsonSerializer.SerializeToUtf8Bytes(events, null);

            var msg = baseBuilder
                .WithPayload(data)
                .WithTopic(config.EventTopic)
                .Build();

            try
            {
                await client.PublishAsync(msg, token);
            }
            catch (Exception e)
            {
                log.Error("Failed to write events to MQTT: {msg}", e.Message);
                return false;
            }
            eventCounter.Inc(evts.Count());
            eventPushCounter.Inc();

            return true;
        }

        private async Task<bool> PushReferencesChunk(IEnumerable<CogniteSdk.Beta.RelationshipCreate> references, CancellationToken token)
        {
            var data = JsonSerializer.SerializeToUtf8Bytes(references, null);

            var msg = baseBuilder
                .WithPayload(data)
                .WithTopic(config.RelationshipTopic)
                .Build();

            try
            {
                await client.PublishAsync(msg, token);
            }
            catch (Exception e)
            {
                log.Error("Failed to write relationships to MQTT: {msg}", e.Message);
                return false;
            }
            return true;
        }

        #endregion

        class MqttState : BaseStorableState
        {
            [StateStoreProperty("created")]
            public DateTime CreatedAt { get; set; }
        }

        class ExistingState : IExtractionState
        {
            public bool Existing { get; set; }
            public string Id { get; set; }
            public DateTime? LastTimeModified { get; set; }
        }
        class RawRequestWrapper<T>
        {
            public string Database { get; set; }
            public string Table { get; set; }
            public IEnumerable<RawRowCreateDto<T>> Rows { get; set; }
        }

        class RawRowCreateDto<T>
        {
            public string Key { get; set; }
            public T Columns { get; set; }
        }
        public async Task Disconnect()
        {
            closed = true;
            await client.DisconnectAsync();
        }

        public void Dispose()
        {
            Disconnect().Wait();
            client.Dispose();
        }
    }
}

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

using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Client;
using Opc.Ua;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Pushers
{
    public sealed class MQTTPusher : IPusher
    {
        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }
        public PusherInput? PendingNodes { get; set; }
        public UAExtractor Extractor { get; set; } = null!;
        public IPusherConfig BaseConfig => config;
        private readonly MqttPusherConfig config;
        private readonly IMqttClient client;
        private readonly MqttClientOptions options;

        private readonly ILogger<MQTTPusher> log;

        private readonly MqttApplicationMessageBuilder baseBuilder;

        private readonly DateTime minDateTime = new DateTime(1971, 1, 1);

        private bool closed;

        private HashSet<string> existingNodes = new HashSet<string>();

        private readonly Dictionary<NodeId, string?> eventParents = new Dictionary<NodeId, string?>();

        private readonly FullConfig fullConfig;

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
        private static readonly Counter createdRelationships = Metrics
            .CreateCounter("opcua_created_relationships_mqtt", "Number of relationships pushed over MQTT");

        /// <summary>
        /// Constructor, also starts the client and sets up correct disconnect handlers.
        /// </summary>
        /// <param name="config">Config to use</param>
        public MQTTPusher(ILogger<MQTTPusher> log, IServiceProvider provider, MqttPusherConfig config)
        {
            this.log = log;
            this.config = config;
            fullConfig = provider.GetRequiredService<FullConfig>();
            var builder = new MqttClientOptionsBuilder()
                .WithClientId(config.ClientId)
                .WithTcpServer(config.Host, config.Port)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(15))
                .WithTimeout(TimeSpan.FromSeconds(10))
                .WithCleanSession()
                .WithTlsOptions(new MqttClientTlsOptions
                {
                    UseTls = config.UseTls,
                    AllowUntrustedCertificates = config.AllowUntrustedCertificates,
                    CertificateValidationHandler = (context) =>
                    {
                        if (context.SslPolicyErrors == System.Net.Security.SslPolicyErrors.None)
                        {
                            return true;
                        }
                        if (string.IsNullOrWhiteSpace(config.CustomCertificateAuthority)) return false;

                        var privateChain = new X509Chain();
                        privateChain.ChainPolicy.RevocationMode = X509RevocationMode.Offline;

                        var certificate = new X509Certificate2(context.Certificate);
                        var signerCertificate = new X509Certificate2(config.CustomCertificateAuthority);

                        privateChain.ChainPolicy.ExtraStore.Add(certificate);
                        privateChain.Build(signerCertificate);

                        bool isValid = true;

                        foreach (X509ChainStatus chainStatus in privateChain.ChainStatus)
                        {
                            if (chainStatus.Status != X509ChainStatusFlags.NoError)
                            {
                                isValid = false;
                                break;
                            }
                        }
                        return isValid;
                    }
                });

            if (!string.IsNullOrEmpty(config.Username) && !string.IsNullOrEmpty(config.Password))
            {
                builder = builder.WithCredentials(config.Username, config.Password);
            }

            options = builder.Build();
            client = new MqttFactory().CreateMqttClient();
            baseBuilder = new MqttApplicationMessageBuilder()
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce);

            if (fullConfig.DryRun) return;

            client.DisconnectedAsync += async e =>
            {
                log.LogWarning(e.Exception, "MQTT client disconnected");
                async Task TryReconnect(int retries)
                {
                    if (client.IsConnected || retries == 0 || closed) return;
                    await Task.Delay(1000);
                    if (client.IsConnected || retries == 0 || closed) return;
                    try
                    {
                        await client.ConnectAsync(options, CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        log.LogWarning("Failed to reconnect to broker: {Message}", ex.Message);
                        await TryReconnect(retries - 1);
                    }
                }
                await TryReconnect(3);
            };
            client.ConnectedAsync += _ =>
            {
                log.LogInformation("MQTT client connected");
                return Task.CompletedTask;
            };
            client.ConnectAsync(options, CancellationToken.None).Wait();
        }
        #region interface
        /// <summary>
        /// Push given list of datapoints over MQTT.
        /// </summary>
        /// <param name="points">Datapoints to push</param>
        /// <returns>True on success, false on failure, null if no points were legal.</returns>
        public async Task<bool?> PushDataPoints(IEnumerable<UADataPoint> points, CancellationToken token)
        {
            if (points == null) return null;
            if (!client.IsConnected)
            {
                log.LogWarning("Client is not connected");
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

                if (!dp.IsString && (dp.DoubleValue.HasValue && !double.IsFinite(dp.DoubleValue.Value)
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

            if (count == 0) return null;

            if (fullConfig.DryRun)
            {
                log.LogInformation("Dry run enabled. Would insert {Count} datapoints over {C2} timeseries using MQTT", count, dataPointList.Count);
                return null;
            }

            var dpChunks = dataPointList.Select(kvp => (kvp.Key, (IEnumerable<UADataPoint>)kvp.Value)).ChunkBy(100000, 10000).ToArray();
            var pushTasks = dpChunks.Select(chunk => PushDataPointsChunk(chunk.ToDictionary(pair => pair.Key, pair => pair.Values), token)).ToList();
            var results = await Task.WhenAll(pushTasks);

            if (!results.All(res => res))
            {
                log.LogDebug("Failed to push {Count} points to CDF over MQTT", count);
                return false;
            }

            log.LogDebug("Successfully pushed {Count} points to CDF over MQTT", count);

            return true;
        }
        /// <summary>
        /// Check if the client is currently connected to MQTT, if not, try to reconnect.
        /// </summary>
        /// <returns>True if connected, false if not</returns>
        public async Task<bool?> TestConnection(FullConfig fullConfig, CancellationToken token)
        {
            if (client.IsConnected || fullConfig.DryRun) return true;
            closed = false;
            try
            {
                await client.ConnectAsync(options, token);
            }
            catch (Exception e)
            {
                log.LogWarning("Failed to connect to MQTT broker: {Message}", e.Message);
                return false;
            }
            log.LogInformation("Connected to MQTT broker");
            return client.IsConnected;
        }
        /// <summary>
        /// Try to push the given lists of objects and variables to CDF over MQTT.
        /// If enabled, stores the created object names in a local state store,
        /// and only creates objects that are not already present.
        /// </summary>
        /// <param name="objects">Objects to create as assets</param>
        /// <param name="variables">Variables to create as timeseries</param>
        /// <param name="update">Configuration for how these should be updated, if enabled</param>
        /// <returns>True on success, false on failure</returns>
        public async Task<PushResult> PushNodes(
            IEnumerable<BaseUANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            UpdateConfig update,
            CancellationToken token)
        {
            if (!client.IsConnected) return new PushResult { Objects = false, References = false, Variables = false };

            var relationships = Enumerable.Empty<RelationshipCreate>();

            if (!config.SkipMetadata)
            {
                relationships = references.Select(rel => rel.ToRelationship(config.DataSetId, Extractor));
            }

            if (fullConfig.DryRun) return new PushResult();

            if (!string.IsNullOrEmpty(config.LocalState) && Extractor.StateStorage != null)
            {
                Dictionary<string, ExistingState> states;
                if (config.SkipMetadata)
                {
                    states = variables
                        .SelectNonNull(node => Extractor.GetUniqueId(node.Id))
                        .Where(node => !existingNodes.Contains(node))
                        .Select(id => new ExistingState(id))
                        .ToDictionary(state => state.Id);
                }
                else
                {
                    states = objects
                       .SelectNonNull(node => Extractor.GetUniqueId(node.Id))
                       .Where(node => !existingNodes.Contains(node))
                       .Concat(variables.SelectNonNull(variable => variable.GetUniqueId(Extractor.Context)))
                       .Concat(relationships.SelectNonNull(rel => rel.ExternalId))
                       .Select(id => new ExistingState(id))
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

            if (existingNodes.Count != 0)
            {
                if (!update.Objects.AnyUpdate)
                {
                    objects = objects
                        .Where(obj => !obj.Id.IsNullNodeId && !existingNodes.Contains(Extractor.GetUniqueId(obj.Id)!)).ToList();
                }
                else
                {
                    foreach (var obj in objects)
                    {
                        string? id = Extractor.GetUniqueId(obj.Id);
                        obj.Changed = id != null && existingNodes.Contains(id);
                    }
                }
                if (!update.Variables.AnyUpdate)
                {
                    variables = variables
                        .Where(variable => !variable.Id.IsNullNodeId
                            && !existingNodes.Contains(variable.GetUniqueId(Extractor.Context)!)).ToList();
                }
                else
                {
                    foreach (var node in variables)
                    {
                        string? id = node.GetUniqueId(Extractor.Context);
                        node.Changed = id != null && existingNodes.Contains(id);
                    }
                }

                relationships = relationships.Where(rel => !existingNodes.Contains(rel.ExternalId));
            }

            var result = new PushResult();

            if (!objects.Any() && !variables.Any() && !references.Any()) return result;

            log.LogInformation("Pushing {ObjCount} assets and {VarCount} timeseries over MQTT", objects.Count(), variables.Count());

            if (objects.Any() && !config.SkipMetadata)
            {
                var results = await Task.WhenAll(objects.ChunkBy(1000).Select(chunk => PushAssets(chunk, update.Objects, token)));
                if (!results.All(res => res)) result.Objects = false;
            }

            if (variables.Any())
            {
                var results = await Task.WhenAll(variables.ChunkBy(1000).Select(chunk => PushTimeseries(chunk, update.Variables, token)));
                if (!results.All(res => res)) result.Variables = false;
                foreach (var ts in variables)
                {
                    if (ts is not UAVariableMember mb || mb.Index == -1) continue;
                    eventParents[ts.Id] = Extractor.GetUniqueId(ts.ParentId);
                }
            }

            if (relationships.Any() && !config.SkipMetadata)
            {
                var results = await Task.WhenAll(relationships.ChunkBy(1000).Select(chunk => PushReferencesChunk(chunk, token)));
                if (!results.All(res => res)) result.References = false;
            }

            if (!result.Objects || !result.Variables || !result.References) return result;

            var newStates = objects
                    .SelectNonNull(node => Extractor.GetUniqueId(node.Id))
                    .Concat(variables.SelectNonNull(variable => variable.GetUniqueId(Extractor.Context)))
                    .Concat(relationships.SelectNonNull(rel => rel.ExternalId))
                    .Select(id => new ExistingState(id) { Existing = true, LastTimeModified = DateTime.UtcNow })
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

            return result;
        }
        /// <summary>
        /// Create the given list of events in CDF over MQTT.
        /// </summary>
        /// <param name="events">Events to create</param>
        /// <returns>True on success, false on failure, null if no events were pushed.</returns>
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

            if (fullConfig.DryRun)
            {
                log.LogInformation("Dry run enabled. Would insert {Count} events using MQTT", count);
                return null;
            }

            var results = await Task.WhenAll(eventList.ChunkBy(1000).Select(chunk => PushEventsChunk(chunk, token)));
            if (!results.All(result => result))
            {
                log.LogDebug("Failed to push {Count} events to CDF over MQTT", count);
                return false;
            }

            log.LogDebug("Successfully pushed {Count} events to CDF over MQTT", count);

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
        /// <summary>
        /// Push a chunk of the full datapoint push over MQTT.
        /// Handles the serialization and actual pushing.
        /// </summary>
        /// <param name="dataPointList">Datapoints to create, grouped by timeseries name.</param>
        /// <returns>True on success, false on failure</returns>
        private async Task<bool> PushDataPointsChunk(IDictionary<string, IEnumerable<UADataPoint>> dataPointList, CancellationToken token)
        {
            var inserts = dataPointList.ToDictionary(
                pair => Identity.Create(pair.Key),
                pair => pair.Value.SelectNonNull(dp => dp.ToCDFDataPoint(fullConfig.Extraction.StatusCodes.IngestStatusCodes, log)));
            var (points, errors) = Sanitation.CleanDataPointsRequest(inserts, SanitationMode.Clean, config.NonFiniteReplacement);
            var req = inserts.ToInsertRequest();

            foreach (var error in errors)
            {
                log.LogCogniteError(error, RequestType.CreateDatapoints, true);
            }

            if (req.Items.Count == 0) return true;

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
                log.LogError("Failed to write to MQTT: {Message}", e.Message);
                return false;
            }

            dataPointPushes.Inc();
            int count = req.Items.Sum(
                x => x.NumericDatapoints?.Datapoints?.Count ?? 0
                + x.StringDatapoints?.Datapoints?.Count ?? 0);
            dataPointsCounter.Inc(count);

            return true;
        }
        /// <summary>
        /// Push the given list of assets over MQTT to CDF, optionally passing "update" to indicate that the
        /// bridge should update the given assets.
        /// </summary>
        /// <param name="objects">Assets to create or update</param>
        /// <param name="update">Configuration for how the assets should be updated.</param>
        /// <returns>True on success, false on failure.</returns>
        private async Task<bool> PushAssets(IEnumerable<BaseUANode> objects, TypeUpdateConfig update, CancellationToken token)
        {
            bool useRawStore = config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.AssetsTable);

            if (useRawStore)
            {
                var jsonAssets = ConvertNodesJson(objects, ConverterType.Node);
                var rawObj = new RawRequestWrapper<JsonElement>(
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.AssetsTable!,
                    jsonAssets.Select(pair => new RawRowCreateDto<JsonElement>(pair.id, pair.node)));
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
                    createdAssets.Inc(jsonAssets.Count());
                }
                catch (Exception ex)
                {
                    log.LogError("Failed to write assets to raw over MQTT: {Message}", ex.Message);
                    return false;
                }

                return true;
            }
            var assets = ConvertNodes(objects, update);

            var data = JsonSerializer.SerializeToUtf8Bytes(assets);

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
                log.LogError("Failed to write assets to MQTT: {Message}", e.Message);
                return false;
            }

            return true;
        }
        private enum ResourceType
        {
            Assets, Timeseries, Relationships
        }

        /// <summary>
        /// Convert nodes to assets, setting fields that should not be updated to null.
        /// </summary>
        /// <param name="nodes">Nodes to create or update</param>
        /// <param name="update">Configuration for which fields should be updated.</param>
        /// <returns>List of assets to create</returns>
        private IEnumerable<AssetCreate> ConvertNodes(IEnumerable<BaseUANode> nodes, TypeUpdateConfig update)
        {
            foreach (var node in nodes)
            {
                var create = node.ToCDFAsset(fullConfig, Extractor, config.DataSetId, config.MetadataMapping?.Assets);
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
        /// <summary>
        /// Convert nodes to raw assets or timeseries with json metadata.
        /// </summary>
        /// <param name="nodes">Nodes to create or update</param>
        /// <returns>List of assets to create</returns>
        private IEnumerable<(string id, JsonElement node)> ConvertNodesJson(IEnumerable<BaseUANode> nodes, ConverterType type)
        {
            if (Extractor == null) throw new InvalidOperationException("Extractor must be set");
            foreach (var node in nodes)
            {
                var create = node.ToJson(log, Extractor.StringConverter, type);
                if (create == null) continue;
                string? id = Extractor.GetUniqueId(node.Id);
                if (id == null) continue;
                yield return (id, create.RootElement);
            }
        }
        /// <summary>
        /// Convert nodes to timeseries, setting fields that should not be updated to null.
        /// </summary>
        /// <param name="variables">Nodes to create or update</param>
        /// <param name="update">Configuration for which fields should be updated.</param>
        /// <returns>List of timeseries to create</returns>
        private IEnumerable<StatelessTimeSeriesCreate> ConvertVariables(IEnumerable<UAVariable> variables, TypeUpdateConfig update)
        {
            foreach (var variable in variables)
            {
                var create = variable.ToStatelessTimeSeries(fullConfig, Extractor, config.DataSetId, config.MetadataMapping?.Timeseries);
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
        /// <summary>
        /// Push the given list of timeseries over MQTT to CDF, optionally passing "update" to indicate that the
        /// bridge should update the given timeseries if they already exist.
        /// </summary>
        /// <param name="variables">Timeseries to push</param>
        /// <param name="update">Configuration for which fields should be updated</param>
        /// <returns>True on success</returns>
        private async Task<bool> PushTimeseries(IEnumerable<UAVariable> variables, TypeUpdateConfig update, CancellationToken token)
        {
            bool useRawStore = config.RawMetadata != null && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.TimeseriesTable);

            bool useMinimalTs = useRawStore || config.SkipMetadata;

            if (useMinimalTs)
            {
                var minimalTimeseries = variables
                    .Where(variable => !update.AnyUpdate || !variable.Changed)
                    .Select(variable => variable.ToMinimalTimeseries(Extractor, config.DataSetId))
                    .Where(variable => variable != null)
                    .ToList();

                if (minimalTimeseries.Count != 0)
                {
                    var minimalData = JsonSerializer.SerializeToUtf8Bytes(minimalTimeseries);

                    var minimalMsg = baseBuilder
                        .WithPayload(minimalData)
                        .WithTopic(config.TsTopic)
                        .Build();

                    try
                    {
                        await client.PublishAsync(minimalMsg, token);
                        createdTimeseries.Inc(minimalTimeseries.Count);
                    }
                    catch (Exception e)
                    {
                        log.LogError("Failed to write minimal timeseries to MQTT: {Message}", e.Message);
                        return false;
                    }
                }
            }

            if (config.SkipMetadata) return true;

            if (useRawStore)
            {
                var rawTimeseries = ConvertNodesJson(variables, ConverterType.Variable);
                var rawObj = new RawRequestWrapper<JsonElement>(
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.TimeseriesTable!,
                    rawTimeseries.Select(pair => new RawRowCreateDto<JsonElement>(pair.id, pair.node)));

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
                    log.LogError("Failed to write timeseries to raw over MQTT: {Message}", e.Message);
                    return false;
                }

                return true;
            }

            var timeseries = ConvertVariables(variables, update);

            var data = JsonSerializer.SerializeToUtf8Bytes(timeseries);
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
                log.LogError("Failed to write timeseries to MQTT: {Message}", e.Message);
                return false;
            }

            return true;
        }
        /// <summary>
        /// Push the given list of events over MQTT to CDF.
        /// </summary>
        /// <param name="evts">Events to create</param>
        /// <returns>True on success</returns>
        private async Task<bool> PushEventsChunk(IEnumerable<UAEvent> evts, CancellationToken token)
        {
            var events = evts
                .Select(evt => evt.ToStatelessCDFEvent(Extractor, config.DataSetId, eventParents))
                .Where(evt => evt != null);

            var data = JsonSerializer.SerializeToUtf8Bytes(events);

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
                log.LogError("Failed to write events to MQTT: {Message}", e.Message);
                return false;
            }
            eventCounter.Inc(evts.Count());
            eventPushCounter.Inc();

            return true;
        }
        /// <summary>
        /// Push the given list of relationships over MQTT to CDF.
        /// </summary>
        /// <param name="references">Relationships to create</param>
        /// <returns>True on success</returns>
        private async Task<bool> PushReferencesChunk(IEnumerable<RelationshipCreate> references, CancellationToken token)
        {
            bool useRawStore = config.RawMetadata != null && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.RelationshipsTable);
            var data = JsonSerializer.SerializeToUtf8Bytes(references);

            if (useRawStore)
            {
                var rawObj = new RawRequestWrapper<RelationshipCreate>(
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.RelationshipsTable!,
                    references.Select(rel => new RawRowCreateDto<RelationshipCreate>(rel.ExternalId, rel)));

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
                    createdRelationships.Inc(references.Count());
                }
                catch (Exception ex)
                {
                    log.LogError("Failed to write relationships to raw over MQTT: {Message}", ex.Message);
                }

                return true;
            }

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
                log.LogError("Failed to write relationships to MQTT: {Message}", e.Message);
                return false;
            }
            createdRelationships.Inc(references.Count());
            return true;
        }

        #endregion

        private class MqttState : BaseStorableState
        {
            [StateStoreProperty("created")]
            public DateTime CreatedAt { get; set; }
        }

        private class ExistingState : IExtractionState
        {
            public bool Existing { get; set; }
            public string Id { get; }
            public DateTime? LastTimeModified { get; set; }

            public ExistingState(string id)
            {
                Id = id;
            }
        }

        private class RawRequestWrapper<T>
        {
            public string Database { get; }
            public string Table { get; }
            public IEnumerable<RawRowCreateDto<T>> Rows { get; }
            public RawRequestWrapper(string database, string table, IEnumerable<RawRowCreateDto<T>> rows)
            {
                Database = database;
                Table = table;
                Rows = rows;
            }
        }

        private class RawRowCreateDto<T>
        {
            public string Key { get; set; }
            public T Columns { get; set; }
            public RawRowCreateDto(string key, T columns)
            {
                Key = key;
                Columns = columns;
            }
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

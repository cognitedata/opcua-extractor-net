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
using Cognite.OpcUa.Utils;
using CogniteSdk;
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
        private UAExtractor? _extractor;
        public UAExtractor Extractor 
        { 
            get => _extractor ?? throw new InvalidOperationException("Extractor must be set");
            set 
            { 
                _extractor = value;
                // Initialize transmission grouper with the extractor
                transmissionGrouper = new MqttTransmissionGrouper(config, log, value);
            } 
        }
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
        private readonly Utils.DataPointCollectionPool collectionPool;
        private readonly Utils.DataFormatCache formatCache;
        private readonly Utils.AdaptiveChunker adaptiveChunker;
        private readonly Utils.ConnectionRecoveryManager connectionRecovery;
        private MqttTransmissionGrouper transmissionGrouper;

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

        private double? nonFiniteReplacement;

        /// <summary>
        /// Get formatted timestamp based on configuration (cached)
        /// </summary>
        /// <param name="timestamp">Timestamp in milliseconds since epoch</param>
        /// <returns>Formatted timestamp as long (epoch) or string (ISO8601)</returns>
        private object GetFormattedTimestamp(long timestamp)
        {
            if (config.TimestampFormat.ToLowerInvariant() == "iso8601")
            {
                return formatCache.GetFormattedTimestamp(timestamp);
            }
            else
            {
                return timestamp;
            }
        }

        /// <summary>
        /// Remove OPC UA NodeId type prefixes (s=, i=, g=, b=) from the given ID string
        /// </summary>
        /// <param name="nodeId">NodeId string that may contain type prefixes</param>
        /// <returns>NodeId string without type prefixes</returns>
        private string RemoveNodeIdPrefix(string nodeId)
        {
            if (string.IsNullOrEmpty(nodeId)) return nodeId;

            // Remove namespace prefix first (ns=X;)
            var idWithoutNamespace = nodeId;
            var nsIndex = nodeId.IndexOf(';');
            if (nsIndex >= 0 && nodeId.StartsWith("ns="))
            {
                idWithoutNamespace = nodeId.Substring(nsIndex + 1);
            }

            // Remove type prefixes: s=, i=, g=, b=
            if (idWithoutNamespace.StartsWith("s=") || 
                idWithoutNamespace.StartsWith("i=") || 
                idWithoutNamespace.StartsWith("g=") || 
                idWithoutNamespace.StartsWith("b="))
            {
                return idWithoutNamespace.Substring(2);
            }

            return idWithoutNamespace;
        }

        /// <summary>
        /// Constructor, also starts the client and sets up correct disconnect handlers.
        /// </summary>
        /// <param name="config">Config to use</param>
        public MQTTPusher(ILogger<MQTTPusher> log, IServiceProvider provider, MqttPusherConfig config)
        {
            this.log = log;
            this.config = config;
            fullConfig = provider.GetRequiredService<FullConfig>();
            collectionPool = new Utils.DataPointCollectionPool();
            formatCache = new Utils.DataFormatCache(10000, config.TimezoneOffset ?? "+00:00", config.TimestampFormat == "iso8601");
            adaptiveChunker = new Utils.AdaptiveChunker(log, config.MaxMessageSize, config.MinChunkSize, config.MaxChunkSize);
            connectionRecovery = new Utils.ConnectionRecoveryManager(log, 100000, TimeSpan.FromSeconds(5), 10);
            transmissionGrouper = new MqttTransmissionGrouper(config, log, null!);
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
        /// <summary>
        /// Attempt to recover MQTT connection and replay buffered data
        /// </summary>
        private async Task AttemptConnectionRecovery(CancellationToken token)
        {
            try
            {
                await connectionRecovery.AttemptRecovery(
                    connectionTest: async () => 
                    {
                        try
                        {
                            if (client.IsConnected) return true;
                            await client.ConnectAsync(options, token);
                            return client.IsConnected;
                        }
                        catch (Exception ex)
                        {
                            log.LogDebug("Connection test failed: {Message}", ex.Message);
                            return false;
                        }
                    },
                    dataPointSender: async (dataPoints, ct) => 
                    {
                        try
                        {
                            return await PushDataPoints(dataPoints, ct);
                        }
                        catch (Exception ex)
                        {
                            log.LogWarning("Failed to replay data points: {Message}", ex.Message);
                            return false;
                        }
                    },
                    groupedDataSender: async (groupedData, ct) =>
                    {
                        try
                        {
                            var chunkStartTime = DateTime.UtcNow;
                            var result = await PushDataPointsChunk(groupedData, chunkStartTime, ct);
                            return result;
                        }
                        catch (Exception ex)
                        {
                            log.LogWarning("Failed to replay grouped data: {Message}", ex.Message);
                            return false;
                        }
                    },
                    token: token
                );
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error during connection recovery: {Message}", ex.Message);
            }
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
                log.LogWarning("Client is not connected, buffering data for recovery");
                
                // Buffer data for recovery
                var validatedPoints = points.Select(dp => Utils.DataPointValidator.ValidateForMqtt(dp, config, log))
                    .Where(dp => dp != null)
                    .Cast<UADataPoint>()
                    .ToList();
                
                if (validatedPoints.Any())
                {
                    connectionRecovery.BufferData(validatedPoints, "MqttPusher");
                    
                    // Attempt recovery in background
                    _ = Task.Run(async () => await AttemptConnectionRecovery(token));
                }
                
                return false;
            }

            int count = 0;
            var dataPointList = collectionPool.GetDictionary();

            foreach (var ldp in points)
            {
                // Use consolidated validation logic
                var dp = Utils.DataPointValidator.ValidateForMqtt(ldp, config, log);
                if (dp == null)
                {
                    skippedDatapoints.Inc();
                    continue;
                }

                count++;
                if (!dataPointList.TryGetValue(dp.Id, out List<UADataPoint>? value))
                {
                    value = collectionPool.GetList();
                    dataPointList[dp.Id] = value;
                }

                value.Add(dp);
            }

            if (count == 0) 
            {
                collectionPool.ReturnDictionary(dataPointList);
                return null;
            }

            // Log data being sent to MQTT broker (only when there's actual data)
            log.LogInformation("[Queue → MQTT] Processing {Count} datapoints from {NodeCount} nodes for MQTT transmission", 
                count, dataPointList.Count);

            if (fullConfig.DryRun)
            {
                log.LogInformation("Dry run enabled. Would insert {Count} datapoints over {C2} timeseries using MQTT", count, dataPointList.Count);
                collectionPool.ReturnDictionary(dataPointList);
                return null;
            }

            // Apply transmission strategy grouping
            var allDataPoints = dataPointList.SelectMany(kvp => kvp.Value);
            
            // CRITICAL FIX: Enumerate and materialize the collection immediately to prevent infinite loops
            var allDataPointsList = allDataPoints.ToList();
            
            // Start timing for complete MQTT processing (including grouping)
            var mqttProcessingStartTime = DateTime.UtcNow;
            log.LogInformation("-----------------------------");
            log.LogInformation("[STARTING GROUPING] Starting transmission strategy grouping with {Strategy}. Total datapoints: {Count}", 
                config.GetEffectiveTransmissionStrategy(), allDataPointsList.Count);
            
            log.LogInformation("[MQTT PUSH DEBUG] About to call GroupDataPoints with {Count} datapoints", allDataPointsList.Count);
            var groupedByStrategy = transmissionGrouper.GroupDataPoints(allDataPointsList);
            log.LogInformation("[MQTT PUSH DEBUG] GroupDataPoints returned {GroupCount} groups", groupedByStrategy.Count());
            
            // Convert grouped data to dictionary format for PushDataPointsChunk
            var convertedDataPointList = groupedByStrategy.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            
            // Log strategy-specific grouping information
            log.LogInformation("[Transmission Strategy] {Strategy} grouped {OriginalGroups} original groups into {NewGroups} strategy-based groups", 
                config.GetEffectiveTransmissionStrategy(), dataPointList.Count, convertedDataPointList.Count);
            
            foreach (var group in convertedDataPointList.Take(5)) // Log first 5 groups for debugging
            {
                log.LogInformation("[Strategy Group] '{GroupKey}' contains {DataPointCount} datapoints", 
                    group.Key, group.Value.Count());
            }
            
            var result = await PushDataPointsChunk(convertedDataPointList, mqttProcessingStartTime, token);

            if (!result)
            {
                log.LogDebug("Failed to push {Count} points to Target over MQTT", count);
                collectionPool.ReturnDictionary(dataPointList);
                return false;
            }

            log.LogDebug("Successfully pushed {Count} points to Target over MQTT (Current chunk size: {ChunkSize}, Max chunk size: {MaxChunkSize})", 
                count, adaptiveChunker.GetCurrentChunkSize(), config.MaxChunkSize);

            // Return collections to pool
            collectionPool.ReturnDictionary(dataPointList);

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
        /// Push a chunk of datapoints to MQTT.
        /// </summary>
        /// <param name="dataPointList">Datapoints to create, grouped by timeseries name.</param>
        /// <returns>True on success, false on failure</returns>
        private async Task<bool> PushDataPointsChunk(IDictionary<string, IEnumerable<UADataPoint>> dataPointList, DateTime mqttProcessingStartTime, CancellationToken token)
        {
            if (dataPointList == null || !dataPointList.Any()) return false;
            if (!client.IsConnected)
            {
                log.LogError("Could not write to MQTT, client not connected");
                return false;
            }
            try
            {
                if (!config.UseGrpc)
                {
                    var options = new JsonSerializerOptions
                    {
                        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
                    };

                    // Create payload creator function for adaptive chunking
                    Func<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>, object?> payloadCreator = chunk =>
                    {
                        // Extract groupKey from chunk - use first group's key as representative
                        var groupKey = chunk.FirstOrDefault().Key;
                        
                        return config.JsonFormatType switch
                        {
                            MqttJsonFormat.PollingSnapshotObject => CreatePollingSnapshotObjectPayload(chunk, groupKey),
                            MqttJsonFormat.PollingSnapshotPlain => CreatePollingSnapshotPlainPayload(chunk, groupKey),
                            MqttJsonFormat.Subscription => CreateSubscriptionPayload(chunk, groupKey),
                            MqttJsonFormat.Legacy or MqttJsonFormat.Timeseries => CreateLegacyPayload(chunk, groupKey),
                            MqttJsonFormat.PollingSnapshot => CreatePollingSnapshotPayload(chunk, groupKey),
                            _ => CreateLegacyPayload(chunk, groupKey)
                        };
                    };

                    // Use parallel processing for better performance
                    Func<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>, object?, Task<object?>> chunkProcessor = 
                        async (IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk, object? payload) =>
                        {
                            if (payload == null) return (object?)null;

                            // Count actual datapoints in this chunk
                            var actualDataPointCount = chunk.Select(pair => pair.Value.Count()).Sum();

                            var bytes = JsonSerializer.SerializeToUtf8Bytes(payload, options);
                            var msg = baseBuilder
                                .WithTopic(config.DatapointTopic)
                                .WithPayload(bytes)
                                .Build();

                            var result = await client.PublishAsync(msg, token);
                            
                            if (result.ReasonCode != MQTTnet.Client.MqttClientPublishReasonCode.Success)
                            {
                                log.LogError("Failed to write to MQTT: {reason}", result.ReasonString);
                                DataFailing = true;
                                throw new Exception(result.ReasonString);
                            }

                            // Log successful chunk transmission - use debug level for TAG_CHANGE_BASED to reduce noise
                            var strategy = config.GetEffectiveTransmissionStrategy();
                            if (strategy == MqttTransmissionStrategy.TAG_CHANGE_BASED)
                            {
                                log.LogDebug("[MQTT Chunk Success] Successfully sent {Count} datapoints to MQTT broker", actualDataPointCount);
                            }
                            else
                            {
                                log.LogInformation("[MQTT Chunk Success] Successfully sent {Count} datapoints to MQTT broker", actualDataPointCount);
                            }

                            return (object?)result;
                        };

                    // Process based on transmission strategy
                    if (config.GetEffectiveTransmissionStrategy() == MqttTransmissionStrategy.CHUNK_BASED)
                    {
                        // Use AdaptiveChunker for CHUNK_BASED strategy
                        var maxConcurrency = Math.Min(config.MaxConcurrency, Environment.ProcessorCount);
                        var success = await adaptiveChunker.ProcessChunksParallel(
                            dataPointList, 
                            payloadCreator, 
                            chunkProcessor, 
                            maxConcurrency, 
                            token);

                        if (!success)
                        {
                            DataFailing = true;
                            return false;
                        }
                    }
                    else
                    {
                        // For other strategies, process based on strategy type
                        log.LogInformation("[MQTT Strategy] Processing {GroupCount} groups using {Strategy} strategy", 
                            dataPointList.Count, config.GetEffectiveTransmissionStrategy());

                        if (config.GetEffectiveTransmissionStrategy() == MqttTransmissionStrategy.ROOT_NODE_BASED || 
                            config.GetEffectiveTransmissionStrategy() == MqttTransmissionStrategy.TAG_LIST_BASED)
                        {
                            // For ROOT_NODE_BASED and TAG_LIST_BASED, each group becomes one JSON message
                            foreach (var group in dataPointList)
                            {
                                // Create a single message with all datapoints in this group
                                var groupData = new[] { group };
                                
                                // For these strategies, directly pass the group key from the transmission grouper
                                var strategyGroupKey = group.Key;
                                
                                var payload = config.JsonFormatType switch
                                {
                                    MqttJsonFormat.PollingSnapshotObject => CreatePollingSnapshotObjectPayload(groupData, strategyGroupKey),
                                    MqttJsonFormat.PollingSnapshotPlain => CreatePollingSnapshotPlainPayload(groupData, strategyGroupKey),
                                    MqttJsonFormat.Subscription => CreateSubscriptionPayload(groupData, strategyGroupKey),
                                    MqttJsonFormat.Legacy or MqttJsonFormat.Timeseries => CreateLegacyPayload(groupData, strategyGroupKey),
                                    MqttJsonFormat.PollingSnapshot => CreatePollingSnapshotPayload(groupData, strategyGroupKey),
                                    _ => CreateLegacyPayload(groupData, strategyGroupKey)
                                };
                                
                                if (payload != null)
                                {
                                    await chunkProcessor(groupData, payload);
                                }
                            }
                        }
                        else if (config.GetEffectiveTransmissionStrategy() == MqttTransmissionStrategy.TAG_CHANGE_BASED)
                        {
                            // For TAG_CHANGE_BASED, each individual tag becomes a separate JSON message
                            foreach (var group in dataPointList)
                            {
                                foreach (var dataPoint in group.Value)
                                {
                                    var singlePointGroup = new[] { new KeyValuePair<string, IEnumerable<UADataPoint>>(group.Key, new[] { dataPoint }) };
                                    
                                    // For TAG_CHANGE_BASED, use the individual tag ID as the group key
                                    var tagChangeGroupKey = group.Key;
                                    
                                    var payload = config.JsonFormatType switch
                                    {
                                        MqttJsonFormat.PollingSnapshotObject => CreatePollingSnapshotObjectPayload(singlePointGroup, tagChangeGroupKey),
                                        MqttJsonFormat.PollingSnapshotPlain => CreatePollingSnapshotPlainPayload(singlePointGroup, tagChangeGroupKey),
                                        MqttJsonFormat.Subscription => CreateSubscriptionPayload(singlePointGroup, tagChangeGroupKey),
                                        MqttJsonFormat.Legacy or MqttJsonFormat.Timeseries => CreateLegacyPayload(singlePointGroup, tagChangeGroupKey),
                                        MqttJsonFormat.PollingSnapshot => CreatePollingSnapshotPayload(singlePointGroup, tagChangeGroupKey),
                                        _ => CreateLegacyPayload(singlePointGroup, tagChangeGroupKey)
                                    };
                                    
                                    if (payload != null)
                                    {
                                        await chunkProcessor(singlePointGroup, payload);
                                    }
                                }
                            }
                        }
                    }

                    // Calculate and log total transmitted datapoints
                    var totalTransmitted = dataPointList.Select(dp => (long)dp.Value.Count()).Sum();
                    var mqttProcessingEndTime = DateTime.UtcNow;
                    var mqttProcessingDuration = mqttProcessingEndTime - mqttProcessingStartTime;
                    
                    log.LogInformation("[MQTT Total Success] Successfully transmitted {Count} datapoints to MQTT broker using {Strategy} strategy", 
                        totalTransmitted, config.GetEffectiveTransmissionStrategy());
                    log.LogInformation("[MQTT Processing Time] Total MQTT transmission processing took {Duration}ms", 
                        mqttProcessingDuration.TotalMilliseconds);
                    log.LogInformation("-----------------------------");
                }
                else // Protobuf
        {
            var inserts = dataPointList.ToDictionary(
                pair => Identity.Create(pair.Key),
                pair => pair.Value.SelectNonNull(dp => dp.ToCDFDataPoint(fullConfig.Extraction.StatusCodes.IngestStatusCodes, log)));
            
            var (points, errors) = Sanitation.CleanDataPointsRequest(inserts, SanitationMode.Clean, config.NonFiniteReplacement);
            var cleaned = points.ToDictionary(pair => (Identity)pair.Key, pair => pair.Value);

            foreach (var error in errors)
            {
                log.LogCogniteError(error, RequestType.CreateDatapoints, true);
            }

                    if (cleaned.Count == 0) return true;

                    var req = cleaned.ToInsertRequest();
            if (req.Items.Count == 0) return true;

            var data = req.ToByteArray();

                    log.LogTrace("Using protobuf serialization for {count} timeseries", dataPointList.Count);
                    
            var msg = baseBuilder
                .WithTopic(config.DatapointTopic)
                        .WithPayload(data)
                .Build();

                    var result = await client.PublishAsync(msg, token);
                    if (result.ReasonCode != MQTTnet.Client.MqttClientPublishReasonCode.Success)
                    {
                        log.LogError("Failed to write to MQTT: {reason}", result.ReasonString);
                        DataFailing = true;
                        return false;
                    }
                }
                dataPointsCounter.Inc(dataPointList.Select(dp => (long)dp.Value.Count()).Sum());
                dataPointPushes.Inc();
                
                DataFailing = false;
                return true;
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to write to MQTT: {msg}", e.Message);
                DataFailing = true;
                return false;
            }
        }

        /// <summary>
        /// Calculate the time range of when data was received from OPC UA server
        /// </summary>
        /// <param name="chunk">Chunk of datapoints</param>
        /// <returns>Tuple of start and end times</returns>
        private (DateTime startTime, DateTime endTime) CalculateReceiveTimeRange(IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk)
        {
            var allDataPoints = chunk.SelectMany(kvp => kvp.Value);
            
            if (!allDataPoints.Any())
            {
                var now = DateTime.UtcNow;
                return (now, now);
            }
            
            var receivedTimes = allDataPoints.Select(dp => dp.ReceivedTimestamp).ToList();
            return (receivedTimes.Min(), receivedTimes.Max());
        }

        /// <summary>
        /// Create metadata object for JSON payload
        /// </summary>
        private Dictionary<string, object>? CreateMetadata(string dataIngestType, string? groupKey = null)
        {
            return CreateMetadata(dataIngestType, null, null, null, groupKey);
        }

        /// <summary>
        /// Create metadata object for JSON payload with specified receive time range
        /// </summary>
        /// <param name="dataIngestType">Type of data ingestion</param>
        /// <param name="msgRecvStart">Start time when data was received from OPC UA server</param>
        /// <param name="msgRecvEnd">End time when data was received from OPC UA server</param>
        /// <param name="chunk">Chunk of datapoints to extract sequence numbers from</param>
        /// <param name="groupKey">Group key for the current transmission strategy</param>
        /// <returns>Metadata dictionary</returns>
        private Dictionary<string, object>? CreateMetadata(string dataIngestType, DateTime? msgRecvStart, DateTime? msgRecvEnd, IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>? chunk = null, string? groupKey = null)
        {
            if (!config.IncludeMetadata) return null;

            // JSON message completion time (when metadata is being created)
            var messageCompletionTime = DateTime.UtcNow;

            var metadata = new Dictionary<string, object>
            {
                ["json_format_type"] = dataIngestType,
                ["message_timestamp"] = GetFormattedTimestamp(messageCompletionTime.ToUnixTimeMilliseconds())
            };

            if (config.IncludeMessageTimestamps && msgRecvStart.HasValue && msgRecvEnd.HasValue)
            {
                metadata["msgRecvStartTimestamp"] = GetFormattedTimestamp(msgRecvStart.Value.ToUnixTimeMilliseconds());
                metadata["msgRecvEndTimestamp"] = GetFormattedTimestamp(msgRecvEnd.Value.ToUnixTimeMilliseconds());
            }
            else if (config.IncludeMessageTimestamps)
            {
                // Fallback to current time if receive times are not provided
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                metadata["msgRecvStartTimestamp"] = GetFormattedTimestamp(now);
                metadata["msgRecvEndTimestamp"] = GetFormattedTimestamp(now);
            }

            // Add sequence number information if available
            if (chunk != null)
            {
                var allDataPoints = chunk.SelectMany(kvp => kvp.Value).ToList();
                var sequenceNumbers = allDataPoints
                    .Where(dp => dp.SequenceNumber.HasValue)
                    .Select(dp => dp.SequenceNumber!.Value)
                    .Distinct()
                    .OrderBy(seq => seq)
                    .ToList();

                if (sequenceNumbers.Any())
                {
                    if (sequenceNumbers.Count == 1)
                    {
                        metadata["sub_seq_num"] = sequenceNumbers.First();
                    }
                    else
                    {
                        metadata["sub_seq_num"] = sequenceNumbers; // Multiple sequence numbers as array
                    }
                }
            }

            // Add transmission strategy specific metadata
            var strategy = config.GetEffectiveTransmissionStrategy();
            
            // Add data_group_by field with the current transmission strategy
            metadata["data_group_by"] = strategy.ToString();
            
            // Add unified publish_group_name field based on strategy
            switch (strategy)
            {
                case MqttTransmissionStrategy.ROOT_NODE_BASED:
                case MqttTransmissionStrategy.TAG_LIST_BASED:
                    // For ROOT_NODE_BASED and TAG_LIST_BASED, use publish-groups.name
                    if (!string.IsNullOrEmpty(groupKey))
                    {
                        metadata["publish_group_name"] = RemoveNodeIdPrefix(groupKey);
                    }
                    break;

                case MqttTransmissionStrategy.CHUNK_BASED:
                    // Generate unique chunk_id based on sequence numbers or timestamp
                    string chunkId;
                    if (chunk != null)
                    {
                        var allDataPoints = chunk.SelectMany(kvp => kvp.Value).ToList();
                        var sequenceNumbers = allDataPoints
                            .Where(dp => dp.SequenceNumber.HasValue)
                            .Select(dp => dp.SequenceNumber!.Value)
                            .Distinct()
                            .OrderBy(seq => seq)
                            .ToList();

                        if (sequenceNumbers.Any())
                        {
                            // Create chunk_id based on sequence numbers
                            chunkId = $"chunk_{string.Join("_", sequenceNumbers)}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
                        }
                        else
                        {
                            // Fallback to timestamp-based chunk_id
                            chunkId = $"chunk_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}_{Guid.NewGuid().ToString("N")[..8]}";
                        }
                    }
                    else
                    {
                        // Fallback to timestamp-based chunk_id
                        chunkId = $"chunk_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}_{Guid.NewGuid().ToString("N")[..8]}";
                    }
                    // For CHUNK_BASED, use the auto-generated chunk_id
                    metadata["publish_group_name"] = chunkId;
                    break;

                case MqttTransmissionStrategy.TAG_CHANGE_BASED:
                    // For TAG_CHANGE_BASED, use the individual tag ID
                    if (!string.IsNullOrEmpty(groupKey))
                    {
                        metadata["publish_group_name"] = RemoveNodeIdPrefix(groupKey);
                    }
                    break;
            }

            return metadata;
        }

        /// <summary>
        /// Get OPC UA data type string (cached)
        /// </summary>
        private string GetDataTypeString(UADataPoint dp)
        {
            return formatCache.GetDataTypeString(dp);
        }

        /// <summary>
        /// Create payload for polling snapshot object format (case1)
        /// </summary>
        private object? CreatePollingSnapshotObjectPayload(IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk, string? groupKey = null)
        {
            if (!chunk.Any()) return null;

            var payload = new Dictionary<string, object>();
            
            var metadata = CreateMetadata("polling_snapshot", null, null, chunk, groupKey);
            if (metadata != null)
            {
                payload["metadata"] = metadata;
            }

            // Find the most common timestamp for shared timestamp
            var allDataPoints = chunk.SelectMany(kvp => kvp.Value).ToList();
            var sharedTimestamp = allDataPoints.GroupBy(dp => dp.Timestamp)
                .OrderByDescending(g => g.Count())
                .FirstOrDefault()?.Key ?? DateTime.UtcNow;

            var data = new Dictionary<string, object>
            {
                ["timestamp"] = GetFormattedTimestamp(sharedTimestamp.ToUnixTimeMilliseconds()),
                ["tags"] = chunk.SelectMany(kvp => kvp.Value.Select(dp => CreateTagObject(dp.Id, dp))).ToList()
            };

            payload["data"] = data;
            return payload;
        }

        /// <summary>
        /// Create payload for polling snapshot plain format (case2)
        /// </summary>
        private object? CreatePollingSnapshotPlainPayload(IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk, string? groupKey = null)
        {
            if (!chunk.Any()) return null;

            var payload = new Dictionary<string, object>();
            
            var metadata = CreateMetadata("polling_snapshot", null, null, chunk, groupKey);
            if (metadata != null)
            {
                // Use camelCase for case2 format
                var camelCaseMetadata = new Dictionary<string, object>();
                foreach (var kvp in metadata)
                {
                    var key = kvp.Key switch
                    {
                        "json_format_type" => "jsonFormatType",
                        "message_timestamp" => "messageTimestamp",
                        "data_group_by" => "dataGroupBy",
                        "publish_group_name" => "publishGroupName",
                        _ => kvp.Key
                    };
                    camelCaseMetadata[key] = kvp.Value;
                }
                payload["metadata"] = camelCaseMetadata;
            }

            // Find the most common timestamp for shared timestamp
            var allDataPoints = chunk.SelectMany(kvp => kvp.Value).ToList();
            var sharedTimestamp = allDataPoints.GroupBy(dp => dp.Timestamp)
                .OrderByDescending(g => g.Count())
                .FirstOrDefault()?.Key ?? DateTime.UtcNow;

            payload["timestamp"] = GetFormattedTimestamp(sharedTimestamp.ToUnixTimeMilliseconds());

            // Add tag values directly to payload
            foreach (var kvp in chunk)
            {
                // Check if this is a ROOT_NODE_BASED strategy where kvp.Value contains multiple tags
                var strategy = config.GetEffectiveTransmissionStrategy();
                
                if (strategy == MqttTransmissionStrategy.ROOT_NODE_BASED || 
                    strategy == MqttTransmissionStrategy.TAG_LIST_BASED)
                {
                    // For ROOT_NODE_BASED and TAG_LIST_BASED, kvp.Value contains multiple datapoints from different tags
                    // Group by individual tag ID and add each tag as a separate property
                    var tagGroups = kvp.Value.GroupBy(dp => dp.Id);
                    
                    foreach (var tagGroup in tagGroups)
                    {
                        var latestDataPoint = tagGroup.OrderByDescending(dp => dp.Timestamp).FirstOrDefault();
                        if (latestDataPoint != null)
                        {
                            payload[RemoveNodeIdPrefix(tagGroup.Key)] = latestDataPoint.IsString ? 
                                (object)(latestDataPoint.StringValue ?? "") : 
                                (latestDataPoint.DoubleValue ?? 0);
                        }
                    }
                }
                else
                {
                    // For other strategies, use the original logic
                    var latestDataPoint = kvp.Value.OrderByDescending(dp => dp.Timestamp).FirstOrDefault();
                    if (latestDataPoint != null)
                    {
                        payload[RemoveNodeIdPrefix(kvp.Key)] = latestDataPoint.IsString ? 
                            (object)(latestDataPoint.StringValue ?? "") : 
                            (latestDataPoint.DoubleValue ?? 0);
                    }
                }
            }

            return payload;
        }

        /// <summary>
        /// Create payload for subscription format (case3)
        /// </summary>
        private object? CreateSubscriptionPayload(IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk, string? groupKey = null)
        {
            if (!chunk.Any()) return null;

            // Calculate receive time range from datapoints
            var (msgRecvStart, msgRecvEnd) = CalculateReceiveTimeRange(chunk);

            var payload = new Dictionary<string, object>();
            
            // Create metadata with accurate receive time information
            var metadata = CreateMetadata("subscription", msgRecvStart, msgRecvEnd, chunk, groupKey);
            if (metadata != null)
            {
                payload["metadata"] = metadata;
            }

            // Handle ROOT_NODE_BASED and TAG_LIST_BASED strategies: group datapoints by tag ID and preserve multiple values per tag
            var strategy = config.GetEffectiveTransmissionStrategy();
            if (strategy == MqttTransmissionStrategy.ROOT_NODE_BASED || 
                strategy == MqttTransmissionStrategy.TAG_LIST_BASED)
            {
                var tagsData = new List<Dictionary<string, object>>();
                
                // Group all datapoints by tag ID across all strategy groups
                var tagGroupedData = new Dictionary<string, List<UADataPoint>>();
                
                foreach (var kvp in chunk)
                {
                    foreach (var dp in kvp.Value)
                    {
                        if (!tagGroupedData.ContainsKey(dp.Id))
                        {
                            tagGroupedData[dp.Id] = new List<UADataPoint>();
                        }
                        tagGroupedData[dp.Id].Add(dp);
                    }
                }
                
                // Create tag entries with all datapoints for each tag
                foreach (var tagGroup in tagGroupedData)
                {
                    tagsData.Add(new Dictionary<string, object>
                    {
                        ["tag"] = RemoveNodeIdPrefix(tagGroup.Key), // Individual Tag ID without prefixes (e.g., "S.A.Tag1")
                        ["data"] = tagGroup.Value.Select(dp => CreateDataPointObject(dp)).ToList() // All datapoints for this tag
                    });
                }
                
                payload["tags_data"] = tagsData;
                
                log.LogTrace("[CreateSubscriptionPayload] {Strategy} strategy: processed {GroupCount} groups into {TagCount} individual tags", 
                    strategy, chunk.Count(), tagGroupedData.Count);
            }
            else
            {
                // For other strategies (CHUNK_BASED, TAG_CHANGE_BASED), use the original logic
                var tagsData = chunk.Select(kvp => new Dictionary<string, object>
                {
                    ["tag"] = RemoveNodeIdPrefix(kvp.Key),
                    ["data"] = kvp.Value.Select(dp => CreateDataPointObject(dp)).ToList()
                }).ToList();

                payload["tags_data"] = tagsData;
            }
            
            return payload;
        }

        /// <summary>
        /// Create payload for legacy format (existing format)
        /// </summary>
        private object? CreateLegacyPayload(IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk, string? groupKey = null)
        {
            var payloadList = new List<object>();
            log.LogTrace("Using JSON serialization for {count} datapoint items", chunk.Count());
            
            // For legacy format, add metadata as a separate object if enabled
            if (config.IncludeMetadata)
            {
                var metadata = CreateMetadata("legacy", null, null, chunk, groupKey);
                if (metadata != null)
                {
                    payloadList.Add(new { metadata = metadata });
                }
            }
            
            foreach (var kvp in chunk)
            {
                var numeric = new List<DataPoint>();
                var str = new List<StringDataPoint>();
                
                foreach (var dp in kvp.Value)
                {
                    if (dp.IsString) 
                        str.Add(new StringDataPoint { Timestamp = GetFormattedTimestamp(dp.Timestamp.ToUnixTimeMilliseconds()), Value = dp.StringValue! });
                    else 
                        numeric.Add(new DataPoint { Timestamp = GetFormattedTimestamp(dp.Timestamp.ToUnixTimeMilliseconds()), Value = dp.DoubleValue ?? 0 });
                }

                payloadList.AddRange(numeric.Cast<object>().Concat(str));
            }
            
            return payloadList.Any() ? payloadList : null;
        }

        /// <summary>
        /// Create payload for polling snapshot format (deprecated, for backward compatibility)
        /// </summary>
        private object? CreatePollingSnapshotPayload(IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk, string? groupKey = null)
        {
            var payloadList = new List<object>();
            log.LogTrace("Using JSON serialization for {count} datapoint items", chunk.Count());
            
            // For polling snapshot format, add metadata as a separate object if enabled
            if (config.IncludeMetadata)
            {
                var metadata = CreateMetadata("polling_snapshot_deprecated", null, null, chunk, groupKey);
                if (metadata != null)
                {
                    payloadList.Add(new { metadata = metadata });
                }
            }
            
            foreach (var kvp in chunk)
            {
                var numeric = new List<DataPoint>();
                var str = new List<StringDataPoint>();
                
                foreach (var dp in kvp.Value)
                {
                    if (dp.IsString) 
                        str.Add(new StringDataPoint { Timestamp = GetFormattedTimestamp(dp.Timestamp.ToUnixTimeMilliseconds()), Value = dp.StringValue! });
                    else 
                        numeric.Add(new DataPoint { Timestamp = GetFormattedTimestamp(dp.Timestamp.ToUnixTimeMilliseconds()), Value = dp.DoubleValue ?? 0 });
                }

                var wrapper = new JsonDataPointWrapper(kvp.Key);
                if (numeric.Any()) wrapper.NumericDatapoints = numeric;
                if (str.Any()) wrapper.StringDatapoints = str;
                payloadList.Add(wrapper);
            }
            
            return payloadList.Any() ? payloadList : null;
        }

        /// <summary>
        /// Create tag object for polling snapshot object format
        /// </summary>
        private Dictionary<string, object> CreateTagObject(string tagId, UADataPoint dp)
        {
            var tagObj = new Dictionary<string, object>
            {
                [RemoveNodeIdPrefix(tagId)] = dp.IsString ? (object)(dp.StringValue ?? "") : (dp.DoubleValue ?? 0)
            };

            if (config.IncludeDataType)
            {
                tagObj["dt"] = GetDataTypeString(dp);
            }

            if (config.IncludeStatusCode)
            {
                tagObj["sc"] = (int)dp.Status.Code;
            }

            return tagObj;
        }

        /// <summary>
        /// Create data point object for subscription format
        /// </summary>
        private Dictionary<string, object> CreateDataPointObject(UADataPoint dp)
        {
            var dataObj = new Dictionary<string, object>
            {
                ["timestamp"] = GetFormattedTimestamp(dp.Timestamp.ToUnixTimeMilliseconds()),
                ["value"] = dp.IsString ? (object)(dp.StringValue ?? "") : (dp.DoubleValue ?? 0)
            };

            if (config.IncludeStatusCode)
            {
                dataObj["sc"] = (int)dp.Status.Code;
            }

            if (config.IncludeDataType)
            {
                dataObj["dt"] = GetDataTypeString(dp);
            }

            return dataObj;
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
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
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

            var data = JsonSerializer.SerializeToUtf8Bytes(assets, new JsonSerializerOptions
            {
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });

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
                    var minimalData = JsonSerializer.SerializeToUtf8Bytes(minimalTimeseries, new JsonSerializerOptions
                    {
                        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                    });

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
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
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

            var data = JsonSerializer.SerializeToUtf8Bytes(timeseries, new JsonSerializerOptions
            {
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });
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

            var data = JsonSerializer.SerializeToUtf8Bytes(events, new JsonSerializerOptions
            {
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });

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
            var data = JsonSerializer.SerializeToUtf8Bytes(references, new JsonSerializerOptions
            {
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });

            if (useRawStore)
            {
                var rawObj = new RawRequestWrapper<RelationshipCreate>(
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.RelationshipsTable!,
                    references.Select(rel => new RawRowCreateDto<RelationshipCreate>(rel.ExternalId, rel)));

                var rawData = JsonSerializer.SerializeToUtf8Bytes(rawObj, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
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

        /// <summary>
        /// JSON format datapoint wrapper for non-gRPC serialization
        /// </summary>
        private class JsonDataPointWrapper
        {
            public string ExternalId { get; set; }
            public IEnumerable<DataPoint>? NumericDatapoints { get; set; }
            public IEnumerable<StringDataPoint>? StringDatapoints { get; set; }

            public JsonDataPointWrapper(string externalId)
            {
                ExternalId = externalId;
            }
        }

        /// <summary>
        /// JSON format datapoint for numeric values
        /// </summary>
        private class DataPoint
        {
            public object Timestamp { get; set; }
            public double Value { get; set; }

            public DataPoint()
            {
            }
        }

        /// <summary>
        /// JSON format datapoint for string values
        /// </summary>
        private class StringDataPoint
        {
            public object Timestamp { get; set; }
            public string Value { get; set; }

            public StringDataPoint()
            {
            }
        }

        private class JsonDataPointObject
        {
            public IEnumerable<DataPoint>? NumericDatapoints { get; set; }
            public IEnumerable<StringDataPoint>? StringDatapoints { get; set; }
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

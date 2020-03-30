using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using Prometheus.Client;
using Serilog;

namespace Cognite.OpcUa.Pushers
{
    public sealed class MqttPusher : IPusher
    {
        public int Index { get; set; }
        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }
        public Extractor Extractor { get; set; }
        public PusherConfig BaseConfig => config;
        private readonly MqttPusherConfig config;
        private readonly IMqttClient client;
        private readonly IMqttClientOptions options;

        private readonly ILogger log = Log.ForContext(typeof(MqttPusher));

        private MqttApplicationMessageBuilder baseBuilder;

        private readonly DateTime minDateTime = new DateTime(1971, 1, 1);
        private readonly ConcurrentDictionary<string, TimeRange> ranges = new ConcurrentDictionary<string, TimeRange>();

        private HashSet<string> existingNodes;

        private bool connected;

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
        public MqttPusher(MqttPusherConfig config)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            var builder = new MqttClientOptionsBuilder()
                .WithClientId(config.ClientId)
                .WithTcpServer(config.Host, config.Port)
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
            Task.Run(async () =>
            {
                try
                {
                    await client.ConnectAsync(options, CancellationToken.None);
                    connected = true;
                }
                catch (Exception e)
                {
                    log.Warning("Failed to connect to MQTT server: {msg}", e.Message);
                }
            });
            client.UseDisconnectedHandler(_ =>
            {
                connected = false;
                Log.Warning("MQTT client disconnected");
                return Task.CompletedTask;
            });
            client.UseConnectedHandler(_ =>
            {
                connected = true;
                Log.Information("MQTT client connected");
                return Task.CompletedTask;
            });
            baseBuilder = new MqttApplicationMessageBuilder()
                .WithAtLeastOnceQoS();
        }
        #region interface
        public async Task<bool?> PushDataPoints(IEnumerable<BufferedDataPoint> points, CancellationToken token)
        {
            if (points == null) return null;
            if (!connected) return false;
            int count = 0;
            var dataPointList = new Dictionary<string, List<BufferedDataPoint>>();

            foreach (var lBuffer in points)
            {
                var buffer = lBuffer;
                if (buffer.Timestamp < minDateTime)
                {
                    skippedDatapoints.Inc();
                    continue;
                }
                // We do not subscribe to changes in history, so an update to a point within the known range is due to
                // something being out of synch.
                if (ranges.ContainsKey(buffer.Id)
                    && buffer.Timestamp < ranges[buffer.Id].End
                    && buffer.Timestamp > ranges[buffer.Id].Start) continue;

                if (!buffer.IsString && (!double.IsFinite(buffer.DoubleValue) || buffer.DoubleValue >= 1E100 || buffer.DoubleValue <= -1E100))
                {
                    if (config.NonFiniteReplacement != null)
                    {
                        buffer = new BufferedDataPoint(buffer, config.NonFiniteReplacement.Value);
                    }
                    else
                    {
                        skippedDatapoints.Inc();
                        continue;
                    }
                }

                if (buffer.IsString && buffer.StringValue == null)
                {
                    buffer = new BufferedDataPoint(buffer, "");
                }

                count++;
                if (!dataPointList.ContainsKey(buffer.Id))
                {
                    dataPointList[buffer.Id] = new List<BufferedDataPoint>();
                }
                dataPointList[buffer.Id].Add(buffer);
            }

            if (count == 0)
            {
                log.Verbose("Push 0 datapoints to CDF");
                return null;
            }
            log.Debug("Push {NumDatapointsToPush} datapoints to CDF", count);
            var dpChunks = ExtractorUtils.ChunkDictOfLists(dataPointList, 100000, 10000).ToArray();
            var pushTasks = dpChunks.Select(chunk => PushDataPointsChunk(chunk, token)).ToList();
            var results = await Task.WhenAll(pushTasks);


            if (!results.All(res => res)) return false;

            foreach ((string key, var value) in dataPointList)
            {
                var last = value.Max(dp => dp.Timestamp);
                var first = value.Min(dp => dp.Timestamp);
                if (!ranges.ContainsKey(key))
                {
                    ranges[key] = new TimeRange(first, last);
                }
                else
                {
                    if (last < ranges[key].End)
                    {
                        ranges[key].End = last;
                    }

                    if (first > ranges[key].Start)
                    {
                        ranges[key].Start = first;
                    }
                }
            }
            return true;
        }
        public async Task<bool?> TestConnection(FullConfig _, CancellationToken token)
        {
            if (connected) return true;
            try
            {
                await client.ConnectAsync(options, token);
                connected = true;
            }
            catch (Exception e)
            {
                log.Warning("Failed to connect to MQTT server: {msg}", e.Message);
                connected = false;
            }

            return connected;
        }

        public async Task<bool> PushNodes(IEnumerable<BufferedNode> objects, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            if (!connected) return false;
            if (!string.IsNullOrEmpty(config.LocalState))
            {
                if (existingNodes == null)
                {
                    var existing = await Extractor.StateStorage.ReadMqttStates(config.LocalState,
                        DateTimeOffset.FromUnixTimeMilliseconds(config.InvalidateBefore).DateTime, token);
                    existingNodes = new HashSet<string>(existing);
                }

                if (existingNodes.Any())
                {
                    objects = objects.Where(obj => !existingNodes.Contains(Extractor.GetUniqueId(obj.Id))).ToList();
                    variables = variables.Where(variable => !existingNodes.Contains(Extractor.GetUniqueId(variable.Id, variable.Index))).ToList();
                }
            }
            if (!objects.Any() && !variables.Any()) return true;
            await Extractor.ReadProperties(objects.Concat(variables), token);

            if (objects.Any())
            {

                var results = await Task.WhenAll(ExtractorUtils.ChunkBy(objects, 1000).Select(chunk => PushAssets(chunk, token)));
                if (!results.All(res => res)) return false;
            }

            if (variables.Any())
            {
                var results = await Task.WhenAll(ExtractorUtils.ChunkBy(variables, 1000).Select(chunk => PushTimeseries(chunk, token)));
                if (!results.All(res => res)) return false;
            }

            if (!string.IsNullOrEmpty(config.LocalState))
            {
                var newIds = objects
                    .Select(obj => Extractor.GetUniqueId(obj.Id))
                    .Concat(variables.Select(variable => Extractor.GetUniqueId(variable.Id, variable.Index)))
                    .ToList();
                await Extractor.StateStorage.StoreMqttStates(config.LocalState, newIds, token);
            }

            return true;
        }
        public async Task<bool?> PushEvents(IEnumerable<BufferedEvent> events, CancellationToken token)
        {
            if (events == null) return null;
            var eventList = new List<BufferedEvent>();
            int count = 0;
            foreach (var buffEvent in events)
            {
                if (buffEvent.Time < minDateTime && !config.Debug)
                {
                    skippedEvents.Inc();
                    continue;
                }
                eventList.Add(buffEvent);
                count++;
            }
            if (count == 0)
            {
                log.Verbose("Push 0 events to CDF");
                return null;
            }
            log.Debug("Push {NumEventsToPush} events to CDF", count);
            if (config.Debug) return null;

            var results = await Task.WhenAll(ExtractorUtils.ChunkBy(eventList, 1000).Select(chunk => PushEventsChunk(chunk, token)));
            return results.All(result => result);
        }

        /// <summary>
        /// Reset the pusher, preparing it to be restarted
        /// </summary>
        public void Reset()
        {
            existingNodes = null;
            ranges.Clear();
        }

        #endregion
        #region pushing
        private async Task<bool> PushDataPointsChunk(IDictionary<string, IEnumerable<BufferedDataPoint>> dataPointList, CancellationToken token)
        {
            if (config.Debug) return true;
            if (!connected) return false;
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
                            Value = ipoint.DoubleValue
                        }));
                }

                count += values.Count();
                return item;
            });

            var req = new DataPointInsertionRequest();
            req.Items.AddRange(inserts);
            if (!req.Items.Any()) return true;

            await using (var outStream = new MemoryStream())
            {
                req.WriteTo(outStream);
                var msg = baseBuilder
                    .WithPayload(outStream)
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
            }

            return true;
        }

        private async Task<bool> PushAssets(IEnumerable<BufferedNode> objects, CancellationToken token)
        {
            var assets = objects.Select(NodeToAsset);
            await using (var outStream = new MemoryStream())
            {
                await JsonSerializer.SerializeAsync(outStream, assets, null, token);
                var msg = baseBuilder
                    .WithPayload(outStream)
                    .WithTopic(config.AssetTopic)
                    .Build();

                try
                {
                    await client.PublishAsync(msg, token);
                }
                catch (Exception e)
                {
                    log.Error("Failed to write assets to MQTT: {msg}", e.Message);
                    return false;
                }
            }

            return true;
        }

        private async Task<bool> PushTimeseries(IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            var timeseries = variables.Select(VariableToTimeseries);
            await using (var outStream = new MemoryStream())
            {
                await JsonSerializer.SerializeAsync(outStream, timeseries, null, token);
                var msg = baseBuilder
                    .WithPayload(outStream)
                    .WithTopic(config.TSTopic)
                    .Build();

                try
                {
                    await client.PublishAsync(msg, token);
                }
                catch (Exception e)
                {
                    log.Error("Failed to write timeseries to MQTT: {msg}", e.Message);
                    return false;
                }
            }

            return true;
        }

        public async Task<bool> PushEventsChunk(IEnumerable<BufferedEvent> evts, CancellationToken token)
        {
            var events = evts.Select(EventToCDFEvent);
            await using (var outStream = new MemoryStream())
            {
                await JsonSerializer.SerializeAsync(outStream, events, null, token);
                var msg = baseBuilder
                    .WithPayload(outStream)
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
            }
            eventCounter.Inc(evts.Count());
            eventPushCounter.Inc();

            return true;
        }

        #endregion
        #region payload
        /// <summary>
        /// Converts BufferedNode into asset write poco.
        /// </summary>
        /// <param name="node">Node to be converted</param>
        /// <returns>Full asset write poco</returns>
        private AssetCreate NodeToAsset(BufferedNode node)
        {
            var writePoco = new AssetCreate
            {
                Description = ExtractorUtils.Truncate(node.Description, 500),
                ExternalId = Extractor.GetUniqueId(node.Id),
                Name = string.IsNullOrEmpty(node.DisplayName)
                    ? ExtractorUtils.Truncate(Extractor.GetUniqueId(node.Id), 140) : ExtractorUtils.Truncate(node.DisplayName, 140),
                DataSetId = config.DataSetId
            };
            if (node.ParentId != null && !node.ParentId.IsNullNodeId)
            {
                writePoco.ParentExternalId = Extractor.GetUniqueId(node.ParentId);
            }
            if (node.Properties != null && node.Properties.Any())
            {
                writePoco.Metadata = node.Properties
                    .Where(prop => prop.Value != null)
                    .Take(16)
                    .ToDictionary(prop => ExtractorUtils.Truncate(prop.DisplayName, 32), prop => ExtractorUtils.Truncate(prop.Value.StringValue, 256));
            }
            return writePoco;
        }
        /// <summary>
        /// Get the value of given object assumed to be a timestamp as the number of milliseconds since 1/1/1970
        /// </summary>
        /// <param name="value">Value of the object. Assumed to be a timestamp or numeric value</param>
        /// <returns>Milliseconds since epoch</returns>
        private static long GetTimestampValue(object value)
        {
            if (value is DateTime dt)
            {
                return new DateTimeOffset(dt).ToUnixTimeMilliseconds();
            }
            else
            {
                return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            }
        }
        private static readonly HashSet<string> excludeMetaData = new HashSet<string> {
            "StartTime", "EndTime", "Type", "SubType"
        };
        /// <summary>
        /// Transform BufferedEvent into EventEntity to be sent to CDF.
        /// </summary>
        /// <param name="evt">Event to be transformed.</param>
        /// <returns>Final EventEntity object</returns>
        private StatelessEventCreate EventToCDFEvent(BufferedEvent evt)
        {
            var entity = new StatelessEventCreate
            {
                Description = ExtractorUtils.Truncate(evt.Message, 500),
                StartTime = evt.MetaData.ContainsKey("StartTime")
                    ? GetTimestampValue(evt.MetaData["StartTime"])
                    : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                EndTime = evt.MetaData.ContainsKey("EndTime")
                    ? GetTimestampValue(evt.MetaData["EndTime"])
                    : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                AssetExternalIds = new List<string> { Extractor.GetUniqueId(evt.SourceNode) },
                ExternalId = ExtractorUtils.Truncate(evt.EventId, 255),
                Type = ExtractorUtils.Truncate(evt.MetaData.ContainsKey("Type")
                    ? Extractor.ConvertToString(evt.MetaData["Type"])
                    : Extractor.GetUniqueId(evt.EventType), 64),
                DataSetId = config.DataSetId
            };
            var finalMetaData = new Dictionary<string, string>();
            int len = 1;
            finalMetaData["Emitter"] = Extractor.GetUniqueId(evt.EmittingNode);
            if (!evt.MetaData.ContainsKey("SourceNode"))
            {
                finalMetaData["SourceNode"] = Extractor.GetUniqueId(evt.SourceNode);
                len++;
            }
            if (evt.MetaData.ContainsKey("SubType"))
            {
                entity.Subtype = ExtractorUtils.Truncate(Extractor.ConvertToString(evt.MetaData["SubType"]), 64);
            }

            foreach (var dt in evt.MetaData)
            {
                if (!excludeMetaData.Contains(dt.Key))
                {
                    finalMetaData[ExtractorUtils.Truncate(dt.Key, 32)] =
                        ExtractorUtils.Truncate(Extractor.ConvertToString(dt.Value), 256);
                }

                if (len++ == 15) break;
            }

            if (finalMetaData.Any())
            {
                entity.Metadata = finalMetaData;
            }
            return entity;
        }
        /// <summary>
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="variable">Variable to be converted</param>
        /// <returns>Complete timeseries write poco</returns>
        private StatelessTimeSeriesCreate VariableToTimeseries(BufferedVariable variable)
        {
            string externalId = Extractor.GetUniqueId(variable.Id, variable.Index);
            var writePoco = new StatelessTimeSeriesCreate
            {
                Description = ExtractorUtils.Truncate(variable.Description, 1000),
                ExternalId = externalId,
                AssetExternalId = Extractor.GetUniqueId(variable.ParentId),
                Name = ExtractorUtils.Truncate(variable.DisplayName, 255),
                LegacyName = externalId,
                IsString = variable.DataType.IsString,
                IsStep = variable.DataType.IsStep,
                DataSetId = config.DataSetId
            };
            if (variable.Properties != null && variable.Properties.Any())
            {
                writePoco.Metadata = variable.Properties
                    .Where(prop => prop.Value != null)
                    .Take(16)
                    .ToDictionary(prop => ExtractorUtils.Truncate(prop.DisplayName, 32), prop => ExtractorUtils.Truncate(prop.Value.StringValue, 256));
            }
            return writePoco;
        }

        class StatelessEventCreate : EventCreate
        {
            public IEnumerable<string> AssetExternalIds { get; set; }
        }

        class StatelessTimeSeriesCreate : TimeSeriesCreate
        {
            public string AssetExternalId { get; set; }
        }
        #endregion
        public void Dispose() { }
    }
}

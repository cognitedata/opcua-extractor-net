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

using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using Opc.Ua.Client;
using AdysTech.InfluxDB.Client.Net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Represents a data point to be sent to InfluxDB
    /// </summary>
    public class InfluxDBDataPoint
    {
        public string NodeId { get; set; } = string.Empty;
        public string NodeName { get; set; } = string.Empty;
        public double Value { get; set; }
        public string StatusCode { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public DateTime SourceTimestamp { get; set; }
        public DateTime ServerTimestamp { get; set; }
        public string Measurement { get; set; } = string.Empty;
        public Dictionary<string, string> Tags { get; set; } = new Dictionary<string, string>();
    }

    /// <summary>
    /// Represents the state of a node being mapped to InfluxDB metrics.
    /// </summary>
    public class InfluxDBMetricState : UAHistoryExtractionState
    {
        private readonly InfluxDBMetricsManager manager;
        private readonly UADataType dt;
        private readonly IUAClientAccess client;
        private readonly string nodeName;
        private readonly string cleanNodeName;

        public InfluxDBMetricState(IUAClientAccess client, NodeId id, UADataType dt, InfluxDBMetricsManager manager, string nodeName) 
            : base(client, id, false, false)
        {
            this.manager = manager;
            this.dt = dt;
            this.client = client;
            this.nodeName = nodeName;
            this.cleanNodeName = new Regex("[^a-zA-Z0-9_:]").Replace(nodeName, "_");
        }

        /// <summary>
        /// Update the metric with the value given in <paramref name="vt"/>, as double.
        /// If the value cannot be mapped as double, then it is not used. Metrics only support numerical values.
        /// </summary>
        /// <param name="vt">Value to set</param>
        public void UpdateMetricValue(Variant vt)
        {
            var dp = dt.ToDataPoint(client, vt, DateTime.UtcNow, Id, StatusCodes.Good, false);
            if (dp.IsString || !dp.DoubleValue.HasValue) return;
            
            var dataPoint = new InfluxDBDataPoint
            {
                NodeId = Id.ToString(),
                NodeName = nodeName,
                Value = dp.DoubleValue.Value,
                StatusCode = dp.Status.ToString(),
                DataType = dt.Name,
                SourceTimestamp = dp.Timestamp,
                ServerTimestamp = dp.ReceivedTimestamp,
                Measurement = $"{manager.Config.MeasurementPrefix}_{cleanNodeName}",
                Tags = new Dictionary<string, string>
                {
                    { "node_id", Id.ToString() },
                    { "node_name", nodeName },
                    { "data_type", dt.Name },
                    { "status_code", dp.Status.ToString() }
                }
            };
            
            manager.EnqueueDataPoint(dataPoint);
        }
    }

    /// <summary>
    /// Utility class handling reading nodes as InfluxDB metrics.
    /// </summary>
    public class InfluxDBMetricsManager : IDisposable, IAsyncDisposable
    {
        private readonly InfluxDBMetricsConfig config;
        private readonly UAClient client;
        private readonly Dictionary<NodeId, InfluxDBMetricState> metrics = new Dictionary<NodeId, InfluxDBMetricState>();
        private readonly ILogger<InfluxDBMetricsManager> log;
        private readonly Queue<InfluxDBDataPoint> dataPointQueue = new Queue<InfluxDBDataPoint>();
        private readonly object queueLock = new object();
        private readonly Timer flushTimer;
        private InfluxDBClient? influxClient;
        private volatile bool disposed = false;
        
        public InfluxDBMetricsConfig Config => config;

        public InfluxDBMetricsManager(UAClient client, InfluxDBMetricsConfig config, ILogger<InfluxDBMetricsManager> log)
        {
            this.config = config;
            this.client = client;
            this.log = log;
            // Use config.Url as is to maintain consistency with InitializeInfluxDB
            influxClient = new InfluxDBClient(config.Url, config.Username, config.Password);
            flushTimer = new Timer(async _ => await FlushDataPointsAsync(), null, TimeSpan.FromMilliseconds(config.FlushIntervalMs), TimeSpan.FromMilliseconds(config.FlushIntervalMs));
        }

        private readonly NodeId[] serverDiagnostics = new[]
        {
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_CurrentSessionCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_CurrentSubscriptionCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_PublishingIntervalCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_ServerViewCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_SessionAbortCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_SessionTimeoutCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_RejectedRequestsCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_RejectedSessionCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_SecurityRejectedRequestsCount,
            VariableIds.Server_ServerDiagnostics_ServerDiagnosticsSummary_SecurityRejectedSessionCount
        };

        private readonly uint[] attributes = new[]
        {
            Attributes.DisplayName,
            Attributes.DataType,
            Attributes.NodeClass,
            Attributes.Description
        };

        /// <summary>
        /// Initialize InfluxDB connection
        /// </summary>
        private async Task InitializeInfluxDB()
        {
            try
            {
                if (influxClient == null)
                {
                    throw new InvalidOperationException("InfluxDB client not initialized");
                }
                // Test connection by trying to get databases
                var databases = await influxClient.GetInfluxDBNamesAsync();
                log.LogInformation("Connected to InfluxDB at {Url}", config.Url);
                // Create database if it doesn't exist
                if (!databases.Contains(config.Database))
                {
                    await influxClient.CreateDatabaseAsync(config.Database);
                    log.LogInformation("Created database {Database}", config.Database);
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to initialize InfluxDB connection to {Url}", config.Url);
                throw;
            }
        }

        /// <summary>
        /// Start or restart the InfluxDB metric manager, by subscribing to requested OPC-UA nodes.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task StartInfluxDBMetrics(TypeManager typeManager, CancellationToken token)
        {
            if (!config.Enabled) return;
            
            await InitializeInfluxDB();
            metrics.Clear();

            var nodes = new List<NodeId>();
            if (config.OtherMetrics != null && config.OtherMetrics.Any())
            {
                foreach (var proto in config.OtherMetrics)
                {
                    nodes.Add(proto.ToNodeId(client.Context));
                }
            }

            if (config.ServerMetrics)
            {
                foreach (var node in serverDiagnostics)
                {
                    nodes.Add(node);
                }
            }

            if (nodes.Count == 0) return;

            var readValueIds = new ReadValueIdCollection(nodes
                .SelectMany(node => attributes.Select(attr => new ReadValueId { AttributeId = attr, NodeId = node }))
            );

            var results = await client.ReadAttributes(readValueIds, nodes.Count, token, "InfluxDB node metrics");

            int attrPerNode = attributes.Length;

            var cleanRegex = new Regex("[^a-zA-Z0-9_:]");

            for (int i = 0; i < nodes.Count; i++)
            {
                var nc = (NodeClass)results[i * attrPerNode + 2].Value;
                if (nc != NodeClass.Variable) continue;
                var rawDt = results[i * attrPerNode + 1].GetValue(NodeId.Null);
                var dt = typeManager.GetDataType(rawDt);
                var name = results[i * attrPerNode].GetValue<LocalizedText?>(null)?.Text;
                if (name == null) continue;

                var desc = results[i * attrPerNode + 3].GetValue<LocalizedText?>(null)?.Text;

                var state = new InfluxDBMetricState(client, nodes[i], dt, this, name);
                metrics[nodes[i]] = state;
            }

            if (metrics.Count == 0) return;

            if (client.SubscriptionManager == null) throw new InvalidOperationException("Client not initialized");

            client.SubscriptionManager.EnqueueTask(new InfluxDBMetricsSubscriptionTask(
                SubscriptionHandler,
                metrics,
                client.Callbacks));
                
            log.LogInformation("Started InfluxDB metrics collection for {Count} nodes", metrics.Count);
        }

        /// <summary>
        /// Converts datapoint callback to InfluxDB metric value
        /// </summary>
        /// <param name="item">MonitoredItem being triggered</param>
        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs _)
        {
            if (item == null) return;
            if (!metrics.TryGetValue(item.ResolvedNodeId, out var state)) return;
            foreach (var datapoint in item.DequeueValues())
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    UAExtractor.BadDataPoints.Inc();
                    continue;
                }
                state.UpdateMetricValue(datapoint.WrappedValue);
            }
        }

        /// <summary>
        /// Enqueue a data point for batch processing
        /// </summary>
        /// <param name="dataPoint">Data point to enqueue</param>
        public void EnqueueDataPoint(InfluxDBDataPoint dataPoint)
        {
            bool shouldFlush = false;
            lock (queueLock)
            {
                dataPointQueue.Enqueue(dataPoint);
                
                // Flush if batch size is reached
                if (dataPointQueue.Count >= config.BatchSize)
                {
                    shouldFlush = true;
                }
            }
            
            if (shouldFlush)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await FlushDataPointsAsync();
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Failed to flush data points from batch trigger");
                    }
                });
            }
        }

        /// <summary>
        /// Flush queued data points to InfluxDB
        /// </summary>
        private async Task FlushDataPointsAsync()
        {
            if (disposed || influxClient == null) return;
            List<InfluxDBDataPoint> pointsToFlush;
            lock (queueLock)
            {
                if (dataPointQueue.Count == 0) return;
                pointsToFlush = new List<InfluxDBDataPoint>(dataPointQueue);
                dataPointQueue.Clear();
            }
            try
            {
                var influxPoints = pointsToFlush.Select(dp => {
                    var point = new InfluxDatapoint<double>
                    {
                        UtcTimestamp = dp.SourceTimestamp,
                        MeasurementName = string.IsNullOrEmpty(dp.Measurement) ? "opcua_metric" : dp.Measurement
                    };
                    point.Fields.Add("value", dp.Value);
                    // 나머지는 Tags로 - null 값들을 안전한 기본값으로 대체
                    point.Tags.Add("node_id", string.IsNullOrEmpty(dp.NodeId) ? "unknown" : dp.NodeId);
                    point.Tags.Add("node_name", string.IsNullOrEmpty(dp.NodeName) ? "unknown" : dp.NodeName);
                    point.Tags.Add("status_code", string.IsNullOrEmpty(dp.StatusCode) ? "unknown" : dp.StatusCode);
                    point.Tags.Add("data_type", string.IsNullOrEmpty(dp.DataType) ? "unknown" : dp.DataType);
                    foreach (var tag in dp.Tags)
                    {
                        if (!string.IsNullOrEmpty(tag.Key) && !string.IsNullOrEmpty(tag.Value) && !point.Tags.ContainsKey(tag.Key))
                            point.Tags.Add(tag.Key, tag.Value);
                    }
                    return (IInfluxDatapoint)point;
                }).ToList();
                
                // Direct await instead of fire-and-forget for proper disposal handling
                await influxClient.PostPointsAsync(config.Database, influxPoints, config.BatchSize);
                log.LogDebug("Flushed {Count} data points to InfluxDB", pointsToFlush.Count);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to write {Count} data points to InfluxDB", pointsToFlush.Count);
            }
        }

        /// <summary>
        /// Flush queued data points to InfluxDB (synchronous wrapper for timer callback)
        /// </summary>
        private void FlushDataPoints(object? state)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await FlushDataPointsAsync();
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to flush data points from timer callback");
                }
            });
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            if (disposed) return;

            // Stop the timer first and wait for any pending callbacks
            flushTimer?.Change(Timeout.Infinite, Timeout.Infinite);
            flushTimer?.Dispose();

            // Flush any remaining data points and wait for completion
            await FlushDataPointsAsync();

            influxClient?.Dispose();
            disposed = true;
        }
    }
} 
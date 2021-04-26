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

using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Represents the state of a node being mapped to prometheus metrics.
    /// </summary>
    public class NodeMetricState : UAHistoryExtractionState
    {
        private readonly Gauge metric;
        private readonly UADataType dt;
        private readonly IUAClientAccess client;

        public NodeMetricState(IUAClientAccess client, NodeId id, UADataType dt, Gauge metric) : base(client, id, false, false)
        {
            this.metric = metric;
            this.dt = dt;
            this.client = client;
        }

        /// <summary>
        /// Update the metric with the value given in <paramref name="vt"/>, as double.
        /// If the value cannot be mapped as double, then it is not used. Metrics only support numerical values.
        /// </summary>
        /// <param name="vt">Value to set</param>
        public void UpdateMetricValue(Variant vt)
        {
            var dp = dt.ToDataPoint(client, vt, DateTime.UtcNow, Id);
            if (dp.IsString) return;
            metric.Set(dp.DoubleValue.Value);
        }
    }

    /// <summary>
    /// Utility class handling reading nodes as metrics.
    /// </summary>
    public class NodeMetricsManager
    {
        private readonly NodeMetricsConfig config;
        private readonly UAClientConfig sourceConfig;
        private readonly UAClient client;
        private readonly Dictionary<NodeId, NodeMetricState> metrics = new Dictionary<NodeId, NodeMetricState>();
        
        public NodeMetricsManager(UAClient client, FullConfig config)
        {
            this.config = config.Metrics.Nodes;
            sourceConfig = config.Source;
            this.client = client;
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
        /// Start or restart the node metric manager, by subscribing to requested OPC-UA nodes.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task StartNodeMetrics(CancellationToken token)
        {
            client.RemoveSubscription("NodeMetrics");
            metrics.Clear();

            var nodes = new List<NodeId>();
            if (config.OtherMetrics != null && config.OtherMetrics.Any())
            {
                foreach (var proto in config.OtherMetrics)
                {
                    nodes.Add(proto.ToNodeId(client));
                }
            }

            if (config.ServerMetrics)
            {
                foreach (var node in serverDiagnostics)
                {
                    nodes.Add(node);
                }
            }

            var readValueIds = new ReadValueIdCollection(nodes
                .SelectMany(node => attributes.Select(attr => new ReadValueId { AttributeId = attr, NodeId = node }))
            );

            var results = await Task.Run(() => client.ReadAttributes(readValueIds, nodes.Count, token));

            int attrPerNode = attributes.Length;

            var cleanRegex = new Regex("[^a-zA-Z0-9_:]");

            for (int i = 0; i < nodes.Count; i++)
            {
                var nc = (NodeClass)results[i * attrPerNode + 2].Value;
                if (nc != NodeClass.Variable) continue;
                var rawDt = results[i * attrPerNode + 1].GetValue(NodeId.Null);
                var dt = client.DataTypeManager.GetDataType(rawDt) ?? new UADataType(rawDt);
                var name = results[i * attrPerNode].GetValue<LocalizedText>(null)?.Text;
                Console.WriteLine(name);
                if (name == null) continue;

                var desc = results[i * attrPerNode + 3].GetValue<LocalizedText>(null)?.Text;

                var cleanName = cleanRegex.Replace(name, "_");

                var state = new NodeMetricState(client, nodes[i], dt, Metrics.CreateGauge($"opcua_node_{cleanName}", desc));
                metrics[nodes[i]] = state;
            }

            if (!metrics.Any()) return;

            await Task.Run(() => client.AddSubscriptions(metrics.Values, "NodeMetrics", SubscriptionHandler,
                state => new MonitoredItem
                {
                    StartNodeId = state.SourceId,
                    SamplingInterval = sourceConfig.SamplingInterval,
                    DisplayName = "Value " + state.Id,
                    QueueSize = 1,
                    DiscardOldest = true,
                    AttributeId = Attributes.Value,
                    NodeClass = NodeClass.Variable,
                    CacheQueueSize = 1
                }, token));
        }
        /// <summary>
        /// Converts datapoint callback to metric value
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
    }
}

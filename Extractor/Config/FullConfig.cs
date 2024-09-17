/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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

using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;
using System.Collections.Generic;
using System.ComponentModel;
using YamlDotNet.Serialization;

namespace Cognite.OpcUa.Config
{
    public class UALoggerConfig : LoggerConfig
    {
        /// <summary>
        /// Optionally capture OPC-UA trace. One of verbose, debug, information, warning, error, fatal.
        /// </summary>
        public string? UaTraceLevel { get; set; }
        /// <summary>
        /// Try to log raw data received from and sent to OPC-UA when possible.
        /// </summary>
        public bool UaSessionTracing { get; set; }
    }
    public class FullConfig : VersionedConfig
    {
        /// <summary>
        /// Dynamic properties based on other config sections.
        /// </summary>
        [YamlIgnore]
        public ConfigToggles Toggles { get; }

        public FullConfig() : base()
        {
            Toggles = new ConfigToggles(this);
        }

        /// <summary>
        /// Configuration for the OPC-UA server.
        /// </summary>
        public SourceConfig Source { get; set; } = null!;
        /// <summary>
        /// Configuration for logging to console or file.
        /// </summary>
        public UALoggerConfig Logger { get; set; } = null!;
        /// <summary>
        /// Configuration for prometheus metrics.
        /// </summary>
        public UAMetricsConfig Metrics { get; set; } = null!;
        /// <summary>
        /// Configuration for writing to CDF.
        /// </summary>
        public CognitePusherConfig? Cognite { get; set; }
        /// <summary>
        /// Configuration for writing to CDF over MQTT.
        /// </summary>
        public MqttPusherConfig? Mqtt { get; set; }
        /// <summary>
        /// Configuration for writing to an influxdb server.
        /// </summary>
        public InfluxPusherConfig? Influx { get; set; }
        /// <summary>
        /// Configuration for how data should be extracted.
        /// </summary>
        public ExtractionConfig Extraction { get; set; } = null!;
        /// <summary>
        /// Configuration for how events should be extracted.
        /// </summary>
        public EventConfig Events { get; set; } = null!;
        /// <summary>
        /// Configuration for storing datapoints and events in a local buffer if
        /// connection to CDF is lost.
        /// </summary>
        public FailureBufferConfig FailureBuffer { get; set; } = null!;
        /// <summary>
        /// Configuration for reading history.
        /// </summary>
        public HistoryConfig History { get; set; } = null!;
        /// <summary>
        /// Configuration for storing the range of extracted datapoints and events to
        /// a local or remote store.
        /// </summary>
        public StateStorageConfig StateStorage { get; set; } = null!;
        /// <summary>
        /// Configuration for how to handle subscriptions to datapoints and events.
        /// </summary>
        public SubscriptionConfig Subscriptions { get; set; } = null!;
        /// <summary>
        /// Configuration for using OPC-UA pubsub to obtain datapoints and events.
        /// </summary>
        public PubSubConfig PubSub { get; set; } = null!;
        /// <summary>
        /// Configuration for high availability support.
        /// </summary>
        public HighAvailabilityConfig HighAvailability { get; set; } = null!;
        /// <summary>
        /// Do not push any data to destinations.
        /// </summary>
        public bool DryRun { get; set; }
        public override void GenerateDefaults()
        {
            Source ??= new SourceConfig();
            Logger ??= new UALoggerConfig();
            Metrics ??= new UAMetricsConfig();
            if (Cognite != null)
            {
                if (Cognite.CdfChunking == null) Cognite.CdfChunking = new ChunkingConfig();
                if (Cognite.CdfThrottling == null) Cognite.CdfThrottling = new ThrottlingConfig();
                if (Cognite.CdfRetries == null) Cognite.CdfRetries = new RetryConfig();
                if (Cognite.SdkLogging == null) Cognite.SdkLogging = new SdkLoggingConfig();
            }
            Extraction ??= new ExtractionConfig();
            Events ??= new EventConfig();
            FailureBuffer ??= new FailureBufferConfig();
            History ??= new HistoryConfig();
            StateStorage ??= new StateStorageConfig();
            Subscriptions ??= new SubscriptionConfig();
            PubSub ??= new PubSubConfig();
            HighAvailability ??= new HighAvailabilityConfig();
        }
    }
    public class UAMetricsConfig : MetricsConfig
    {
        /// <summary>
        /// Configuration to treat OPC-UA nodes as metrics.
        /// Values will be mapped to opcua_nodes_NODE-DISPLAY-NAME in prometheus.
        /// </summary>
        public NodeMetricsConfig? Nodes { get; set; }
    }
    public class NodeMetricsConfig
    {
        /// <summary>
        /// Map relevant static diagnostics contained in ServerDiagnosticsSummary.
        /// </summary>
        public bool ServerMetrics { get; set; }
        /// <summary>
        /// Map other nodes, given by a list of ProtoNodeIds.
        /// </summary>
        public IEnumerable<ProtoNodeId>? OtherMetrics { get; set; }
    }
    public class PubSubConfig
    {
        /// <summary>
        /// Enable pubsub.
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// Default true, prefer using the UADP binary format, if false, will prefer JSON.
        /// </summary>
        [DefaultValue(true)]
        public bool PreferUadp { get; set; } = true;
        /// <summary>
        /// Save or read configuration from a file. If the file does not exist, it will be created
        /// from server configuration. If this is pre-created manually, the server does not need to expose
        /// pubsub configuration.
        /// </summary>
        public string? FileName { get; set; }
    }
}

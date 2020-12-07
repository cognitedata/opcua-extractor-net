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

using Opc.Ua;
using System;
using System.Collections.Generic;
using Cognite.OpcUa.Pushers;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;
using Cognite.Extractor.StateStorage;
using Cognite.Extensions;

namespace Cognite.OpcUa
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:Uri properties should not be strings", Justification = "Yaml Deserialization")]
    public class UAClientConfig
    {
        public string ConfigRoot { get; set; } = "config";
        public string EndpointUrl { get; set; }
        public bool AutoAccept { get; set; } = true;
        public int PublishingInterval { get; set; } = 500;
        public int SamplingInterval { get; set; } = 100;
        public int QueueLength { get; set; } = 10;
        public string Username { get; set; }
        public string Password { get; set; }
        public bool Secure { get; set; }
        public bool ForceRestart { get; set; }
        public bool ExitOnFailure { get; set; }
        public int BrowseNodesChunk { get => browseNodesChunk; set => browseNodesChunk = Math.Max(1, value); }
        private int browseNodesChunk = 1000;
        public int BrowseChunk { get => browseChunk; set => browseChunk = Math.Max(0, value); }
        private int browseChunk = 1000;
        // 0 means server defined:
        public int AttributesChunk { get => attributesChunk; set => attributesChunk = Math.Max(0, value); }
        private int attributesChunk = 10000;
        public int SubscriptionChunk { get => subscriptionChunk; set => subscriptionChunk = Math.Max(1, value); }
        private int subscriptionChunk = 1000;
        public int KeepAliveInterval { get; set; } = 5000;
        public bool RestartOnReconnect { get; set; }
    }
    public class ExtractionConfig
    {
        public string IdPrefix { get; set; }
        public IEnumerable<string> IgnoreNamePrefix { get; set; }
        public IEnumerable<string> IgnoreName { get; set; }
        public ProtoNodeId RootNode { get => rootNode; set => rootNode = value ?? rootNode; }
        private ProtoNodeId rootNode = new ProtoNodeId();
        public Dictionary<string, ProtoNodeId> NodeMap { get; set; }
        public Dictionary<string, string> NamespaceMap { get => namespaceMap; set => namespaceMap = value ?? namespaceMap; }
        private Dictionary<string, string> namespaceMap = new Dictionary<string, string>();
        public int AutoRebrowsePeriod { get; set; }
        public bool EnableAuditDiscovery { get; set; }
        public int DataPushDelay { get; set; } = 1000;
        public UpdateConfig Update { get => update; set => update = value ?? update; }
        private UpdateConfig update = new UpdateConfig();
        public DataTypeConfig DataTypes { get => dataTypes; set => dataTypes = value ?? dataTypes; }
        private DataTypeConfig dataTypes = new DataTypeConfig();
        public string PropertyNameFilter { get; set; }
        public string PropertyIdFilter { get; set; }
        public RelationshipConfig Relationships { get => relationships; set => relationships = value ?? relationships; }
        private RelationshipConfig relationships = new RelationshipConfig();
        public NodeTypeConfig NodeTypes { get => nodeTypes; set => nodeTypes = value ?? nodeTypes; }
        private NodeTypeConfig nodeTypes = new NodeTypeConfig();
    }
    public class DataTypeConfig
    {
        public IEnumerable<ProtoDataType> CustomNumericTypes { get; set; }
        public IEnumerable<ProtoNodeId> IgnoreDataTypes { get; set; }
        public bool UnknownAsScalar { get; set; }
        public int MaxArraySize { get; set; }
        public bool AllowStringVariables { get; set; }
        public bool AutoIdentifyTypes { get; set; }
        public bool EnumsAsStrings { get; set; }
        public bool DataTypeMetadata { get; set; }
        public bool NullAsNumeric { get; set; }
    }

    public class RelationshipConfig
    {
        public bool Enabled { get; set; }
    }
    public class NodeTypeConfig
    {
        public bool Metadata { get; set; }
    }

    public class UpdateConfig
    {
        public bool AnyUpdate => objects.AnyUpdate || variables.AnyUpdate;
        public TypeUpdateConfig Objects { get => objects; set => objects = value ?? objects; }
        private TypeUpdateConfig objects = new TypeUpdateConfig();
        public TypeUpdateConfig Variables { get => variables; set => variables = value ?? variables; }
        private TypeUpdateConfig variables = new TypeUpdateConfig();
    }
    public class TypeUpdateConfig
    {
        public bool AnyUpdate => Description || Name || Metadata || Context;
        public bool Description { get; set; }
        public bool Name { get; set; }
        public bool Metadata { get; set; }
        public bool Context { get; set; }

    }
    public interface IPusherConfig
    {
        bool Debug { get; set; }
        bool ReadExtractedRanges { get; set; }
        public double? NonFiniteReplacement { get; set; }
        public IPusher ToPusher(IServiceProvider provider);
    }
    public class CognitePusherConfig : CogniteConfig, IPusherConfig
    {
        public long? DataSetId { get; set; }
        public bool Debug { get; set; }
        public bool ReadExtractedRanges { get; set; } = true;
        public bool SkipMetadata { get; set; }
        public RawMetadataConfig RawMetadata { get; set; }
        public MetadataMapConfig MetadataMapping { get; set; }
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set => nonFiniteReplacement = value == null || double.IsFinite(value.Value)
                && value.Value > CogniteUtils.NumericValueMin
                && value.Value < CogniteUtils.NumericValueMax ? value : null;
        }
        private double? nonFiniteReplacement;
        public IPusher ToPusher(IServiceProvider provider)
        {
            return new CDFPusher(provider, this);
        }
    }
    public class RawMetadataConfig
    {
        public string Database { get; set; }
        public string AssetsTable { get; set; }
        public string TimeseriesTable { get; set; }
    }

    public class MetadataMapConfig
    {
        public Dictionary<string, string> Assets { get; set; }
        public Dictionary<string, string> Timeseries { get; set; }
    }

    public class InfluxPusherConfig : IPusherConfig
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }
        public int PointChunkSize { get; set; } = 100000;
        public bool Debug { get; set; }
        public bool ReadExtractedRanges { get; set; } = true;
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set
            {
                if (value == null) return;
                nonFiniteReplacement = double.IsFinite(value.Value) ? value : null;
            }
        }
        private double? nonFiniteReplacement;
        public IPusher ToPusher(IServiceProvider _)
        {
            return new InfluxPusher(this);
        }
    }

    public class MqttPusherConfig : IPusherConfig
    {
        public string Host { get; set; }
        public int? Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public bool UseTls { get; set; }
        public string ClientId { get; set; } = "cognite-opcua-extractor";
        public long? DataSetId { get; set; }
        public string AssetTopic { get; set; } = "cognite/opcua/assets";
        public string TsTopic { get; set; } = "cognite/opcua/timeseries";
        public string EventTopic { get; set; } = "cognite/opcua/events";
        public string DatapointTopic { get; set; } = "cognite/opcua/datapoints";
        public string RawTopic { get; set; } = "cognite/opcua/raw";
        public string LocalState { get; set; }
        public long InvalidateBefore { get; set; }
        public bool Debug { get; set; }
        public bool ReadExtractedRanges { get; set; }
        public bool SkipMetadata { get; set; }
        public RawMetadataConfig RawMetadata { get; set; }
        public MetadataMapConfig MetadataMapping { get; set; }
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set
            {
                if (value == null) return;
                nonFiniteReplacement = double.IsFinite(value.Value) ? value : null;
            }
        }
        private double? nonFiniteReplacement;
        public IPusher ToPusher(IServiceProvider _)
        {
            return new MQTTPusher(this);
        }
    }

    public class FailureBufferConfig
    {
        public bool Enabled { get; set; }
        public bool InfluxStateStore { get; set; }
        public bool Influx { get; set; }
        public string DatapointPath { get; set; }
        public string EventPath { get; set; }
    }
    public class FullConfig : VersionedConfig
    {
        public UAClientConfig Source { get; set; }
        public LoggerConfig Logger { get; set; }
        public MetricsConfig Metrics { get; set; }
        public CognitePusherConfig Cognite { get; set; }
        public MqttPusherConfig Mqtt { get; set; }
        public InfluxPusherConfig Influx { get; set; }
        public ExtractionConfig Extraction { get; set; }
        public EventConfig Events { get; set; }
        public FailureBufferConfig FailureBuffer { get; set; }
        public HistoryConfig History { get; set; }
        public StateStorageConfig StateStorage { get; set; }
        public override void GenerateDefaults()
        {
            if (Source == null) Source = new UAClientConfig();
            if (Logger == null) Logger = new LoggerConfig();
            if (Metrics == null) Metrics = new MetricsConfig();
            if (Cognite != null)
            {
                if (Cognite.CdfChunking == null) Cognite.CdfChunking = new ChunkingConfig();
                if (Cognite.CdfThrottling == null) Cognite.CdfThrottling = new ThrottlingConfig();
                if (Cognite.CdfRetries == null) Cognite.CdfRetries = new RetryConfig();
                if (Cognite.SdkLogging == null) Cognite.SdkLogging = new SdkLoggingConfig();
            }
            if (Extraction == null) Extraction = new ExtractionConfig();
            if (Events == null) Events = new EventConfig();
            if (FailureBuffer == null) FailureBuffer = new FailureBufferConfig();
            if (History == null) History = new HistoryConfig();
            if (StateStorage == null) StateStorage = new StateStorageConfig();
        }
    }
    public class EventConfig
    {
        public IEnumerable<ProtoNodeId> EventIds { get; set; }
        public IEnumerable<ProtoNodeId> EmitterIds { get; set; }
        public IEnumerable<ProtoNodeId> HistorizingEmitterIds { get; set; }
        public bool Enabled { get; set; }
        public bool AllEvents { get; set; } = true;
        public bool History { get; set; }
        public string ExcludeEventFilter { get; set; }
        public IEnumerable<string> ExcludeProperties { get => excludeProperties; set => excludeProperties = value ?? excludeProperties; }
        private IEnumerable<string> excludeProperties = new List<string>();
        public IEnumerable<string> BaseExcludeProperties { get; } = new List<string> { "LocalTime", "ReceiveTime", "SourceName" };
        public Dictionary<string, string> DestinationNameMap { get => destinationNameMap; set => destinationNameMap = value ?? destinationNameMap; }
        private Dictionary<string, string> destinationNameMap = new Dictionary<string, string>();
    }
    public class HistoryConfig
    {
        public bool Enabled { get; set; }
        public bool Data { get; set; } = true;
        public bool Backfill { get; set; }
        public int DataChunk { get => dataChunk; set => dataChunk = Math.Max(0, value); }
        private int dataChunk = 1000;
        public int DataNodesChunk { get => dataNodesChunk; set => dataNodesChunk = Math.Max(1, value); }
        private int dataNodesChunk = 100;
        public int EventChunk { get => eventPointsChunk; set => eventPointsChunk = Math.Max(0, value); }
        private int eventPointsChunk = 1000;
        public int EventNodesChunk { get => eventNodesChunk; set => eventNodesChunk = Math.Max(1, value); }
        private int eventNodesChunk = 100;
        public long StartTime { get; set; }
        public int Granularity { get; set; } = 600;
    }
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:Uri properties should not be strings", Justification = "Yaml Deserialization")]
    public class ProtoNodeId
    {
        public string NamespaceUri { get; set; }
        public string NodeId { get; set; }
        public NodeId ToNodeId(UAClient client, NodeId defaultValue = null)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            var node = client.ToNodeId(NodeId, NamespaceUri);
            if (node.IsNullNodeId)
            {
                return defaultValue ?? Opc.Ua.NodeId.Null;
            }
            return node;
        }
    }
    public class ProtoDataType
    {
        public ProtoNodeId NodeId { get; set; }
        public bool IsStep { get; set; }
        public bool Enum { get; set; }
    }

    public class StateStorageConfig : StateStoreConfig
    {
        public int Interval { get; set; }
        public string VariableStore { get; set; } = "variable_states";
        public string EventStore { get; set; } = "event_states";
        public string InfluxVariableStore { get; set; } = "influx_variable_states";
        public string InfluxEventStore { get; set; } = "influx_event_states";

    }
}

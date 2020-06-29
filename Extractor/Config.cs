﻿using Opc.Ua;
using System;
using System.Collections.Generic;
using Cognite.OpcUa.Pushers;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;
using Cognite.Extractor.StateStorage;

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
        public bool Secure { get; set; } = false;
        public bool ForceRestart { get; set; } = false;
        public bool ExitOnFailure { get; set; } = false;
        public int BrowseNodesChunk { get => browseNodesChunk; set => browseNodesChunk = Math.Max(1, value); }
        private int browseNodesChunk = 1000;
        public int BrowseChunk { get => browseChunk; set => browseChunk = Math.Max(0, value); }
        private int browseChunk = 1000;
        // 0 means server defined:
        public int AttributesChunk { get => attributesChunk; set => attributesChunk = Math.Max(0, value); }
        private int attributesChunk = 1000;
        public int SubscriptionChunk { get => subscriptionChunk; set => subscriptionChunk = Math.Max(1, value); }
        private int subscriptionChunk = 1000;
    }
    public class ExtractionConfig
    {
        public string IdPrefix { get; set; }
        public IEnumerable<string> IgnoreNamePrefix { get; set; }
        public IEnumerable<string> IgnoreName { get; set; }
        public ProtoNodeId RootNode { get => rootNode; set => rootNode = value ?? rootNode; }
        private ProtoNodeId rootNode = new ProtoNodeId();
        public Dictionary<string, ProtoNodeId> NodeMap { get; set; }
        public IEnumerable<ProtoNodeId> IgnoreDataTypes { get; set; }
        public int MaxArraySize { get; set; } = 0;
        public bool AllowStringVariables { get; set; } = false;
        public Dictionary<string, string> NamespaceMap { get => namespaceMap; set => namespaceMap = value ?? namespaceMap; }
        private Dictionary<string, string> namespaceMap = new Dictionary<string, string>();
        public IEnumerable<ProtoDataType> CustomNumericTypes { get; set; }
        public int AutoRebrowsePeriod { get; set; } = 0;
        public bool EnableAuditDiscovery { get; set; } = false;
        public int DataPushDelay { get; set; } = 1000;
        public bool UnknownAsScalar { get; set; } = false;
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
        public bool Debug { get; set; } = false;
        public bool ReadExtractedRanges { get; set; } = true;
        public RawMetadataConfig RawMetadata { get; set; }
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

    public class InfluxPusherConfig : IPusherConfig
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }
        public int PointChunkSize { get; set; } = 100000;
        public bool Debug { get; set; } = false;
        public bool ReadExtractedRanges { get; set; } = true;
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set => nonFiniteReplacement = double.IsFinite(value.Value) ? value : null;
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
        public string LocalState { get; set; }
        public long InvalidateBefore { get; set; }
        public bool Debug { get; set; }
        public bool ReadExtractedRanges { get; set; }
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set => nonFiniteReplacement = double.IsFinite(value.Value) ? value : null;
        }
        private double? nonFiniteReplacement;
        public IPusher ToPusher(IServiceProvider _)
        {
            return new MQTTPusher(this);
        }
    }

    public class FailureBufferConfig
    {
        public bool Enabled { get; set; } = false;
        public bool InfluxStateStore { get; set; } = false;
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
        public IEnumerable<string> ExcludeProperties { get => excludeProperties; set => excludeProperties = value ?? excludeProperties; }
        private IEnumerable<string> excludeProperties = new List<string>();
        public IEnumerable<string> BaseExcludeProperties { get; } = new List<string> { "LocalTime", "ReceiveTime", "SourceName" };
        public Dictionary<string, string> DestinationNameMap { get => destinationNameMap; set => destinationNameMap = value ?? destinationNameMap; }
        private Dictionary<string, string> destinationNameMap = new Dictionary<string, string>();
        public IEnumerable<ProtoNodeId> HistorizingEmitterIds { get; set; }
    }
    public class HistoryConfig
    {
        public bool Enabled { get; set; } = false;
        public bool Data { get; set; } = true;
        public bool Backfill { get; set; } = false;
        public int DataChunk { get => dataChunk; set => dataChunk = Math.Max(0, value); }
        private int dataChunk = 1000;
        public int DataNodesChunk { get => dataNodesChunk; set => dataNodesChunk = Math.Max(1, value); }
        private int dataNodesChunk = 100;
        public int EventChunk { get => eventPointsChunk; set => eventPointsChunk = Math.Max(0, value); }
        private int eventPointsChunk = 1000;
        public int EventNodesChunk { get => eventNodesChunk; set => eventNodesChunk = Math.Max(1, value); }
        private int eventNodesChunk = 100;
        public long StartTime { get; set; } = 0;
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
        public bool IsStep { get; set; } = false;
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

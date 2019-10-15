using Opc.Ua;
using System;
using System.Collections.Generic;

namespace Cognite.OpcUa
{
    public class UAClientConfig
    {
        public string ConfigRoot { get; set; } = "config";
        public string EndpointURL { get; set; }
        public bool AutoAccept { get; set; } = true;
        public int PollingInterval { get; set; } = 500;
        public string Username { get; set; }
        public string Password { get; set; }
        public bool Secure { get; set; } = false;
        public bool History { get; set; } = true;
        public int HistoryGranularity { get; set; } = 600;
        public bool ForceRestart { get; set; } = false;
        public int BrowseNodesChunk { get => _browseNodesChunk; set => _browseNodesChunk = Math.Max(1, value); }
        private int _browseNodesChunk = 1000;
        public int BrowseChunk { get => _browseChunk; set => _browseChunk = Math.Max(1, value); }
        private int _browseChunk = 1000;
        public int HistoryReadChunk { get => _uaHistoryReadPoints; set => _uaHistoryReadPoints = Math.Max(1, value); }
        private int _uaHistoryReadPoints = 1000;
        public int HistoryReadNodesChunk { get => _uaHistoryReadNodes; set => _uaHistoryReadNodes = Math.Max(1, value); }
        private int _uaHistoryReadNodes = 100;
        // 0 means server defined:
        public int AttributesChunk { get => _attributesChunk; set => _attributesChunk = Math.Max(0, value); }
        private int _attributesChunk = 1000;
        public int SubscriptionChunk { get => _subscriptionChunk; set => _subscriptionChunk = Math.Max(1, value); }
        private int _subscriptionChunk = 1000;
    }
    public class ExtractionConfig
    {
        public string IdPrefix { get; set; }
        public IEnumerable<string> IgnoreNamePrefix { get; set; }
        public IEnumerable<string> IgnoreName { get; set; }
        public ProtoNodeId RootNode { get => _rootNode; set => _rootNode = value ?? _rootNode; }
        private ProtoNodeId _rootNode = new ProtoNodeId();
        public Dictionary<string, ProtoNodeId> NodeMap { get; set; }
        public IEnumerable<ProtoNodeId> IgnoreDataTypes { get; set; }
        public int MaxArraySize { get; set; } = 0;
        public bool AllowStringVariables { get; set; } = false;
        public Dictionary<string, string> NamespaceMap { get => _namespaceMap; set => _namespaceMap = value ?? _namespaceMap; }
        private Dictionary<string, string> _namespaceMap = new Dictionary<string, string>();
        public IEnumerable<ProtoDataType> CustomNumericTypes { get; set; }
        public double? NonFiniteReplacement
        {
            get => _nonFiniteReplacement;
            set =>_nonFiniteReplacement = value == null || double.IsFinite(value.Value) ? value : null;
        }
        private double? _nonFiniteReplacement;
        public long HistoryStartTime { get; set; } = 0;
    }
    public abstract class PusherConfig
    {
        public bool Debug { get; set; } = false;
        public int DataPushDelay { get; set; } = 1000;
        public abstract IPusher ToPusher(IServiceProvider provider);
    }
    public class CogniteClientConfig : PusherConfig
    {
        public string Project { get; set; }
        public string ApiKey { get; set; }
        public string Host { get; set; } = "https://api.cognitedata.com";
        public bool BufferOnFailure { get; set; } = false;
        public string BufferFile { get; set; } = "buffer.bin";
        public override IPusher ToPusher(IServiceProvider provider)
        {
            return new CDFPusher(provider, this);
        }

        // Limits can change without notice in CDF API end-points.
        // The limit on number of time series on the "latest" end-point is currently 100.
        public int LatestChunk { get; set; } = 100;
        public int TimeSeriesChunk { get; set; } = 1000;
        public int AssetChunk { get; set; } = 1000;
    }
    public class InfluxClientConfig : PusherConfig
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }
        public int PointChunkSize { get; set; } = 100000;
        public override IPusher ToPusher(IServiceProvider _)
        {
            return new InfluxPusher(this);
        }
    }
    public class FullConfig
    {
        public UAClientConfig Source { get => _uaConfig; set => _uaConfig = value ?? _uaConfig; }
        private UAClientConfig _uaConfig = new UAClientConfig();
        public LoggerConfig Logging { get => _loggerConfig; set => _loggerConfig = value ?? _loggerConfig; }
        private LoggerConfig _loggerConfig = new LoggerConfig();
        public MetricsConfig Metrics { get => _metricsConfig; set => _metricsConfig = value ?? _metricsConfig; }
        private MetricsConfig _metricsConfig = new MetricsConfig();
        public List<PusherConfig> Pushers { get => _pushers; set => _pushers = value ?? _pushers; }
        private List<PusherConfig> _pushers = new List<PusherConfig>();
        public ExtractionConfig Extraction { get => _extractionConfig; set => _extractionConfig = value ?? _extractionConfig; }
        private ExtractionConfig _extractionConfig = new ExtractionConfig();
        public EventConfig Events { get => _eventConfig; set => _eventConfig = value ?? _eventConfig; }
        private EventConfig _eventConfig = new EventConfig();
    }
    public class LoggerConfig
    {
        public string ConsoleLevel { get; set; }
        public string FileLevel { get; set; }
        public string LogFolder { get; set; }
        public int RetentionLimit { get; set; } = 31;
        public string StackdriverCredentials { get; set; }
        public string StackdriverLogName { get; set; }
    }
    public class MetricsConfig
    {
        public string URL { get; set; }
        public string Job { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public int PushInterval { get; set; } = 1000;
        public string Instance { get; set; }
    }
    public class EventConfig
    {
        public IEnumerable<ProtoNodeId> EventIds { get; set; }
        public IEnumerable<ProtoNodeId> EmitterIds { get; set; }
        public IEnumerable<string> ExcludeProperties { get => _excludeProperties; set => _excludeProperties = value ?? _excludeProperties; }
        private IEnumerable<string> _excludeProperties = new List<string>();
        public IEnumerable<string> BaseExcludeProperties { get; } = new List<string> { "LocalTime", "ReceiveTime", "SourceName" };
        public Dictionary<string, string> DestinationNameMap { get => _destinationNameMap; set => _destinationNameMap = value ?? _destinationNameMap; }
        private Dictionary<string, string> _destinationNameMap = new Dictionary<string, string>();
        public IEnumerable<ProtoNodeId> HistorizingEmitterIds { get; set; }
        public string ReceiveTimeProperty { get; set; } = "ReceiveTime";
        public int HistoryReadChunk { get; set; } = 1000;
    }
    public class ProtoNodeId
    {
        public string NamespaceUri { get; set; }
        public string NodeId { get; set; }
        public NodeId ToNodeId(UAClient client, NodeId defaultValue = null)
        {
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
}

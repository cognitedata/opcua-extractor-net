using Opc.Ua;
using System;
using System.Collections.Generic;

namespace Cognite.OpcUa
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:Uri properties should not be strings", Justification = "Yaml Deserialization")]
    public class UAClientConfig
    {
        public string ConfigRoot { get; set; } = "config";
        public string EndpointURL { get; set; }
        public bool AutoAccept { get; set; } = true;
        public int PollingInterval { get; set; } = 500;
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
    }
    public abstract class PusherConfig
    {
        public bool Debug { get; set; } = false;
        public bool Critical { get; set; } = true;
        public bool ReadExtractedRanges { get; set; } = true;
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set => nonFiniteReplacement = value == null || double.IsFinite(value.Value) ? value : null;
        }
        private double? nonFiniteReplacement;
        public abstract IPusher ToPusher(int index, IServiceProvider provider);
    }
    public class CogniteClientConfig : PusherConfig
    {
        public string Project { get; set; }
        public string ApiKey { get; set; }
        public string Host { get; set; } = "https://api.cognitedata.com";
        public override IPusher ToPusher(int index, IServiceProvider provider)
        {
            return new CDFPusher(provider, this) {Index = index};
        }

        public int EarliestChunk { get; set; } = 1000;
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
        public override IPusher ToPusher(int index, IServiceProvider _)
        {
            return new InfluxPusher(this) { Index = index };
        }
    }

    public class FailureBufferConfig
    {
        public bool Enabled { get; set; } = false;
        public InfluxBufferConfig Influx { get; set; }
        public bool LocalQueue { get; set; }
        public bool StoreHistorizing { get; set; }
    }

    public class InfluxBufferConfig
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }
        public bool Write { get; set; } = true;
        public int PointChunkSize { get; set; } = 100000;
        public bool StateStorage { get; set; }
    }
    public class FullConfig
    {
        public UAClientConfig Source { get => uaConfig; set => uaConfig = value ?? uaConfig; }
        private UAClientConfig uaConfig = new UAClientConfig();
        public LoggerConfig Logging { get => loggerConfig; set => loggerConfig = value ?? loggerConfig; }
        private LoggerConfig loggerConfig = new LoggerConfig();
        public MetricsConfig Metrics { get => metricsConfig; set => metricsConfig = value ?? metricsConfig; }
        private MetricsConfig metricsConfig = new MetricsConfig();
        public List<PusherConfig> Pushers { get => pushers; set => pushers = value ?? pushers; }
        private List<PusherConfig> pushers = new List<PusherConfig>();
        public ExtractionConfig Extraction { get => extractionConfig; set => extractionConfig = value ?? extractionConfig; }
        private ExtractionConfig extractionConfig = new ExtractionConfig();
        public EventConfig Events { get => eventConfig; set => eventConfig = value ?? eventConfig; }
        private EventConfig eventConfig = new EventConfig();
        public FailureBufferConfig FailureBuffer { get => failureBufferConfig; set => failureBufferConfig = value ?? failureBufferConfig; }
        private FailureBufferConfig failureBufferConfig = new FailureBufferConfig();
        public HistoryConfig History { get => historyConfig; set => historyConfig = value ?? historyConfig; }
        private HistoryConfig historyConfig = new HistoryConfig();
        public StateStorageConfig StateStorage { get => stateStorage; set => stateStorage = value ?? stateStorage; }
        private StateStorageConfig stateStorage = new StateStorageConfig();
    }
    public class LoggerConfig
    {
        public string ConsoleLevel { get; set; } = "information";
        public string FileLevel { get; set; }
        public string LogFolder { get; set; }
        public int RetentionLimit { get; set; } = 31;
        public string StackdriverCredentials { get; set; }
        public string StackdriverLogName { get; set; }
    }
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:Uri properties should not be strings", Justification = "Yaml Deserialization")]
    public class MetricsConfig
    {
        public MetricsServerConfig Server { get; set; }
#pragma warning disable CA2227 // Collection properties should be read only
        public List<PushGatewayConfig> PushGateways { get; set; }
#pragma warning restore CA2227 // Collection properties should be read only

    }

    public class MetricsServerConfig
    {
        public string Host { get; set; }
        public int Port { get; set; }
    }

    public class PushGatewayConfig
    {
        public string Host { get; set; }
        public string Job { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
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

    public class StateStorageConfig
    {
        public string Location { get; set; }
        public int Interval { get; set; }
    }
}

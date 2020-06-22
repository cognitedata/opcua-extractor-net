using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;

namespace Cognite.Bridge
{
    public class MQTTConfig
    {
        public string Host { get; set; }
        public int? Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public bool UseTls { get; set; }
        public string ClientId { get; set; } = "cognite-cdf-bridge";
        public string AssetTopic { get; set; } = "cognite/opcua/assets";
        public string TsTopic { get; set; } = "cognite/opcua/timeseries";
        public string EventTopic { get; set; } = "cognite/opcua/events";
        public string DatapointTopic { get; set; } = "cognite/opcua/datapoints";
    }

    public class BridgeConfig : VersionedConfig
    {
        public LoggerConfig Logger { get; set; }
        public CogniteConfig Cognite { get; set; }
        public MQTTConfig Mqtt { get; set; }

        public override void GenerateDefaults()
        {
            if (Logger == null) Logger = new LoggerConfig();
            if (Cognite == null) Cognite = new CogniteConfig();
            if (Cognite.CdfChunking == null) Cognite.CdfChunking = new ChunkingConfig();
            if (Cognite.CdfRetries == null) Cognite.CdfRetries = new RetryConfig();
            if (Cognite.CdfThrottling == null) Cognite.CdfThrottling = new ThrottlingConfig();
            if (Cognite.SdkLogging == null) Cognite.SdkLogging = new SdkLoggingConfig();
            if (Mqtt == null) Mqtt = new MQTTConfig();
        }
    }
}

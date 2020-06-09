using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;

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

    public class CDFConfig
    {
        public string Project { get; set; }
        public string ApiKey { get; set; }
        public string Host { get; set; } = "https://api.cognitedata.com";
    }

    public class BridgeConfig : VersionedConfig
    {
        public LoggerConfig Logger { get; set; }
        public CDFConfig Cognite { get; set; }
        public MQTTConfig Mqtt { get; set; }

        public override void GenerateDefaults()
        {
            if (Logger == null) Logger = new LoggerConfig();
            if (Cognite == null) Cognite = new CDFConfig();
            if (Mqtt == null) Mqtt = new MQTTConfig();
        }
    }
}

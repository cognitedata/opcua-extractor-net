using Cognite.Extractor.Configuration;
using System;
using System.IO;
using YamlDotNet.Serialization;

namespace Cognite.Bridge
{
    public class LoggerConfig
    {
        public string ConsoleLevel { get; set; } = "information";
        public string FileLevel { get; set; }
        public string LogFolder { get; set; }
        public int RetentionLimit { get; set; } = 31;
        public string StackdriverCredentials { get; set; }
        public string StackdriverLogName { get; set; }
    }

    public class MQTTConfig
    {
        public string Host { get; set; }
        public int? Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public bool UseTls { get; set; }
        public string ClientId { get; set; } = "cognite-cdf-bridge";
        public string AssetTopic { get; set; } = "cognite/opcua/assets";
        public string TSTopic { get; set; } = "cognite/opcua/timeseries";
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
        public LoggerConfig Logging { get; set; }
        public CDFConfig Cognite { get; set; }
        public MQTTConfig Mqtt { get; set; }

        public override void GenerateDefaults()
        {
            if (Logging == null) Logging = new LoggerConfig();
            if (Cognite == null) Cognite = new CDFConfig();
            if (Mqtt == null) Mqtt = new MQTTConfig();
        }
    }
}

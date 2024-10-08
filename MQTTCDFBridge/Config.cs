﻿using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;

namespace Cognite.Bridge
{
    public class MQTTConfig
    {
        public string? Host { get; set; }
        public int? Port { get; set; }
        public string? Username { get; set; }
        public string? Password { get; set; }
        public bool UseTls { get; set; }
        public string ClientId { get; set; } = "cognite-cdf-bridge";
        public string AssetTopic { get; set; } = "cognite/opcua/assets";
        public string TsTopic { get; set; } = "cognite/opcua/timeseries";
        public string EventTopic { get; set; } = "cognite/opcua/events";
        public string DatapointTopic { get; set; } = "cognite/opcua/datapoints";
        public string RawTopic { get; set; } = "cognite/opcua/raw";
        public string RelationshipTopic { get; set; } = "cognite/opcua/relationships";
    }

    public class CogniteDestConfig : CogniteConfig
    {
        public bool Update { get; set; }
    }

    public class BridgeConfig : VersionedConfig
    {
        public LoggerConfig Logger { get; set; } = null!;
        public CogniteDestConfig Cognite { get; set; } = null!;
        public MQTTConfig Mqtt { get; set; } = null!;

        public override void GenerateDefaults()
        {
            Logger ??= new LoggerConfig();
            Cognite ??= new CogniteDestConfig();
            if (Cognite.CdfChunking == null) Cognite.CdfChunking = new ChunkingConfig();
            if (Cognite.CdfRetries == null) Cognite.CdfRetries = new RetryConfig();
            if (Cognite.CdfThrottling == null) Cognite.CdfThrottling = new ThrottlingConfig();
            if (Cognite.SdkLogging == null) Cognite.SdkLogging = new SdkLoggingConfig();
            Mqtt ??= new MQTTConfig();
        }
    }
}

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

    public class BridgeConfig
    {
        public LoggerConfig Logging { get => logging; set => logging = value ?? logging; }
        private LoggerConfig logging = new LoggerConfig();
        public CDFConfig CDF { get; set; }
        public MQTTConfig MQTT { get; set; }
    }

    public static class Config
    {
        /// <summary>
        /// Map yaml config to the FullConfig object
        /// </summary>
        /// <param name="configPath">Path to config file</param>
        /// <returns>A <see cref="BridgeConfig"/> object representing the entire config file</returns>
        public static BridgeConfig GetConfig(string configPath)
        {
            BridgeConfig bridgeConfig;
            using (var rawConfig = new StringReader(File.ReadAllText(configPath)))
            {
                var deserializer = new DeserializerBuilder()
                    .Build();
                bridgeConfig = deserializer.Deserialize<BridgeConfig>(rawConfig);
            }
            string envLogdir = Environment.GetEnvironmentVariable("BRIDGE_LOGGER_DIR");
            if (!string.IsNullOrWhiteSpace(envLogdir))
            {
                bridgeConfig.Logging.LogFolder = envLogdir;
            }
            return bridgeConfig;
        }
    }

}

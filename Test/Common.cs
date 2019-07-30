using System;
using Cognite.OpcUa;

namespace Test
{
    public static class Common
    {
        public static FullConfig BuildConfig(string serverType, int index)
        {
            var fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null) throw new Exception("Failed to load config file");
            fullConfig.CogniteConfig.BufferFile = $"buffer{index}";
            if (serverType == "basic")
            {
                fullConfig.UAConfig.EndpointURL = "opc.tcp://localhost:4840";
            }
            else if (serverType == "full")
            {
                fullConfig.UAConfig.EndpointURL = "opc.tcp://localhost:4841";
            }
            return fullConfig;
        }
    }
}

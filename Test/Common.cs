using System;
using System.Threading.Tasks;
using Cognite.OpcUa;

namespace Test
{
    public static class Common
    {
        public static FullConfig BuildConfig(string serverType, int index)
        {
            var fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null) throw new Exception("Failed to load config file");
            fullConfig.CogniteConfig.BufferFile = $"buffer{index}.bin";
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
        public static bool TestRunResult(Exception e)
        {
            if (!(e is TaskCanceledException || e is AggregateException && e.InnerException is TaskCanceledException))
            {
                return false;
            }
            return true;
        }
    }
}

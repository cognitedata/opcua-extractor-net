using System;
using System.Threading;
using Serilog;

namespace Cognite.OpcUa.Config
{
    class Program
    {
        static int Main()
        {
            Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();

            string configDir = Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR");
            configDir = string.IsNullOrEmpty(configDir) ? "config/" : configDir;

            FullConfig fullConfig;
            FullConfig baseConfig;

            try
            {
                string configFile = System.IO.Path.Combine(configDir, "config.config-tool.yml");
                Log.Information($"Loading config from {configFile}");
                fullConfig = Utils.GetConfig(configFile);
                Log.Information("Loading config 2");
                baseConfig = Utils.GetConfig(configFile);
            }
            catch (YamlDotNet.Core.YamlException e)
            {
                Log.Error("Failed to load config at {start}: {msg}", e.Start, e.InnerException?.Message ?? e.Message);
                return -1;
            }

            Logger.Configure(fullConfig.Logging);

            var explorer = new UAServerExplorer(fullConfig, baseConfig);

            var source = new CancellationTokenSource();
            try
            {
                explorer.GetEndpoints(source.Token).Wait();
                explorer.GetBrowseChunkSizes(source.Token).Wait();
                explorer.GetVariableChunkSizes(source.Token).Wait();
                explorer.ReadCustomTypes(source.Token);
                explorer.IdentifyDataTypeSettings(source.Token).Wait();
                explorer.GetSubscriptionChunkSizes(source.Token).Wait();
                explorer.GetHistoryReadConfig().Wait();
                explorer.GetEventConfig(source.Token).Wait();
                explorer.GetNamespaceMap();
                explorer.LogSummary();
            }
            catch (Exception e)
            {
                Log.Error(e, "ConfigurationTool failed fatally");
                return 1;
            }


            return 0;
        }
    }
}

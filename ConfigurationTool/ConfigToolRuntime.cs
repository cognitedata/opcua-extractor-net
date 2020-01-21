using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace Cognite.OpcUa.Config
{
    public class ConfigToolRuntime
    {
        private readonly string output;
        private readonly FullConfig config;
        private readonly FullConfig baseConfig;
        public ConfigToolRuntime(FullConfig config, FullConfig baseConfig, string output)
        {
            this.config = config;
            this.baseConfig = baseConfig;
            this.output = output;
        }

        public async Task Run()
        {
            var explorer = new UAServerExplorer(config, baseConfig);

            var source = new CancellationTokenSource();
            try
            {
                await explorer.GetEndpoints(source.Token);
                await explorer.GetBrowseChunkSizes(source.Token);
                await explorer.GetAttributeChunkSizes(source.Token);
                explorer.ReadCustomTypes(source.Token);
                await explorer.IdentifyDataTypeSettings(source.Token);
                await explorer.GetSubscriptionChunkSizes(source.Token);
                await explorer.GetHistoryReadConfig();
                await explorer.GetEventConfig(source.Token);
                explorer.GetNamespaceMap();
                explorer.LogSummary();
            }
            catch (Exception e)
            {
                Log.Error(e, "ConfigurationTool failed fatally");
                return;
            }
            explorer.Close();

            var result = ToolUtil.ConfigResultToString(explorer.GetFinalConfig());

            Log.Information("");
            File.WriteAllText(output, result);
            Log.Information("Emitted suggested config file to {path}", output);
        }
    }
}

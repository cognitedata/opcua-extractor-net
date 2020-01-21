/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

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

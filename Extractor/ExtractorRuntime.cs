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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Pushers;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using Polly.Timeout;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Container for the Extractor process. Use this when running the extractor to properly handle errors and
    /// reduce overhead on restart.
    /// </summary>
    public class ExtractorRuntime
    {
        private readonly FullConfig config;
        private IServiceProvider provider;

        private readonly ILogger log = Log.Logger.ForContext(typeof(ExtractorRuntime));

        /// <summary>
        /// Constructor, takes fully configured FullConfig
        /// </summary>
        /// <param name="config"></param>
        public ExtractorRuntime(FullConfig config, IServiceProvider provider)
        {
            this.config = config;
            this.provider = provider;
        }
        /// <summary>
        /// Start the extractor. This creates pushers and tests their connection
        /// </summary>
        /// <param name="source">CancellationTokenSource used to create tokens and terminate the run-task on failure</param>
        public async Task Run(CancellationTokenSource source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            var client = new UAClient(config);
            var pushers = new List<IPusher>();

            if (config.Cognite != null)
            {
                pushers.Add(new CDFPusher(provider, config.Cognite));
            }
            if (config.Mqtt != null)
            {
                pushers.Add(new MQTTPusher(config.Mqtt));
            }
            if (config.Influx != null)
            {
                pushers.Add(new InfluxPusher(config.Influx));
            }

            await Task.WhenAll(pushers.Select(async pusher =>
            {
                var res = await pusher.TestConnection(config, source.Token);
                if (!(res ?? false))
                {
                    pusher.NoInit = true;
                }
            }));

            log.Information("Building extractor");
            using var extractor = new UAExtractor(config, pushers, client, provider.GetService<IExtractionStateStore>());

            try
            {
                await extractor.RunExtractor(source.Token);
                source.Cancel();
            }
            catch
            {
                extractor.Close();
                throw;
            }
            finally
            {
                source.Cancel();
                foreach (var pusher in pushers)
                {
                    pusher.Dispose();
                }
            }
        }
    }
}

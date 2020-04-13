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

        private static readonly ILogger log = Log.Logger.ForContext(typeof(ExtractorRuntime));

        /// <summary>
        /// Constructor, takes fully configured FullConfig
        /// </summary>
        /// <param name="config"></param>
        public ExtractorRuntime(FullConfig config)
        {
            this.config = config;
        }
        /// <summary>
        /// Creates the IServiceProvider instance to be used with this extractorRuntime
        /// </summary>
        public void Configure()
        {
            var services = new ServiceCollection();
            Configure(services);
            provider = services.BuildServiceProvider();
        }

        /// <summary>
        /// Configure two different configurations for the CDF client. One terminates on 410 or after 4 attempts. The other tries forever. Both terminate on 400.
        /// </summary>
        /// <param name="services"></param>
        private static void Configure(IServiceCollection services)
        {
            services.AddHttpClient("Context", client => { client.Timeout = TimeSpan.FromSeconds(120); })
                .AddPolicyHandler(GetRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
            services.AddHttpClient("Data", client => { client.Timeout = TimeSpan.FromSeconds(60); })
                .AddPolicyHandler(GetDataRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
        }
        /// <summary>
        /// Returns a retry policy to be used with requests to CDF for assets and timeseries.
        /// </summary>
        /// <returns>Retry policy with a high number of retries</returns>
        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {
            return Policy
                .HandleResult<HttpResponseMessage>(msg =>
                    !msg.IsSuccessStatusCode
                    && ((int)msg.StatusCode >= 500
                        || msg.StatusCode == HttpStatusCode.Unauthorized
                        || msg.StatusCode == HttpStatusCode.TooManyRequests))
                .Or<TimeoutRejectedException>()
                .WaitAndRetryAsync(8, retry => TimeSpan.FromMilliseconds(125 * Math.Pow(2, Math.Min(retry - 1, 9))));
        }
        /// <summary>
        /// Returns a retry policy to be used with requests to CDF for datapoints and events
        /// </summary>
        /// <returns>Retry policy with lower number of retries</returns>
        private static IAsyncPolicy<HttpResponseMessage> GetDataRetryPolicy()
        {
            return Policy
                .HandleResult<HttpResponseMessage>(msg =>
                    !msg.IsSuccessStatusCode
                    && ((int)msg.StatusCode >= 500
                        || msg.StatusCode == HttpStatusCode.Unauthorized
                        || msg.StatusCode == HttpStatusCode.TooManyRequests))
                .Or<TimeoutRejectedException>()
                .WaitAndRetryAsync(4, retry => TimeSpan.FromMilliseconds(125 * Math.Pow(2, Math.Min(retry - 1, 9))));
        }
        /// <summary>
        /// Return a 20 second timeout policy
        /// </summary>
        /// <returns>20 second timeout policy</returns>
        private static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy()
        {
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(20));
        }

        /// <summary>
        /// Start the extractor. This creates pushers and tests their connection
        /// </summary>
        /// <param name="source">CancellationTokenSource used to create tokens and terminate the run-task on failure</param>
        public async Task Run(CancellationTokenSource source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            var client = new UAClient(config);
            int index = 0;
            IEnumerable<IPusher> pushers = config.Pushers.Select(pusher => pusher.ToPusher(index++, provider)).ToList();
            await Task.WhenAll(pushers.Select(async pusher =>
            {
                var res = await pusher.TestConnection(config, source.Token);
                if (!(res ?? false))
                {
                    pusher.NoInit = true;
                }
            }));

            log.Information("Building extractor");
            using var extractor = new Extractor(config, pushers, client);

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
                foreach (var pusher in pushers)
                {
                    pusher.Dispose();
                }
            }
        }
    }
}

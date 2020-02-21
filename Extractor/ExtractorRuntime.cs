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
    public class ExtractorRuntime
    {
        private readonly FullConfig config;
        private IServiceProvider provider;

        private static readonly ILogger log = Log.Logger.ForContext(typeof(ExtractorRuntime));

        public ExtractorRuntime(FullConfig config)
        {
            this.config = config;
        }

        public void Configure()
        {
            var services = new ServiceCollection();
            Configure(services);
            provider = services.BuildServiceProvider();
            var factory = provider.GetRequiredService<IHttpClientFactory>();
            factory.CreateClient("Context");
        }

        /// <summary>
        /// Configure two different configurations for the CDF client. One terminates on 410 or after 4 attempts. The other tries forever. Both terminate on 400.
        /// </summary>
        /// <param name="services"></param>
        private static void Configure(IServiceCollection services)
        {
            services.AddHttpClient("Context", client => { client.Timeout = Timeout.InfiniteTimeSpan; })
                .AddPolicyHandler(GetRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
            services.AddHttpClient("Data", client => { client.Timeout = TimeSpan.FromSeconds(10); })
                .AddPolicyHandler(GetDataRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
        }
        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {
            return Policy
                .HandleResult<HttpResponseMessage>(msg =>
                    !msg.IsSuccessStatusCode
                    && ((int)msg.StatusCode >= 500
                        || msg.StatusCode == HttpStatusCode.Unauthorized
                        || msg.StatusCode == HttpStatusCode.TooManyRequests))
                .Or<TimeoutRejectedException>()
                .WaitAndRetryForeverAsync(retry => TimeSpan.FromMilliseconds(125 * Math.Pow(2, Math.Min(retry - 1, 9))));
        }
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
        private static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy()
        {
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(60));
        }

        /// <summary>
        /// Start the extractor.
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="provider">ServiceProvider with any required service for the pushers.</param>
        /// <param name="source">CancellationTokenSource used to create tokens and terminate the run-task on failure</param>
        public async Task Run(CancellationTokenSource source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            var client = new UAClient(config);
            int index = 0;
            IEnumerable<IPusher> pushers = config.Pushers.Select(pusher => pusher.ToPusher(index++, provider)).ToList();
            var removePushers = new List<IPusher>();

            await Task.WhenAll(pushers.Select(async pusher =>
            {
                var result = await pusher.TestConnection(source.Token);
                if (pusher.BaseConfig.Critical && !result.Value)
                {
                    throw new ExtractorFailureException("Critical pusher failed to connect");
                }

                if (!result.Value)
                {
                    Log.Warning("Removing pusher of type {type}", pusher.GetType());
                    removePushers.Add(pusher);
                }
            }));
            log.Information("Building extractor");
            pushers = pushers.Except(removePushers).ToList();
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
        }
    }
}

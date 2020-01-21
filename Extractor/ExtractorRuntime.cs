using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using Polly.Timeout;

namespace Cognite.OpcUa
{
    public class ExtractorRuntime
    {
        private readonly FullConfig config;
        private IServiceProvider provider;

        public ExtractorRuntime(FullConfig config)
        {
            this.config = config;
        }

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
        private void Configure(IServiceCollection services)
        {
            services.AddHttpClient<ContextCDFClient>(client => { client.Timeout = Timeout.InfiniteTimeSpan; })
                .AddPolicyHandler(GetRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
            services.AddHttpClient<DataCDFClient>(client => { client.Timeout = TimeSpan.FromSeconds(300); })
                .AddPolicyHandler(GetDataRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
        }
        private IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
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
        private IAsyncPolicy<HttpResponseMessage> GetDataRetryPolicy()
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
        private IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy()
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
            var client = new UAClient(config);
            int index = 0;
            IEnumerable<IPusher> pushers = config.Pushers.Select(pusher => pusher.ToPusher(index++, provider)).ToList();
            var removePushers = new List<IPusher>();
            try
            {
                Task.WhenAll(pushers.Select(pusher => pusher.TestConnection(source.Token).ContinueWith(result =>
                {
                    if (pusher.BaseConfig.Critical && !result.Result)
                    {
                        throw new Exception("Critical pusher failed to connect");
                    }
                    if (!result.Result)
                    {
                        removePushers.Add(pusher);
                    }
                })).ToArray()).Wait();
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to connect to a critical destination", ex);
            }

            pushers = pushers.Except(removePushers).ToList();
            var extractor = new Extractor(config, pushers, client);

            var runTask = extractor.RunExtractor(source.Token)
                .ContinueWith(task =>
                {
                    source.Cancel();
                    if (task.IsFaulted)
                    {
                        throw task.Exception;
                    }
                });

            try
            {
                await runTask;
            }
            catch
            {
                extractor.Close();
                throw;
            }
        }
    }
    public class DataCDFClient : Client { public DataCDFClient(HttpClient httpClient) : base(httpClient) { } }
    public class ContextCDFClient : Client { public ContextCDFClient(HttpClient httpClient) : base(httpClient) { } }
}

using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using Polly.Timeout;
using Serilog;

namespace Cognite.Bridge
{
    class Program
    {
        private static readonly ILogger log = Log.ForContext<Program>();

        static void Main(string[] args)
        {
            var provider = Configure();
            var configPath = args.Length > 0 ? args[0] : "config/config.yml";
            var config = Config.GetConfig(configPath);
            Logger.Configure(config.Logging);
            log.Information(config.ToString());
            RunBridge(config, provider).Wait();
        }
        public static IServiceProvider Configure()
        {
            var services = new ServiceCollection();
            Configure(services);
            var provider = services.BuildServiceProvider();
            return provider;
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
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(20));
        }
        /// <summary>
        /// Public to expose for tests, start the bridge with given configuration and HTTP-Client configured serviceprovider.
        /// </summary>
        /// <param name="config">Full configuration to use</param>
        /// <param name="provider">HTTP-Client configured serviceprovider</param>
        /// <returns></returns>
        public static async Task RunBridge(BridgeConfig config, IServiceProvider provider)
        {
            var destination = new Destination(config.CDF, provider);
            using var bridge = new MQTTBridge(destination, config);

            using var source = new CancellationTokenSource();

            using var quitEvent = new ManualResetEvent(false);
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                quitEvent?.Set();
                eArgs.Cancel = true;
                source?.Cancel();
            };

            while (true)
            {
                try
                {
                    await bridge.StartBridge(source.Token);
                    break;
                }
                catch (Exception ex)
                {
                    log.Warning("Failed to start bridge: {msg}, retrying", ex.Message);
                }

                await Task.Delay(2000);
            }

            quitEvent.WaitOne();

            Log.CloseAndFlush();
        }
    }
}

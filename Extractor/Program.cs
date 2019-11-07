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
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using System.Net.Http;
using System.Collections.Generic;
using Polly;
using Prometheus.Client.MetricPusher;
using CogniteSdk;
using System.Threading.Tasks;
using Polly.Timeout;
using System.Linq;
using System.Net;
using Serilog;

namespace Cognite.OpcUa
{
    class Program
    {
        static MetricPushServer _worker;
        /// <summary>
        /// Load config, start the <see cref="Logger"/>, start the <see cref="Extractor"/> then wait for exit signal
        /// </summary>
        /// <returns></returns>
        static int Main()
        {
            // Temporary logger config for capturing logs during configuration.
            Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();

            string configDir = Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR");
            configDir = string.IsNullOrEmpty(configDir) ? "config/" : configDir;
            FullConfig fullConfig;
            try
            {
                string configFile = System.IO.Path.Combine(configDir, "config.yml");
                Log.Information($"Loading config from {configFile}");
                fullConfig = Utils.GetConfig(configFile);
            }
            catch (YamlDotNet.Core.YamlException e)
            {
                Log.Error("Failed to load config at {start}: {msg}", e.Start, e.InnerException?.Message ?? e.Message);
                return -1;
            }

            Log.Information("Starting OPC UA Extractor version {version}", Version.GetVersion());
            Log.Information("Revision information: {status}", Version.Status());

            fullConfig.Source.ConfigRoot = configDir;

            try
            {
                ValidateConfig(fullConfig);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to validate config");
                return -1;
            }

            Logger.Configure(fullConfig.Logging);

            var services = new ServiceCollection();
            Configure(services);
            var provider = services.BuildServiceProvider();

            try
            {
                SetupMetrics(fullConfig.Metrics);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to start metrics pusher");
            }

            CancellationTokenSource source = null;
            using (var quitEvent = new ManualResetEvent(false))
            {
                bool canceled = false;
                Console.CancelKeyPress += (sender, eArgs) =>
                {
                    quitEvent?.Set();
                    eArgs.Cancel = true;
                    source?.Cancel();
                    canceled = true;
                };
                while (true)
                {
                    using (source = new CancellationTokenSource())
                    {
                        if (canceled)
                        {
                            Log.Warning("Extractor stopped manually");
                            break;
                        }

                        try
                        {
                            Run(fullConfig, provider, source);
                        }
                        catch (TaskCanceledException)
                        {
                            Log.Warning("Extractor stopped manually");
                            break;
                        }
                        catch
                        {
                            Log.Error("Extractor crashed, restarting");
                        }
                        try
                        {
                            Task.Delay(1000, source.IsCancellationRequested ? CancellationToken.None : source.Token).Wait();
                        }
                        catch (TaskCanceledException)
                        {
                            Log.Warning("Extractor stopped manually");
                            break;
                        }
                    }
                }
            }
            Log.CloseAndFlush();
            return 0;
        }
        /// <summary>
        /// Tests that the config is correct and valid
        /// </summary>
        /// <param name="config">The config object</param>
        /// <exception cref="Exception">On invalid config</exception>
        private static void ValidateConfig(FullConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.Source.EndpointURL)) throw new Exception("Invalid EndpointURL");
            if (config.Source.PollingInterval < 0) throw new Exception("PollingInterval must be a positive number");
        }
        /// <summary>
        /// Configure two different configurations for the CDF client. One terminates on 410 or after 4 attempts. The other tries forever. Both terminate on 400.
        /// </summary>
        /// <param name="services"></param>
        private static void Configure(IServiceCollection services)
        {
            services.AddHttpClient<ContextCDFClient>()
                .AddPolicyHandler(GetRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
            services.AddHttpClient<DataCDFClient>()
                .AddPolicyHandler(GetDataRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
        }
        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {
            int maxRetryAttempt = (int)Math.Ceiling(Math.Log(120, 2));
            return Policy
                .HandleResult<HttpResponseMessage>(msg =>
                    !msg.IsSuccessStatusCode
                    && ((int)msg.StatusCode >= 500
                        || msg.StatusCode == HttpStatusCode.Unauthorized
                        || msg.StatusCode == HttpStatusCode.TooManyRequests))
                .Or<TimeoutRejectedException>()
                .WaitAndRetryForeverAsync(retryAttempt =>
                    TimeSpan.FromMilliseconds(retryAttempt > maxRetryAttempt ? 60000 : Math.Pow(2, retryAttempt)));
        }
        private static IAsyncPolicy<HttpResponseMessage> GetDataRetryPolicy()
        {
            int maxRetryAttempt = (int)Math.Ceiling(Math.Log(120, 2));
            return Policy
                .HandleResult<HttpResponseMessage>(msg =>
                    !msg.IsSuccessStatusCode
                    && ((int)msg.StatusCode >= 500
                        || msg.StatusCode == HttpStatusCode.Unauthorized
                        || msg.StatusCode == HttpStatusCode.TooManyRequests))
                .Or<TimeoutRejectedException>()
                .WaitAndRetryAsync(4, retryAttempt =>
                    TimeSpan.FromMilliseconds(retryAttempt > maxRetryAttempt ? 60000 : Math.Pow(2, retryAttempt)));
        }
        private static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy()
        {
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(120));
        }
        /// <summary>
        /// Starts prometheus pushgateway client
        /// </summary>
        /// <param name="config">The metrics config object</param>
        private static void SetupMetrics(MetricsConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.URL) || string.IsNullOrWhiteSpace(config.Job))
            {
                Log.Information("Not pushing metrics, missing URL or Job");
                return;
            }
            var additionalHeaders = new Dictionary<string, string>();
            if (!string.IsNullOrWhiteSpace(config.Username) && !string.IsNullOrWhiteSpace(config.Password))
            {
                string encoded = Convert.ToBase64String(
                    System.Text.Encoding
                        .GetEncoding("ISO-8859-1")
                        .GetBytes($"{config.Username}:{config.Password}")
                );
                additionalHeaders.Add("Authorization", $"Basic {encoded}");
            }
            var pusher = new MetricPusher(config.URL, config.Job, config.Instance, additionalHeaders);
            _worker = new MetricPushServer(pusher, TimeSpan.FromMilliseconds(config.PushInterval));
            _worker.Start();
        }
        /// <summary>
        /// Start the extractor.
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="provider">ServiceProvider with any required service for the pushers.</param>
        /// <param name="source">CancellationTokenSource used to create tokens and terminate the run-task on failure</param>
        private static void Run(FullConfig config, IServiceProvider provider, CancellationTokenSource source)
        {
            var client = new UAClient(config);
            IEnumerable<IPusher> pushers = config.Pushers.Select(pusher => pusher.ToPusher(provider)).ToList();
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
                runTask.Wait();
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

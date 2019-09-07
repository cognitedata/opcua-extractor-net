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
using Opc.Ua;
using CogniteSdk;
using System.Threading.Tasks;
using Polly.Timeout;
using System.Runtime.ExceptionServices;
using System.Linq;
using Serilog;

namespace Cognite.OpcUa
{
    class Program
    {
        static MetricPushServer worker;
        /// <summary>
        /// Load config, start the <see cref="Logger"/>, start the <see cref="Extractor"/> then wait for exit signal
        /// </summary>
        /// <returns></returns>
        static int Main()
        {
            var configDir = Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR");
            configDir = string.IsNullOrEmpty(configDir) ? "config/" : configDir;
            FullConfig fullConfig = Utils.GetConfig($"{configDir}/config.yml");
            fullConfig.UAConfig.ConfigRoot = configDir;

            if (fullConfig == null) return -1;
            try
            {
                ValidateConfig(fullConfig);
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to load config");
                Console.WriteLine(e.Message);
                return -1;
            }

            Logger.Configure(fullConfig.LoggerConfig);


            var services = new ServiceCollection();
            Configure(services);
            var provider = services.BuildServiceProvider();

            try
            {
                SetupMetrics(fullConfig.MetricsConfig);
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
                    quitEvent.Set();
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
                            Run(fullConfig, quitEvent, provider, source);
                        }
                        catch (TaskCanceledException)
                        {
                            Log.Warning("Extractor stopped manually");
                            break;
                        }
                        catch (Exception e)
                        {
                            Log.Error(e, "Exception in Run");
                        }
                        if (source.IsCancellationRequested) continue;
                        try
                        {
                            Task.Delay(1000, source.Token).Wait();
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
            if (string.IsNullOrWhiteSpace(config.UAConfig.EndpointURL)) throw new Exception("Invalid EndpointURL");
            if (config.UAConfig.PollingInterval < 0) throw new Exception("PollingInterval must be a positive number");
        }
        private static void Configure(IServiceCollection services)
        {
            services.AddHttpClient<Client>()
                .AddPolicyHandler(GetRetryPolicy())
                .AddPolicyHandler(GetTimeoutPolicy());
        }
        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {
            int maxRetryAttempt = (int)Math.Ceiling(Math.Log(60000 / 500, 2));
            return Policy
                .HandleResult<HttpResponseMessage>(msg =>
                    !msg.IsSuccessStatusCode && msg.StatusCode != System.Net.HttpStatusCode.BadRequest)
                .Or<TimeoutRejectedException>()
                .WaitAndRetryForeverAsync(retryAttempt =>
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
                Log.Warning("Unable to start metrics, missing URL or Job");
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
            worker = new MetricPushServer(pusher, TimeSpan.FromMilliseconds(config.PushInterval));
            worker.Start();
        }
        private static void Run(FullConfig config, ManualResetEvent quitEvent, ServiceProvider provider, CancellationTokenSource source)
        {
            UAClient client = new UAClient(config);
            // As it turns out, linq does some insane stuff when you use the result of a "select" query that does transformation.
            var pushers = config.Pushers.Select(pusher => pusher.ToPusher(provider)).ToList();
            Extractor extractor = new Extractor(config, pushers, client);

            Task runTask = extractor.RunExtractor(source.Token)
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
            catch (Exception)
            {
            }

            if (runTask.IsFaulted)
            {
                if (runTask.Exception.InnerException is TaskCanceledException)
                {
                    extractor.Close();
                    throw new TaskCanceledException();
                }
                ExceptionDispatchInfo.Capture(runTask.Exception).Throw();
                return;
            }

            if (source.IsCancellationRequested)
            {
                extractor.Close();
                throw new TaskCanceledException();
            }
        }
    }
    public class UAClientConfig
    {
        public string ConfigRoot { get; set; } = "config";
        public string EndpointURL { get; set; }
        public bool Autoaccept { get; set; } = true;
        public int PollingInterval { get; set; } = 500;
        public string Username { get; set; }
        public string Password { get; set; }
        public bool Secure { get; set; }
        public int HistoryGranularity { get; set; }
        public bool ForceRestart { get; set; }
        public int BrowseChunk { get; set; }
        private int _uaHistoryReadPoints;
        public int HistoryReadChunk { get { return _uaHistoryReadPoints; } set { _uaHistoryReadPoints = Math.Max(0, value); } }
        private int _uaHistoryReadNodes;
        public int HistoryReadNodesChunk { get { return _uaHistoryReadNodes; } set { _uaHistoryReadNodes = Math.Max(1, value); } }
        public int AttributesChunk { get; set; }
    }
    public class ExtractionConfig
    {
        public string GlobalPrefix { get; set; }
        public IEnumerable<string> IgnorePrefix { get; set; }
        public IEnumerable<string> IgnoreName { get; set; }
        public ProtoNodeId RootNode { get; set; }
        public Dictionary<string, ProtoNodeId> NameOverrides { get; set; }
        public IEnumerable<ProtoNodeId> IgnoreDataTypes { get; set; }
        public int MaxArraySize { get; set; }
        public bool AllowStringVariables { get; set; }
        public Dictionary<string, string> NSMaps { get; set; }
    }
    public abstract class PusherConfig
    {
        public bool Debug { get; set; }
        public int DataPushDelay { get; set; }
        public abstract IPusher ToPusher(IServiceProvider provider);
    }
    public class CogniteClientConfig : PusherConfig
    {
        public string Project { get; set; }
        public string ApiKey { get; set; }
        public string Host { get; set; }
        public bool BufferOnFailure { get; set; }
        public string BufferFile { get; set; }
        public override IPusher ToPusher(IServiceProvider provider)
        {
            return new CDFPusher(provider, this);
        }
    }
    public class InfluxClientConfig : PusherConfig
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }
        public int PointChunkSize { get; set; } = 100000;
        public override IPusher ToPusher(IServiceProvider _)
        {
            return new InfluxPusher(this);
        }
    }
    public class FullConfig
    {
        public UAClientConfig UAConfig { get; set; }
        public LoggerConfig LoggerConfig { get; set; }
        public MetricsConfig MetricsConfig { get; set; }
        public List<PusherConfig> Pushers { get; set; }
        public ExtractionConfig ExtractionConfig { get; set; }
    }
    public class LoggerConfig
    {
        public string ConsoleLevel { get; set; }
        public string FileLevel { get; set; }
        public string LogFolder { get; set; }
        public int RetentionLimit { get; set; } = 31;
        public string StackdriverCredentials { get; set; }
        public string StackdriverLogName { get; set; }
    }
    public class MetricsConfig
    {
        public string URL { get; set; }
        public string Job { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public int PushInterval { get; set; }
        public string Instance { get; set; }
    }
    public class ProtoNodeId
    {
        public string NamespaceUri { get; set; }
        public string NodeId { get; set; }
        public NodeId ToNodeId(UAClient client)
        {
            var node = client.ToNodeId(NodeId, NamespaceUri);
            if (node.IsNullNodeId)
            {
                return ObjectIds.ObjectsFolder;
            }
            return node;
        }
    }
}

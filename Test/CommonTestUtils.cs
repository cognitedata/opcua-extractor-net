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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdysTech.InfluxDB.Client.Net;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using LiteDB;
using Microsoft.Extensions.DependencyInjection;
using Prometheus.Client;
using Serilog;
using Xunit.Abstractions;
using Xunit;

namespace Test
{
    public class MakeConsoleWork : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly TextWriter _originalOut;
        private readonly TextWriter _textWriter;

        public MakeConsoleWork(ITestOutputHelper output)
        {
            _output = output;
            _originalOut = Console.Out;
            _textWriter = new StringWriter();
            Console.SetOut(_textWriter);
        }

        public void Dispose()
        {

            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            _output.WriteLine(_textWriter.ToString());
            _textWriter.Dispose();
            Console.SetOut(_originalOut);
        }
    }
    public static class CommonTestUtils
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(CommonTestUtils));

        public static FullConfig BuildConfig(string serverType, string configname = "config.test.yml")
        {
            var fullConfig = ExtractorUtils.GetConfig(configname);
            if (fullConfig == null) throw new ConfigurationException("Failed to load config file");
            switch (serverType)
            {
                case "basic":
                    fullConfig.Source.EndpointURL = "opc.tcp://localhost:4840";
                    break;
                case "full":
                    fullConfig.Source.EndpointURL = "opc.tcp://localhost:4841";
                    break;
                case "array":
                    fullConfig.Source.EndpointURL = "opc.tcp://localhost:4842";
                    break;
                case "events":
                    fullConfig.Source.EndpointURL = "opc.tcp://localhost:4843";
                    break;
                case "audit":
                    fullConfig.Source.EndpointURL = "opc.tcp://localhost:4844";
                    break;
            }
            return fullConfig;
        }
        public static bool TestRunResult(Exception e)
        {
            if (!(e is TaskCanceledException || e is AggregateException && e.InnerException is TaskCanceledException))
            {
                return false;
            }
            return true;
        }
        public static IServiceProvider GetDummyProvider(CDFMockHandler handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            var services = new ServiceCollection();
            services.AddHttpClient("Context")
                .ConfigurePrimaryHttpMessageHandler(handler.GetHandler);
            services.AddHttpClient("Data")
                .ConfigurePrimaryHttpMessageHandler(handler.GetHandler);
            return services.BuildServiceProvider();
        }

        public static double GetMetricValue(string name)
        {
            Metrics.DefaultCollectorRegistry.TryGet(name, out var collector);
            return collector switch
            {
                Gauge gauge => gauge.Value,
                Counter counter => counter.Value,
                _ => 0
            };
        }
        public static bool TestMetricValue(string name, double value)
        {
            Metrics.DefaultCollectorRegistry.TryGet(name, out var collector);
            return collector switch
            {
                Gauge gauge => (Math.Abs(gauge.Value - value) < 0.01),
                Counter counter => (Math.Abs(counter.Value - value) < 0.01),
                _ => false
            };
        }

        public static bool VerifySuccessMetrics()
        {
            return TestMetricValue("opcua_attribute_request_failures", 0)
                && TestMetricValue("opcua_history_read_failures", 0)
                && TestMetricValue("opcua_browse_failures", 0);
        }

        private static void ResetMetricValue(string name)
        {
            Metrics.DefaultCollectorRegistry.TryGet(name, out var collector);
            switch (collector)
            {
                case Gauge gauge:
                    gauge.Set(0);
                    break;
                case Counter counter:
                    counter.Reset();
                    break;
            }
        }
        public static void ResetTestMetrics()
        {
            var metrics = new List<string>
            {
                "opcua_attribute_request_failures", "opcua_history_read_failures", "opcua_browse_failures",
                "opcua_browse_operations", "opcua_history_reads", "opcua_tracked_timeseries",
                "opcua_tracked_assets", "opcua_node_ensure_failures", "opcua_datapoint_pushes",
                "opcua_datapoint_push_failures", "opcua_backfill_data_count", "opcua_frontfill_data_count",
                "opcua_backfill_events_count", "opcua_frontfill_events_count"
            };
            foreach (var metric in metrics)
            {
                ResetMetricValue(metric);
            }
        }
        public static Process Bash(string cmd)
        {
            if (cmd == null) throw new ArgumentNullException(nameof(cmd));
            var escapedArgs = cmd.Replace("\"", "\\\"", StringComparison.InvariantCulture);
            log.Information(escapedArgs);

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"{escapedArgs}\"",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = false,
                }
            };
            process.Start();
            return process;
        }

        public static Process GetProxyProcess()
        {
            return Bash("ncat -lk 4839 -c \"ncat localhost 4840\"");
        }

        public static void StopProxyProcess()
        {
            using var process = Bash("kill $(ps aux | grep '[n]cat' | awk '{print $2}')");
            process.WaitForExit();
        }
    }
    public enum ServerName { Basic, Full, Array, Events, Audit, Proxy }
    public enum ConfigName { Events, Influx, Test }

    public sealed class ExtractorTester : IDisposable
    {
        public static readonly Dictionary<ServerName, string> hostNames = new Dictionary<ServerName, string>
        {
            {ServerName.Basic, "opc.tcp://localhost:4840"},
            {ServerName.Full, "opc.tcp://localhost:4841"},
            {ServerName.Array, "opc.tcp://localhost:4842"},
            {ServerName.Events, "opc.tcp://localhost:4843"},
            {ServerName.Audit, "opc.tcp://localhost:4844"},
            {ServerName.Proxy, "opc.tcp://localhost:4839"}
        };

        private static readonly Dictionary<ConfigName, string> configNames = new Dictionary<ConfigName, string>
        {
            {ConfigName.Test, "config.test.yml"},
            {ConfigName.Events, "config.events.yml"},
            {ConfigName.Influx, "config.influxtest.yml"}
        };


        public FullConfig Config { get; }
        public CDFMockHandler Handler { get; }
        public Extractor Extractor { get; }
        public UAClient UAClient { get; }
        public IPusher Pusher { get; }
        public InfluxClientConfig InfluxConfig { get; }
        public CogniteClientConfig CogniteConfig { get; }
        public CancellationTokenSource Source { get; }
        public InfluxDBClient IfDbClient { get; }
        private readonly bool influx;
        public Task RunTask { get; private set; }
        private readonly ExtractorTestParameters testParams;
        private static readonly ILogger log = Log.Logger.ForContext(typeof(ExtractorTester));

        public ExtractorTester(ExtractorTestParameters testParams)
        {
            this.testParams = testParams ?? throw new ArgumentNullException(nameof(testParams));
            Config = ExtractorUtils.GetConfig(configNames[testParams.ConfigName]);

            if (testParams.HistoryGranularity != null)
            {
                Config.History.Granularity = testParams.HistoryGranularity.Value;
            }
            Config.Logging.ConsoleLevel = testParams.LogLevel;
            Logger.Configure(Config.Logging);
            Config.Source.EndpointURL = hostNames[testParams.ServerName];

            FullConfig pusherConfig = null;
            if (testParams.PusherConfig != null)
            {
                pusherConfig = ExtractorUtils.GetConfig(configNames[testParams.PusherConfig.Value]);
            }

            if (testParams.FailureInflux != null)
            {
                Config.FailureBuffer.Enabled = true;
                if (testParams.FailureInflux != null)
                {
                    var failureInflux = ExtractorUtils.GetConfig(configNames[testParams.FailureInflux.Value]);
                    if (failureInflux.Pushers.First() is InfluxClientConfig influxConfig)
                    {
                        influx = true;
                        InfluxConfig = influxConfig;
                        IfDbClient = new InfluxDBClient(InfluxConfig.Host, InfluxConfig.Username, InfluxConfig.Password);
                        Config.FailureBuffer.Influx = new InfluxBufferConfig
                        {
                            Database = influxConfig.Database,
                            Host = influxConfig.Host,
                            Password = influxConfig.Password,
                            PointChunkSize = influxConfig.PointChunkSize,
                            Username = influxConfig.Username,
                            Write = testParams.FailureInfluxWrite
                        };
                    }
                }
            }

            switch ((pusherConfig ?? Config).Pushers.First())
            {
                case CogniteClientConfig cogniteClientConfig:
                    CogniteConfig = cogniteClientConfig;
                    Handler = new CDFMockHandler(CogniteConfig.Project, testParams.MockMode);
                    Handler.StoreDatapoints = testParams.StoreDatapoints;
                    Pusher = CogniteConfig.ToPusher(0, CommonTestUtils.GetDummyProvider(Handler));
                    break;
                case InfluxClientConfig influxClientConfig:
                    InfluxConfig = influxClientConfig;
                    Pusher = InfluxConfig.ToPusher(0, null);
                    influx = true;
                    IfDbClient = new InfluxDBClient(InfluxConfig.Host, InfluxConfig.Username, InfluxConfig.Password);
                    break;
            }

            if (testParams.InfluxOverride != null && !influx)
            {
                influx = true;
                InfluxConfig = testParams.InfluxOverride;
                IfDbClient = new InfluxDBClient(InfluxConfig.Host, InfluxConfig.Username, InfluxConfig.Password);
            }
            UAClient = new UAClient(Config);
            Source = new CancellationTokenSource();
            if (testParams.Builder != null)
            {
                Extractor = testParams.Builder(Config, Pusher, UAClient);
            }
            else
            {
                Extractor = new Extractor(Config, Pusher, UAClient);
            }
        }

        public async Task ClearPersistentData()
        {
            CommonTestUtils.ResetTestMetrics();
            if (influx)
            {
                await IfDbClient.DropDatabaseAsync(new InfluxDatabase(InfluxConfig.Database));
                await IfDbClient.CreateDatabaseAsync(InfluxConfig.Database);
            }

            if (Config.StateStorage.Location != null)
            {
                using var db = new LiteDatabase(Config.StateStorage.Location);
                var collections = db.GetCollectionNames();
                foreach (var collection in collections)
                {
                    db.DropCollection(collection);
                }
            }

        }

        public void StartExtractor()
        {
            RunTask = Extractor.RunExtractor(Source.Token, testParams.QuitAfterMap);
        }
        public async Task WaitForCondition(Func<Task<bool>> condition, int seconds, Func<string> assertion)
        {
            bool triggered = false;
            for (int i = 0; i < seconds * 5; i++)
            {
                if (await condition())
                {
                    triggered = true;
                    break;
                }

                if (RunTask.IsFaulted)
                {
                    log.Error(RunTask.Exception, "RunTask failed during WaitForCondition");
                    break;
                }

                await Task.Delay(200);
            }
            Assert.True(triggered, assertion());
        }
        public async Task WaitForCondition(Func<bool> condition, int seconds,
            string assertion = "Expected condition to trigger")
        { 
            await WaitForCondition(() => Task.FromResult(condition()), seconds, () => assertion);
        }
        public async Task WaitForCondition(Func<bool> condition, int seconds,
            Func<string> assertion)
        {
            await WaitForCondition(() => Task.FromResult(condition()), seconds, assertion);
        }
        public async Task WaitForCondition(Func<Task<bool>> condition, int seconds,
            string assertion = "Expected condition to trigger")
        {
            await WaitForCondition(condition, seconds, () => assertion);
        }

        public async Task TerminateRunTask(Func<Exception, bool> testResult = null)
        {
            if (RunTask == null) throw new FatalException("Run task is not started");
            if (!testParams.QuitAfterMap)
            {
                await Extractor.WaitForNextPush();
                Source.Cancel();
            }
            try
            {
                await RunTask;
            }
            catch (Exception e)
            {
                if (testResult != null && !testResult(e)) throw;
                if (testResult == null && !CommonTestUtils.TestRunResult(e)) throw;
            }
            Extractor.Close();
        }

        public void TestContinuity(string id)
        {
            var dps = Handler.datapoints[id].Item1;
            var intdps = dps.GroupBy(dp => dp.Timestamp).Select(dp => (int)Math.Round(dp.First().Value)).ToList();
            TestContinuity(intdps);
        }

        public static void TestContinuity(List<int> intdps)
        {
            if (intdps == null) throw new ArgumentNullException(nameof(intdps));
            int min = intdps.Min();
            var check = new int[intdps.Count];

            int last = min - 1;
            foreach (int dp in intdps)
            {
                if (last != dp - 1)
                {
                    log.Verbose("Out of order points at {dp}, {last}", dp, last);
                }
                last = dp;
                check[dp - min]++;
            }
            Assert.All(check, val => Assert.Equal(1, val));
        }
        public void Dispose()
        {
            Source?.Dispose();
            IfDbClient?.Dispose();
            Extractor?.Dispose();
        }
    }
    public class ExtractorTestParameters
    {
        public ServerName ServerName { get; set; } = ServerName.Basic;
        public ConfigName ConfigName { get; set; } = ConfigName.Test;
        public ConfigName? PusherConfig { get; set; } = null;
        public CDFMockHandler.MockMode MockMode { get; set; } = CDFMockHandler.MockMode.None;
        public string LogLevel { get; set; } = "information";
        public bool QuitAfterMap { get; set; } = false;
        public bool StoreDatapoints { get; set; } = false;
        public int? HistoryGranularity { get; set; } = null;
        public ConfigName? FailureInflux { get; set; } = null;
        public bool FailureInfluxWrite { get; set; } = true;
        public Func<FullConfig, IPusher, UAClient, Extractor> Builder { get; set; } = null;
        public InfluxClientConfig InfluxOverride { get; set; } = null;
    }
}

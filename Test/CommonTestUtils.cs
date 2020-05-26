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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdysTech.InfluxDB.Client.Net;
using Cognite.Bridge;
using Cognite.OpcUa;
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
            double val = collector switch
            {
                Gauge gauge => gauge.Value,
                Counter counter => counter.Value,
                _ => 0
            };
            if (Math.Abs(val - value) > 0.01)
            {
                log.Information("Expected {val} but got {value} for metric {name}", 
                    value, val, name);
                return false;
            }

            return true;
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
                "opcua_attribute_request_failures",
                "opcua_history_read_failures",
                "opcua_browse_failures",
                "opcua_browse_operations",
                "opcua_history_reads",
                "opcua_tracked_timeseries",
                "opcua_tracked_assets",
                "opcua_node_ensure_failures",
                "opcua_datapoint_pushes",
                "opcua_datapoint_push_failures",
                "opcua_backfill_data_count",
                "opcua_frontfill_data_count",
                "opcua_backfill_events_count",
                "opcua_frontfill_events_count",
                "opcua_datapoint_push_failures_influx",
                "opcua_event_push_failures",
                "opcua_event_push_failures_influx",
                "opcua_duplicated_events",
                "opcua_created_assets_mqtt",
                "opcua_created_timeseries_mqtt"
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

        /// <summary>
        /// Test that the event contains the appropriate data for the event server test
        /// </summary>
        /// <param name="ev"></param>
        public static void TestEvent(EventDummy ev, CDFMockHandler factory)
        {
            if (ev == null) throw new ArgumentNullException(nameof(ev));
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            Assert.False(ev.description.StartsWith("propOther2 ", StringComparison.InvariantCulture));
            Assert.False(ev.description.StartsWith("basicBlock ", StringComparison.InvariantCulture));
            Assert.False(ev.description.StartsWith("basicNoVarSource ", StringComparison.InvariantCulture));
            Assert.False(ev.description.StartsWith("basicExcludeSource ", StringComparison.InvariantCulture));
            if (ev.description.StartsWith("prop ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !string.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
                Assert.Equal("TestSubType", ev.subtype);
                Assert.Equal("gp.efg:i=12", ev.type);
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            else if (ev.description.StartsWith("propOther ", StringComparison.InvariantCulture))
            {
                // This node is not historizing, so the first event should be lost
                Assert.NotEqual("propOther 0", ev.description);
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !String.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            else if (ev.description.StartsWith("basicPass ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(String.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
            }
            // both source1 and 2
            else if (ev.description.StartsWith("basicPassSource", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject2", false));
                if (ev.description.StartsWith("basicPassSource2 ", StringComparison.InvariantCulture))
                {
                    Assert.NotEqual("basicPassSource2 0", ev.description);
                }
            }
            else if (ev.description.StartsWith("basicVarSource ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
                Assert.True(EventSourceIs(ev, factory, "MyVariable", true));
            }
            else if (ev.description.StartsWith("mappedType ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("TypeProp"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, factory, "MyObject", false));
                Assert.Equal("MySpecialType", ev.type);
            }
            else
            {
                throw new Exception("Unknown event found");
            }
        }

        private static bool EventSourceIs(EventDummy ev, CDFMockHandler handler, string name, bool rawSource)
        {
            var asset = handler.Assets.Values.FirstOrDefault(ast => ast.name == name);
            var timeseries = handler.Timeseries.Values.FirstOrDefault(ts => ts.name == name);
            if (asset == null && timeseries == null) return false;
            return rawSource
                ? asset != null && asset.externalId == ev.metadata["SourceNode"] || timeseries != null && timeseries.externalId == ev.metadata["SourceNode"]
                : asset != null && ev.assetIds.Contains(asset.id);
        }
    }
    public enum ServerName { Basic, Full, Array, Events, Audit, Proxy }
    public enum ConfigName { Events, Influx, Test, Mqtt }

    public sealed class ExtractorTester : IDisposable
    {
        public static readonly Dictionary<ServerName, string> HostNames = new Dictionary<ServerName, string>
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
            {ConfigName.Influx, "config.influxtest.yml"},
            {ConfigName.Mqtt, "config.mqtt.yml"}
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
        public MQTTBridge Bridge { get; }
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
            Cognite.OpcUa.Logger.Configure(Config.Logging);
            Config.Source.EndpointURL = HostNames[testParams.ServerName];

            FullConfig pusherConfig = null;
            if (testParams.PusherConfig != null)
            {
                pusherConfig = ExtractorUtils.GetConfig(configNames[testParams.PusherConfig.Value]);
            }

            if (testParams.StateStorage || testParams.BufferQueue || testParams.StateInflux || testParams.MqttState)
            {
                Config.StateStorage.Location = "testbuffer.db";
            }

            if (testParams.StateStorage)
            {
                Config.StateStorage.Interval = 3;
            }

            if (testParams.BufferQueue)
            {
                Config.FailureBuffer.Enabled = true;
                Config.FailureBuffer.LocalQueue = true;
            }

            if (testParams.DataBufferPath != null)
            {
                Config.FailureBuffer.Enabled = true;
                Config.FailureBuffer.DatapointPath = testParams.DataBufferPath;
            }

            if (testParams.EventBufferPath != null)
            {
                Config.FailureBuffer.Enabled = true;
                Config.FailureBuffer.EventPath = testParams.EventBufferPath;
            }

            if (testParams.FailureInflux != null)
            {
                Config.FailureBuffer.Enabled = true;
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
                        Write = testParams.FailureInfluxWrite,
                        StateStorage = testParams.StateInflux
                    };
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
                case MQTTPusherConfig mqttPusherConfig:
                    var mqttConfig = Cognite.Bridge.Config.GetConfig("config.bridge.yml");
                    Handler = new CDFMockHandler(mqttConfig.CDF.Project, testParams.MockMode);
                    Handler.StoreDatapoints = testParams.StoreDatapoints;
                    Bridge = new MQTTBridge(new Destination(mqttConfig.CDF, CommonTestUtils.GetDummyProvider(Handler)), mqttConfig);
                    Bridge.StartBridge(CancellationToken.None).Wait();
                    for (int i = 0; i < 30; i++)
                    {
                        if (Bridge.IsConnected()) break;
                        Task.Delay(100).Wait();
                    }

                    if (!Bridge.IsConnected())
                    {
                        log.Warning("Bridge did not connect within 30 seconds");
                    }
                    if (testParams.MqttState)
                    {
                        mqttPusherConfig.LocalState = "mqtt_created_states";
                    }
                    Pusher = mqttPusherConfig.ToPusher(0, null);
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
                Log.Information("Clearing database: {db}", InfluxConfig.Database);
                await IfDbClient.DropDatabaseAsync(new InfluxDatabase(InfluxConfig.Database));
                await IfDbClient.CreateDatabaseAsync(InfluxConfig.Database);
            }

            if (Config.FailureBuffer.DatapointPath != null)
            {
                File.Create(Config.FailureBuffer.DatapointPath).Close();
            }

            if (Config.FailureBuffer.EventPath != null)
            {
                File.Create(Config.FailureBuffer.EventPath).Close();
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
            int i;
            for (i = 0; i < seconds * 5; i++)
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

            if (!triggered)
            {
                log.Error("Condition failed to appear within {sec} seconds", seconds);
            }
            log.Information("Waited for {cnt} seconds", i/5.0);
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
                await Extractor.Looper.WaitForNextPush();
                await Task.Delay(100);
                Source.Cancel();
            }
            try
            {
                await RunTask;
                if (testParams.QuitAfterMap)
                {
                    Source.Cancel();
                }
            }
            catch (Exception e)
            {
                if (testResult != null && !testResult(e)) throw;
                if (testResult == null && !CommonTestUtils.TestRunResult(e)) throw;
            }
            Extractor.Close();
            if (Bridge != null)
            {
                await Bridge.Disconnect();
            }
        }

        public void TestContinuity(string id)
        {
            var dps = Handler.Datapoints[id].NumericDatapoints;
            var intdps = dps.GroupBy(dp => dp.Timestamp).Select(dp => (int)Math.Round(dp.First().Value)).ToList();
            TestContinuity(intdps);
        }

        public static void TestContinuity(List<int> intdps)
        {
            if (intdps == null) throw new ArgumentNullException(nameof(intdps));
            intdps = intdps.Distinct().ToList();
            intdps.Sort();
            int min = intdps.Min();
            int max = intdps.Max();
            int last = min - 1;
            string msg = "";
            foreach (int dp in intdps)
            {
                if (last != dp - 1)
                {
                    msg += $"\nOut of order points at {dp}, {last}";
                }
                last = dp;
            }
            
            Assert.True(max - min == intdps.Count - 1, $"Continuity impossible, min is {min}, max is {max}, count is {intdps.Count}: {msg}");
        }
        /// <summary>
        /// Test that the points given by the id is within ms +/- 200ms of eachother.
        /// This does introduce some issues in tests when jenkins hiccups, but it is needed to test
        /// continuity on points that aren't incrementing.
        /// </summary>
        /// <param name="ms">Expected interval</param>
        /// <param name="id">Id in handler (numeric datapoints only)</param>
        /// <param name="delta">Allowed deviation. For OPC-UA above 100ms has been observed.</param>
        public void TestConstantRate(int ms, string id, int delta = 200)
        {
            var dps = Handler.Datapoints[id].NumericDatapoints;
            var tss = dps.Select(dp => dp.Timestamp).Distinct().ToList();
            tss.Sort();
            long last = 0;
            foreach (var ts in tss)
            {
                Assert.True(last == 0
                            || Math.Abs(ts - last) < ms + delta,
                    $"Expected difference to be less than {ms + delta}, but it was {ts - last}");
                last = ts;
            }
        }
        public void Dispose()
        {
            if (Bridge != null && Bridge.IsConnected())
            {
                Bridge.Disconnect().Wait();
            }
            Bridge?.Dispose();
            Source?.Cancel();
            Source?.Dispose();
            IfDbClient?.Dispose();
            Extractor?.Close();
            Extractor?.Dispose();
            Pusher?.Dispose();
        }
    }
    public class ExtractorTestParameters
    {
        public ServerName ServerName { get; set; } = ServerName.Basic;
        public ConfigName ConfigName { get; set; } = ConfigName.Test;
        public ConfigName? PusherConfig { get; set; } = null;
        public CDFMockHandler.MockMode MockMode { get; set; } = CDFMockHandler.MockMode.None;
        public string LogLevel { get; set; } = "debug";
        public bool QuitAfterMap { get; set; } = false;
        public bool StoreDatapoints { get; set; } = false;
        public int? HistoryGranularity { get; set; } = null;
        public ConfigName? FailureInflux { get; set; } = null;
        public bool FailureInfluxWrite { get; set; } = true;
        public Func<FullConfig, IPusher, UAClient, Extractor> Builder { get; set; } = null;
        public InfluxClientConfig InfluxOverride { get; set; } = null;
        public bool StateStorage { get; set; } = false;
        public bool StateInflux { get; set; } = false;
        public bool BufferQueue { get; set; } = false;
        public bool MqttState { get; set; } = false;
        public string DataBufferPath { get; set; }
        public string EventBufferPath { get; set; }
    }
}

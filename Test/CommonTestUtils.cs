/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

using AdysTech.InfluxDB.Client.Net;
using Cognite.Bridge;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using CogniteSdk;
using LiteDB;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Prometheus;
using Serilog;
using Server;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using File = System.IO.File;

[assembly: CLSCompliant(false)]
namespace Test
{
    public class ConfigInitFixture
    {
        public ConfigInitFixture()
        {
            var defaultConfig = new LoggerConfig();
            defaultConfig.Console = new LogConfig() { Level = "debug" };
            LoggingUtils.Configure(defaultConfig);
        }
    }

    [CollectionDefinition("Extractor tests")]
    public class ExtractorCollectionDefinition : ICollectionFixture<ConfigInitFixture> { }


    public class MakeConsoleWork : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly TextWriter _originalOut;
        private readonly TextWriter _textWriter;
        private bool disposed;

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
            if (disposed) return;
            if (disposing)
            {
                _output.WriteLine(_textWriter.ToString());
                _textWriter.Dispose();
                Console.SetOut(_originalOut);
            }
            disposed = true;
        }
    }
    public static class CommonTestUtils
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(CommonTestUtils));

        public static bool TestRunResult(Exception e)
        {
            if (!(e is TaskCanceledException || e is AggregateException && e.InnerException is TaskCanceledException))
            {
                return false;
            }
            return true;
        }
        public static void AddDummyProvider(CDFMockHandler handler, IServiceCollection services)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            services.AddHttpClient<Client.Builder>()
                .ConfigurePrimaryHttpMessageHandler(handler.CreateHandler);
        }

        private static Collector GetCollector(string name)
        {
            var prop = Metrics.DefaultRegistry.GetType().GetField("_collectors", BindingFlags.NonPublic | BindingFlags.Instance);
            var dict = (ConcurrentDictionary<string, Collector>)prop.GetValue(Metrics.DefaultRegistry);
            return dict.GetValueOrDefault(name);
        }

        public static double GetMetricValue(string name)
        {
            var collector = GetCollector(name);
            return collector switch
            {
                Gauge gauge => gauge.Value,
                Counter counter => counter.Value,
                _ => 0
            };
        }
        public static bool TestMetricValue(string name, double value)
        {
            var collector = GetCollector(name);
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
        public static void ResetMetricValues(params string[] names)
        {
            foreach (var name in names)
            {
                ResetMetricValue(name);
            }
        }
        public static void ResetMetricValue(string name)
        {
            var collector = GetCollector(name);
            switch (collector)
            {
                case Gauge gauge:
                    gauge.Set(0);
                    break;
                case Counter counter:
                    // See the prometheus-net source code. Since they refuse to make it possible to do anything
                    // not in the basic use case, this crazy dynamic hacking is necessary.
                    // This is not the best way to do things, and it might
                    // randomly break due to internal changes in prometheus-net.
                    // It does get the job done, however.

                    // Get the internal counter child (Counter.Child)
                    var internalChild = counter
                        .GetType()
                        .GetProperty("Unlabelled", BindingFlags.NonPublic | BindingFlags.Instance)
                        .GetValue(counter);
                    // Get the internal _value. The exposed Value property is read-only
                    var internalValue = internalChild
                        .GetType()
                        .GetField("_value", BindingFlags.NonPublic | BindingFlags.Instance)
                        .GetValue(internalChild);
                    // _value is a ThreadSafeDouble internal struct, so it cannot be modified easily
                    // for some reason modifying structs using reflection tends to just give you a new instance.
                    // We can, however, just create a new one.
                    var newSafeDouble = Activator.CreateInstance(internalValue.GetType(), 0.0);
                    internalChild.GetType()
                        .GetField("_value", BindingFlags.NonPublic | BindingFlags.Instance)
                        .SetValue(internalChild, newSafeDouble);
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
                "opcua_node_ensure_failures_cdf",
                "opcua_datapoint_pushes_cdf",
                "opcua_datapoint_push_failures_cdf",
                "opcua_frontfill_events",
                "opcua_backfill_events",
                "opcua_frontfill_data",
                "opcua_backfill_data",
                "opcua_backfill_data_count",
                "opcua_frontfill_data_count",
                "opcua_backfill_events_count",
                "opcua_frontfill_events_count",
                "opcua_datapoint_push_failures_influx",
                "opcua_event_push_failures_cdf",
                "opcua_event_push_failures_influx",
                "opcua_duplicated_events_cdf",
                "opcua_created_assets_mqtt",
                "opcua_created_timeseries_mqtt",
                "opcua_array_points_missed"
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

        public static Process GetProxyProcess(int source, int target)
        {
            return Bash($"ncat -lk {source} -c \"ncat localhost {target}\"");
        }

        public static void StopProxyProcess()
        {
            using (var process = Bash($"kill $(ps aux | grep '[n]cat' | awk '{{print $2}}')"))
            {
                process.WaitForExit();
            }
        }

        /// <summary>
        /// Test that the event contains the appropriate data for the event server test
        /// </summary>
        /// <param name="ev"></param>
        public static void TestEvent(EventDummy ev, CDFMockHandler handler)
        {
            if (ev == null) throw new ArgumentNullException(nameof(ev));
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            Assert.False(ev.description.StartsWith("basic-block ", StringComparison.InvariantCulture));
            if (ev.description.StartsWith("prop ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !string.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
                Assert.Equal("sub-type", ev.subtype);
                Assert.Equal("gp.tl:i=7", ev.type);
                Assert.True(EventSourceIs(ev, handler, "Object 1", false));
            }
            else if (ev.description.StartsWith("prop-e2 ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !string.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(EventSourceIs(ev, handler, "Object 1", false));
            }
            else if (ev.description.StartsWith("basic-pass ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, handler, "Object 1", false));
            }
            // both source1 and 2
            else if (ev.description.StartsWith("basic-pass-", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, handler, "Object 2", false));
            }
            else if (ev.description.StartsWith("basic-varsource ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyNum"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, handler, "Object 1", false));
                Assert.True(EventSourceIs(ev, handler, "Variable 1", true));
            }
            else if (ev.description.StartsWith("mapped ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("TypeProp"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.True(EventSourceIs(ev, handler, "Object 1", false));
                Assert.Equal("CustomType", ev.type);
            }
            else if (ev.description.StartsWith("basic-nosource", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.Null(ev.assetIds);
            }
            else if (ev.description.StartsWith("basic-excludeobj", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata == null || !ev.metadata.ContainsKey("PropertyString"));
                Assert.True(string.IsNullOrEmpty(ev.subtype));
                Assert.Null(ev.assetIds);
            }
            else if (ev.description.StartsWith("prop-e3 ", StringComparison.InvariantCulture))
            {
                Assert.True(ev.metadata.ContainsKey("PropertyString") && !string.IsNullOrEmpty(ev.metadata["PropertyString"]));
                Assert.False(ev.metadata.ContainsKey("PropertyNum"));
            }
            else
            {
                throw new FatalException("Unknown event found");
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
        public static void VerifyStartingConditions(
            Dictionary<string, AssetDummy> assets,
            Dictionary<string, TimeseriesDummy> timeseries,
            UpdateConfig upd,
            IUAClientAccess client,
            CustomNodeReference ids,
            bool raw)
        {
            if (assets == null) throw new ArgumentNullException(nameof(assets));
            if (timeseries == null) throw new ArgumentNullException(nameof(timeseries));
            if (upd == null) upd = new UpdateConfig();
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (ids == null) throw new ArgumentNullException(nameof(ids));
            Assert.Equal(6, assets.Count);
            Assert.Equal(16, timeseries.Count);

            var rootId = client.GetUniqueId(ids.Root);
            var obj1Id = client.GetUniqueId(ids.Obj1);
            var obj2Id = client.GetUniqueId(ids.Obj2);
            var stringyId = client.GetUniqueId(ids.StringyVar);
            var mysteryId = client.GetUniqueId(ids.MysteryVar);

            if (!upd.Objects.Name) Assert.Equal("CustomRoot", assets[rootId].name);
            if (!upd.Objects.Description) Assert.True(string.IsNullOrEmpty(assets[rootId].description));


            if (!upd.Variables.Name) Assert.Equal("StringyVar", timeseries[stringyId].name);
            if (!upd.Variables.Description) Assert.True(string.IsNullOrEmpty(timeseries[stringyId].description));

            if (raw)
            {
                if (!upd.Variables.Context) Assert.Equal(rootId, (timeseries[stringyId] as StatelessTimeseriesDummy).assetExternalId);
            }
            else
            {
                if (!upd.Variables.Context) Assert.Equal(assets[rootId].id, timeseries[stringyId].assetId);
            }

            if (!upd.Objects.Context) Assert.Equal(rootId, assets[obj2Id].parentExternalId);

            if (!upd.Objects.Metadata)
            {
                Assert.True(assets[obj1Id].metadata == null
                    || !assets[obj1Id].metadata.Any());
                Assert.Equal(2, assets[obj2Id].metadata.Count);
                Assert.Equal("1234", assets[obj2Id].metadata["NumericProp"]);
            }
            if (!upd.Variables.Metadata)
            {
                Assert.True(timeseries[stringyId].metadata == null || !timeseries[stringyId].metadata.Any());
                Assert.Equal(2, timeseries[mysteryId].metadata.Count);
                Assert.Equal("(0, 100)", timeseries[mysteryId].metadata["EURange"]);
            }
        }

        public static void VerifyModified(
            Dictionary<string, AssetDummy> assets,
            Dictionary<string, TimeseriesDummy> timeseries,
            UpdateConfig upd,
            IUAClientAccess client,
            CustomNodeReference ids,
            bool raw)
        {
            if (assets == null) throw new ArgumentNullException(nameof(assets));
            if (timeseries == null) throw new ArgumentNullException(nameof(timeseries));
            if (upd == null) upd = new UpdateConfig();
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (ids == null) throw new ArgumentNullException(nameof(ids));
            Assert.Equal(6, assets.Count);
            Assert.Equal(16, timeseries.Count);

            var rootId = client.GetUniqueId(ids.Root);
            var obj1Id = client.GetUniqueId(ids.Obj1);
            var obj2Id = client.GetUniqueId(ids.Obj2);
            var stringyId = client.GetUniqueId(ids.StringyVar);
            var mysteryId = client.GetUniqueId(ids.MysteryVar);

            if (upd.Objects.Name) Assert.Equal("CustomRoot updated", assets[rootId].name);
            if (upd.Objects.Description) Assert.Equal("custom root description", assets[rootId].description);

            if (upd.Variables.Name) Assert.Equal("StringyVar updated", timeseries[stringyId].name);
            if (upd.Variables.Description) Assert.Equal("Stringy var description", timeseries[stringyId].description);
            if (raw)
            {
                if (upd.Objects.Context) Assert.Equal(obj1Id, assets[obj2Id].parentExternalId);
                if (upd.Variables.Context) Assert.Equal(obj1Id, (timeseries[stringyId] as StatelessTimeseriesDummy).assetExternalId);
            }
            else
            {
                if (upd.Objects.Context) Assert.Equal(obj1Id, assets[obj2Id].parentExternalId);
                if (upd.Variables.Context) Assert.Equal(assets[obj1Id].id, timeseries[stringyId].assetId);
            }


            if (upd.Objects.Metadata)
            {
                Assert.Single(assets[obj1Id].metadata);
                Assert.Equal("New asset prop value", assets[obj1Id].metadata["NewAssetProp"]);
                Assert.Equal(3, assets[obj2Id].metadata.Count);
                Assert.Equal("4321", assets[obj2Id].metadata["NumericProp"]);
                Assert.True(assets[obj2Id].metadata.ContainsKey("StringProp"));
                Assert.True(assets[obj2Id].metadata.ContainsKey("StringProp updated"));
            }
            if (upd.Variables.Metadata)
            {
                Assert.Single(timeseries[stringyId].metadata);
                Assert.Equal("New prop value", timeseries[stringyId].metadata["NewProp"]);
                Assert.Equal(3, timeseries[mysteryId].metadata.Count);
                Assert.Equal("(0, 200)", timeseries[mysteryId].metadata["EURange"]);
            }
        }
        public static async Task WaitForCondition(Func<Task<bool>> condition, int seconds, Func<string> assertion)
        {
            if (condition == null) throw new ArgumentNullException(nameof(condition));
            if (assertion == null) throw new ArgumentNullException(nameof(assertion));
            bool triggered = false;
            int i;
            for (i = 0; i < seconds * 5; i++)
            {
                if (await condition())
                {
                    triggered = true;
                    break;
                }

                await Task.Delay(200);
            }

            if (!triggered)
            {
                log.Error("Condition failed to appear within {sec} seconds", seconds);
            }
            log.Information("Waited for {cnt} seconds", i / 5.0);
            Assert.True(triggered, assertion());
        }
        public static async Task WaitForCondition(Func<bool> condition, int seconds,
            string assertion = "Expected condition to trigger")
        {
            await WaitForCondition(() => Task.FromResult(condition()), seconds, () => assertion);
        }
        public static async Task WaitForCondition(Func<bool> condition, int seconds,
            Func<string> assertion)
        {
            await WaitForCondition(() => Task.FromResult(condition()), seconds, assertion);
        }
        public static async Task WaitForCondition(Func<Task<bool>> condition, int seconds,
            string assertion = "Expected condition to trigger")
        {
            await WaitForCondition(condition, seconds, () => assertion);
        }
        public static ProtoNodeId ToProtoNodeId(this NodeId id, UAClient client)
        {
            if (id == null || id.IsNullNodeId || client == null) return null;
            var buffer = new StringBuilder();
            NodeId.Format(buffer, id.Identifier, id.IdType, 0);
            var ns = client.NamespaceTable.GetString(id.NamespaceIndex);
            return new ProtoNodeId
            {
                NodeId = buffer.ToString(),
                NamespaceUri = ns
            };
        }
        public static UAVariable GetSimpleVariable(string name, UADataType dt, int dim = 0, NodeId id = null)
        {
            var variable = new UAVariable(id ?? new NodeId(name), name, NodeId.Null);
            variable.VariableAttributes.DataType = dt;
            if (dim > 0)
            {
                variable.VariableAttributes.ArrayDimensions = new Collection<int> { dim };
            }
            return variable;
        }
    }
    public enum ServerName { Basic, Full, Array, Events, Audit, Proxy, Wrong }
    public enum ConfigName { Events, Test }

    public sealed class ExtractorTester : IDisposable
    {
        public static readonly Dictionary<ServerName, PredefinedSetup> SetupMap = new Dictionary<ServerName, PredefinedSetup>
        {
            {ServerName.Basic, PredefinedSetup.Base},
            {ServerName.Full, PredefinedSetup.Full},
            {ServerName.Array, PredefinedSetup.Custom},
            {ServerName.Events, PredefinedSetup.Events},
            {ServerName.Audit, PredefinedSetup.Auditing},
            {ServerName.Proxy, PredefinedSetup.Base},
            {ServerName.Wrong, PredefinedSetup.Wrong}
        };

        private static readonly Dictionary<ConfigName, string> configNames = new Dictionary<ConfigName, string>
        {
            {ConfigName.Test, "config.test.yml"},
            {ConfigName.Events, "config.events.yml"}
        };

        public static readonly string HostName = "opc.tcp://localhost:62546";

        public ServerController Server { get; private set; }
        public FullConfig Config { get; }
        public BridgeConfig BridgeConfig { get; }
        public CDFMockHandler Handler { get; }
        public UAExtractor Extractor { get; private set; }
        public UAClient UAClient { get; }
        public IPusher Pusher { get; }
        public CancellationTokenSource Source { get; }
        public InfluxDBClient IfDbClient { get; }
        private readonly bool influx;
        public MQTTBridge Bridge { get; }
        public Task RunTask { get; private set; }
        private readonly ExtractorTestParameters testParams;
        private readonly ILogger log = Log.Logger.ForContext(typeof(ExtractorTester));
        private IServiceProvider provider;
        public Collection<IPusher> Pushers { get; } = new Collection<IPusher>();

        public ExtractorTester(ExtractorTestParameters testParams)
        {
            this.testParams = testParams ?? throw new ArgumentNullException(nameof(testParams));

            var services = new ServiceCollection();

            Config = services.AddConfig<FullConfig>(configNames[testParams.ConfigName]);
            services.AddLogger();
            services.AddMetrics();


            if (testParams.HistoryGranularity != null)
            {
                Config.History.Granularity = testParams.HistoryGranularity.Value;
            }
            Config.Source.EndpointUrl = testParams.ServerName == ServerName.Proxy ? "opc.tcp://localhost:4839" : "opc.tcp://localhost:62546";

            if (testParams.StateStorage || testParams.StateInflux || testParams.MqttState)
            {
                Config.StateStorage.Location = "testbuffer.db";
                Config.StateStorage.Database = StateStoreConfig.StorageType.LiteDb;
            }

            if (testParams.StateStorage)
            {
                Config.StateStorage.Interval = 3;
            }

            services.AddStateStore();

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

            if (testParams.FailureInflux)
            {
                Config.FailureBuffer.Enabled = true;
                Config.FailureBuffer.Influx = true;
                Config.FailureBuffer.InfluxStateStore = testParams.StateInflux;
            }

            if (testParams.References)
            {
                Config.Extraction.Relationships.Enabled = true;
            }


            switch (testParams.Pusher)
            {
                case "cdf":
                    Handler = new CDFMockHandler(Config.Cognite.Project, testParams.MockMode) { StoreDatapoints = testParams.StoreDatapoints };
                    CommonTestUtils.AddDummyProvider(Handler, services);
                    services.AddCogniteClient("OPC-UA Extractor", true, true, false);
                    provider = services.BuildServiceProvider();
                    Pusher = Config.Cognite.ToPusher(provider);
                    break;
                case "influx":
                    Pusher = Config.Influx.ToPusher(null);
                    influx = true;
                    IfDbClient = new InfluxDBClient(Config.Influx.Host, Config.Influx.Username, Config.Influx.Password);
                    break;
                case "mqtt":
                    var mqttConfig = ConfigurationUtils.Read<BridgeConfig>("config.bridge.yml");
                    mqttConfig.GenerateDefaults();
                    Handler = new CDFMockHandler(mqttConfig.Cognite.Project, testParams.MockMode) { StoreDatapoints = testParams.StoreDatapoints };
                    CommonTestUtils.AddDummyProvider(Handler, services);
                    services.AddSingleton(mqttConfig.Cognite);
                    services.AddCogniteClient("MQTT-CDF Bridge", true, true, false);
                    provider = services.BuildServiceProvider();
                    Bridge = new MQTTBridge(new Destination(mqttConfig.Cognite, provider), mqttConfig);
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
                        Config.Mqtt.LocalState = "mqtt_created_states";
                    }
                    Pusher = Config.Mqtt.ToPusher(null);
                    BridgeConfig = mqttConfig;
                    break;
            }

            if (Pusher != null)
            {
                Pushers.Add(Pusher);
            }

            if (testParams.FailureInflux)
            {
                influx = true;
                IfDbClient = new InfluxDBClient(Config.Influx.Host, Config.Influx.Username, Config.Influx.Password);
                Pushers.Add(Config.Influx.ToPusher(null));
            }

            if (provider == null)
            {
                provider = services.BuildServiceProvider();
            }

            if (!influx && testParams.InfluxOverride)
            {
                influx = true;
                IfDbClient = new InfluxDBClient(Config.Influx.Host, Config.Influx.Username, Config.Influx.Password);
            }

            UAClient = new UAClient(Config);
            Source = new CancellationTokenSource();
            if (testParams.Builder != null)
            {
                Extractor = testParams.Builder(Config, Pushers, UAClient, Source);
            }
            else
            {
                Extractor = new UAExtractor(Config, Pushers, UAClient, provider.GetService<IExtractionStateStore>(), Source.Token);
            }

            Server = new ServerController(new[] { SetupMap[testParams.ServerName] });
        }

        public Task<IEnumerable<UADataPoint>> GetAllInfluxPoints(NodeId node, bool isString = false, int index = -1)
        {
            var dummy = new InfluxBufferState(Extractor.State.GetNodeState(node));
            dummy.SetComplete();
            dummy.Type = isString ? InfluxBufferType.StringType : InfluxBufferType.DoubleType;
            return ((InfluxPusher)Pusher).ReadDataPoints(
                new Dictionary<string, InfluxBufferState> { { UAClient.GetUniqueId(node, index), dummy } },
                CancellationToken.None);
        }

        public Task<IEnumerable<UAEvent>> GetAllInfluxEvents(NodeId emitter)
        {
            var dummy = new InfluxBufferState(Extractor.State.GetEmitterState(emitter));
            dummy.SetComplete();
            dummy.Type = InfluxBufferType.EventType;
            return ((InfluxPusher)Pusher).ReadEvents(
                new Dictionary<string, InfluxBufferState> { { dummy.Id, dummy } },
                CancellationToken.None);
        }

        public void ReInitExtractor()
        {
            Extractor?.Dispose();
            if (testParams.Builder != null)
            {
                Extractor = testParams.Builder(Config, Pushers, UAClient, Source);
            }
            else
            {
                Extractor = new UAExtractor(Config, Pushers, UAClient, provider.GetService<IExtractionStateStore>(), Source.Token);
            }
        }

        public ProtoNodeId IdToProto(NodeId id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            return new ProtoNodeId
            {
                NamespaceUri = Server.Server.GetNamespace(id.NamespaceIndex),
                NodeId = id.ToString().Substring($"ns={id.NamespaceIndex};".Length)
            };
        }

        public ProtoDataType IdToProtoDataType(NodeId id, bool step = false)
        {
            return new ProtoDataType
            {
                IsStep = step,
                NodeId = IdToProto(id)
            };
        }

        public async Task ClearPersistentData()
        {
            CommonTestUtils.ResetTestMetrics();
            if (influx)
            {
                log.Information("Clearing database: {db}", Config.Influx.Database);
                await IfDbClient.DropDatabaseAsync(new InfluxDatabase(Config.Influx.Database));
                await IfDbClient.CreateDatabaseAsync(Config.Influx.Database);
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
                var db = (Extractor.StateStorage as LiteDBStateStore).GetDatabase();
                var cols = db.GetCollectionNames();
                foreach (var col in cols)
                {
                    db.DropCollection(col);
                }
            }
        }

        public async Task StartServer()
        {
            if (Server == null)
            {
                Server = new ServerController(new[] { SetupMap[testParams.ServerName] });
            }
            await Server.Start();
        }
        public void StartExtractor()
        {
            RunTask = Extractor.RunExtractor(testParams.QuitAfterMap);
        }
        public async Task WaitForCondition(Func<Task<bool>> condition, int seconds, Func<string> assertion)
        {
            if (condition == null) throw new ArgumentNullException(nameof(condition));
            if (assertion == null) throw new ArgumentNullException(nameof(assertion));
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
            log.Information("Waited for {cnt} seconds", i / 5.0);
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

        public async Task TerminateRunTask(bool waitForPush, Func<Exception, bool> testResult = null)
        {
            if (RunTask == null) throw new FatalException("Run task is not started");

            if (!testParams.QuitAfterMap)
            {
                if (waitForPush)
                {
                    await Extractor.Looper.WaitForNextPush();
                }
                Extractor.Close();
            }
            try
            {
                await RunTask;
                if (testParams.QuitAfterMap)
                {
                    Extractor.Close();
                }
            }
            catch (Exception e)
            {
                if (testResult != null && !testResult(e)) throw;
                if (testResult == null && !CommonTestUtils.TestRunResult(e)) throw;
            }
            if (Bridge != null)
            {
                await Bridge.Disconnect();
            }
        }

        public void CloseServer()
        {
            Server?.Stop();
            Server?.Dispose();
            Server = null;
        }

        public void TestContinuity(string id)
        {
            var dps = Handler.Datapoints[id].NumericDatapoints;
            var intdps = dps.GroupBy(dp => dp.Timestamp).Select(dp => (int)Math.Round(dp.First().Value)).ToList();
            TestContinuity(intdps);
        }

        public static void TestContinuity(IEnumerable<int> intdps)
        {
            if (intdps == null) throw new ArgumentNullException(nameof(intdps));
            intdps = intdps.Distinct().OrderBy(dp => dp).ToList();
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

            Assert.True(max - min == intdps.Count() - 1, $"Continuity impossible, min is {min}, max is {max}, count is {intdps.Count()}: {msg}");
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
            // Disposing singletons is usually bad, but we really have no choice.
            Extractor?.StateStorage?.Dispose();
            if (Extractor != null)
            {
                IEnumerable<IPusher> pushers = Extractor.GetType().GetField("pushers", BindingFlags.NonPublic | BindingFlags.Instance)
                    .GetValue(Extractor) as IEnumerable<IPusher>;
                if (pushers != null)
                {
                    foreach (var pusher in pushers)
                    {
                        pusher.Dispose();
                    }
                }
            }
            Extractor?.Close();
            Extractor?.Dispose();
            Server?.Dispose();
        }
    }
    public class ExtractorTestParameters
    {
        public ServerName ServerName { get; set; } = ServerName.Basic;
        public ConfigName ConfigName { get; set; } = ConfigName.Test;
        public string Pusher { get; set; } = "cdf";
        public CDFMockHandler.MockMode MockMode { get; set; } = CDFMockHandler.MockMode.None;
        public bool QuitAfterMap { get; set; }
        public bool StoreDatapoints { get; set; }
        public int? HistoryGranularity { get; set; }
        public bool FailureInflux { get; set; }
        public bool InfluxOverride { get; set; }
        public Func<FullConfig, IEnumerable<IPusher>, UAClient, CancellationTokenSource, UAExtractor> Builder { get; set; }
        public bool StateStorage { get; set; }
        public bool StateInflux { get; set; }
        public bool MqttState { get; set; }
        public string DataBufferPath { get; set; }
        public string EventBufferPath { get; set; }
        public bool References { get; set; }
    }
}

using Cognite.OpcUa.Pushers.Writers;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Server;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Linq;
using Opc.Ua.Client;
using Cognite.OpcUa.Subscriptions;
using Cognite.Extractor.Common;
using System.Runtime.ExceptionServices;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;

namespace Test.Utils
{
    public abstract class BaseExtractorTestFixture : LoggingTestFixture, IAsyncLifetime, IDisposable
    {
        private bool disposedValue;

        public int Port { get; }
        public NodeIdReference Ids => Server.Ids;
        public UAClient Client { get; private set; }
        public FullConfig Config { get; }
        public ServerController Server { get; private set; }
        public CancellationTokenSource Source { get; protected set; }
        public ServiceProvider Provider { get; protected set; }
        protected ServiceCollection Services { get; }
        protected PredefinedSetup[] Setups { get; }
        public DummyClientCallbacks Callbacks { get; private set; }
        public ILogger Log { get; }
        public DummyIntegrationSink TaskSink { get; }
        protected BaseExtractorTestFixture(PredefinedSetup[] setups = null)
        {
            Port = CommonTestUtils.NextPort;
            // Set higher min thread count, this is required due to running both server and client in the same process.
            // The server uses the threadPool in a weird way that can cause starvation if this is set too low.
            ThreadPool.SetMinThreads(20, 20);
            try
            {
                ConfigurationUtils.AddTypeConverter(new FieldFilterConverter());
            }
            catch { }
            Services = new ServiceCollection();
            Config = Services.AddConfig<FullConfig>("config.test.yml", new[] {
                    typeof(BaseCogniteConfig),
                    typeof(LoggerConfig),
                    typeof(HighAvailabilityConfig),
                    typeof(MetricsConfig),
                    typeof(StateStoreConfig),
                }, 1);
            Services.AddTransient<ExtractorTaskScheduler>();
            Config.Source.EndpointUrl = $"opc.tcp://localhost:{Port}";
            TaskSink = new DummyIntegrationSink();
            Services.AddSingleton(new ConnectionConfig
            {
                Project = "test",
            });
            Configure(Services);
            Services.AddSingleton<IIntegrationSink>(TaskSink);
            Provider = Services.BuildServiceProvider();

            Log = Provider.GetRequiredService<ILogger<BaseExtractorTestFixture>>();

            setups ??= new[] {
                    PredefinedSetup.Custom, PredefinedSetup.Base, PredefinedSetup.Events,
                    PredefinedSetup.Wrong, PredefinedSetup.Full, PredefinedSetup.Auditing };
            Setups = setups;
        }

        private async Task Start()
        {
            Server = new ServerController(Setups, Provider, Port);
            await Server.Start();
            Source = new CancellationTokenSource();
            Callbacks = new DummyClientCallbacks(Source.Token);

            for (int i = 0; i < 10; i++)
            {
                Client = new UAClient(Provider, Config)
                {
                    Callbacks = Callbacks
                };
                try
                {
                    await Client.Run(Source.Token, 0);
                    return;
                }
                catch (Exception ex)
                {
                    Log.LogError(ex, "Failed to start OPC-UA client");
                    await Task.Delay(1000);
                    if (i >= 9) throw;
                    continue;
                }
            }

        }

        public static void ResetType(object obj, object reference)
        {
            if (obj == null) return;
            var type = obj.GetType();
            foreach (var prop in type.GetProperties())
            {
                bool hasSet = prop.SetMethod != null;
                if (prop.PropertyType.Namespace.StartsWith("Cognite", StringComparison.InvariantCulture))
                {
                    var current = prop.GetValue(obj);
                    var old = prop.GetValue(reference);

                    if (prop.PropertyType.IsValueType)
                    {
                        if (!hasSet) continue;
                        prop.SetValue(obj, old);
                    }
                    else if (current is null && old is not null || current is not null && old is null)
                    {
                        if (!hasSet) continue;
                        prop.SetValue(obj, old);
                    }
                    else if (current is null && old is null) continue;
                    else
                    {
                        ResetType(current, old);
                    }
                }
                else
                {
                    if (!hasSet) continue;
                    var old = prop.GetValue(reference);
                    prop.SetValue(obj, old);
                }
            }
        }

        public void ResetConfig()
        {
            var raw = ConfigurationUtils.Read<FullConfig>("config.test.yml");
            raw.GenerateDefaults();
            ResetType(Config, raw);
            Config.Source.EndpointUrl = $"opc.tcp://localhost:{Port}";
            Config.GenerateDefaults();
        }
        public UAExtractor BuildExtractor(IPusher pusher = null, bool clear = true, IExtractionStateStore stateStore = null, UAClient client = null)
        {
            if (clear)
            {
                RemoveSubscription(null, SubscriptionName.Events).Wait();
                RemoveSubscription(null, SubscriptionName.DataPoints).Wait();
                RemoveSubscription(null, SubscriptionName.Audit).Wait();
                RemoveSubscription(null, SubscriptionName.RebrowseTriggers).Wait();
                Client.Browser.Transformations = null;
            }
#pragma warning disable CA2000 // Dispose objects before losing scope. No idea why C# doesn't understand this.
            pusher ??= new DummyPusher(new DummyPusherConfig());
#pragma warning restore CA2000 // Dispose objects before losing scope
            var configWrapper = new ConfigWrapper<FullConfig>(Config, null);
            var taskScheduler = Provider.GetRequiredService<ExtractorTaskScheduler>();
            var sink = Provider.GetRequiredService<IIntegrationSink>();
            var ext = new UAExtractor(configWrapper, Provider, taskScheduler, pusher, client ?? Client, sink, stateStore);
            ext.CloseClientOnClose = false;

            return ext;
        }

        /// <summary>
        /// Used for running the extractor without calling Start(token) and getting locked
        /// into waiting for cancellation.
        /// </summary>
        /// <param name="quitAfterMap">False to wait for cancellation</param>
        /// <param name="startTimeout">Timeout in milliseconds to wait for start.
        /// <returns></returns>
        public async Task RunExtractor(UAExtractor extractor, bool quitAfterMap = false, int startTimeout = -1)
        {
            var startTask = extractor.Start(Source.Token);
            if (!quitAfterMap)
            {
                await startTask;
                return;
            }


            var timeout = startTimeout >= 0 ? TimeSpan.FromMilliseconds(startTimeout) : Timeout.InfiniteTimeSpan;
            var res = await Task.WhenAny(CommonUtils.WaitAsync(extractor.OnInit, timeout, Source.Token), startTask);
            if (res.IsFaulted)
            {
                ExceptionDispatchInfo.Capture(res.Exception!).Throw();
            }
            // Now we're initialized, and can wait for browse.
            res = await Task.WhenAny(extractor.WaitForBrowseCompletion(timeout), startTask);
            if (res.IsFaulted)
            {
                ExceptionDispatchInfo.Capture(res.Exception!).Throw();
            }

            // We're done with browsing, and we can return to the test.
            // This lets tests easily wait for initialization and browsing to complete.
        }

        public (CDFMockHandler, CDFPusher) GetCDFPusher()
        {
            var newServices = new ServiceCollection();
            foreach (var service in Services)
            {
                newServices.Add(service);
            }
            CommonTestUtils.AddDummyProvider("test", CDFMockHandler.MockMode.None, true, newServices);
            DestinationUtilsUnstable.AddCogniteClient(newServices, "appid", null, true, true, false);
            DestinationUtilsUnstable.AddCogniteDestination(newServices);
            newServices.AddWriters(Config);
            var provider = newServices.BuildServiceProvider();
            var client = provider.GetRequiredService<CogniteSdk.Client>();
            var logger = provider.GetRequiredService<ILogger<CogniteDestinationWithIDM>>();
            var config = provider.GetRequiredService<BaseCogniteConfig>();
            var connection = provider.GetRequiredService<ConnectionConfig>();
            var destination = provider.GetRequiredService<CogniteDestinationWithIDM>();
            var pusher = new CDFPusher(
                Provider.GetRequiredService<ILogger<CDFPusher>>(),
                Config,
                Config.Cognite,
                destination,
                provider.GetRequiredService<ConnectionConfig>(),
                provider);
            var handler = provider.GetRequiredService<CDFMockHandler>();
            return (handler, pusher);
        }

        public void DeleteFiles(string prefix)
        {
            try
            {
                var files = Directory.GetFiles(".");
                foreach (var file in files)
                {
                    var fileName = Path.GetFileName(file);
                    if (fileName.StartsWith(prefix, StringComparison.InvariantCulture)
                        && (fileName.EndsWith(".bin", StringComparison.InvariantCulture)
                        || fileName.EndsWith(".db", StringComparison.InvariantCulture)))
                    {
                        File.Delete(file);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.LogError("Failed to clear files: {Message}", ex.Message);
            }
        }

        public static async Task TerminateRunTask(Task runTask, UAExtractor extractor)
        {
            ArgumentNullException.ThrowIfNull(extractor);
            await extractor.Close();
            try
            {
                await runTask;
            }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
        }
        public void ResetCustomServerValues()
        {
            var ids = Server.Ids.Custom;
            Server.UpdateNode(ids.StringyVar, "value");
            Server.UpdateNode(ids.StringArray, new[] { "test1", "test2" });
            Server.UpdateNode(ids.MysteryVar, 0);
            Server.UpdateNode(ids.Array, new[] { 0, 0, 0, 0 });
            Server.UpdateNode(ids.EnumVar1, 1);
            Server.UpdateNode(ids.IgnoreVar, 0);
            Server.UpdateNode(ids.EnumVar2, 123);
            Server.UpdateNode(ids.EnumVar3, new[] { 123, 123, 321, 123 });
        }
        public void WipeCustomHistory()
        {
            var ids = Server.Ids.Custom;
            Server.WipeHistory(ids.StringyVar, null);
            Server.WipeHistory(ids.MysteryVar, 0);
            Server.WipeHistory(ids.Array, new[] { 0, 0, 0, 0 });
        }
        public void WipeBaseHistory()
        {
            var ids = Server.Ids.Base;
            Server.WipeHistory(ids.DoubleVar1, 0.0);
            Server.WipeHistory(ids.StringVar, null);
            Server.WipeHistory(ids.IntVar, 0);
        }
        public void WipeEventHistory()
        {
            Server.WipeEventHistory(Server.Ids.Event.Obj1);
            Server.WipeEventHistory(ObjectIds.Server);
        }

        public virtual async Task InitializeAsync()
        {
            var startTask = Start();
            var resultTask = await Task.WhenAny(startTask, Task.Delay(30000));
            Assert.Equal(startTask, resultTask);
            if (startTask.Exception != null)
            {
                throw startTask.Exception;
            }
        }

        public virtual async Task DisposeAsync()
        {
            if (Source != null)
            {
                await Source.CancelAsync();
                Source.Dispose();
                Source = null;
            }
            if (Client != null)
            {
                await Client.DisposeAsync();
                Client = null;
            }
            Server?.Dispose();
            if (Provider != null)
            {
                await Provider.DisposeAsync();
                Provider = null;
            }
        }

        public async Task RemoveSubscription(UAExtractor extractor, SubscriptionName name)
        {
            if (TryGetSubscription(name, out var subscription) && subscription!.Created)
            {
                try
                {
                    await Client.SessionManager.Session!.RemoveSubscriptionAsync(subscription);
                    extractor?.RemoveKnownSubscription(name);
                }
                catch
                {
                    // A failure to delete the subscription generally means it just doesn't exist.
                }
                finally
                {
                    subscription!.Dispose();
                }
            }
        }

        public bool TryGetSubscription(SubscriptionName name, out Subscription subscription)
        {
            subscription = Client.SessionManager.Session?.Subscriptions?.FirstOrDefault(sub =>
                sub.DisplayName.StartsWith(name.Name(), StringComparison.InvariantCulture));
            return subscription != null;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Source?.Cancel();
                    Source?.Dispose();
                }

                disposedValue = true;
            }
        }


        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

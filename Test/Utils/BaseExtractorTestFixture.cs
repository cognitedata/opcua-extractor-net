using AdysTech.InfluxDB.Client.Net;
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
            Config = Services.AddConfig<FullConfig>("config.test.yml", 1);
            Config.Source.EndpointUrl = $"opc.tcp://localhost:{Port}";
            Configure(Services);
            Provider = Services.BuildServiceProvider();

            Log = Provider.GetRequiredService<ILogger<BaseExtractorTestFixture>>();

            if (setups == null)
            {
                setups = new[] {
                    PredefinedSetup.Custom, PredefinedSetup.Base, PredefinedSetup.Events,
                    PredefinedSetup.Wrong, PredefinedSetup.Full, PredefinedSetup.Auditing };
            }
            Setups = setups;
        }

        private async Task Start()
        {
            Server = new ServerController(Setups, Provider, Port);
            await Server.Start();

            Client = new UAClient(Provider, Config);
            Source = new CancellationTokenSource();
            Callbacks = new DummyClientCallbacks(Source.Token);
            Client.Callbacks = Callbacks;
            await Client.Run(Source.Token, 0);
        }

        private static void ResetType(object obj, object reference)
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
        public UAExtractor BuildExtractor(bool clear = true, IExtractionStateStore stateStore = null, params IPusher[] pushers)
        {
            if (clear)
            {
                RemoveSubscription(SubscriptionName.Events).Wait();
                RemoveSubscription(SubscriptionName.DataPoints).Wait();
                RemoveSubscription(SubscriptionName.Audit).Wait();
                RemoveSubscription(SubscriptionName.RebrowseTriggers).Wait();
                Client.Browser.Transformations = null;
            }
            var ext = new UAExtractor(Config, Provider, pushers, Client, stateStore);
            ext.InitExternal(Source.Token);

            return ext;
        }

        public (InfluxPusher pusher, InfluxDBClient client) GetInfluxPusher(string dbName, bool clear = true)
        {
            if (Config.Influx == null)
            {
                Config.Influx = new InfluxPusherConfig();
            }
            Config.Influx.Database = dbName;
            Config.Influx.Host ??= "http://localhost:8086";

            var client = new InfluxDBClient(Config.Influx.Host, Config.Influx.Username, Config.Influx.Password);
            if (clear)
            {
                ClearLiteDB(client).Wait();
            }
            var pusher = new InfluxPusher(Provider.GetRequiredService<ILogger<InfluxPusher>>(), Config);
            return (pusher, client);
        }

        public async Task ClearLiteDB(InfluxDBClient client)
        {
            if (client == null) return;
            try
            {
                await client.DropDatabaseAsync(new InfluxDatabase(Config.Influx.Database));
            }
            catch
            {
                Log.LogError("Failed to drop database: {DB}", Config.Influx.Database);
            }
            await client.CreateDatabaseAsync(Config.Influx.Database);
        }

        public (CDFMockHandler, CDFPusher) GetCDFPusher()
        {
            var newServices = new ServiceCollection();
            foreach (var service in Services)
            {
                newServices.Add(service);
            }
            CommonTestUtils.AddDummyProvider("test", CDFMockHandler.MockMode.None, true, newServices);
            newServices.AddCogniteClient("appid", null, true, true, false);
            newServices.AddWriters(Config);
            var provider = newServices.BuildServiceProvider();
            var destination = provider.GetRequiredService<CogniteDestination>();
            var pusher = new CDFPusher(Provider.GetRequiredService<ILogger<CDFPusher>>(),
                Config, Config.Cognite, destination, provider);
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
            await extractor.Close(false);
            try
            {
                await runTask;
            }
            catch (TaskCanceledException) { }
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
            var resultTask = await Task.WhenAny(startTask, Task.Delay(20000));
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
                await Client.Close(CancellationToken.None);
                Client.Dispose();
                Client = null;
            }
            Server?.Dispose();
            if (Provider != null)
            {
                await Provider.DisposeAsync();
                Provider = null;
            }
        }

        public async Task RemoveSubscription(SubscriptionName name)
        {
            if (TryGetSubscription(name, out var subscription) && subscription!.Created)
            {
                try
                {
                    await Client.SessionManager.Session!.RemoveSubscriptionAsync(subscription);
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

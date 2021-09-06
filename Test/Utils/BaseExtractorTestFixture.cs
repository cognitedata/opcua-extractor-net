using AdysTech.InfluxDB.Client.Net;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Pushers;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using Server;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Test.Utils
{
    public abstract class BaseExtractorTestFixture : IDisposable
    {
        public int Port { get; }
        public NodeIdReference Ids => Server.Ids;
        public UAClient Client { get; }
        public FullConfig Config { get; }
        public ServerController Server { get; }
        public CancellationTokenSource Source { get; protected set; }
        public IServiceProvider Provider { get; protected set; }
        protected ServiceCollection Services { get; }
        protected BaseExtractorTestFixture()
        {
            Port = CommonTestUtils.NextPort;
            // Set higher min thread count, this is required due to running both server and client in the same process.
            // The server uses the threadPool in a weird way that can cause starvation if this is set too low.
            ThreadPool.SetMinThreads(20, 20);
            Services = new ServiceCollection();
            Config = Services.AddConfig<FullConfig>("config.test.yml", 1);
            Console.WriteLine($"Add logger: {Config.Logger}");
            Config.Source.EndpointUrl = $"opc.tcp://localhost:{Port}";
            Services.AddLogger();
            LoggingUtils.Configure(Config.Logger);
            Provider = Services.BuildServiceProvider();

            Server = new ServerController(new[] {
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, Port);
            Server.Start().Wait();

            Client = new UAClient(Config);
            Source = new CancellationTokenSource();
            Client.Run(Source.Token).Wait();
        }
        private void ResetType(object obj, object reference)
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
                    else if (current is null && !(old is null) || !(current is null) && old is null)
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
                Client.ClearNodeOverrides();
                Client.ClearEventFields();
                Client.Browser.ResetVisitedNodes();
                Client.DataTypeManager.Reset();
                Client.RemoveSubscription("EventListener");
                Client.RemoveSubscription("DataChangeListener");
                Client.RemoveSubscription("AuditListener");
                Client.Browser.IgnoreFilters = null;
            }
            return new UAExtractor(Config, pushers, Client, stateStore, Source.Token);
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
            var pusher = Config.Influx.ToPusher(null) as InfluxPusher;
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
                Console.WriteLine("Failed to drop database: " + Config.Influx.Database);
            }
            await client.CreateDatabaseAsync(Config.Influx.Database);
        }

        public (CDFMockHandler, CDFPusher) GetCDFPusher()
        {
            var handler = new CDFMockHandler("test", CDFMockHandler.MockMode.None);
            handler.StoreDatapoints = true;
            CommonTestUtils.AddDummyProvider(handler, Services);
            Services.AddCogniteClient("appid", null, true, true, false);
            var provider = Services.BuildServiceProvider();
            var pusher = Config.Cognite.ToPusher(provider) as CDFPusher;
            return (handler, pusher);
        }

        public static void DeleteFiles(string prefix)
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
                Console.WriteLine($"Failed to clear files: {ex.Message}");
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Source.Cancel();
                Source.Dispose();
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public static async Task TerminateRunTask(Task runTask, UAExtractor extractor)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            extractor.Close(false);
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
    }
}

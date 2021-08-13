using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Cognite.OpcUa.Pushers;
using Microsoft.Extensions.DependencyInjection;
using Server;
using System;
using System.Collections;
using System.Collections.Generic;
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

                    if (prop.PropertyType.IsValueType && !current.Equals(old))
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
                Client.ResetVisitedNodes();
                Client.DataTypeManager.Reset();
                Client.RemoveSubscription("EventListener");
                Client.RemoveSubscription("DataChangeListener");
                Client.RemoveSubscription("AuditListener");
                Client.IgnoreFilters = null;
            }
            return new UAExtractor(Config, pushers, Client, stateStore, Source.Token);
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
    }
}

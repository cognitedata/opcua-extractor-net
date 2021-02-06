using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Microsoft.Extensions.DependencyInjection;
using Server;
using System;
using System.Threading;

namespace Test.Utils
{
    public abstract class BaseExtractorTestFixture : IDisposable
    {
        public UAClient Client { get; }
        public FullConfig Config { get; }
        public ServerController Server { get; }
        public CancellationTokenSource Source { get; protected set; }
        public IServiceProvider Provider { get; protected set; }
        protected ServiceCollection Services { get; }
        protected BaseExtractorTestFixture(int port)
        {
            Services = new ServiceCollection();
            Config = Services.AddConfig<FullConfig>("config.test.yml", 1);
            Console.WriteLine($"Add logger: {Config.Logger}");
            Config.Source.EndpointUrl = $"opc.tcp://localhost:{port}";
            Services.AddLogger();
            LoggingUtils.Configure(Config.Logger);
            Provider = Services.BuildServiceProvider();

            Server = new ServerController(new[] {
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, port);
            Server.Start().Wait();

            Client = new UAClient(Config);
            Source = new CancellationTokenSource();
            Client.Run(Source.Token).Wait();
        }

        public UAExtractor BuildExtractor(bool clear = true, IExtractionStateStore stateStore = null, params IPusher[] pushers)
        {
            if (clear)
            {
                Client.ClearNodeOverrides();
                Client.ClearEventFields();
                Client.ResetVisitedNodes();
                Client.DataTypeManager.Reset();
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
    }
}

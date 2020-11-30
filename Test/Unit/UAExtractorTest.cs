using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Microsoft.Extensions.DependencyInjection;
using Server;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{

    public sealed class ExtractorTestFixture : IDisposable
    {
        public UAClient Client { get; }
        public FullConfig Config { get; }
        public ServerController Server { get; }
        public CancellationTokenSource Source { get; }
        public IServiceProvider Provider { get; }
        public ExtractorTestFixture()
        {
            Server = new ServerController(new[] {
                PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Auditing,
                PredefinedSetup.Custom, PredefinedSetup.Events, PredefinedSetup.Wrong }, 62100);
            Server.Start().Wait();

            var services = new ServiceCollection();
            Config = services.AddConfig<FullConfig>("config.test.yml", 1);
            Config.Source.EndpointUrl = $"opc.tcp://localhost:62100";
            services.AddLogging();
            LoggingUtils.Configure(Config.Logger);
            Provider = services.BuildServiceProvider();

            Client = new UAClient(Config);
            Source = new CancellationTokenSource();
            Client.Run(Source.Token).Wait();
        }

        public void Dispose()
        {
            Source.Cancel();
            Source.Dispose();
        }

        public UAExtractor BuildExtractor(IExtractionStateStore stateStore = null, params IPusher[] pushers)
        {
            return new UAExtractor(Config, pushers, Client, stateStore, Source.Token);
        }
    }
    public class UAExtractorTest : MakeConsoleWork, IClassFixture<ExtractorTestFixture>
    {
        private ExtractorTestFixture tester;
        public UAExtractorTest(ITestOutputHelper output, ExtractorTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        [Fact]
        public async Task TestClientStartFailure()
        {
            tester.Config.Source.EndpointUrl = "opc.tcp://localhost:60000";
            tester.Client.Close();

            try
            {
                using var extractor = tester.BuildExtractor();
                await Assert.ThrowsAsync<SilentServiceException>(() => extractor.RunExtractor(true));
            }
            finally
            {
                tester.Config.Source.EndpointUrl = "opc.tcp://localhost:62100";
                await tester.Client.Run(tester.Source.Token);
            }
        }
        [Fact]
        public async Task TestMapping()
        {
            using var extractor = tester.BuildExtractor();
        }
    }
}

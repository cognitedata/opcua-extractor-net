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
using Test.Utils;
using System.Runtime.InteropServices;
using System.Linq;

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

        public UAExtractor BuildExtractor(bool clear = true, IExtractionStateStore stateStore = null, params IPusher[] pushers)
        {
            if (clear)
            {
                Client.ClearNodeOverrides();
                Client.ClearEventFields();
                Client.ResetVisitedNodes();
            }
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
            tester.Config.Extraction.RootNode = tester.Server.Ids.Full.Root.ToProtoNodeId(tester.Client);
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            try
            {
                await extractor.RunExtractor(true);

                Assert.Equal(153, pusher.PushedNodes.Count);
                Assert.Equal(2000, pusher.PushedVariables.Count);

                Assert.Contains(pusher.PushedNodes.Values, node => node.DisplayName == "DeepObject 4, 25");
                Assert.Contains(pusher.PushedVariables.Values, node => node.DisplayName == "SubVariable 1234");
            }
            finally
            {
                tester.Config.Extraction.RootNode = null;
            }

        }
        private static void TriggerEventExternally(string field, object parent)
        {
            var dg = parent.GetType()
                .GetField(field, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .GetValue(parent) as MulticastDelegate;
            foreach (var handler in dg.GetInvocationList())
            {
                handler.Method.Invoke(
                    handler.Target,
                    new object[] { parent, EventArgs.Empty });
            }
        }

        [Fact]
        public async Task TestForceRestart()
        {
            tester.Config.Source.ForceRestart = true;
            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            try
            {
                var task = extractor.RunExtractor();
                await extractor.WaitForSubscriptions();

                Assert.False(task.IsCompleted);

                TriggerEventExternally("OnServerDisconnect", tester.Client);

                await Task.WhenAny(task, Task.Delay(10000));
                Assert.True(task.IsCompleted);
            }
            finally
            {
                tester.Config.Source.ForceRestart = false;
            }
        }
        [Fact]
        public async Task TestRestartOnReconnect()
        {
            tester.Config.Source.RestartOnReconnect = true;

            var pusher = new DummyPusher(new DummyPusherConfig());
            using var extractor = tester.BuildExtractor(pushers: pusher);

            try
            {
                var task = extractor.RunExtractor();
                await extractor.WaitForSubscriptions();
                Assert.True(pusher.PushedNodes.Any());
                pusher.PushedNodes.Clear();
                TriggerEventExternally("OnServerReconnect", tester.Client);

                Assert.True(pusher.OnReset.WaitOne(10000));

                await CommonTestUtils.WaitForCondition(() => pusher.PushedNodes.Count > 0, 10);

                extractor.Close();
            }
            finally
            {
                tester.Config.Source.RestartOnReconnect = false;
            }
        }
    }
}

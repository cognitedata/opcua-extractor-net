using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Fusion;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Test
{
    [CollectionDefinition("Pusher_tests", DisableParallelization = true)]
    public class CDFPusherTests
    {
        [Trait("Category", "both")]
        [Trait("Tests", "cdfpusher")]
        [Theory]
        [InlineData(DummyFactory.MockMode.All, "basic")]
        [InlineData(DummyFactory.MockMode.Some, "basic")]
        [InlineData(DummyFactory.MockMode.None, "basic")]
        [InlineData(DummyFactory.MockMode.FailAsset, "basic")]
        [InlineData(DummyFactory.MockMode.All, "full")]
        [InlineData(DummyFactory.MockMode.Some, "full")]
        [InlineData(DummyFactory.MockMode.None, "full")]
        [InlineData(DummyFactory.MockMode.FailAsset, "full")]
        public async Task TestBasicPushing(DummyFactory.MockMode mode, string serverType)
        {
            var fullConfig = Common.BuildConfig(serverType, 3);
            Logger.Startup(fullConfig.LoggerConfig);
            Logger.LogInfo("Testing with MockMode " + mode.ToString());
            UAClient client = new UAClient(fullConfig);
            var factory = new DummyFactory(fullConfig.CogniteConfig.Project, mode);
            var pusher = new CDFPusher(GetDummyProvider(factory), fullConfig);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            try
            {
                await extractor.RunExtractor(CancellationToken.None, true);
            }
            catch (Exception e)
            {
                if (mode != DummyFactory.MockMode.FailAsset)
                {
                    if (!Common.TestRunResult(e)) throw;
                }
            }
            extractor.Close();
        }
        [Trait("Category", "basicserver")]
        [Trait("Tests", "cdfpusher")]
        [Trait("Tests", "autobuffer")]
        [Fact]
        public async Task TestAutoBuffering()
        {
            var fullConfig = Common.BuildConfig("basic", 4);
            if (fullConfig == null)
            {
                throw new Exception("No config");
            }
            Logger.Startup(fullConfig.LoggerConfig);
            UAClient client = new UAClient(fullConfig);
            var factory = new DummyFactory(fullConfig.CogniteConfig.Project, DummyFactory.MockMode.None);
            var pusher = new CDFPusher(GetDummyProvider(factory), fullConfig);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);

                File.Create(fullConfig.CogniteConfig.BufferFile).Close();
                factory.AllowPush = false;
                bool gotData = false;
                for (int i = 0; i < 20; i++)
                {
                    if (new FileInfo(fullConfig.CogniteConfig.BufferFile).Length > 0)
                    {
                        gotData = true;
                        break;
                    }
                    Thread.Sleep(1000);
                }
                Assert.True(gotData, "Some data must be written");
                factory.AllowPush = true;
                gotData = false;
                for (int i = 0; i < 20; i++)
                {
                    if (new FileInfo(fullConfig.CogniteConfig.BufferFile).Length == 0)
                    {
                        gotData = true;
                        break;
                    }
                    Thread.Sleep(1000);
                }
                Assert.True(gotData, "Expecting file to be emptied");
                source.Cancel();
                try
                {
                    await runTask;
                }
                catch (Exception e)
                {
                    if (!Common.TestRunResult(e)) throw;
                }
                extractor.Close();
            }
        }
        [Trait("Tests", "basicserver")]
        [Trait("Tests", "cdfpusher")]
        [Fact]
        public async Task TestDebugMode()
        {
            var fullConfig = Common.BuildConfig("basic", 5);
            if (fullConfig == null) throw new Exception("No config");
            fullConfig.CogniteConfig.Debug = true;

            Logger.Startup(fullConfig.LoggerConfig);
            UAClient client = new UAClient(fullConfig);
            var factory = new DummyFactory(fullConfig.CogniteConfig.Project, DummyFactory.MockMode.None);
            var pusher = new CDFPusher(GetDummyProvider(factory), fullConfig);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            using (var source = new CancellationTokenSource())
            {
                var runTask = extractor.RunExtractor(source.Token);
                Thread.Sleep(2000);
                source.Cancel();
                try
                {
                    await runTask;
                }
                catch (Exception e)
                {
                    if (!Common.TestRunResult(e)) throw;
                }
                Assert.Equal(0, factory.RequestCount);
                extractor.Close();
            }
        }
        public static IServiceProvider GetDummyProvider(DummyFactory factory)
        {
            var services = new ServiceCollection();
            services.AddHttpClient<Client>()
                .ConfigurePrimaryHttpMessageHandler(() => factory.GetHandler());
            return services.BuildServiceProvider();
        }
    }
}

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Xunit;

namespace Testing
{
    public class CDFPusherTests
    {
        [Trait("Category", "both")]
        [Trait("Tests", "cdfpusher")]
        [Theory]
        [InlineData(DummyFactory.MockMode.All)]
        [InlineData(DummyFactory.MockMode.Some)]
        [InlineData(DummyFactory.MockMode.None)]
        [InlineData(DummyFactory.MockMode.FailAsset)]
        public async Task TestBasicPushing(DummyFactory.MockMode mode)
        {
            FullConfig fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null)
            {
                throw new Exception("No config");
            }
            Logger.Startup(fullConfig.LoggerConfig);
            Logger.LogInfo("Testing with MockMode " + mode.ToString());
            UAClient client = new UAClient(fullConfig);
            var pusher = new CDFPusher(new DummyFactory(fullConfig.CogniteConfig.Project, mode), fullConfig);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            extractor.Start();
            Assert.True(extractor.Started);
            try
            {
                await extractor.MapUAToCDF();
            }
            catch (Exception e)
            {
                if (mode != DummyFactory.MockMode.FailAsset)
                {
                    throw e;
                }
            }
            Thread.Sleep(4000);
            extractor.Close();
        }
        [Trait("Category", "basicserver")]
        [Trait("Tests", "cdfpusher")]
        [Trait("Tests", "autobuffer")]
        [Fact]
        public async Task TestAutoBuffering()
        {
            FullConfig fullConfig = Utils.GetConfig("config.test.yml");
            fullConfig.CogniteConfig.BufferFile = "autobuffer.bin";
            fullConfig.CogniteConfig.BufferOnFailure = true;
            if (fullConfig == null)
            {
                throw new Exception("No config");
            }
            Logger.Startup(fullConfig.LoggerConfig);
            UAClient client = new UAClient(fullConfig);
            var factory = new DummyFactory(fullConfig.CogniteConfig.Project, DummyFactory.MockMode.None);
            var pusher = new CDFPusher(factory, fullConfig);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            extractor.Start();
            Assert.True(extractor.Started);
            await extractor.MapUAToCDF();
            factory.AllowPush = false;
            Thread.Sleep(10000);
            Assert.True(new FileInfo(fullConfig.CogniteConfig.BufferFile).Length > 0, "Some data must be written");
            factory.AllowPush = true;
            Thread.Sleep(4000);
            Assert.Equal(0, new FileInfo(fullConfig.CogniteConfig.BufferFile).Length);
            extractor.Close();

        }
    }
}

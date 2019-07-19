using System;
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
        public async Task TestBasicPushing(DummyFactory.MockMode mode)
        {
            FullConfig fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null) return;
            Logger.Startup(fullConfig.LoggerConfig);
            Logger.LogInfo("Testing with MockMode " + mode.ToString());
            UAClient client = new UAClient(fullConfig);
            var pusher = new CDFPusher(new DummyFactory(fullConfig.CogniteConfig.Project, mode), fullConfig);

            Extractor extractor = new Extractor(fullConfig, pusher, client);
            extractor.Start();
            if (!extractor.Started)
            {
                Logger.Shutdown();
                return;
            }
            await extractor.MapUAToCDF();
            Thread.Sleep(4000);
            extractor.Close();
            Logger.Shutdown();
        }
    }
}

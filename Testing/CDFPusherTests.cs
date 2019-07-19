using System;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Xunit;

namespace Testing
{
    public class CDFPusherTests
    {
        [Trait("Category", "fullserver")]
        [Trait("Tests", "cdfpusher")]
        [Fact]
        public async Task TestBasicPushing()
        {
            FullConfig fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null) return;
            Logger.Startup(fullConfig.LoggerConfig);
            UAClient client = new UAClient(fullConfig);
            var pusher = new CDFPusher(new DummyFactory(fullConfig.CogniteConfig.Project), fullConfig);

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
        }
    }
}

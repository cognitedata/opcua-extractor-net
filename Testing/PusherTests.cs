using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Xunit;

namespace Testing
{
    public class PusherTests
    {
        [Trait("Category", "basicserver")]
        [Fact]
        public async Task TestBasicMapping()
        {
            FullConfig fullConfig = Utils.GetConfig("config.yml");
            if (fullConfig == null) return;
            Logger.Startup(fullConfig.LoggerConfig);
            int totalDps = 0;
            TestPusher pusher = new TestPusher(new Dictionary<string, System.Action<List<BufferedNode>, List<BufferedVariable>, List<BufferedVariable>>>
            {
                { "afterdata", (assetList, tsList, histTsList) =>
                {
                    Assert.Single(assetList);
                    Assert.Single(tsList);
                    Assert.Single(histTsList);
                } },
                { "afterProperties", (assetList, tsList, histTsList) =>
                {
                    Assert.NotNull(histTsList.First().properties);
                    Assert.Equal(2, histTsList.First().properties.Count);
                    Assert.Equal(2, assetList.First().properties.Count);
                } },
                { "afterSynchronize", (assetList, tsList, histTsList) =>
                {
                    Thread.Sleep(2000);
                    Assert.True(totalDps > 0, "Expected some datapoints");
                    int lastDps = totalDps;
                    Thread.Sleep(2000);
                    Assert.True(totalDps > lastDps, "Expected dp count to be increasing");
                } }
            }, (dpList) => totalDps += dpList.Count);
            UAClient client = new UAClient(fullConfig);
            Extractor extractor = new Extractor(fullConfig, pusher, client);
            extractor.Start();
            Assert.True(extractor.Started);
            if (!extractor.Started)
            {
                Logger.Shutdown();
                return;
            }
			IList<Task> tasks = new List<Task>();
            tasks.Add(Task.Run(() => extractor.MapUAToCDF()));
			Thread.Sleep(6000);
			tasks.Add(Task.Run(() => extractor.RestartExtractor()));
			Thread.Sleep(2000);
			tasks.Add(Task.Run(() => extractor.RestartExtractor()));
			Thread.Sleep(50);
			tasks.Add(Task.Run(() => extractor.RestartExtractor()));
			Thread.Sleep(4000);
			await Task.WhenAll(tasks);
            Assert.All(tasks, (task) => Assert.False(task.IsFaulted));
			extractor.Close();
			Logger.Shutdown();
            return;
        }
    }
}

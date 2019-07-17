using System;
using System.Collections.Concurrent;
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
        [Trait("Tests", "Mapping")]
        [Fact]
        public async Task TestBasicMapping()
        {
            FullConfig fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null) return;
            Logger.Startup(fullConfig.LoggerConfig);
            int totalDps = 0;
            TestPusher pusher = new TestPusher(new Dictionary<string, Action<List<BufferedNode>, List<BufferedVariable>, List<BufferedVariable>>>
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
			Thread.Sleep(3000);
			tasks.Add(Task.Run(() => extractor.RestartExtractor()));
			Thread.Sleep(2000);
			tasks.Add(Task.Run(() => extractor.RestartExtractor()));
			Thread.Sleep(50);
			tasks.Add(Task.Run(() => extractor.RestartExtractor()));
			Thread.Sleep(3000);
			await Task.WhenAll(tasks);
            Assert.All(tasks, (task) => Assert.False(task.IsFaulted));
			extractor.Close();
			Logger.Shutdown();
        }
        [Trait("Category", "basicserver")]
        [Trait("Tests", "Buffer")]
        [Fact]
        public void TestBufferReadWrite()
        {
            FullConfig fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null) return;
            Logger.Startup(fullConfig.LoggerConfig);
            int dpRuns = 0;
            int totalStored = 0;
            var quitEvent = new ManualResetEvent(false);
            TestPusher pusher = new TestPusher(null, (dpList) =>
            {
                dpRuns++;
                if (dpRuns < 5)
                {
                    totalStored += dpList.Count;
                    Utils.WriteBufferToFile(dpList, fullConfig.CogniteConfig);
                }
                else if (dpRuns == 5)
                {
                    Logger.LogInfo("Read from file...");
                    var queue = new ConcurrentQueue<BufferedDataPoint>();
                    Utils.ReadBufferFromFile(queue, fullConfig.CogniteConfig);
                    Assert.Equal(totalStored, queue.Count);
                    quitEvent.Set();
                }
            });
            UAClient client = new UAClient(fullConfig);
            Extractor extractor = new Extractor(fullConfig, pusher, client);
            extractor.Start();
            if (!extractor.Started)
            {
                Logger.Shutdown();
                return;
            }
            extractor.MapUAToCDF();
            Assert.True(quitEvent.WaitOne(20000), "Timeout");
            Assert.Equal(0, new System.IO.FileInfo(fullConfig.CogniteConfig.BufferFile).Length);
            extractor.Close();
            Logger.Shutdown();
        }
        [Trait("Category", "fullserver")]
        [Trait("Tests", "Bulk")]
        [Fact]
        public void TestBulkRequests()
        {
            FullConfig fullConfig = Utils.GetConfig("config.test.yml");
            if (fullConfig == null) return;
            Logger.Startup(fullConfig.LoggerConfig);
            int totalDps = 0;
            var quitEvent = new ManualResetEvent(false);
            TestPusher pusher = new TestPusher(new Dictionary<string, Action<List<BufferedNode>, List<BufferedVariable>, List<BufferedVariable>>>
            {
                { "afterdata", (assetList, tsList, histTsList) =>
                {
                    Assert.Equal(153, assetList.Count);
                    Assert.Equal(2001, tsList.Count);
                    Assert.Single(histTsList);
                } },
                { "afterSynchronize", (assetList, tsList, histTsList) =>
                {
                    Thread.Sleep(2000);
                    Assert.True(totalDps > 0, "Expected some datapoints");
                    int lastDps = totalDps;
                    Thread.Sleep(2000);
                    Assert.True(totalDps > lastDps, "Expected dp count to be increasing");
                    quitEvent.Set();
                } }
            }, (dpList) => totalDps += dpList.Count);
            UAClient client = new UAClient(fullConfig);
            Extractor extractor = new Extractor(fullConfig, pusher, client);
            extractor.Start();
            if (!extractor.Started)
            {
                Logger.Shutdown();
                return;
            }
            extractor.MapUAToCDF();
            Assert.True(quitEvent.WaitOne(20000), "Timeout");
            extractor.Close();
            Logger.Shutdown();
        }
    }
}

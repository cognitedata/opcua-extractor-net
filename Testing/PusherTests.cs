using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Xunit;

namespace Testing
{
    public class PusherTests
    {
        [Fact]
        public async Task TestBasicMapping()
        {
            FullConfig fullConfig = Utils.GetConfig("config.yml");
            if (fullConfig == null) return;
            Logger.Startup(fullConfig.LoggerConfig);
            TestPusher pusher = new TestPusher();
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

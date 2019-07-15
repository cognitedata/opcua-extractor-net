using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;
using Xunit;

namespace Testing
{
    class Program
    {
        static int Main(string[] args)
        {
            FullConfig fullConfig = Utils.GetConfig(args.Length > 0 ? args[0] : "config.yml");
            if (fullConfig == null) return -1;
            Logger.Startup(fullConfig.LoggerConfig);
            TestPusher pusher = new TestPusher();
            UAClient client = new UAClient(fullConfig);
            Extractor extractor = new Extractor(fullConfig, pusher, client);
            extractor.Start();
            if (!extractor.Started)
            {
                Logger.Shutdown();
                return -1;
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
			Task.WhenAll(tasks).Wait();
            Assert.All(tasks, (task) => Assert.False(task.IsFaulted));
			extractor.Close();
			Logger.Shutdown();
            return 0;
        }
    }
}

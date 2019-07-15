using System;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa;

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
            var quitEvent = new ManualResetEvent(false);
            Task runtask = Task.Run(() =>
            {
                try
                {
                    extractor.MapUAToCDF();
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to map directory");
                    Logger.LogException(e);
                    quitEvent.Set();
                }
            });
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                quitEvent.Set();
                eArgs.Cancel = true;
            };
            Console.WriteLine("Press ^C to exit");
            quitEvent.WaitOne(-1);
            Logger.LogInfo("Shutting down extractor...");
            extractor.Close();
            Logger.Shutdown();
            if (runtask != null && runtask.IsFaulted)
            {
                Logger.Shutdown();
                extractor.Close();
                return -1;
            }
            return 0;
        }
    }
}

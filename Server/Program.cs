using Serilog;
using System;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Server
{
    class Program
    {
        static void Main()
        {
            var logConfig = new LoggerConfiguration();
            logConfig.MinimumLevel.Verbose();
            logConfig.WriteTo.Console();
            Log.Logger = logConfig.CreateLogger();

            using var server = new ServerController(new[] { PredefinedSetup.Custom, PredefinedSetup.Base,
                    PredefinedSetup.Events, PredefinedSetup.Wrong, PredefinedSetup.Auditing, PredefinedSetup.PubSub
                    /*, PredefinedSetup.Full, PredefinedSetup.VeryLarge */ });

            server.Start().Wait();
            server.PopulateEvents();
            int idx = 0;
            while (true)
            {
                //server.TriggerEvents(0);
                //server.DirectGrowth(idx++);
                //server.ReferenceGrowth(idx++);
                server.UpdateNode(server.Ids.Base.DoubleVar1, idx++);
                server.UpdateNode(server.Ids.Custom.Array, new double[] { idx, idx + 1, idx + 2, idx + 3 });
                Task.Delay(1000).Wait();
            }
        }
    }
}

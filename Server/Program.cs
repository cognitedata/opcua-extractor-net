using Serilog;
using System;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Server
{
    internal class Program
    {
        private static void Main()
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

                server.UpdateBaseNodes(idx);
                server.UpdateCustomNodes(idx);

                idx++;
                Task.Delay(1000).Wait();
            }
        }
    }
}

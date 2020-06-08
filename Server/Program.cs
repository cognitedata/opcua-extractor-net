using System;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Configuration;
using Serilog;

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

            using var server = new ServerController(new[] {PredefinedSetup.Base, /* PredefinedSetup.Full, */ PredefinedSetup.Custom,
                    PredefinedSetup.Events, PredefinedSetup.Auditing });
            server.Start().Wait();
            server.PopulateEvents();

            int idx = 0;
            while(true)
            {
                //server.TriggerEvents(0);
                //server.DirectGrowth(idx++);
                //server.ReferenceGrowth(idx++);
                Task.Delay(1000).Wait();
            }
        }
    }
}

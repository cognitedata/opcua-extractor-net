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
                    PredefinedSetup.Events, PredefinedSetup.Wrong, PredefinedSetup.Auditing, PredefinedSetup.VeryLarge /* PredefinedSetup.PubSub */
                    /*, PredefinedSetup.Full, PredefinedSetup.VeryLarge */ });

            server.Start().Wait();
            server.PopulateEvents();
            int idx = 0;
            while (true)
            {
                //server.TriggerEvents(0);
                //server.DirectGrowth(idx++);
                //server.ReferenceGrowth(idx++);
                server.UpdateNode(server.Ids.Base.DoubleVar1, idx);
                server.UpdateNode(server.Ids.Base.DoubleVar2, -idx);
                server.UpdateNode(server.Ids.Base.BoolVar, idx % 2 == 0);
                server.UpdateNode(server.Ids.Base.IntVar, idx);
                server.UpdateNode(server.Ids.Base.StringVar, $"Idx: {idx}");

                server.UpdateNode(server.Ids.Custom.Array, new double[] { idx, idx + 1, idx + 2, idx + 3 });
                server.UpdateNode(server.Ids.Custom.StringArray, new string[] { $"str{idx}", $"str{-idx}" });
                server.UpdateNode(server.Ids.Custom.StringyVar, $"Idx: {idx}");
                server.UpdateNode(server.Ids.Custom.MysteryVar, idx);
                server.UpdateNode(server.Ids.Custom.IgnoreVar, $"Idx: {idx}");
                server.UpdateNode(server.Ids.Custom.NumberVar, idx);
                server.UpdateNode(server.Ids.Custom.EnumVar1, idx % 3);
                server.UpdateNode(server.Ids.Custom.EnumVar2, idx % 2 == 0 ? 123 : 321);
                server.UpdateNode(server.Ids.Custom.EnumVar3, idx % 2 == 0
                    ? new[] { 123, 123, 321, 123 } : new[] { 123, 123, 123, 321 });

                idx++;
                Task.Delay(1000).Wait();
            }
        }
    }
}

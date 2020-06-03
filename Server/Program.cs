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

            ApplicationInstance app = new ApplicationInstance();
            app.ConfigSectionName = "Server.Test";
            try
            {
                app.LoadApplicationConfiguration("config/Server.Test.Config.xml", false).Wait();
                app.CheckApplicationInstanceCertificate(false, 0).Wait();
                using var server = new Server(new[] {PredefinedSetup.Base, PredefinedSetup.Full, PredefinedSetup.Custom,
                    PredefinedSetup.Events, PredefinedSetup.Auditing });
                app.Start(server).Wait();
                Log.Information("Server started");

                var baseNodeId = new NodeId(2, 2);

                var random = new Random();
                while (true)
                {
                    server.UpdateNode(baseNodeId, random.NextDouble());
                    Task.Delay(500).Wait();
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to start server");
            }
        }
    }
}

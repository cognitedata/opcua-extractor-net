using Opc.Ua;
using Opc.Ua.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Server
{
    sealed public class ServerController : IDisposable
    {
        public NodeIdReference Ids => Server.Ids;
        public Server Server { get; private set; }
        private IEnumerable<PredefinedSetup> setups;

        public ServerController(IEnumerable<PredefinedSetup> setups)
        {
            this.setups = setups;
        }

        public void Dispose()
        {
            Log.Information("Closing server");
            Server?.Stop();
            Server?.Dispose();
        }

        public async Task Start()
        {
            ApplicationInstance app = new ApplicationInstance();
            app.ConfigSectionName = "Server.Test";
            try
            {
                app.LoadApplicationConfiguration("config/Server.Test.Config.xml", false).Wait();
                app.CheckApplicationInstanceCertificate(false, 0).Wait();
                Server = new Server(setups);
                await app.Start(Server);
                Log.Information("Server started");
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to start server");
            }
        }
        public void Stop()
        {
            Server.Stop();
        }

        public void PopulateArrayHistory()
        {
            Server.PopulateHistory(Server.Ids.Custom.Array, 999, "custom", 10, (i => new int[] { i, i, i, i }));
            Server.PopulateHistory(Server.Ids.Custom.MysteryVar, 999, "int");
            Server.UpdateNode(Server.Ids.Custom.Array, new int[] { 999, 999, 999, 999 });
            Server.UpdateNode(Server.Ids.Custom.MysteryVar, 999);
        }

        public void UpdateNode(NodeId id, object value)
        {
            Server.UpdateNode(id, value);
        }
    }
}

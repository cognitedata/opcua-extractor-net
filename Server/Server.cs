using System;
using System.Collections.Generic;
using System.Text;
using Opc.Ua;
using Opc.Ua.Server;

namespace Server
{
    class Server : StandardServer
    {
        private CoreNodeManager core;
        private TestNodeManager custom;
        protected override MasterNodeManager CreateMasterNodeManager(IServerInternal server, ApplicationConfiguration configuration)
        {
            custom = new TestNodeManager(server, configuration);
            var nodeManagers = new List<INodeManager> {custom};

            // create the custom node managers.

            // create master node manager.
            var mgr = new MasterNodeManager(server, configuration, null, nodeManagers.ToArray());
            core = mgr.CoreNodeManager;
            return mgr;
        }
        protected override ServerProperties LoadServerProperties()
        {
            ServerProperties properties = new ServerProperties
            {
                ManufacturerName = "Cognite",
                ProductName = "Test Server",
                SoftwareVersion = Utils.GetAssemblySoftwareVersion(),
                BuildNumber = Utils.GetAssemblyBuildNumber(),
                BuildDate = Utils.GetAssemblyTimestamp()
            };

            return properties;
        }

        public void UpdateNode(NodeId id, object value)
        {
            custom.UpdateNode(id, value);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Opc.Ua;
using Opc.Ua.Server;
using Serilog;

namespace Server
{
    public sealed class Server : StandardServer
    {
        private TestNodeManager custom;
        public NodeIdReference Ids => custom.Ids;

        private IEnumerable<PredefinedSetup> setups;

        public Server(IEnumerable<PredefinedSetup> setups)
        {
            this.setups = setups;
        }

        protected override MasterNodeManager CreateMasterNodeManager(IServerInternal server, ApplicationConfiguration configuration)
        {
            custom = new TestNodeManager(server, configuration, setups);
            var nodeManagers = new List<INodeManager> { custom };
            // create the custom node managers.

            // create master node manager.
            return new MasterNodeManager(server, configuration, null, nodeManagers.ToArray());
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

        public string GetNamespace(uint index)
        {
            return ServerInternal.NamespaceUris.GetString(index);
        }

        public IEnumerable<DataValue> GetHistory(NodeId id)
        {
            return custom.FetchHistory(id);
        }

        public IEnumerable<BaseEventState> GetEventHistory(NodeId id)
        {
            return custom.FetchEventHistory(id);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1062:Validate arguments of public methods",
            Justification = "Handled later")]
        public void TriggerEvent<T>(NodeId eventId, NodeId emitter, NodeId source, string message, Action<ManagedEvent> builder = null)
            where T : ManagedEvent
        {
            custom.TriggerEvent<T>(eventId, emitter, source, message, builder);
        }
        public void PopulateHistory(NodeId id, int count, string type = "int", int msdiff = 10, Func<int, object> valueBuilder = null)
        {
            custom.PopulateHistory(id, count, type, msdiff, valueBuilder);
        }

        public void PopulateEventHistory<T>(NodeId eventId,
            NodeId emitter,
            NodeId source,
            string message,
            int count,
            int msdiff = 10,
            Action<ManagedEvent> builder = null)
            where T : ManagedEvent
        {
            custom.PopulateEventHistory<T>(eventId, emitter, source, message, count, msdiff, builder);
        }

        public void AddObject(NodeId parentId, string name, bool audit = false)
        {
            custom.AddObject(parentId, name, audit);
        }

        public void AddVariable(NodeId parentId, string name, NodeId dataType, bool audit = false)
        {
            custom.AddVariable(parentId, name, dataType, audit);
        }
        public void AddReference(NodeId sourceId, NodeId targetId, NodeId type, bool audit = false)
        {
            custom.AddReference(sourceId, targetId, type, audit);
        }
    }
}

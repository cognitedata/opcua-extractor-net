﻿using System;
using System.Collections.Generic;
using Opc.Ua;
using Opc.Ua.Server;

namespace Server
{
    public sealed class TestServer : StandardServer
    {
        private TestNodeManager custom;
        public NodeIdReference Ids => custom.Ids;

        private IEnumerable<PredefinedSetup> setups;

        public TestServer(IEnumerable<PredefinedSetup> setups)
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
            Action<ManagedEvent, int> builder = null)
            where T : ManagedEvent
        {
            custom.PopulateEventHistory<T>(eventId, emitter, source, message, count, msdiff, builder);
        }

        public NodeId AddObject(NodeId parentId, string name, bool audit = false)
        {
            return custom.AddObject(parentId, name, audit);
        }

        public NodeId AddVariable(NodeId parentId, string name, NodeId dataType, bool audit = false)
        {
            return custom.AddVariable(parentId, name, dataType, audit);
        }
        public void AddReference(NodeId sourceId, NodeId parentId, NodeId type, bool audit = false)
        {
            custom.AddReference(sourceId, parentId, type, audit);
        }
        public NodeId AddProperty<T>(NodeId parentId, string name, NodeId dataType, object value, int rank = -1)
        {
            return custom.AddProperty<T>(parentId, name, dataType, value, rank);
        }
        public void MutateNode(NodeId id, Action<NodeState> mutation)
        {
            if (mutation == null) throw new ArgumentNullException(nameof(mutation));
            custom.MutateNode(id, mutation);
        }
        public void ReContextualize(NodeId id, NodeId oldParentId, NodeId newParentId, NodeId referenceType)
        {
            custom.ReContextualize(id, oldParentId, newParentId, referenceType);
        }
    }
}

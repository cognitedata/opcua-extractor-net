using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Opc.Ua;

namespace Cognite.OpcUa
{
    public class State
    {
        private readonly ConcurrentDictionary<NodeId, NodeExtractionState> nodeStates =
            new ConcurrentDictionary<NodeId, NodeExtractionState>();

        private readonly ConcurrentDictionary<string, NodeExtractionState> nodeStatesByExtId =
            new ConcurrentDictionary<string, NodeExtractionState>();

        private readonly ConcurrentDictionary<NodeId, EventExtractionState> emitterStates=
            new ConcurrentDictionary<NodeId, EventExtractionState>();

        private readonly ConcurrentDictionary<string, NodeId> externalToNodeId =
            new ConcurrentDictionary<string, NodeId>();

        private readonly ConcurrentDictionary<string, EventExtractionState> emitterStatesByExtId =
            new ConcurrentDictionary<string, EventExtractionState>();

        private readonly ConcurrentDictionary<NodeId, string> managedNodes =
            new ConcurrentDictionary<NodeId, string>();

        public ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> ActiveEvents { get; }
            = new ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>>();

        private readonly ConcurrentDictionary<(NodeId, int), BufferedNode> activeNodes =
            new ConcurrentDictionary<(NodeId, int), BufferedNode>();

        public IEnumerable<NodeExtractionState> NodeStates => nodeStates.Values;
        public IEnumerable<EventExtractionState> EmitterStates => emitterStates.Values;
        public IEnumerable<NodeId> AllActiveIds => managedNodes.Keys;
        public IEnumerable<string> AllActiveExternalIds => managedNodes.Values;

        public IEnumerable<BufferedNode> ActiveNodes => activeNodes.Values;

        private readonly Extractor extractor;

        public State(Extractor extractor)
        {
            this.extractor = extractor;
        }

        public NodeExtractionState GetNodeState(string externalId)
        {
            return nodeStatesByExtId.GetValueOrDefault(externalId);
        }

        public NodeExtractionState GetNodeState(NodeId id)
        {
            return nodeStates.GetValueOrDefault(id);
        }

        public EventExtractionState GetEmitterState(string externalId)
        {
            return emitterStatesByExtId.GetValueOrDefault(externalId);
        }

        public EventExtractionState GetEmitterState(NodeId id)
        {
            return emitterStates.GetValueOrDefault(id);
        }

        public void SetNodeState(NodeExtractionState state, string uniqueId = null)
        {
            if (state == null) throw new ArgumentNullException(nameof(state));
            nodeStates[state.Id] = state;
            nodeStatesByExtId[uniqueId ?? extractor.GetUniqueId(state.Id)] = state;
        }

        public void SetEmitterState(EventExtractionState state, string uniqueId = null)
        {
            if (state == null) throw new ArgumentNullException(nameof(state));
            emitterStates[state.Id] = state;
            emitterStatesByExtId[uniqueId ?? extractor.GetUniqueId(state.Id)] = state;
        }

        public void AddManagedNode(NodeId id)
        {
            managedNodes[id] = extractor.GetUniqueId(id);
        }

        public NodeId GetNodeId(string uniqueId)
        {
            return externalToNodeId[uniqueId];
        }

        public void RegisterNode(NodeId nodeId, string id)
        {
            externalToNodeId[id] = nodeId;
        }

        public bool IsMappedNode(NodeId id)
        {
            return managedNodes.ContainsKey(id);
        }

        public string GetUniqueId(NodeId id)
        {
            return managedNodes.GetValueOrDefault(id);
        }

        public void AddActiveNode(BufferedNode node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            activeNodes[(node.Id, -1)] = node;
        }

        public void AddActiveNode(BufferedVariable node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            activeNodes[(node.Id, node.Index)] = node;
        }

        public BufferedNode GetActiveNode(NodeId id, int index = -1)
        {
            return activeNodes.GetValueOrDefault((id, index));
        }
    }
}

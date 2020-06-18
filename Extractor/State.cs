using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Opc.Ua;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Used to wrap the significant state of the extractor and provide utility functions and properties
    /// </summary>
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

        public ConcurrentDictionary<NodeId, IEnumerable<(NodeId Root, QualifiedName BrowseName)>> ActiveEvents { get; }
            = new ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>>();

        private readonly ConcurrentDictionary<(NodeId Id, int Index), BufferedNode> activeNodes =
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
        /// <summary>
        /// Return a NodeExtractionState by externalId
        /// </summary>
        /// <param name="externalId">ExternalId for lookup</param>
        /// <returns>State if it exists</returns>
        public NodeExtractionState GetNodeState(string externalId)
        {
            return nodeStatesByExtId.GetValueOrDefault(externalId);
        }
        /// <summary>
        /// Return a NodeExtractionState by nodeId
        /// </summary>
        /// <param name="id">NodeId for lookup</param>
        /// <returns>State if it exists</returns>
        public NodeExtractionState GetNodeState(NodeId id)
        {
            return nodeStates.GetValueOrDefault(id);
        }
        /// <summary>
        /// Return an EventExtractionState by externalId
        /// </summary>
        /// <param name="externalId">ExternalId for lookup</param>
        /// <returns>State if it exists</returns>
        public EventExtractionState GetEmitterState(string externalId)
        {
            return emitterStatesByExtId.GetValueOrDefault(externalId);
        }
        /// <summary>
        /// Return an EventExtractionState by nodeId
        /// </summary>
        /// <param name="id">NodeId for lookup</param>
        /// <returns>State if it exists</returns>
        public EventExtractionState GetEmitterState(NodeId id)
        {
            return emitterStates.GetValueOrDefault(id);
        }

        /// <summary>
        /// Add node state to storage
        /// </summary>
        /// <param name="state">State to add</param>
        /// <param name="uniqueId">ExternalId, leave empty to auto generate</param>
        public void SetNodeState(NodeExtractionState state)
        {
            if (state == null) throw new ArgumentNullException(nameof(state));
            nodeStates[state.SourceId] = state;
            nodeStatesByExtId[state.Id] = state;
        }
        /// <summary>
        /// Add event state to storage
        /// </summary>
        /// <param name="state">State to add</param>
        /// <param name="uniqueId">ExternalId, leave empty to auto generate</param>
        public void SetEmitterState(EventExtractionState state)
        {
            if (state == null) throw new ArgumentNullException(nameof(state));
            emitterStates[state.SourceId] = state;
            emitterStatesByExtId[state.Id] = state;
        }
        /// <summary>
        /// Indicate that the given node is managed by the extractor.
        /// </summary>
        /// <param name="id">Id to add</param>
        public void AddManagedNode(NodeId id)
        {
            managedNodes[id] = extractor.GetUniqueId(id);
        }
        /// <summary>
        /// Returns corresponding NodeId to given uniqueId if it exists.
        /// </summary>
        /// <param name="uniqueId">Id to map</param>
        /// <returns>NodeId if it exists</returns>
        public NodeId GetNodeId(string uniqueId)
        {
            return externalToNodeId.GetValueOrDefault(uniqueId) ?? NodeId.Null;
        }
        /// <summary>
        /// Register a reverse node mapping
        /// </summary>
        /// <param name="nodeId">NodeId value</param>
        /// <param name="id">UniqueId key</param>
        public void RegisterNode(NodeId nodeId, string id)
        {
            externalToNodeId[id] = nodeId;
        }
        /// <summary>
        /// Returns true if given NodeId is a managed node
        /// </summary>
        /// <param name="id">NodeId to test</param>
        /// <returns>True if id exists in managed nodes</returns>
        public bool IsMappedNode(NodeId id)
        {
            return managedNodes.ContainsKey(id);
        }
        /// <summary>
        /// Returns mapping from uniqueId to managed node if one exists.
        /// </summary>
        /// <param name="id">Id to look up</param>
        /// <returns>UniqueId or null</returns>
        public string GetUniqueId(NodeId id)
        {
            return managedNodes.GetValueOrDefault(id);
        }
        /// <summary>
        /// Add node to overview of known mapped nodes
        /// </summary>
        /// <param name="node">Node to add</param>
        public void AddActiveNode(BufferedNode node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            activeNodes[(node.Id, -1)] = node;
        }
        /// <summary>
        /// Add variable to overview of known mapped nodes
        /// </summary>
        /// <param name="node">Node to add</param>
        public void AddActiveNode(BufferedVariable node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            activeNodes[(node.Id, node.Index)] = node;
        }
        /// <summary>
        /// Get active node by NodeId and index if it exists
        /// </summary>
        /// <param name="id">NodeId to use for lookup</param>
        /// <param name="index">Index of node, default is -1</param>
        /// <returns></returns>
        public BufferedNode GetActiveNode(NodeId id, int index = -1)
        {
            return activeNodes.GetValueOrDefault((id, index));
        }
    }
}

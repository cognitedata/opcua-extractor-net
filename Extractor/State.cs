/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Used to wrap the significant state of the extractor and provide utility functions and properties
    /// </summary>
    public class State
    {
        private readonly ConcurrentDictionary<NodeId, VariableExtractionState> nodeStates =
            new ConcurrentDictionary<NodeId, VariableExtractionState>();

        private readonly ConcurrentDictionary<string, VariableExtractionState> nodeStatesByExtId =
            new ConcurrentDictionary<string, VariableExtractionState>();

        private readonly ConcurrentDictionary<NodeId, EventExtractionState> emitterStates =
            new ConcurrentDictionary<NodeId, EventExtractionState>();

        private readonly ConcurrentDictionary<string, NodeId> externalToNodeId =
            new ConcurrentDictionary<string, NodeId>();

        private readonly ConcurrentDictionary<string, EventExtractionState> emitterStatesByExtId =
            new ConcurrentDictionary<string, EventExtractionState>();

        public ConcurrentDictionary<NodeId, UAEventType> ActiveEvents { get; }
            = new ConcurrentDictionary<NodeId, UAEventType>();

        private readonly ConcurrentDictionary<NodeId, int> nodeChecksums =
            new ConcurrentDictionary<NodeId, int>();

        public IEnumerable<VariableExtractionState> NodeStates => nodeStates.Values;
        public IEnumerable<EventExtractionState> EmitterStates => emitterStates.Values;

        /// <summary>
        /// Return a NodeExtractionState by externalId
        /// </summary>
        /// <param name="externalId">ExternalId for lookup</param>
        /// <returns>State if it exists</returns>
        public VariableExtractionState? GetNodeState(string externalId)
        {
            if (externalId == null) return null;
            return nodeStatesByExtId.GetValueOrDefault(externalId);
        }
        /// <summary>
        /// Return a NodeExtractionState by nodeId
        /// </summary>
        /// <param name="id">NodeId for lookup</param>
        /// <returns>State if it exists</returns>
        public VariableExtractionState? GetNodeState(NodeId id)
        {
            if (id == null || id.IsNullNodeId) return null;
            return nodeStates.GetValueOrDefault(id);
        }
        /// <summary>
        /// Return an EventExtractionState by externalId
        /// </summary>
        /// <param name="externalId">ExternalId for lookup</param>
        /// <returns>State if it exists</returns>
        public EventExtractionState? GetEmitterState(string externalId)
        {
            if (externalId == null) return null;
            return emitterStatesByExtId.GetValueOrDefault(externalId);
        }
        /// <summary>
        /// Return an EventExtractionState by nodeId
        /// </summary>
        /// <param name="id">NodeId for lookup</param>
        /// <returns>State if it exists</returns>
        public EventExtractionState? GetEmitterState(NodeId id)
        {
            if (id == null || id.IsNullNodeId) return null;
            return emitterStates.GetValueOrDefault(id);
        }

        /// <summary>
        /// Add node state to storage
        /// </summary>
        /// <param name="state">State to add</param>
        /// <param name="uniqueId">ExternalId, leave empty to auto generate</param>
        public void SetNodeState(VariableExtractionState state, string? uniqueId = null)
        {
            nodeStates[state.SourceId] = state;
            nodeStatesByExtId[uniqueId ?? state.Id] = state;
        }
        /// <summary>
        /// Add event state to storage
        /// </summary>
        /// <param name="state">State to add</param>
        /// <param name="uniqueId">ExternalId, leave empty to auto generate</param>
        public void SetEmitterState(EventExtractionState state)
        {
            emitterStates[state.SourceId] = state;
            emitterStatesByExtId[state.Id] = state;
        }

        /// <summary>
        /// Returns corresponding NodeId to given uniqueId if it exists.
        /// </summary>
        /// <param name="uniqueId">Id to map</param>
        /// <returns>NodeId if it exists</returns>
        public NodeId GetNodeId(string uniqueId)
        {
            if (string.IsNullOrEmpty(uniqueId)) return NodeId.Null;
            return externalToNodeId.GetValueOrDefault(uniqueId) ?? NodeId.Null;
        }
        /// <summary>
        /// Register a reverse node mapping
        /// </summary>
        /// <param name="nodeId">NodeId value</param>
        /// <param name="id">UniqueId key</param>
        public void RegisterNode(NodeId nodeId, string? id)
        {
            if (id == null) return;
            externalToNodeId[id] = nodeId;
        }
        /// <summary>
        /// Returns true if given NodeId is a managed node
        /// </summary>
        /// <param name="id">NodeId to test</param>
        /// <returns>True if id exists in managed nodes</returns>
        public bool IsMappedNode(NodeId id)
        {
            if (id == null || id.IsNullNodeId) return false;
            return nodeChecksums.ContainsKey(id);
        }
        /// <summary>
        /// Add node to overview of known mapped nodes
        /// </summary>
        /// <param name="node">Node to add</param>
        public void AddActiveNode(UANode node, TypeUpdateConfig update, bool dataTypeMetadata, bool nodeTypeMetadata)
        {
            if (node is UAVariable variable && variable.Index != -1) throw new InvalidOperationException();
            nodeChecksums[node.Id] = node.GetUpdateChecksum(update, dataTypeMetadata, nodeTypeMetadata);
        }
        /// <summary>
        /// Get node checksum by NodeId and index if it exists
        /// </summary>
        /// <param name="id">NodeId to use for lookup</param>
        /// <returns></returns>
        public int? GetNodeChecksum(NodeId id)
        {
            if (id == null || id.IsNullNodeId) return null;
            if (nodeChecksums.TryGetValue(id, out var checksum)) return checksum;
            return null;
        }

        /// <summary>
        /// Number of currently managed nodes.
        /// </summary>
        public int NumActiveNodes => nodeChecksums.Count;

        /// <summary>
        /// Wipe the state
        /// </summary>
        public void Clear()
        {
            nodeChecksums.Clear();
            nodeStates.Clear();
            nodeStatesByExtId.Clear();
            emitterStates.Clear();
            emitterStatesByExtId.Clear();
            externalToNodeId.Clear();
            ActiveEvents.Clear();
        }
    }
}

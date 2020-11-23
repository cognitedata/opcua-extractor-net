/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public interface IUAClient : IDisposable
    {
        DataTypeManager DataTypeManager { get; }
        NamespaceTable NamespaceTable { get; }
        bool Started { get; }

        public ISystemContext SystemContext { get; }

        event EventHandler<IUAClient> OnServerDisconnect;
        event EventHandler<IUAClient> OnServerReconnect;

        #region session-management
        /// <summary>
        /// Entrypoint for starting the opcua Session. Must be called before any further requests can be made.
        /// </summary>
        Task Run(CancellationToken token);
        /// <summary>
        /// Remove collected event fields
        /// </summary>
        void ClearEventFields();
        /// <summary>
        /// Remove all externalId overrides
        /// </summary>
        void ClearNodeOverrides();
        /// <summary>
        /// Close the Session, cleaning up any client data on the server
        /// </summary>
        void Close();
        /// <summary>
        /// Wait for all opcua operations to finish
        /// </summary>
        Task WaitForOperations();
        /// <summary>
        /// Clear internal list of visited nodes, allowing callbacks to be called for visited nodes again.
        /// </summary>
        void ResetVisitedNodes();
        #endregion

        #region browse
        /// <summary>
        /// Add externalId override for a single node
        /// </summary>
        /// <param name="nodeId">Id of node to be overridden</param>
        /// <param name="externalId">ExternalId to be used</param>
        void AddNodeOverride(NodeId nodeId, string externalId);
        /// <summary>
        /// Get all children of root nodes recursively and invoke the callback for each.
        /// </summary>
        /// <param name="roots">Root nodes to browse</param>
        /// <param name="callback">Callback for each node</param>
        /// <param name="referenceTypes">Permitted reference types, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes as described in the OPC-UA specification</param>
        /// <param name="ignoreVisited">True to ignore visited nodes</param>
        void BrowseDirectory(IEnumerable<NodeId> roots, Action<ReferenceDescription, NodeId> callback,
            CancellationToken token, NodeId referenceTypes = null, uint nodeClassMask = 3, bool ignoreVisited = true);
        /// <summary>
        /// Browse an opcua directory, calling callback for all relevant nodes found.
        /// </summary>
        /// <param name="roots">Initial nodes to start mapping.</param>
        /// <param name="callback">Callback for each mapped node, takes a description of a single node, and its parent id</param>
        Task BrowseNodeHierarchy(IEnumerable<NodeId> roots, Action<ReferenceDescription, NodeId> callback, CancellationToken token, bool ignoreVisited = true);
        /// <summary>
        /// Browse node hierarchy for single root node
        /// </summary>
        /// <param name="root">Root node to browse for</param>
        /// <param name="callback">Callback to call for each found node</param>
        Task BrowseNodeHierarchy(NodeId root, Action<ReferenceDescription, NodeId> callback, CancellationToken token, bool ignoreVisited = true);
        /// <summary>
        /// Retrieve a representation of the server node
        /// </summary>
        BufferedNode GetServerNode(CancellationToken token);
        /// <summary>
        /// Get all children of the given list of parents as a map from parentId to list of children descriptions
        /// </summary>
        /// <param name="parents">List of parents to browse</param>
        /// <param name="referenceTypes">Referencetype to browse, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes, as specified in the OPC-UA specification</param>
        /// <returns>Dictionary from parent nodeId to collection of children as ReferenceDescriptions</returns>
        Dictionary<NodeId, ReferenceDescriptionCollection> GetNodeChildren(IEnumerable<NodeId> parents, NodeId referenceTypes, uint nodeClassMask, CancellationToken token);
        #endregion

        #region node-data
        /// <summary>
        /// Gets Description for all nodes, and DataType, Historizing and ValueRank for Variable nodes, then updates the given list of nodes
        /// </summary>
        /// <param name="nodes">Nodes to be updated with data from the opcua server</param>
        void ReadNodeData(IEnumerable<BufferedNode> nodes, CancellationToken token);
        /// <summary>
        /// Gets the values of the given list of variables, then updates each variable with a BufferedDataPoint
        /// </summary>
        /// <remarks>
        /// Note that there is a fixed maximum message size, and we here fetch a large number of values at the same time.
        /// To avoid complications, avoid fetching data of unknown large size here.
        /// </remarks>
        /// <param name="nodes">List of variables to be updated</param>
        void ReadNodeValues(IEnumerable<BufferedVariable> nodes, CancellationToken token);
        /// <summary>
        /// Get the raw values for each given node id.
        /// Nodes must be variables
        /// </summary>
        /// <param name="ids">Nodes to get values for</param>
        /// <returns>A map from given nodeId to DataValue</returns>
        Dictionary<NodeId, DataValue> ReadRawValues(IEnumerable<NodeId> ids, CancellationToken token);
        /// <summary>
        /// Gets properties for variables in nodes given, then updates all properties in given list of nodes with relevant data and values.
        /// </summary>
        /// <param name="nodes">Nodes to be updated with properties</param>
        void GetNodeProperties(IEnumerable<BufferedNode> nodes, CancellationToken token);
        #endregion

        #region synchronization
        /// <summary>
        /// Deletes a subscription starting with the given name.
        /// The client manages three subscriptions: EventListener, DataChangeListener and AuditListener,
        /// if the subscription does not exist, nothing happens.
        /// </summary>
        /// <param name="name"></param>
        void RemoveSubscription(string name);
        /// <summary>
        /// Constructs a filter from the given list of permitted eventids, the already constructed field map and an optional receivedAfter property.
        /// </summary>
        /// <param name="nodeIds">Permitted SourceNode ids</param>
        /// <param name="receivedAfter">Optional, if defined, attempt to filter out events with [ReceiveTimeProperty] > receivedAfter</param>
        /// <returns>The final event filter</returns>
        EventFilter BuildEventFilter();
        /// <summary>
        /// Fetch event fields from the server and store them on the client
        /// </summary>
        /// <param name="token"></param>
        /// <returns>The collected event fields</returns>
        Dictionary<NodeId, IEnumerable<(NodeId root, QualifiedName browseName)>> GetEventFields(CancellationToken token);
        /// <summary>
        /// Modifies passed HistoryReadParams while doing a single config-limited iteration of history read.
        /// </summary>
        /// <param name="readParams"></param>
        /// <returns>Pairs of NodeId and history read results as IEncodable</returns>
        IEnumerable<(NodeId Id, IEncodeable RawData)> DoHistoryRead(HistoryReadParams readParams);
        /// <summary>
        /// Subscribe to audit events on the server node
        /// </summary>
        /// <param name="callback">Callback to use for subscriptions</param>
        void SubscribeToAuditEvents(MonitoredItemNotificationEventHandler callback);
        /// <summary>
        /// Subscribe to events from the given list of emitters.
        /// </summary>
        /// <param name="emitters">List of emitters. These are the actual targets of the subscription.</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        /// <returns>Map of fields, EventTypeId->(SourceTypeId, BrowseName)</returns>
        void SubscribeToEvents(IEnumerable<EventExtractionState> emitters, MonitoredItemNotificationEventHandler subscriptionHandler, CancellationToken token);
        /// <summary>
        /// Create datapoint subscriptions for given list of nodes
        /// </summary>
        /// <param name="nodeList">List of buffered variables to synchronize</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        void SubscribeToNodes(IEnumerable<NodeExtractionState> nodeList, MonitoredItemNotificationEventHandler subscriptionHandler, CancellationToken token);
        #endregion

        #region utils
        /// <summary>
        /// Converts an ExpandedNodeId into a NodeId using the Session
        /// </summary>
        /// <param name="nodeid"></param>
        /// <returns>Resulting NodeId</returns>
        NodeId ToNodeId(ExpandedNodeId nodeid);
        /// <summary>
        /// Converts identifier string and namespaceUri into NodeId. Identifier will be on form i=123 or s=abc etc.
        /// </summary>
        /// <param name="identifier">Full identifier on form i=123 or s=abc etc.</param>
        /// <param name="namespaceUri">Full namespaceUri</param>
        /// <returns>Resulting NodeId</returns>
        NodeId ToNodeId(string identifier, string namespaceUri);
        /// <summary>
        /// Converts object fetched from ua server to string, contains cases for special types we want to represent in CDF
        /// </summary>
        /// <param name="value">Object to convert</param>
        /// <returns>Metadata suitable string</returns>
        string ConvertToString(object value);
        /// <summary>
        /// Returns consistent unique string representation of a <see cref="NodeId"/> given its namespaceUri
        /// </summary>
        /// <remarks>
        /// NodeId is, according to spec, unique in combination with its namespaceUri. We use this to generate a consistent, unique string
        /// to be used for mapping assets and timeseries in CDF to opcua nodes.
        /// To avoid having to send the entire namespaceUri to CDF, we allow mapping Uris to prefixes in the config file.
        /// </remarks>
        /// <param name="rNodeId">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        string GetUniqueId(ExpandedNodeId rNodeId, int index = -1);
        #endregion

    }
}
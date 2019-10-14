/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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

using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System.Linq;
using Prometheus.Client;
using System.Threading;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Client managing the connection to the opcua server, and providing wrapper methods to simplify interaction with the server.
    /// </summary>
    public class UAClient
    {
        private readonly UAClientConfig config;
        private readonly ExtractionConfig extractionConfig;
        private readonly EventConfig eventConfig;
        private Session session;
        private SessionReconnectHandler reconnectHandler;
        public Extractor Extractor { get; set; }
        private readonly object visitedNodesLock = new object();
        private readonly ISet<NodeId> visitedNodes = new HashSet<NodeId>();
        private readonly object subscriptionLock = new object();
        private readonly Dictionary<NodeId, string> nodeOverrides = new Dictionary<NodeId, string>();
        private Dictionary<NodeId, ProtoDataType> numericDataTypes = new Dictionary<NodeId, ProtoDataType>();
        public bool Started { get; private set; }
        public bool Failed { get; private set; }
        private readonly TimeSpan historyGranularity;
        private readonly DateTime historyStartTime;
        private CancellationToken liveToken;
        private Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> eventFields;

        private int pendingOperations;
        private readonly object pendingOpLock = new object();

        private static readonly Counter connects = Metrics
            .CreateCounter("opcua_connects", "Number of times the client has connected to and mapped the opcua server");
        private static readonly Gauge connected = Metrics
            .CreateGauge("opcua_connected", "Whether or not the client is currently connected to the opcua server");
        private static readonly Counter attributeRequests = Metrics
            .CreateCounter("opcua_attribute_requests", "Number of attributes fetched from the server");
        private static readonly Gauge numSubscriptions = Metrics
            .CreateGauge("opcua_subscriptions", "Number of variables with an active subscription");
        private static readonly Counter numHistoryReads = Metrics
            .CreateCounter("opcua_history_reads", "Number of historyread operations performed");
        private static readonly Counter numBrowse = Metrics
            .CreateCounter("opcua_browse_operations", "Number of browse operations performed");
        private static readonly Gauge depth = Metrics
            .CreateGauge("opcua_tree_depth", "Depth of node tree from rootnode");
        private static readonly Counter attributeRequestFailures = Metrics
            .CreateCounter("opcua_attribute_request_failures", "Number of failed requests for attributes to OPC-UA");
        private static readonly Counter historyReadFailures = Metrics
            .CreateCounter("opcua_history_read_fauilures", "Number of failed history read operations");
        private static readonly Counter browseFailures = Metrics
            .CreateCounter("opcua_browse_failures", "Number of failures on browse operations");

        /// <summary>
        /// Constructor, does not start the client.
        /// </summary>
        /// <param name="config">Full configuartion object</param>
        public UAClient(FullConfig config)
        {
            this.config = config.Source;
            extractionConfig = config.Extraction;
            eventConfig = config.Events;
            historyGranularity = config.Source.HistoryGranularity <= 0 ? TimeSpan.Zero
                : TimeSpan.FromSeconds(config.Source.HistoryGranularity);
            historyStartTime = DateTimeOffset.FromUnixTimeMilliseconds(config.Extraction.HistoryStartTime).DateTime;
        }
        #region Session management
        /// <summary>
        /// Entrypoint for starting the opcua session. Must be called before any further requests can be made.
        /// </summary>
        public async Task Run(CancellationToken token)
        {
            liveToken = token;
            await StartSession();
        }
        /// <summary>
        /// Close the session, cleaning up any client data on the server
        /// </summary>
        public void Close()
        {
            session.CloseSession(null, true);
            connected.Set(0);
        }
        /// <summary>
        /// Load security configuration for the session, then start the server.
        /// </summary>
        private async Task StartSession()
        {
            visitedNodes.Clear();
            // A restarted session might mean a restarted server, so all server-relevant data must be cleared.
            // This includes any stored NodeId, which may refer to an outdated namespaceIndex
            eventFields?.Clear();
            nodeOverrides?.Clear();
            numericDataTypes?.Clear();

            var application = new ApplicationInstance
            {
                ApplicationName = ".NET OPC-UA Extractor",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "opc.ua.net.extractor"
            };
            var appconfig = await application.LoadApplicationConfiguration($"{config.ConfigRoot}/opc.ua.net.extractor.Config.xml", false);
            var certificateDir = Environment.GetEnvironmentVariable("OPCUA_CERTIFICATE_DIR");
            if (!string.IsNullOrEmpty(certificateDir))
            {
                appconfig.SecurityConfiguration.ApplicationCertificate.StorePath = $"{certificateDir}/instance";
                appconfig.SecurityConfiguration.TrustedIssuerCertificates.StorePath = $"{certificateDir}/pki/issuer";
                appconfig.SecurityConfiguration.TrustedPeerCertificates.StorePath = $"{certificateDir}/pki/trusted";
                appconfig.SecurityConfiguration.RejectedCertificateStore.StorePath = $"{certificateDir}/pki/rejected";
            }

            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!validAppCert)
            {
                Log.Warning("Missing application certificate, using insecure connection.");
            }
            else
            {
                appconfig.ApplicationUri = Opc.Ua.Utils.GetApplicationUriFromCertificate(
                    appconfig.SecurityConfiguration.ApplicationCertificate.Certificate);
                config.AutoAccept |= appconfig.SecurityConfiguration.AutoAcceptUntrustedCertificates;
                appconfig.CertificateValidator.CertificateValidation += CertificateValidationHandler;
            }
            Log.Information("Attempt to select endpoint from: {EndpointURL}", config.EndpointURL);
            var selectedEndpoint = CoreClientUtils.SelectEndpoint(config.EndpointURL, validAppCert && config.Secure);
            var endpointConfiguration = EndpointConfiguration.Create(appconfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            Log.Information("Attempt to connect to endpoint with security: {SecurityPolicyUri}", endpoint.Description.SecurityPolicyUri);
            session = await Session.Create(
                appconfig,
                endpoint,
                false,
                ".NET OPC-UA Extractor Client",
                0,
                (config.Username == null || !config.Username.Trim().Any())
                    ? new UserIdentity(new AnonymousIdentityToken())
                    : new UserIdentity(config.Username, config.Password),
                null
            );

            session.KeepAlive += ClientKeepAlive;
            Started = true;
            connects.Inc();
            connected.Set(1);
            Log.Information("Successfully connected to server at {EndpointURL}", config.EndpointURL);

            if (extractionConfig.CustomNumericTypes != null)
            {
                numericDataTypes = extractionConfig.CustomNumericTypes.ToDictionary(elem => elem.NodeId.ToNodeId(this), elem => elem);
            }
        }
        /// <summary>
        /// Event triggered after a succesfull reconnect.
        /// </summary>
        private void ClientReconnectComplete(object sender, EventArgs eventArgs)
        {
            if (!ReferenceEquals(sender, reconnectHandler)) return;
            session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            Log.Warning("--- RECONNECTED ---");
            if (extractionConfig.CustomNumericTypes != null)
            {
                numericDataTypes = extractionConfig.CustomNumericTypes.ToDictionary(elem => elem.NodeId.ToNodeId(this), elem => elem);
            }
            nodeOverrides?.Clear();
            eventFields?.Clear();
            Task.Run(() => Extractor?.RestartExtractor(liveToken));
            lock (visitedNodesLock)
            {
                visitedNodes.Clear();
            }
            connects.Inc();
            connected.Set(1);
            reconnectHandler = null;

        }
        /// <summary>
        /// Called on client keep alive, handles the case where the server has stopped responding and the connection timed out.
        /// </summary>
        private void ClientKeepAlive(Session sender, KeepAliveEventArgs eventArgs)
        {
            if (eventArgs.Status != null && ServiceResult.IsNotGood(eventArgs.Status))
            {
                Log.Warning(eventArgs.Status.ToString());
                if (reconnectHandler == null)
                {
                    connected.Set(0);
                    Log.Warning("--- RECONNECTING ---");
                    if (!config.ForceRestart && !liveToken.IsCancellationRequested)
                    {
                        reconnectHandler = new SessionReconnectHandler();
                        reconnectHandler.BeginReconnect(sender, 5000, ClientReconnectComplete);
                    }
                    else
                    {
                        Failed = true;
                        try
                        {
                            session.Close();
                        }
                        catch
                        {
                            Log.Warning("Client failed to close");
                        }
                    }
                }
            }
        }
        /// <summary>
        /// Called after succesful validation of a server certificate. Handles the case where the certificate is untrusted.
        /// </summary>
        private void CertificateValidationHandler(CertificateValidator validator,
            CertificateValidationEventArgs eventArgs)
        {
            if (eventArgs.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                eventArgs.Accept = config.AutoAccept;
                // TODO Verify client acceptance here somehow?
                if (eventArgs.Accept)
                {
                    Log.Warning("Accepted Bad Certificate {CertificateSubject}", eventArgs.Certificate.Subject);
                }
                else
                {
                    Log.Error("Rejected Bad Certificate {CertificateSubject}", eventArgs.Certificate.Subject);
                }
            }
        }
        /// <summary>
        /// Safely increment number of active opcua operations
        /// </summary>
        private void IncOperations()
        {
            lock (pendingOpLock)
            {
                pendingOperations++;
            }
        }
        /// <summary>
        /// Safely decrement number of active opcua operations
        /// </summary>
        private void DecOperations()
        {
            lock (pendingOpLock)
            {
                pendingOperations--;
            }
        }
        /// <summary>
        /// Wait for all opcua operations to finish
        /// </summary>
        /// <returns></returns>
        public async Task WaitForOperations()
        {
            while (pendingOperations > 0) await Task.Delay(100);
        }
        #endregion

        #region Browse
        /// <summary>
        /// Browse an opcua directory, calling callback for all relevant nodes found.
        /// </summary>
        /// <param name="root">Initial node to start mapping. Will not be sent to callback</param>
        /// <param name="callback">Callback for each mapped node, takes a description of a single node, and its parent id</param>
        public async Task BrowseDirectoryAsync(NodeId root, Action<ReferenceDescription, NodeId> callback, CancellationToken token)
        {
            Log.Debug("BrowseDirectoryAsync {root}", root);
            lock (visitedNodesLock)
            {
                visitedNodes.Clear();
                visitedNodes.Add(root);
            }
            try
            {
                var rootNode = GetRootNode(root);
                if (rootNode == null) throw new Exception("Root node does not exist");
                callback(rootNode, null);
                await Task.Run(() => BrowseDirectory(new List<NodeId> { root }, callback, token), token);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to browse directory");
            }
        }
        /// <summary>
        /// Get the root node and return it as a reference description.
        /// </summary>
        /// <param name="nodeId">Id of the root node</param>
        /// <returns>A partial description of the root node</returns>
        private ReferenceDescription GetRootNode(NodeId nodeId)
        {
            var attributes = new List<uint>
            {
                Attributes.NodeId,
                Attributes.BrowseName,
                Attributes.DisplayName,
                Attributes.NodeClass
            };
            var readValueIds = attributes.Select(attr => new ReadValueId { NodeId = nodeId, AttributeId = attr }).ToList();
            session.Read(null, 0, TimestampsToReturn.Neither, new ReadValueIdCollection(readValueIds), out DataValueCollection results, out _);
            var refd = new ReferenceDescription();
            var enumerator = results.GetEnumerator();
            enumerator.MoveNext();
            refd.NodeId = enumerator.Current.GetValue(NodeId.Null);
            if (refd.NodeId == NodeId.Null) return null;
            enumerator.MoveNext();
            refd.BrowseName = enumerator.Current.GetValue(QualifiedName.Null);
            enumerator.MoveNext();
            refd.DisplayName = enumerator.Current.GetValue(LocalizedText.Null);
            enumerator.MoveNext();
            refd.NodeClass = (NodeClass)enumerator.Current.GetValue<int>(0);
            refd.ReferenceTypeId = null;
            refd.IsForward = true;
            return refd;
        }
        /// <summary>
        /// Add externalId override for a single node
        /// </summary>
        /// <param name="nodeId">Id of node to be overridden</param>
        /// <param name="externalId">ExternalId to be used</param>
        public void AddNodeOverride(NodeId nodeId, string externalId)
        {
            if (nodeId == null || nodeId == NodeId.Null) return;
            nodeOverrides[nodeId] = externalId;
        }
        /// <summary>
        /// Get all children of the given list of parents as a map from parentId to list of children descriptions
        /// </summary>
        /// <param name="parents">List of parents to browse</param>
        /// <param name="referenceTypes">Referencetype to browse, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes, as specified in the OPC-UA specification</param>
        /// <returns></returns>
        private Dictionary<NodeId, ReferenceDescriptionCollection> GetNodeChildren(
            IEnumerable<NodeId> parents,
            CancellationToken token,
            NodeId referenceTypes,
            uint nodeClassMask)
        {
            var finalResults = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            foreach (var lparents in Utils.ChunkBy(parents, config.BrowseChunk))
            {
                IncOperations();
                var tobrowse = new BrowseDescriptionCollection(lparents.Select(id =>
                    new BrowseDescription
                    {
                        NodeId = id,
                        ReferenceTypeId = referenceTypes ?? ReferenceTypeIds.HierarchicalReferences,
                        IncludeSubtypes = true,
                        NodeClassMask = nodeClassMask,
                        BrowseDirection = BrowseDirection.Forward,
                        ResultMask = (uint)BrowseResultMask.NodeClass | (uint)BrowseResultMask.DisplayName
                            | (uint)BrowseResultMask.ReferenceTypeId | (uint)BrowseResultMask.TypeDefinition
                    }
                ));
                try
                {
                    session.Browse(
                        null,
                        null,
                        0,
                        tobrowse,
                        out BrowseResultCollection results,
                        out DiagnosticInfoCollection diagnostics
                    );

                    foreach (var d in diagnostics)
                    {
                        Log.Warning("GetNodeChildren Browse diagnostics {msg}", d);
                    }

                    var indexMap = new NodeId[lparents.Count()];
                    var continuationPoints = new ByteStringCollection();
                    int index = 0;
                    int bindex = 0;
                    Log.Debug("GetNodeChildren lparents {count}", lparents.Count());
                    Log.Debug("GetNodeChildren results {count}", results.Count);
                    foreach (var result in results)
                    {
                        NodeId nodeId = lparents.ElementAt(bindex++);
                        Log.Debug("GetNodeChildren Browse result {nodeId}", nodeId);
                        finalResults[nodeId] = result.References;
                        if (result.ContinuationPoint != null)
                        {
                            indexMap[index++] = nodeId;
                            continuationPoints.Add(result.ContinuationPoint);
                        }
                    }
                    numBrowse.Inc();
                    while (continuationPoints.Any() && !token.IsCancellationRequested)
                    {
                        session.BrowseNext(
                            null,
                            false,
                            continuationPoints,
                            out BrowseResultCollection nextResults,
                            out DiagnosticInfoCollection diagnosticsNext
                        );

                        foreach (var d in diagnosticsNext)
                        {
                            Log.Warning("GetNodeChildren BrowseNext diagnostics {msg}", d);
                        }

                        int nindex = 0;
                        int pindex = 0;
                        continuationPoints.Clear();
                        foreach (var result in nextResults)
                        {
                            NodeId nodeId = indexMap[pindex++];
                            Log.Debug("GetNodeChildren BrowseNext result {nodeId}", nodeId);
                            finalResults[nodeId].AddRange(result.References);
                            if (result.ContinuationPoint != null)
                            {
                                indexMap[nindex++] = nodeId;
                                continuationPoints.Add(result.ContinuationPoint);
                            }
                        }

                        numBrowse.Inc();
                    }
                }
                catch (Exception)
                {
                    browseFailures.Inc();
                    Log.Error("Failed during browse session");
                    throw;
                }
                finally
                {
                    DecOperations();
                }
            }
            return finalResults;
        }
        /// <summary>
        /// Get all children of root nodes recursively and invoke the callback for each.
        /// </summary>
        /// <param name="roots">Root nodes to browse</param>
        /// <param name="callback">Callback for each node</param>
        /// <param name="referenceTypes">Permitted reference types, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes as described in the OPC-UA specification</param>
        private void BrowseDirectory(
            IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            NodeId referenceTypes = null,
            uint nodeClassMask = (uint)NodeClass.Variable | (uint)NodeClass.Object)
        {
            var nextIds = roots.ToList();
            int levelCnt = 0;
            int nodeCnt = 0;
            do
            {
                var references = Utils.ChunkBy(nextIds, config.BrowseNodesChunk)
                    .Select(ids => GetNodeChildren(ids, token, referenceTypes, nodeClassMask))
                    .SelectMany(dict => dict)
                    .ToDictionary(val => val.Key, val => val.Value);

                nextIds.Clear();
                levelCnt++;
                foreach (var rdlist in references)
                {
                    NodeId parentId = rdlist.Key;
                    nodeCnt += rdlist.Value.Count;
                    foreach (var rd in rdlist.Value)
                    {
                        if (rd.NodeId == ObjectIds.Server)
                            continue;
                        if (extractionConfig.IgnoreNamePrefix != null && extractionConfig.IgnoreNamePrefix.Any(prefix =>
                            rd.DisplayName.Text.StartsWith(prefix, StringComparison.CurrentCulture))
                            || extractionConfig.IgnoreName != null && extractionConfig.IgnoreName.Contains(rd.DisplayName.Text))
                        {
                            Log.Debug("Ignoring filtered {expandedNodeId} {nodeId}", rd.NodeId, ToNodeId(rd.NodeId));
                            continue;
                        }
                        lock (visitedNodesLock)
                        {
                            if (!visitedNodes.Add(ToNodeId(rd.NodeId)))
                            {
                                // if (Log.IsEnabled(Debug))
                                Log.Debug("Ignoring visited {expandedNodeId} {nodeId}", rd.NodeId, ToNodeId(rd.NodeId));
                                continue;
                            }
                        }
                        callback(rd, parentId);
                        if (rd.NodeClass == NodeClass.Variable)
                            continue;
                        nextIds.Add(ToNodeId(rd.NodeId));
                    }
                }
            } while (nextIds.Any());
            Log.Information("Found {NumUANodes} nodes in {NumNodeLevels} levels", nodeCnt, levelCnt);
            depth.Set(levelCnt);
        }
        #endregion

        #region Get node data
        /// <summary>
        /// Generates DataValueId pairs, then fetches a list of <see cref="DataValue"/>s from the opcua server 
        /// </summary>
        /// <param name="nodes">List of nodes to fetch attributes for</param>
        /// <param name="common">List of attributes to fetch for all nodes</param>
        /// <param name="variables">List of attributes to fetch for variable nodes only</param>
        /// <returns>A list of <see cref="DataValue"/>s</returns>
        private IEnumerable<DataValue> GetNodeAttributes(IEnumerable<BufferedNode> nodes,
            IEnumerable<uint> common,
            IEnumerable<uint> variables,
            CancellationToken token)
        {
            if (!nodes.Any()) return new List<DataValue>();
            var readValueIds = new ReadValueIdCollection();
            foreach (var node in nodes)
            {
                if (node == null) continue;
                foreach (var attribute in common)
                {
                    readValueIds.Add(new ReadValueId
                    {
                        AttributeId = attribute,
                        NodeId = node.Id
                    });
                }
                if (node.IsVariable)
                {
                    foreach (var attribute in variables)
                    {
                        readValueIds.Add(new ReadValueId
                        {
                            AttributeId = attribute,
                            NodeId = node.Id
                        });
                    }
                }
            }
            IEnumerable<DataValue> values = new DataValueCollection();
            IncOperations();
            try
            {
                int count = 0;
                foreach (var nextValues in Utils.ChunkBy(readValueIds, config.AttributesChunk))
                {
                    if (token.IsCancellationRequested) break;
                    count++;
                    session.Read(
                        null,
                        0,
                        TimestampsToReturn.Source,
                        new ReadValueIdCollection(nextValues),
                        out DataValueCollection lvalues,
                        out _
                    );
                    attributeRequests.Inc();
                    values = values.Concat(lvalues);
                    Log.Information("Read {NumAttributesRead} attributes", lvalues.Count);
                }
                Log.Information("Read {TotalAttributesRead} attributes with {NumAttributeReadOperations} operations", readValueIds.Count, count);
            }
            catch (Exception)
            {
                Log.Error("Failed to fetch attributes from opcua");
                attributeRequestFailures.Inc();
                throw;
            }
            finally
            {
                DecOperations();
            }
            return values;
        }
        /// <summary>
        /// Gets Description for all nodes, and DataType, Historizing and ValueRank for Variable nodes, then updates the given list of nodes
        /// </summary>
        /// <param name="nodes">Nodes to be updated with data from the opcua server</param>
        public void ReadNodeData(IEnumerable<BufferedNode> nodes, CancellationToken token)
        {
            nodes = nodes.Where(node => !node.IsVariable || node is BufferedVariable variable && variable.Index == -1);
            var variableAttributes = new List<uint>
            {
                Attributes.DataType,
                Attributes.Historizing,
                Attributes.ValueRank
            };
            if (extractionConfig.MaxArraySize > 0)
            {
                variableAttributes.Add(Attributes.ArrayDimensions);
            }
            var values = GetNodeAttributes(nodes, new List<uint>
            {
                Attributes.Description
            }, variableAttributes, token);
            var enumerator = values.GetEnumerator();
            foreach (BufferedNode node in nodes)
            {
                if (token.IsCancellationRequested) return;
                enumerator.MoveNext();
                node.Description = enumerator.Current.GetValue("");
                if (node.IsVariable && node is BufferedVariable vnode)
                {
                    enumerator.MoveNext();
                    NodeId dataType = enumerator.Current.GetValue(NodeId.Null);
                    vnode.SetDataType(dataType, numericDataTypes);
                    enumerator.MoveNext();
                    vnode.Historizing = config.History && enumerator.Current.GetValue(false);
                    enumerator.MoveNext();
                    vnode.ValueRank = enumerator.Current.GetValue(0);
                    if (extractionConfig.MaxArraySize > 0)
                    {
                        enumerator.MoveNext();
                        vnode.ArrayDimensions = (int[])enumerator.Current.GetValue(typeof(int[]));
                    }
                }
            }
        }
        /// <summary>
        /// Gets the values of the given list of variables, then updates each variable with a BufferedDataPoint
        /// </summary>
        /// <remarks>
        /// Note that there is a fixed maximum message size, and we here fetch a large number of values at the same time.
        /// To avoid complications, avoid fetching data of unknown large size here.
        /// Due to this, only nodes with ValueRank -1 (Scalar) will be fetched.
        /// </remarks>
        /// <param name="nodes">List of variables to be updated</param>
        public void ReadNodeValues(IEnumerable<BufferedVariable> nodes, CancellationToken token)
        {
            nodes = nodes.Where(node => !node.DataRead && node.Index == -1).ToList();
            var values = GetNodeAttributes(nodes.Where((BufferedVariable buff) => buff.ValueRank == ValueRanks.Scalar),
                new List<uint>(),
                new List<uint> { Attributes.Value },
                token
            );
            var enumerator = values.GetEnumerator();
            foreach (var node in nodes)
            {
                node.DataRead = true;
                if (node.ValueRank == ValueRanks.Scalar)
                {
                    enumerator.MoveNext();
                    node.SetDataPoint(enumerator.Current?.Value,
                        enumerator.Current == null ? enumerator.Current.SourceTimestamp : DateTime.MinValue,
                        this);
                }
            }
        }
        /// <summary>
        /// Gets properties for variables in nodes given, then updates all properties in given list of nodes with relevant data and values.
        /// </summary>
        /// <param name="nodes">Nodes to be updated with properties</param>
        public void GetNodeProperties(IEnumerable<BufferedNode> nodes, CancellationToken token)
        {
            var properties = new HashSet<BufferedVariable>();
            Log.Information("Get properties for {NumNodesToPropertyRead} nodes", nodes.Count());
            var idsToCheck = new List<NodeId>();
            foreach (var node in nodes)
            {
                if (node.IsVariable)
                {
                    if (node is BufferedVariable variable && variable.Index == -1)
                    {
                        idsToCheck.Add(node.Id);
                    }
                }
                else
                {
                    if (node.properties != null)
                    {
                        foreach (var property in node.properties)
                        {
                            properties.Add(property);
                        }
                    }
                }
            }
            var result = GetNodeChildren(idsToCheck, token, ReferenceTypeIds.HasProperty, (uint)NodeClass.Variable);
            foreach (var parent in nodes)
            {
                if (!result.ContainsKey(parent.Id)) continue;
                foreach (var child in result[parent.Id])
                {
                    var property = new BufferedVariable(ToNodeId(child.NodeId), child.DisplayName.Text, parent.Id) { IsProperty = true };
                    properties.Add(property);
                    if (parent.properties == null)
                    {
                        parent.properties = new List<BufferedVariable>();
                    }
                    parent.properties.Add(property);
                }
            }
            ReadNodeData(properties, token);
            ReadNodeValues(properties, token);
        }
        #endregion

        #region Synchronization
        /// <summary>
        /// General method to perform history-read operations for a list of nodes, with a given callback and HistoryReadDetails
        /// </summary>
        /// <param name="details">Fully configured HistoryReadDetails to be used</param>
        /// <param name="toRead">Collection of nodeIds to read</param>
        /// <param name="callback">
        /// Callback on each history read iteration.
        /// Takes result as IEncodable, bool indicating if this is the final iteration, source NodeId, HistoryReadDetails, and returns the number of nodes processed.
        /// </param>
        private void DoHistoryRead(HistoryReadDetails details,
            IEnumerable<NodeId> toRead,
            Func<IEncodeable, bool, NodeId, HistoryReadDetails, int> callback,
            CancellationToken token)
        {
            int opCnt = 0;
            int ptCnt = 0;
            IncOperations();
            var ids = new HistoryReadValueIdCollection();
            var indexMap = new NodeId[toRead.Count()];
            int index = 0;
            foreach (var id in toRead)
            {
                ids.Add(new HistoryReadValueId
                {
                    NodeId = id,
                });
                indexMap[index] = id;
                index++;
            }
            try
            {
                do
                {
                    session.HistoryRead(
                        null,
                        new ExtensionObject(details),
                        TimestampsToReturn.Source,
                        false,
                        ids,
                        out HistoryReadResultCollection results,
                        out _
                    );
                    numHistoryReads.Inc();
                    ids.Clear();
                    int prevIndex = 0;
                    int nextIndex = 0;
                    opCnt++;
                    foreach (var data in results)
                    {
                        var hdata = ExtensionObject.ToEncodeable(data.HistoryData);
                        ptCnt += callback(hdata, data == null || hdata == null || data.ContinuationPoint == null, indexMap[prevIndex], details);
                        if (data.ContinuationPoint != null)
                        {
                            ids.Add(new HistoryReadValueId
                            {
                                NodeId = indexMap[prevIndex],
                                ContinuationPoint = data.ContinuationPoint
                            });
                            indexMap[nextIndex] = indexMap[prevIndex];
                            nextIndex++;
                        }
                        prevIndex++;
                    }
                } while (ids.Any() && !token.IsCancellationRequested);
            }
            catch (Exception)
            {
                historyReadFailures.Inc();
                Log.Error("Failed during HistoryRead");
                throw;
            }
            finally
            {
                DecOperations();
                Log.Information("Fetched {NumHistoricalPoints} historical datapoints with {NumHistoryReadOperations} operations for {NumHistoryReadNodes} nodes",
                    ptCnt, opCnt, index);
            }
        }
        /// <summary>
        /// Read historydata for the requested nodes and call the callback after each call to HistoryRead
        /// </summary>
        /// <param name="toRead">Variables to read for</param>
        /// <param name="callback">Callback, takes a IEncodable which should be an instance of a HistoryData object,
        /// a bool indicating if this is the final iteration, the source NodeId, the ReadRawDetails used and returns the number of nodes processed</param>
        private void HistoryReadDataChunk(IEnumerable<NodeExtractionState> toRead,
            Func<IEncodeable, bool, NodeId, HistoryReadDetails, int> callback,
            CancellationToken token)
        {
            DateTime lowest = DateTime.MinValue;
            lowest = toRead.Select((bvar) => { return bvar.DestLatestTimestamp; }).Min();
            lowest = lowest < historyStartTime ? historyStartTime : lowest;
			
            var details = new ReadRawModifiedDetails
            {
                StartTime = lowest,
                EndTime = DateTime.Now.AddDays(1),
                NumValuesPerNode = (uint)config.HistoryReadChunk
            };
            try
            {
                DoHistoryRead(details, toRead.Select(bv => bv.Id), callback, token);
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// Read historydata for the requested nodes and call the callback after each call to HistoryRead, performs chunking according to config,
        /// limiting each chunk by size, as well as grouping by timestamp according to historyGranularity.
        /// </summary>
        /// <param name="toRead">NodeExtractionStates for historical nodes to be read.</param>
        /// <param name="callback">Callback, takes a IEncodable which should be an instance of a HistoryData object,
        /// a bool indicating if this is the final iteration, the source NodeId, the ReadRawDetails used and returns the number of nodes processed</param>
        public async Task HistoryReadData(IEnumerable<NodeExtractionState> toRead,
            Func<IEncodeable, bool, NodeId, HistoryReadDetails, int> callback,
            CancellationToken token)
        {
            var tasks = new List<Task>();
            if (historyGranularity == TimeSpan.Zero)
            {
                foreach (var state in toRead)
                {
                    if (state.Historizing)
                    {
                        tasks.Add(Task.Run(() => HistoryReadDataChunk(new List<NodeExtractionState> { state }, callback, token), token));
                    }
                    else
                    {
                        callback(null, true, state.Id, null);
                    }
                }
                try
                {
                    await Task.WhenAll(tasks);
                }
                catch (Exception)
                {
                    throw;
                }
                return;
            }
            int cnt = 0;
            var groupedStates = new Dictionary<long, IList<NodeExtractionState>>();
            foreach (var state in toRead)
            {
                if (state.Historizing)
                {
                    cnt++;
                    long group = state.DestLatestTimestamp.Ticks / historyGranularity.Ticks;
                    if (!groupedStates.ContainsKey(group))
                    {
                        groupedStates[group] = new List<NodeExtractionState>();
                    }
                    groupedStates[group].Add(state);
                }
                else
                {
                    callback(null, true, state.Id, null);
                }
            }
            if (!groupedStates.Any()) return;
            foreach (var nodes in groupedStates.Values)
            {
                foreach (var nextNodes in Utils.ChunkBy(nodes, config.HistoryReadNodesChunk))
                {
                    tasks.Add(Task.Run(() => HistoryReadDataChunk(nextNodes, callback, token)));
                }
            }
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// Read history data for a list of events, calls callback after each history read iteration.
        /// </summary>
        /// <param name="emitters">Nodes to be extracted from, these must all be emitters.</param>
        /// <param name="eventIds">Events to extract.</param>
        /// <param name="nodeIds">NodeIds permitted as SourceNode.</param>
        /// <param name="callback">Callback to be called for each iteration. Takes a HistoryEvent as an IEncodable,
        /// a bool indicating if this is the final iteration, NodeId of the node in question, ReadEventDetails containing the filter.
        /// Returns the number of events processed.</param>
        /// <param name="token"></param>
        public Task HistoryReadEvents(IEnumerable<NodeId> emitters,
            IEnumerable<NodeId> nodeIds,
            Func<IEncodeable, bool, NodeId, HistoryReadDetails, int> callback,
            CancellationToken token)
        {
            if (eventFields == null) throw new Exception("EventFields not defined");
            // Read the latest local event write time from file, then set the startTime to the largest of that minus 10 minutes, and
            // the HistoryStartTime config option. We have generally have no way of finding the latest event in the destinations,
            // so we approximate the time we want to read from using a local buffer.
            // We both filter on "ReceivedTime" on the server, and read from a specific time in HistoryRead. Servers have varying support
            // for this feature, as history read on events is a bit of an edge case.
            var latestTime = Utils.ReadLastEventTimestamp();
            var startTime = latestTime.Subtract(TimeSpan.FromMinutes(10));
            startTime = startTime < historyStartTime ? historyStartTime : startTime;
            var filter = BuildEventFilter(nodeIds, startTime);
            var details = new ReadEventDetails
            {
                StartTime = startTime,
                EndTime = DateTime.Now.AddDays(1),
                NumValuesPerNode = (uint)eventConfig.HistoryReadChunk,
                Filter = filter
            };
            return Task.Run(() => DoHistoryRead(details, emitters, callback, token));
        }
        /// <summary>
        /// Create datapoint subscriptions for given list of nodes
        /// </summary>
        /// <param name="nodeList">List of buffered variables to synchronize</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        public void SubscribeToNodes(IEnumerable<NodeExtractionState> nodeList,
            MonitoredItemNotificationEventHandler subscriptionHandler,
            CancellationToken token)
        {
            if (!nodeList.Any()) return;
            var subscription = session.Subscriptions.FirstOrDefault(sub => sub.DisplayName == "DataChangeListener");
            if (subscription == null)
            {
                subscription = new Subscription(session.DefaultSubscription)
                {
                    PublishingInterval = config.PollingInterval,
                    DisplayName = "DataChangeListener"
                };
            }
            int count = 0;
            var hasSubscription = subscription.MonitoredItems
                .Select(sub => sub.ResolvedNodeId)
                .ToHashSet();

            foreach (var chunk in Utils.ChunkBy(nodeList, config.SubscriptionChunk))
            {
                if (token.IsCancellationRequested) break;
                subscription.AddItems(chunk
                    .Where(node => !hasSubscription.Contains(node.Id))
                    .Select(node =>
                    {
                        var monitor = new MonitoredItem(subscription.DefaultItem)
                        {
                            StartNodeId = node.Id,
                            DisplayName = "Value: " + node.DisplayName,
                            SamplingInterval = config.PollingInterval
                        };
                        monitor.Notification += subscriptionHandler;
                        count++;
                        return monitor;
                    })
                );

                Log.Information("Add {NumAddedSubscriptions} subscriptions", chunk.Count());
                lock (subscriptionLock)
                {
                    IncOperations();
                    try
                    {
                        if (count > 0)
                        {
                            if (subscription.Created)
                            {
                                subscription.CreateItems();
                            }
                            else
                            {
                                session.AddSubscription(subscription);
                                subscription.Create();
                            }
                        }
                    }
                    catch (Exception)
                    {
                        Log.Error("Failed to create subscriptions");
                        throw;
                    }
                    finally
                    {
                        DecOperations();
                    }
                    numSubscriptions.Set(subscription.MonitoredItemCount);
                }
            }
            Log.Information("Added {TotalAddedSubscriptions} subscriptions", count);
        }
        /// <summary>
        /// Subscribe to events from the given list of emitters.
        /// </summary>
        /// <param name="emitters">List of ids of nodes to serve as emitters. These are the actual targets of the subscription.</param>
        /// <param name="eventIds">List of ids of events to subscribe to. Used to construct a filter.</param>
        /// <param name="nodeIds">List of ids of nodes permitted as SourceNodes. Used to construct a filter.</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        /// <returns>Map of fields, EventTypeId->(SourceTypeId, BrowseName)</returns>
        public void SubscribeToEvents(IEnumerable<NodeId> emitters,
            IEnumerable<NodeId> nodeIds,
            MonitoredItemNotificationEventHandler subscriptionHandler,
            CancellationToken token)
        {
            var subscription = session.Subscriptions.FirstOrDefault(sub => sub.DisplayName == "EventListener");
            if (subscription == null)
            {
                subscription = new Subscription(session.DefaultSubscription)
                {
                    PublishingInterval = config.PollingInterval,
                    DisplayName = "EventListener"
                };
            }
            int count = 0;
            var hasSubscription = subscription.MonitoredItems
                .Select(sub => sub.ResolvedNodeId)
                .ToHashSet();

            if (eventFields == null) throw new Exception("EventFields not defined");

            var filter = BuildEventFilter(nodeIds);
            foreach (var emitter in emitters)
            {
                if (token.IsCancellationRequested) return;
                if (!hasSubscription.Contains(emitter))
                {
                    var item = new MonitoredItem
                    {
                        StartNodeId = emitter,
                        Filter = filter,
                        AttributeId = Attributes.EventNotifier
                    };
                    count++;
                    item.Notification += subscriptionHandler;
                    subscription.AddItem(item);
                }
            }
            lock (subscriptionLock)
            {
                IncOperations();
                try
                {
                    if (count > 0)
                    {
                        if (subscription.Created)
                        {
                            subscription.CreateItems();
                        }
                        else
                        {
                            session.AddSubscription(subscription);
                            subscription.Create();
                        }
                    }
                }
                catch (Exception)
                {
                    Log.Error("Failed to create event subscriptions");
                    throw;
                }
                finally
                {
                    DecOperations();
                }
            }
            Log.Information("Created {EventSubCount} event subscriptions", count);
        }
        #endregion

        #region events
        public Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> GetEventFields(IEnumerable<NodeId> eventIds, CancellationToken token)
        {
            if (eventFields != null) return eventFields;
            var collector = new EventFieldCollector(this, eventIds);
            eventFields = collector.GetEventIdFields(token);
            return eventFields;
        }
        /// <summary>
        /// Constructs a filter from the given list of permitted SourceNodes, the already constructed field map and an optional receivedAfter property.
        /// </summary>
        /// <param name="nodeIds">Permitted SourceNode ids</param>
        /// <param name="receivedAfter">Optional, if defined, attempt to filter out events with [ReceiveTimeProperty] > receivedAfter</param>
        /// <returns>The final event filter</returns>
        private EventFilter BuildEventFilter(IEnumerable<NodeId> nodeIds,
            DateTime? receivedAfter = null)
        {
            /*
             * Essentially equivalent to SELECT Message, EventId, SourceNode, Time FROM [source] WHERE EventId IN eventIds AND SourceNode IN nodeIds;
             * using the internal query language in OPC-UA
             * If receivedAfter is specified, then also "AND [ReceiveTimeProperty] > receivedAfter"
             */
            var whereClause = new ContentFilter();
            var eventListOperand = new SimpleAttributeOperand
            {
                TypeDefinitionId = ObjectTypeIds.BaseEventType,
                AttributeId = Attributes.Value
            };
            eventListOperand.BrowsePath.Add(BrowseNames.EventType);
            IEnumerable<FilterOperand> eventOperands = eventFields.Keys.Select(id =>
                new LiteralOperand
                {
                    Value = id
                });
            // This does not do what it looks like, rather it replaces whatever operation exists in the where clause and returns 
            // this operation as a ContentFilterElement.
            var elem1 = whereClause.Push(FilterOperator.InList, eventOperands.Prepend(eventListOperand).ToArray());

            var nodeListOperand = new SimpleAttributeOperand
            {
                TypeDefinitionId = ObjectTypeIds.BaseEventType,
                AttributeId = Attributes.Value
            };
            nodeListOperand.BrowsePath.Add(BrowseNames.SourceNode);
            IEnumerable<FilterOperand> nodeOperands = nodeIds.Select(id =>
                new LiteralOperand
                {
                    Value = id
                });

            var elem2 = whereClause.Push(FilterOperator.InList, nodeOperands.Prepend(nodeListOperand).ToArray());
            var elem3 = whereClause.Push(FilterOperator.And, elem1, elem2);

            if (receivedAfter != null && receivedAfter > DateTime.MinValue)
            {
                var eventTimeOperand = new SimpleAttributeOperand
                {
                    TypeDefinitionId = ObjectTypeIds.BaseEventType,
                    AttributeId = Attributes.Value
                };
                eventTimeOperand.BrowsePath.Add(eventConfig.ReceiveTimeProperty);
                var timeOperand = new LiteralOperand
                {
                    Value = receivedAfter.Value
                };
                var elem4 = whereClause.Push(FilterOperator.GreaterThan, eventTimeOperand, timeOperand);
                whereClause.Push(FilterOperator.And, elem3, elem4);
            }

            var fieldList = eventFields
                .Aggregate((IEnumerable<(NodeId, QualifiedName)>)new List<(NodeId, QualifiedName)>(), (agg, kvp) => agg.Concat(kvp.Value))
                .GroupBy(variable => variable.Item2)
                .Select(items => items.FirstOrDefault());

            if (!fieldList.Any())
            {
                Log.Warning("Missing valid event fields, no results will be returned");
            }
            var selectClauses = new SimpleAttributeOperandCollection();
            foreach (var field in fieldList)
            {
                if (eventConfig.ExcludeProperties.Contains(field.Item2.Name)
                    || eventConfig.BaseExcludeProperties.Contains(field.Item2.Name) && field.Item1 == ObjectTypeIds.BaseEventType) continue;
                var operand = new SimpleAttributeOperand
                {
                    AttributeId = Attributes.Value,
                    TypeDefinitionId = field.Item1
                };
                operand.BrowsePath.Add(field.Item2);
                selectClauses.Add(operand);
                Log.Information("Select event attribute {id}: {name}", field.Item1, field.Item2);
            }
            return new EventFilter
            {
                WhereClause = whereClause,
                SelectClauses = selectClauses
            };
        }
        /// <summary>
        /// Collects the fields of a given list of eventIds. It does this by mapping out the entire event type hierarchy,
        /// and collecting the fields of each node on the way.
        /// </summary>
        private class EventFieldCollector
        {
            readonly UAClient UAClient;
            readonly Dictionary<NodeId, IEnumerable<ReferenceDescription>> properties = new Dictionary<NodeId, IEnumerable<ReferenceDescription>>();
            readonly Dictionary<NodeId, IEnumerable<ReferenceDescription>> localProperties = new Dictionary<NodeId, IEnumerable<ReferenceDescription>>();
            readonly IEnumerable<NodeId> targetEventIds;
            /// <summary>
            /// Construct the collector.
            /// </summary>
            /// <param name="parent">UAClient to be used for browse calls.</param>
            /// <param name="targetEventIds">Target event ids</param>
            public EventFieldCollector(UAClient parent, IEnumerable<NodeId> targetEventIds)
            {
                UAClient = parent;
                this.targetEventIds = targetEventIds;
            }
            /// <summary>
            /// Main collection function. Calls BrowseDirectory on BaseEventType, waits for it to complete, which should populate properties and localProperties,
            /// then collects the resulting fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).
            /// </summary>
            /// <returns>The collected fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).</returns>
            public Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> GetEventIdFields(CancellationToken token)
            {
                properties[ObjectTypeIds.BaseEventType] = new List<ReferenceDescription>();
                localProperties[ObjectTypeIds.BaseEventType] = new List<ReferenceDescription>();

                UAClient.BrowseDirectory(new List<NodeId> { ObjectTypeIds.BaseEventType },
                    EventTypeCallback, token, ReferenceTypeIds.HierarchicalReferences, (uint)NodeClass.ObjectType | (uint)NodeClass.Variable);
                var propVariables = new Dictionary<ExpandedNodeId, (NodeId, QualifiedName)>();
                foreach (var kvp in localProperties)
                {
                    foreach (var description in kvp.Value)
                    {
                        if (!propVariables.ContainsKey(description.NodeId))
                        {
                            propVariables[description.NodeId] = (kvp.Key, description.BrowseName);
                        }
                    }
                }
                return targetEventIds
                    .Where(id => properties.ContainsKey(id))
                    .ToDictionary(id => id, id => properties[id]
                        .Where(desc => propVariables.ContainsKey(desc.NodeId))
                        .Select(desc => propVariables[desc.NodeId]));
            }
            /// <summary>
            /// HandleNode callback for the event type mapping.
            /// </summary>
            /// <param name="child">Type or property to be handled</param>
            /// <param name="parent">Parent type id</param>
            private void EventTypeCallback(ReferenceDescription child, NodeId parent)
            {
                var id = UAClient.ToNodeId(child.NodeId);
                if (child.NodeClass == NodeClass.ObjectType && !properties.ContainsKey(id))
                {
                    var parentProperties = new List<ReferenceDescription>();
                    if (properties.ContainsKey(parent))
                    {
                        foreach (var prop in properties[parent])
                        {
                            parentProperties.Add(prop);
                        }
                    }
                    properties[id] = parentProperties;
                    localProperties[id] = new List<ReferenceDescription>();
                }
                if (child.ReferenceTypeId == ReferenceTypeIds.HasProperty)
                {
                    properties[parent] = properties[parent].Append(child);
                    localProperties[parent] = localProperties[parent].Append(child);
                }
            }

        }
        #endregion

        #region Utils
        /// <summary>
        /// Converts an ExpandedNodeId into a NodeId using the session
        /// </summary>
        /// <param name="nodeid"></param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(ExpandedNodeId nodeid)
        {
            return ExpandedNodeId.ToNodeId(nodeid, session.NamespaceUris);
        }
        /// <summary>
        /// Converts identifier string and namespaceUri into NodeId. Identifier will be on form i=123 or s=abc etc.
        /// </summary>
        /// <param name="identifier">Full identifier on form i=123 or s=abc etc.</param>
        /// <param name="namespaceUri">Full namespaceUri</param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(string identifier, string namespaceUri)
        {
            if (identifier == null || namespaceUri == null) return NodeId.Null;
            string nsString = "ns=" + session.NamespaceUris.GetIndex(namespaceUri);
            if (session.NamespaceUris.GetIndex(namespaceUri) == -1)
            {
                return NodeId.Null;
            }
            return new NodeId(nsString + ";" + identifier);
        }
        /// <summary>
        /// Convert a datavalue into a double representation, testing for edge cases.
        /// </summary>
        /// <param name="datavalue">Datavalue to be converted</param>
        /// <returns>Double value, will return 0 if the datavalue is invalid</returns>
        public static double ConvertToDouble(object datavalue)
        {
            if (datavalue == null) return 0;
            if (datavalue.GetType().IsArray)
            {
                return Convert.ToDouble((datavalue as IEnumerable<object>)?.First());
            }
            return Convert.ToDouble(datavalue);
        }
        /// <summary>
        /// Converts object fetched from ua server to string, contains cases for special types we want to represent in CDF
        /// </summary>
        /// <param name="value">Object to convert</param>
        /// <returns>Metadata suitable string</returns>
        public string ConvertToString(object value)
        {
            if (value == null) return "";
            if (value.GetType().IsArray)
            {
                string result = "[";
                if (value is object[] values)
                {
                    int count = 0;
                    foreach (var dvalue in values)
                    {
                        result += ((count++ > 0) ? ", " : "") + ConvertToString(dvalue);
                    }
                }
                return result + "]";
            }
            if (value.GetType() == typeof(NodeId))
            {
                return GetUniqueId((NodeId)value);
            }
            if (value.GetType() == typeof(ExpandedNodeId))
            {
                return GetUniqueId((NodeId)value);
            }
            if (value.GetType() == typeof(LocalizedText))
            {
                return ((LocalizedText)value).Text;
            }
            if (value.GetType() == typeof(ExtensionObject))
            {
                return ConvertToString(((ExtensionObject)value).Body);
            }
            if (value.GetType() == typeof(Opc.Ua.Range))
            {
                return $"({((Opc.Ua.Range)value).Low}, {((Opc.Ua.Range)value).High})";
            }
            if (value.GetType() == typeof(EUInformation))
            {
                return $"{((EUInformation)value).DisplayName}: {((EUInformation)value).Description}";
            }
            if (value.GetType() == typeof(EnumValueType))
            {
                return $"{((EnumValueType)value).DisplayName}: {((EnumValueType)value).Value}";
            }
            return value.ToString();
        }
        /// <summary>
        /// Converts DataValue fetched from ua server to string, contains cases for special types we want to represent in CDF
        /// </summary>
        /// <param name="datavalue">Datavalue to convert</param>
        /// <returns>Metadata suitable string</returns>
        public string ConvertToString(DataValue datavalue)
        {
            if (datavalue == null) return "";
            return ConvertToString(datavalue.Value);
        }
        /// <summary>
        /// Returns consistent unique string representation of a <see cref="NodeId"/> given its namespaceUri
        /// </summary>
        /// <remarks>
        /// NodeId is, according to spec, unique in combination with its namespaceUri. We use this to generate a consistent, unique string
        /// to be used for mapping assets and timeseries in CDF to opcua nodes.
        /// To avoid having to send the entire namespaceUri to CDF, we allow mapping Uris to prefixes in the config file.
        /// </remarks>
        /// <param name="nodeid">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        public string GetUniqueId(ExpandedNodeId nodeid, int index = -1)
        {
            if (nodeOverrides.ContainsKey((NodeId)nodeid)) return nodeOverrides[(NodeId)nodeid];

            string namespaceUri = nodeid.NamespaceUri;
            if (namespaceUri == null)
            {
                namespaceUri = session.NamespaceUris.GetString(nodeid.NamespaceIndex);
            }
            string prefix;
            if (extractionConfig.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode))
            {
                prefix = prefixNode;
            }
            else
            {
                prefix = namespaceUri;
            }
            // Strip the ns=namespaceIndex; part, as it may be inconsistent between sessions
            // We still want the identifierType part of the id, so we just remove the first ocurrence of ns=..
            // If we can find out if the value of the key alone is unique, then we can remove the identifierType, though I suspect
            // that i=1 and s=1 (1 as string key) would be considered distinct.
            string nodeidstr = nodeid.ToString();
            string nsstr = $"ns={nodeid.NamespaceIndex};";
            int pos = nodeidstr.IndexOf(nsstr, StringComparison.CurrentCulture);
            if (pos == 0)
            {
                nodeidstr = nodeidstr.Substring(0, pos) + nodeidstr.Substring(pos + nsstr.Length);
            }
            string extId = $"{extractionConfig.IdPrefix}{prefix}{nodeidstr}".Replace("\n", "");

            // ExternalId is limited to 128 characters
            extId = extId.Trim();
            if (extId.Length > 255)
            {
                if (index > -1)
                {
                    var indexSub = $"{index}";
                    return extId.Substring(0, 255 - indexSub.Length) + indexSub;
                }
                return extId.Substring(0, 255);
            }
            if (index > -1)
            {
                extId += $"[{index}]";
            }
            return extId;
        }
        #endregion
    }
}
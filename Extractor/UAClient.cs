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
using System.Collections.ObjectModel;
using System.Globalization;
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
    public class UAClient : IDisposable
    {
        private readonly UAClientConfig config;
        private readonly ExtractionConfig extractionConfig;
        private readonly EventConfig eventConfig;
        private readonly HistoryConfig historyConfig;
        protected Session Session { get; set; }
        protected ApplicationConfiguration Appconfig { get; set; }
        private SessionReconnectHandler reconnectHandler;
        public Extractor Extractor { get; set; }
        private readonly object visitedNodesLock = new object();
        protected ISet<NodeId> VisitedNodes { get; }= new HashSet<NodeId>();
        private readonly object subscriptionLock = new object();
        private readonly Dictionary<NodeId, string> nodeOverrides = new Dictionary<NodeId, string>();
        private Dictionary<NodeId, ProtoDataType> numericDataTypes = new Dictionary<NodeId, ProtoDataType>();
        public bool Started { get; private set; }
        private CancellationToken liveToken;
        private Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> eventFields;

        private int pendingOperations;

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
            .CreateCounter("opcua_history_read_failures", "Number of failed history read operations");
        private static readonly Counter browseFailures = Metrics
            .CreateCounter("opcua_browse_failures", "Number of failures on browse operations");

        private static readonly ILogger log = Log.Logger.ForContext(typeof(UAClient));

        /// <summary>
        /// Constructor, does not start the client.
        /// </summary>
        /// <param name="config">Full configuartion object</param>
        public UAClient(FullConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            this.config = config.Source;
            extractionConfig = config.Extraction;
            eventConfig = config.Events;
            historyConfig = config.History;
        }
        #region Session management
        /// <summary>
        /// Entrypoint for starting the opcua Session. Must be called before any further requests can be made.
        /// </summary>
        public async Task Run(CancellationToken token)
        {
            liveToken = token;
            await StartSession();
        }
        /// <summary>
        /// Close the Session, cleaning up any client data on the server
        /// </summary>
        public void Close()
        {
            if (!Session.Disposed)
            {
                Session.CloseSession(null, true);
            }
            connected.Set(0);
        }
        /// <summary>
        /// Load security configuration for the Session, then start the server.
        /// </summary>
        private async Task StartSession()
        {
            lock (visitedNodesLock)
            {
                VisitedNodes.Clear();
            }
            // A restarted Session might mean a restarted server, so all server-relevant data must be cleared.
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
            log.Information("Load OPC-UA Configuration from {root}/opc.ua.net.extractor.Config.xml", config.ConfigRoot);
            Appconfig = await application.LoadApplicationConfiguration($"{config.ConfigRoot}/opc.ua.net.extractor.Config.xml", false);
            string certificateDir = Environment.GetEnvironmentVariable("OPCUA_CERTIFICATE_DIR");
            if (!string.IsNullOrEmpty(certificateDir))
            {
                Appconfig.SecurityConfiguration.ApplicationCertificate.StorePath = $"{certificateDir}/instance";
                Appconfig.SecurityConfiguration.TrustedIssuerCertificates.StorePath = $"{certificateDir}/pki/issuer";
                Appconfig.SecurityConfiguration.TrustedPeerCertificates.StorePath = $"{certificateDir}/pki/trusted";
                Appconfig.SecurityConfiguration.RejectedCertificateStore.StorePath = $"{certificateDir}/pki/rejected";
            }

            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!validAppCert)
            {
                log.Warning("Missing application certificate, using insecure connection.");
            }
            else
            {
                Appconfig.ApplicationUri = Opc.Ua.Utils.GetApplicationUriFromCertificate(
                    Appconfig.SecurityConfiguration.ApplicationCertificate.Certificate);
                config.AutoAccept |= Appconfig.SecurityConfiguration.AutoAcceptUntrustedCertificates;
                Appconfig.CertificateValidator.CertificateValidation += CertificateValidationHandler;
            }
            log.Information("Attempt to select endpoint from: {EndpointURL}", config.EndpointURL);
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(config.EndpointURL, config.Secure);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.SelectEndpoint);
            }
            var endpointConfiguration = EndpointConfiguration.Create(Appconfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            log.Information("Attempt to connect to endpoint with security: {SecurityPolicyUri}", endpoint.Description.SecurityPolicyUri);
            try
            {
                Session?.Dispose();
                Session = await Session.Create(
                    Appconfig,
                    endpoint,
                    false,
                    ".NET OPC-UA Extractor Client",
                    0,
                    (config.Username == null || !config.Username.Trim().Any())
                        ? new UserIdentity(new AnonymousIdentityToken())
                        : new UserIdentity(config.Username, config.Password),
                    null
                );
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.CreateSession);
            }


            Session.KeepAlive += ClientKeepAlive;
            Started = true;
            connects.Inc();
            connected.Set(1);
            log.Information("Successfully connected to server at {EndpointURL}", config.EndpointURL);

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
            if (reconnectHandler == null) return;
            Session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            log.Warning("--- RECONNECTED ---");
            if (extractionConfig.CustomNumericTypes != null)
            {
                numericDataTypes = extractionConfig.CustomNumericTypes.ToDictionary(elem => elem.NodeId.ToNodeId(this), elem => elem);
            }
            nodeOverrides?.Clear();
            eventFields?.Clear();
            Task.Run(() => Extractor?.RestartExtractor());
            lock (visitedNodesLock)
            {
                VisitedNodes.Clear();
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
            if (eventArgs.Status == null || !ServiceResult.IsNotGood(eventArgs.Status)) return;
            log.Warning(eventArgs.Status.ToString());
            if (reconnectHandler != null) return;
            connected.Set(0);
            log.Warning("--- RECONNECTING ---");
            if (!config.ForceRestart && !liveToken.IsCancellationRequested)
            {
                reconnectHandler = new SessionReconnectHandler();
                reconnectHandler.BeginReconnect(sender, 5000, ClientReconnectComplete);
            }
            else
            {
                try
                {
                    Session.Close();
                    Extractor?.QuitExtractorInternally();
                }
                catch
                {
                    log.Warning("Client failed to close");
                }
            }
        }
        /// <summary>
        /// Called after succesful validation of a server certificate. Handles the case where the certificate is untrusted.
        /// </summary>
        private void CertificateValidationHandler(CertificateValidator validator,
            CertificateValidationEventArgs eventArgs)
        {
            if (eventArgs.Error.StatusCode != StatusCodes.BadCertificateUntrusted) return;
            eventArgs.Accept = config.AutoAccept;
            // TODO Verify client acceptance here somehow?
            if (eventArgs.Accept)
            {
                log.Warning("Accepted Bad Certificate {CertificateSubject}", eventArgs.Certificate.Subject);
            }
            else
            {
                log.Error("Rejected Bad Certificate {CertificateSubject}", eventArgs.Certificate.Subject);
            }
        }
        /// <summary>
        /// Safely increment number of active opcua operations
        /// </summary>
        private void IncOperations()
        {
            Interlocked.Increment(ref pendingOperations);
        }
        /// <summary>
        /// Safely decrement number of active opcua operations
        /// </summary>
        private void DecOperations()
        {
            Interlocked.Decrement(ref pendingOperations);
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

        public Task BrowseNodeHierarchy(NodeId root, Action<ReferenceDescription, NodeId> callback, CancellationToken token)
        {
            return BrowseNodeHierarchy(new[] {root}, callback, token);
        }
        /// <summary>
        /// Browse an opcua directory, calling callback for all relevant nodes found.
        /// </summary>
        /// <param name="roots">Initial nodes to start mapping.</param>
        /// <param name="callback">Callback for each mapped node, takes a description of a single node, and its parent id</param>
        public async Task BrowseNodeHierarchy(IEnumerable<NodeId> roots, Action<ReferenceDescription, NodeId> callback, CancellationToken token)
        {
            if (roots == null) throw new ArgumentNullException(nameof(roots));
            log.Debug("Browse node tree for nodes {nodes}", string.Join(", ", roots));
            foreach (var root in roots)
            {
                bool docb = true;
                lock (visitedNodesLock)
                {
                    if (!VisitedNodes.Add(root))
                    {
                        docb = false;
                    }
                }
                if (docb)
                {
                    var rootNode = GetRootNode(root);
                    if (rootNode == null) throw new ExtractorFailureException($"Root node does not exist: {root}");
                    callback(rootNode, null);
                }
            }
            await Task.Run(() => BrowseDirectory(roots, callback, token), token);
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
            DataValueCollection results;
            try
            {
                Session.Read(null, 0, TimestampsToReturn.Neither, new ReadValueIdCollection(readValueIds), out results, out _);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.ReadRootNode);
            }
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
            refd.NodeClass = (NodeClass)enumerator.Current.GetValue(0);
            refd.ReferenceTypeId = null;
            refd.IsForward = true;
            enumerator.Dispose();
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
            NodeId referenceTypes,
            uint nodeClassMask,
            CancellationToken token)
        {
            var finalResults = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            IncOperations();
            var tobrowse = new BrowseDescriptionCollection(parents.Select(id =>
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
            if (!parents.Any())
            {
                DecOperations();
                return finalResults;
            }
            try
            {
                BrowseResultCollection results;
                DiagnosticInfoCollection diagnostics;
                try
                {
                    Session.Browse(
                        null,
                        null,
                        (uint)config.BrowseChunk,
                        tobrowse,
                        out results,
                        out diagnostics
                    );
                }
                catch (ServiceResultException ex)
                {
                    throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.Browse);
                }

                var indexMap = new NodeId[parents.Count()];
                var continuationPoints = new ByteStringCollection();
                int index = 0;
                int bindex = 0;
                log.Debug("GetNodeChildren parents {count}", parents.Count());
                log.Debug("GetNodeChildren results {count}", results.Count);
                foreach (var result in results)
                {
                    var nodeId = parents.ElementAt(bindex++);
                    log.Debug("GetNodeChildren Browse result {nodeId}", nodeId);
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
                    try
                    {
                        Session.BrowseNext(
                            null,
                            false,
                            continuationPoints,
                            out results,
                            out diagnostics
                        );
                    }
                    catch (ServiceResultException ex)
                    {
                        throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.BrowseNext);
                    }


                    foreach (var d in diagnostics)
                    {
                        log.Warning("GetNodeChildren BrowseNext diagnostics {msg}", d);
                    }

                    int nindex = 0;
                    int pindex = 0;
                    continuationPoints.Clear();
                    foreach (var result in results)
                    {
                        var nodeId = indexMap[pindex++];
                        log.Debug("GetNodeChildren BrowseNext result {nodeId}", nodeId);
                        finalResults[nodeId].AddRange(result.References);
                        if (result.ContinuationPoint == null) continue;
                        indexMap[nindex++] = nodeId;
                        continuationPoints.Add(result.ContinuationPoint);
                    }

                    numBrowse.Inc();
                }
            }
            catch
            {
                browseFailures.Inc();
                throw;
            }
            finally
            {
                DecOperations();
            }
            return finalResults;
        }

        public void ResetVisitedNodes()
        {
            lock (visitedNodesLock)
            {
                VisitedNodes.Clear();
            }
        }
        /// <summary>
        /// Get all children of root nodes recursively and invoke the callback for each.
        /// </summary>
        /// <param name="roots">Root nodes to browse</param>
        /// <param name="callback">Callback for each node</param>
        /// <param name="referenceTypes">Permitted reference types, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes as described in the OPC-UA specification</param>
        protected void BrowseDirectory(
            IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            NodeId referenceTypes = null,
            uint nodeClassMask = (uint)NodeClass.Variable | (uint)NodeClass.Object,
            bool ignoreVisited = true)
        {
            if (roots == null) throw new ArgumentNullException(nameof(roots));
            var nextIds = roots.ToList();
            int levelCnt = 0;
            int nodeCnt = 0;
            var localVisitedNodes = new HashSet<NodeId>();
            foreach (var root in roots)
            {
                localVisitedNodes.Add(root);
            }
            do
            {
                var references = ExtractorUtils.ChunkBy(nextIds, config.BrowseNodesChunk)
                    .Select(ids => GetNodeChildren(ids.ToList(), referenceTypes, nodeClassMask, token))
                    .SelectMany(dict => dict)
                    .ToDictionary(val => val.Key, val => val.Value);

                nextIds.Clear();
                levelCnt++;
                foreach (var (parentId, children) in references)
                {
                    nodeCnt += children.Count;
                    foreach (var rd in children)
                    {
                        var nodeId = ToNodeId(rd.NodeId);
                        if (rd.NodeId == ObjectIds.Server) continue;
                        if (extractionConfig.IgnoreNamePrefix != null && extractionConfig.IgnoreNamePrefix.Any(prefix =>
                            rd.DisplayName.Text.StartsWith(prefix, StringComparison.CurrentCulture))
                            || extractionConfig.IgnoreName != null && extractionConfig.IgnoreName.Contains(rd.DisplayName.Text))
                        {
                            log.Verbose("Ignoring filtered {nodeId}", nodeId);
                            continue;
                        }

                        bool docb = true;
                        lock (visitedNodesLock)
                        {
                            if (!VisitedNodes.Add(nodeId) && ignoreVisited)
                            {
                                docb = false;
                                log.Verbose("Ignoring visited {nodeId}", nodeId);
                            }
                        }
                        if (docb)
                        {
                            log.Debug("Discovered new node {nodeid}", nodeId);
                            callback(rd, parentId);
                        }
                        if (rd.NodeClass == NodeClass.Variable) continue;
                        if (localVisitedNodes.Add(nodeId) || !ignoreVisited)
                        {
                            nextIds.Add(nodeId);
                        }
                    }
                }
            } while (nextIds.Any());
            log.Information("Found {NumUANodes} nodes in {NumNodeLevels} levels", nodeCnt, levelCnt);
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
                readValueIds.AddRange(common.Select(attribute => new ReadValueId {AttributeId = attribute, NodeId = node.Id}));
                if (node.IsVariable)
                {
                    readValueIds.AddRange(variables.Select(attribute => new ReadValueId {AttributeId = attribute, NodeId = node.Id}));
                }
            }
            IEnumerable<DataValue> values = new DataValueCollection();
            IncOperations();
            try
            {
                int count = 0;
                foreach (var nextValues in ExtractorUtils.ChunkBy(readValueIds, config.AttributesChunk))
                {
                    if (token.IsCancellationRequested) break;
                    count++;
                    Session.Read(
                        null,
                        0,
                        TimestampsToReturn.Source,
                        new ReadValueIdCollection(nextValues),
                        out DataValueCollection lvalues,
                        out _
                    );
                    attributeRequests.Inc();
                    values = values.Concat(lvalues);
                    log.Information("Read {NumAttributesRead} attributes", lvalues.Count);
                }
                log.Information("Read {TotalAttributesRead} attributes with {NumAttributeReadOperations} operations for {numNodesRead} nodes",
                    readValueIds.Count, count, nodes.Count());
            }
            catch
            {
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

            IEnumerable<DataValue> values;
            try
            {
                values = GetNodeAttributes(nodes, new List<uint>
                {
                    Attributes.Description
                }, variableAttributes, token);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.ReadAttributes);
            }

            if (values.Count() < nodes.Count(node => node.IsVariable) * 3 + nodes.Count())
            {
                throw new ExtractorFailureException("Too few results in ReadNodeData, this is a bug in the OPC-UA server implementation");
            }

            var enumerator = values.GetEnumerator();
            foreach (var node in nodes)
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
                    vnode.Historizing = historyConfig.Enabled && enumerator.Current.GetValue(false);
                    enumerator.MoveNext();
                    vnode.ValueRank = enumerator.Current.GetValue(0);
                    if (extractionConfig.MaxArraySize > 0)
                    {
                        enumerator.MoveNext();
                        vnode.ArrayDimensions = new Collection<int>((int[])enumerator.Current.GetValue(typeof(int[])));
                    }
                }
            }
            enumerator.Dispose();
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
            IEnumerable<DataValue> values;
            try
            {
                values = GetNodeAttributes(nodes.Where(buff => buff.ValueRank == ValueRanks.Scalar),
                    new List<uint>(),
                    new List<uint> {Attributes.Value},
                    token
                );
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.ReadAttributes);
            }

            var enumerator = values.GetEnumerator();
            foreach (var node in nodes)
            {
                node.DataRead = true;
                if (node.ValueRank == ValueRanks.Scalar)
                {
                    enumerator.MoveNext();
                    node.SetDataPoint(enumerator.Current?.Value,
                        enumerator.Current?.SourceTimestamp ?? DateTime.MinValue,
                        this);
                }
            }
            enumerator.Dispose();
        }
        /// <summary>
        /// Gets properties for variables in nodes given, then updates all properties in given list of nodes with relevant data and values.
        /// </summary>
        /// <param name="nodes">Nodes to be updated with properties</param>
        public void GetNodeProperties(IEnumerable<BufferedNode> nodes, CancellationToken token)
        {
            if (nodes == null || !nodes.Any()) return;
            var properties = new HashSet<BufferedVariable>();
            log.Information("Get properties for {NumNodesToPropertyRead} nodes", nodes.Count());
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
                    if (node.Properties != null)
                    {
                        foreach (var property in node.Properties)
                        {
                            properties.Add(property);
                        }
                    }
                }
            }
            var result = GetNodeChildren(idsToCheck, ReferenceTypeIds.HasProperty, (uint)NodeClass.Variable, token);
            foreach (var parent in nodes)
            {
                if (!result.ContainsKey(parent.Id)) continue;
                foreach (var child in result[parent.Id])
                {
                    var property = new BufferedVariable(ToNodeId(child.NodeId), child.DisplayName.Text, parent.Id) { IsProperty = true };
                    properties.Add(property);
                    if (parent.Properties == null)
                    {
                        parent.Properties = new List<BufferedVariable>();
                    }
                    parent.Properties.Add(property);
                }
            }
            ReadNodeData(properties, token);
            ReadNodeValues(properties, token);
        }
        #endregion

        #region Synchronization


        /// <summary>
        /// Modifies passed HistoryReadParams while doing a single config-limited iteration of history read.
        /// </summary>
        /// <param name="readParams"></param>
        /// <returns>Pairs of NodeId and history read results as IEncodable</returns>
        public IEnumerable<(NodeId, IEncodeable)> DoHistoryRead(HistoryReadParams readParams)
        {
            if (readParams == null) throw new ArgumentNullException(nameof(readParams));
            IncOperations();
            var ids = new HistoryReadValueIdCollection();
            var nodesIndices = readParams.Nodes.ToArray();
            foreach (var id in readParams.Nodes)
            {
                ids.Add(new HistoryReadValueId
                {
                    NodeId = id,
                    ContinuationPoint = readParams.ContinuationPoints[id]
                });
            }

            var result = new List<(NodeId, IEncodeable)>();
            try
            {
                Session.HistoryRead(
                    null,
                    new ExtensionObject(readParams.Details),
                    TimestampsToReturn.Source,
                    false,
                    ids,
                    out HistoryReadResultCollection results,
                    out _
                );
                numHistoryReads.Inc();
                int idx = 0;
                foreach (var data in results)
                {
                    var nodeId = nodesIndices[idx];
                    result.Add((nodeId, ExtensionObject.ToEncodeable(data.HistoryData)));
                    if (data.ContinuationPoint == null)
                    {
                        readParams.Completed[nodeId] = true;
                    }
                    else
                    {
                        readParams.ContinuationPoints[nodeId] = data.ContinuationPoint;
                    }

                    idx++;
                }

                log.Debug("Fetched historical "
                          + (readParams.Details is ReadEventDetails ? "events" : "datapoints")
                          + " for {nodeCount} nodes", readParams.Nodes.Count());
            }
            catch (ServiceResultException ex)
            {
                historyReadFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(ex, readParams.Details is ReadEventDetails
                    ? ExtractorUtils.SourceOp.HistoryReadEvents
                    : ExtractorUtils.SourceOp.HistoryRead);
            }
            catch
            {
                historyReadFailures.Inc();
                throw;
            }
            finally
            {
                DecOperations();
            }

            return result;
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

            var subscription = Session.Subscriptions.FirstOrDefault(sub => sub.DisplayName.StartsWith("DataChangeListener", StringComparison.InvariantCulture))
#pragma warning disable CA2000 // Dispose objects before losing scope. The subscription is disposed properly or added to the client.
                               ?? new Subscription(Session.DefaultSubscription)
            {
                PublishingInterval = config.PollingInterval,
                DisplayName = "DataChangeListener"
            };
#pragma warning restore CA2000 // Dispose objects before losing scope
            int count = 0;
            var hasSubscription = subscription.MonitoredItems
                .Select(sub => sub.ResolvedNodeId)
                .ToHashSet();

            foreach (var chunk in ExtractorUtils.ChunkBy(nodeList, config.SubscriptionChunk))
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

                log.Information("Add subscriptions for {numnodes} nodes", chunk.Count());
                lock (subscriptionLock)
                {
                    IncOperations();
                    try
                    {
                        if (count > 0)
                        {
                            if (subscription.Created)
                            {
                                try
                                {
                                    subscription.CreateItems();
                                }
                                catch (ServiceResultException ex)
                                {
                                    throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.CreateMonitoredItems);
                                }
                            }
                            else
                            {
                                try
                                {
                                    Session.AddSubscription(subscription);
                                    subscription.Create();
                                }
                                catch (ServiceResultException ex)
                                {
                                    throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.CreateSubscription);
                                }
                            }
                        }
                        else if (!subscription.Created)
                        {
                            subscription.Dispose();
                        }
                    }
                    finally
                    {
                        DecOperations();
                    }
                    numSubscriptions.Set(subscription.MonitoredItemCount);
                }
            }
            log.Information("Added {TotalAddedSubscriptions} subscriptions", count);
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
            if (emitters == null) throw new ArgumentNullException(nameof(emitters));
            if (nodeIds == null) throw new ArgumentNullException(nameof(nodeIds));
            var subscription = Session.Subscriptions.FirstOrDefault(sub => sub.DisplayName.StartsWith("EventListener", StringComparison.InvariantCulture))
#pragma warning disable CA2000 // Dispose objects before losing scope
                               ?? new Subscription(Session.DefaultSubscription)
            {
                PublishingInterval = config.PollingInterval,
                DisplayName = "EventListener"
            };
#pragma warning restore CA2000 // Dispose objects before losing scope
            int count = 0;
            var hasSubscription = subscription.MonitoredItems
                .Select(sub => sub.ResolvedNodeId)
                .ToHashSet();

            if (eventFields == null) throw new ExtractorFailureException("EventFields not defined");

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
                            try
                            {
                                subscription.CreateItems();
                            }
                            catch (ServiceResultException ex)
                            {
                                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.CreateMonitoredItems);
                            }
                        }
                        else
                        {
                            try
                            {
                                Session.AddSubscription(subscription);
                                subscription.Create();
                            }
                            catch (ServiceResultException ex)
                            {
                                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.CreateSubscription);
                            }

                        }
                    }
                    else if (!subscription.Created)
                    {
                        subscription.Dispose();
                    }
                }
                finally
                {
                    DecOperations();
                }
            }
            log.Information("Created {EventSubCount} event subscriptions", count);
        }
        #endregion

        #region Events

        public ISystemContext GetSystemContext()
        {
            return Session?.SystemContext;
        }
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
        public EventFilter BuildEventFilter(IEnumerable<NodeId> nodeIds)
        {
            /*
             * Essentially equivalent to SELECT Message, EventId, SourceNode, Time FROM [source] WHERE EventId IN eventIds AND SourceNode IN nodeIds;
             * using the internal query language in OPC-UA
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
            var elem1 = whereClause.Push(FilterOperator.InList, eventOperands.Prepend(eventListOperand).ToArray<object>());

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

            var elem2 = whereClause.Push(FilterOperator.InList, nodeOperands.Prepend(nodeListOperand).ToArray<object>());
            whereClause.Push(FilterOperator.And, elem1, elem2);

            var fieldList = eventFields
                .Aggregate((IEnumerable<(NodeId, QualifiedName)>)new List<(NodeId, QualifiedName)>(), (agg, kvp) => agg.Concat(kvp.Value))
                .GroupBy(variable => variable.Item2)
                .Select(items => items.FirstOrDefault());

            if (!fieldList.Any())
            {
                log.Warning("Missing valid event fields, no results will be returned");
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
                log.Debug("Select event attribute {id}: {name}", field.Item1, field.Item2);
            }
            return new EventFilter
            {
                WhereClause = whereClause,
                SelectClauses = selectClauses
            };
        }

        private static EventFilter BuildAuditFilter()
        {
            var whereClause = new ContentFilter();
            var eventTypeOperand = new SimpleAttributeOperand
            {
                TypeDefinitionId = ObjectTypeIds.BaseEventType,
                AttributeId = Attributes.Value
            };
            eventTypeOperand.BrowsePath.Add(BrowseNames.EventType);
            var op1 = new LiteralOperand
            {
                Value = ObjectTypeIds.AuditAddNodesEventType
            };
            var op2 = new LiteralOperand
            {
                Value = ObjectTypeIds.AuditAddReferencesEventType
            };
            var elem1 = whereClause.Push(FilterOperator.Equals, eventTypeOperand, op1);
            var elem2 = whereClause.Push(FilterOperator.Equals, eventTypeOperand, op2);
            whereClause.Push(FilterOperator.Or, elem1, elem2);
            var selectClauses = new SimpleAttributeOperandCollection();
            foreach ((var source, string path) in new[]
            {
                (ObjectTypeIds.BaseEventType, BrowseNames.EventType),
                (ObjectTypeIds.AuditAddNodesEventType, BrowseNames.NodesToAdd),
                (ObjectTypeIds.AuditAddReferencesEventType, BrowseNames.ReferencesToAdd)
            })
            {
                var op = new SimpleAttributeOperand
                {
                    AttributeId = Attributes.Value,
                    TypeDefinitionId = source
                };
                op.BrowsePath.Add(path);
                selectClauses.Add(op);
            }

            return new EventFilter
            {
               WhereClause = whereClause,
                SelectClauses = selectClauses
            };
        }
        public void SubscribeToAuditEvents(MonitoredItemNotificationEventHandler callback)
        {
            var filter = BuildAuditFilter();
            var subscription = Session.Subscriptions.FirstOrDefault(sub => sub.DisplayName.StartsWith("AuditListener", StringComparison.InvariantCulture))
#pragma warning disable CA2000 // Dispose objects before losing scope
                               ?? new Subscription(Session.DefaultSubscription)
            {
                PublishingInterval = config.PollingInterval,
                DisplayName = "AuditListener"
            };
#pragma warning restore CA2000 // Dispose objects before losing scope
            if (subscription.MonitoredItemCount != 0) return;
            var item = new MonitoredItem
            {
                StartNodeId = ObjectIds.Server,
                Filter = filter,
                AttributeId = Attributes.EventNotifier
            };
            item.Notification += callback;
            subscription.AddItem(item);
            log.Information("Subscribe to auditing events on the server node");
            lock (subscriptionLock)
            {
                IncOperations();
                try
                {
                    if (subscription.Created && subscription.MonitoredItemCount == 0)
                    { 
                        subscription.CreateItems();
                    }
                    else if (!subscription.Created)
                    {
                        log.Information("Add subscription to the Session");
                        Session.AddSubscription(subscription); 
                        subscription.Create();
                    }
                    else
                    {
                        subscription.Dispose();
                    }
                }
                catch (Exception)
                {
                    log.Error("Failed to create audit subscription");
                    throw;
                }
                finally
                {
                    DecOperations();
                }
            }
        }
        /// <summary>
        /// Collects the fields of a given list of eventIds. It does this by mapping out the entire event type hierarchy,
        /// and collecting the fields of each node on the way.
        /// </summary>
        private class EventFieldCollector
        {
            readonly UAClient uaClient;
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
                uaClient = parent;
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

                uaClient.BrowseDirectory(new List<NodeId> { ObjectTypeIds.BaseEventType },
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
                var id = uaClient.ToNodeId(child.NodeId);
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
        /// Converts an ExpandedNodeId into a NodeId using the Session
        /// </summary>
        /// <param name="nodeid"></param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(ExpandedNodeId nodeid)
        {
            return ExpandedNodeId.ToNodeId(nodeid, Session.NamespaceUris);
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
            string nsString = "ns=" + Session.NamespaceUris.GetIndex(namespaceUri);
            if (Session.NamespaceUris.GetIndex(namespaceUri) == -1)
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
                return Convert.ToDouble((datavalue as IEnumerable<object>)?.First(), CultureInfo.InvariantCulture);
            }
            return Convert.ToDouble(datavalue, CultureInfo.InvariantCulture);
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
        /// Returns consistent unique string representation of a <see cref="NodeId"/> given its namespaceUri
        /// </summary>
        /// <remarks>
        /// NodeId is, according to spec, unique in combination with its namespaceUri. We use this to generate a consistent, unique string
        /// to be used for mapping assets and timeseries in CDF to opcua nodes.
        /// To avoid having to send the entire namespaceUri to CDF, we allow mapping Uris to prefixes in the config file.
        /// </remarks>
        /// <param name="nodeid">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        public string GetUniqueId(ExpandedNodeId rNodeid, int index = -1)
        {
            NodeId nodeId = (NodeId)rNodeid;
            if (nodeId == null)
            {
                nodeId = NodeId.Null;
                log.Warning("Null converted to ExternalId");
                throw new Exception("Null converted to ExternalId");
            }
            if (nodeOverrides.ContainsKey(nodeId)) return nodeOverrides[nodeId];

            string namespaceUri = Session.NamespaceUris.GetString(nodeId.NamespaceIndex);
            string prefix = extractionConfig.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");
            // Strip the ns=namespaceIndex; part, as it may be inconsistent between sessions
            // We still want the identifierType part of the id, so we just remove the first ocurrence of ns=..
            // If we can find out if the value of the key alone is unique, then we can remove the identifierType, though I suspect
            // that i=1 and s=1 (1 as string key) would be considered distinct.
            string nodeidstr = nodeId.ToString();
            string nsstr = $"ns={nodeId.NamespaceIndex};";
            int pos = nodeidstr.IndexOf(nsstr, StringComparison.CurrentCulture);
            if (pos == 0)
            {
                nodeidstr = nodeidstr.Substring(0, pos) + nodeidstr.Substring(pos + nsstr.Length);
            }
            string extId = $"{extractionConfig.IdPrefix}{prefix}{nodeidstr}".Replace("\n", "", StringComparison.InvariantCulture);

            // ExternalId is limited to 255 characters
            extId = extId.Trim();
            if (extId.Length > 255)
            {
                if (index <= -1) return extId.Substring(0, 255);
                string indexSub = $"[{index}]";
                return extId.Substring(0, 255 - indexSub.Length) + indexSub;
            }
            if (index > -1)
            {
                extId += $"[{index}]";
            }
            return extId;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            Session?.Dispose();
            reconnectHandler?.Dispose();

        }

        #endregion
    }
    public class HistoryReadParams
    {
        public HistoryReadDetails Details { get; }
        public IEnumerable<NodeId> Nodes { get; set; }
        public IDictionary<NodeId, byte[]> ContinuationPoints { get; }
        public IDictionary<NodeId, bool> Completed { get; }

        public HistoryReadParams(IEnumerable<NodeId> nodes, HistoryReadDetails details)
        {
            Nodes = nodes ?? throw new ArgumentNullException(nameof(nodes));
            ContinuationPoints = new Dictionary<NodeId, byte[]>();
            Completed = new Dictionary<NodeId, bool>();
            Details = details;
            foreach (var node in nodes)
            {
                ContinuationPoints[node] = null;
                Completed[node] = false;
            }
        }
    }
}
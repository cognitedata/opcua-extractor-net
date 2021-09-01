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

using Cognite.Extractor.Common;
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using Prometheus;
using Serilog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Client managing the connection to the opcua server, and providing wrapper methods to simplify interaction with the server.
    /// </summary>
    public class UAClient : IDisposable, IUAClientAccess
    {
        protected FullConfig config { get; set; }
        protected Session? Session { get; set; }
        protected ApplicationConfiguration? AppConfig { get; set; }
        private ReverseConnectManager? reverseConnectManager;
        private SessionReconnectHandler? reconnectHandler;
        public DataTypeManager DataTypeManager { get; }
        public NodeTypeManager ObjectTypeManager { get; }
        private readonly object visitedNodesLock = new object();
        protected ISet<NodeId> VisitedNodes { get; } = new HashSet<NodeId>();
        private readonly object subscriptionLock = new object();
        private readonly Dictionary<NodeId, string> nodeOverrides = new Dictionary<NodeId, string>();
        public bool Started { get; private set; }
        private CancellationToken liveToken;
        private Dictionary<NodeId, HashSet<EventField>>? eventFields;

        private Dictionary<ushort, string> nsPrefixMap = new Dictionary<ushort, string>();

        public IEnumerable<NodeFilter>? IgnoreFilters { get; set; }

        public event EventHandler? OnServerDisconnect;
        public event EventHandler? OnServerReconnect;

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

        private readonly NodeMetricsManager? metricsManager;

        private readonly ILogger log = Log.Logger.ForContext(typeof(UAClient));

        public StringConverter StringConverter { get; }


        /// <summary>
        /// Constructor, does not start the client.
        /// </summary>
        /// <param name="config">Full configuartion object</param>
        public UAClient(FullConfig config)
        {
            this.config = config;
            DataTypeManager = new DataTypeManager(this, config.Extraction.DataTypes);
            ObjectTypeManager = new NodeTypeManager(this);
            if (config.Metrics.Nodes != null)
            {
                metricsManager = new NodeMetricsManager(this, config.Source, config.Metrics.Nodes);
            }
            StringConverter = new StringConverter(this, config);
        }
        #region Session management
        /// <summary>
        /// Entrypoint for starting the opcua Session. Must be called before any further requests can be made.
        /// </summary>
        public async Task Run(CancellationToken token)
        {
            liveToken = token;
            await StartSession();
            await StartNodeMetrics();
        }
        /// <summary>
        /// Close the Session, cleaning up any client data on the server
        /// </summary>
        public void Close()
        {
            reconnectHandler?.Dispose();
            reconnectHandler = null;
            if (Session != null && !Session.Disposed)
            {
                Session.Close(1000);
                Session.Dispose();
                Session = null;
            }
            connected.Set(0);
            Started = false;
        }

        /// <summary>
        /// Load XML configuration file, override certain fields with environment variables if set.
        /// </summary>
        protected async Task LoadAppConfig()
        {
            var application = new ApplicationInstance
            {
                ApplicationName = ".NET OPC-UA Extractor",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "opc.ua.net.extractor"
            };
            log.Information("Load OPC-UA Configuration from {root}/opc.ua.net.extractor.Config.xml", config.Source.ConfigRoot);
            try
            {
                AppConfig = await application.LoadApplicationConfiguration($"{config.Source.ConfigRoot}/opc.ua.net.extractor.Config.xml", false);
            }
            catch (ServiceResultException exc)
            {
                throw new ExtractorFailureException("Failed to load OPC-UA xml configuration file", exc);
            }
            catch (DirectoryNotFoundException dex)
            {
                throw new ExtractorFailureException(
                    $"Failed to load OPC-UA xml configuration, the {config.Source.ConfigRoot} directory does not exist", dex);
            }
            catch (IOException exc)
            {
                throw new ExtractorFailureException("Failed to load OPC-UA xml configuration file", exc);
            }

            string certificateDir = Environment.GetEnvironmentVariable("OPCUA_CERTIFICATE_DIR");
            if (!string.IsNullOrEmpty(certificateDir))
            {
                AppConfig.SecurityConfiguration.TrustedIssuerCertificates.StorePath = $"{certificateDir}/pki/issuer";
                AppConfig.SecurityConfiguration.TrustedPeerCertificates.StorePath = $"{certificateDir}/pki/trusted";
                AppConfig.SecurityConfiguration.RejectedCertificateStore.StorePath = $"{certificateDir}/pki/rejected";
            }

            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!validAppCert)
            {
                log.Warning("Missing application certificate, using insecure connection.");
            }
            else
            {
                AppConfig.ApplicationUri = X509Utils.GetApplicationUriFromCertificate(
                    AppConfig.SecurityConfiguration.ApplicationCertificate.Certificate);
                config.Source.AutoAccept |= AppConfig.SecurityConfiguration.AutoAcceptUntrustedCertificates;
                AppConfig.CertificateValidator.CertificateValidation += CertificateValidationHandler;
            }
        }

        private async Task CreateSessionDirect()
        {
            log.Information("Attempt to select endpoint from: {EndpointURL}", config.Source.EndpointUrl);
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(config.Source.EndpointUrl, config.Source.Secure);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.SelectEndpoint);
            }
            var endpointConfiguration = EndpointConfiguration.Create(AppConfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            var identity = AuthenticationUtils.GetUserIdentity(config.Source);
            log.Information("Attempt to connect to endpoint with security: {SecurityPolicyUri} using user identity {idt}",
                endpoint.Description.SecurityPolicyUri,
                identity.DisplayName);
            try
            {
                if (Session?.Connected ?? false)
                {
                    Session.Close();
                }
                Session?.Dispose();


                Session = await Session.Create(
                    AppConfig,
                    endpoint,
                    false,
                    ".NET OPC-UA Extractor Client",
                    0,
                    identity,
                    null
                );
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }
        private async Task WaitForReverseConnect()
        {
            if (AppConfig == null) throw new InvalidOperationException("AppConfig must be initialized");
            reverseConnectManager?.Dispose();

            AppConfig.ClientConfiguration.ReverseConnect = new ReverseConnectClientConfiguration
            {
                WaitTimeout = 300000,
                HoldTime = 30000
            };

            reverseConnectManager = new ReverseConnectManager();
            var endpointUrl = new Uri(config.Source.EndpointUrl);
            var reverseUrl = new Uri(config.Source.ReverseConnectUrl);
            reverseConnectManager.AddEndpoint(reverseUrl);
            reverseConnectManager.StartService(AppConfig);

            log.Information("Waiting for reverse connection from: {EndpointURL}", config.Source.EndpointUrl);
            var connection = await reverseConnectManager.WaitForConnection(endpointUrl, null);
            if (connection == null)
            {
                log.Error("Reverse connect failed, no connection established");
                throw new ExtractorFailureException("Failed to obtain reverse connection from server");
            }
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(AppConfig, connection, config.Source.Secure, 30000);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.SelectEndpoint);
            }
            var endpointConfiguration = EndpointConfiguration.Create(AppConfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            var identity = AuthenticationUtils.GetUserIdentity(config.Source);
            log.Information("Attempt to connect to endpoint with security: {SecurityPolicyUri} using user identity {idt}",
                endpoint.Description.SecurityPolicyUri,
                identity.DisplayName);

            try
            {
                if (Session?.Connected ?? false)
                {
                    Session.Close();
                }
                Session?.Dispose();

                connection = await reverseConnectManager.WaitForConnection(endpointUrl, null);
                if (connection == null)
                {
                    log.Error("Reverse connect failed, no connection established");
                    throw new ExtractorFailureException("Failed to obtain reverse connection from server");
                }


                Session = await Session.Create(
                    AppConfig,
                    connection,
                    endpoint,
                    false,
                    false,
                    ".NET OPC-UA Extractor Client",
                    0,
                    identity,
                    null);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
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

            await LoadAppConfig();

            if (!string.IsNullOrEmpty(config.Source.ReverseConnectUrl))
            {
                await WaitForReverseConnect();
            }
            else
            {
                await CreateSessionDirect();
            }
            if (Session == null) return;
            Session.KeepAliveInterval = config.Source.KeepAliveInterval;
            Session.KeepAlive += ClientKeepAlive;
            Started = true;
            connects.Inc();
            connected.Set(1);
            log.Information("Successfully connected to server at {EndpointURL}", config.Source.EndpointUrl);
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

            OnServerReconnect?.Invoke(this, EventArgs.Empty);

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
            if (!config.Source.ForceRestart && !liveToken.IsCancellationRequested)
            {
                reconnectHandler = new SessionReconnectHandler();
                if (reverseConnectManager != null)
                {
                    reconnectHandler.BeginReconnect(sender, reverseConnectManager, 5000, ClientReconnectComplete);
                }
                else
                {
                    reconnectHandler.BeginReconnect(sender, 5000, ClientReconnectComplete);
                }
            }
            else
            {
                try
                {
                    Session?.Close();
                }
                catch
                {
                    log.Warning("Client failed to close, quitting");
                }
            }
            OnServerDisconnect?.Invoke(this, EventArgs.Empty);
        }
        /// <summary>
        /// Called after succesful validation of a server certificate. Handles the case where the certificate is untrusted.
        /// </summary>
        private void CertificateValidationHandler(CertificateValidator validator,
            CertificateValidationEventArgs eventArgs)
        {

            if (eventArgs.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                eventArgs.Accept |= config.Source.AutoAccept;
            }
            else if (!StatusCode.IsGood(eventArgs.Error.StatusCode) && config.Source.IgnoreCertificateIssues)
            {
                log.Warning("Ignoring bad certificate: {err}", eventArgs.Error.StatusCode);
                eventArgs.Accept = true;
            }

            if (eventArgs.Accept)
            {
                log.Warning("Accepted Certificate {CertificateSubject}", eventArgs.Certificate.Subject);
            }
            else
            {
                log.Error("Rejected Bad Certificate {CertificateSubject}, {err}",
                    eventArgs.Certificate.Subject, eventArgs.Error.StatusCode);
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
        public async Task WaitForOperations()
        {
            while (pendingOperations > 0) await Task.Delay(100);
        }

        /// <summary>
        /// Start collecting metrics from configured nodes, if enabled.
        /// </summary>
        private async Task StartNodeMetrics()
        {
            if (metricsManager == null) return;
            await metricsManager.StartNodeMetrics(liveToken);
        }
        #endregion

        #region Browse
        /// <summary>
        /// Browse node hierarchy for single root node
        /// </summary>
        /// <param name="root">Root node to browse for</param>
        /// <param name="callback">Callback to call for each found node</param>
        /// <param name="ignoreVisited">Default true, do not call callback for previously visited nodes</param>
        public Task BrowseNodeHierarchy(NodeId root,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            bool ignoreVisited = true)
        {
            return BrowseNodeHierarchy(new[] { root }, callback, token, ignoreVisited);
        }
        /// <summary>
        /// Browse an opcua directory, calling callback for all relevant nodes found.
        /// </summary>
        /// <param name="roots">Initial nodes to start mapping.</param>
        /// <param name="callback">Callback for each mapped node, takes a description of a single node, and its parent id</param>
        /// <param name="ignoreVisited">Default true, do not call callback for previously visited nodes</param>
        public async Task BrowseNodeHierarchy(IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            bool ignoreVisited = true)
        {
            log.Debug("Browse node tree for nodes {nodes}", string.Join(", ", roots));
            foreach (var root in roots)
            {
                bool docb = true;
                lock (visitedNodesLock)
                {
                    if (!VisitedNodes.Add(root) && ignoreVisited)
                    {
                        docb = false;
                    }
                }
                if (docb)
                {
                    var rootNode = GetRootNode(root);
                    if (rootNode == null) throw new ExtractorFailureException($"Root node does not exist: {root}");
                    callback?.Invoke(rootNode, NodeId.Null);
                }
            }
            uint classMask = (uint)NodeClass.Variable | (uint)NodeClass.Object;
            if (config.Extraction.NodeTypes.AsNodes)
            {
                classMask |= (uint)NodeClass.VariableType | (uint)NodeClass.ObjectType;
            }
            await Task.Run(() => BrowseDirectory(roots, callback, token, null,
                classMask, ignoreVisited), token);
        }
        /// <summary>
        /// Get the root node and return it as a reference description.
        /// </summary>
        /// <param name="nodeId">Id of the root node</param>
        /// <returns>A partial description of the root node</returns>
        private ReferenceDescription? GetRootNode(NodeId nodeId)
        {
            if (Session == null) throw new InvalidOperationException("Requires open session");
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
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadRootNode);
            }
            var refd = new ReferenceDescription();
            refd.NodeId = results[0].GetValue(NodeId.Null);
            if (refd.NodeId == NodeId.Null) return null;
            refd.BrowseName = results[1].GetValue(QualifiedName.Null);
            refd.DisplayName = results[2].GetValue(LocalizedText.Null);
            refd.NodeClass = (NodeClass)results[3].GetValue(0);

            if (config.Extraction.NodeTypes.Metadata)
            {
                try
                {
                    Session.Browse(null, null, nodeId, 1, BrowseDirection.Forward, ReferenceTypeIds.HasTypeDefinition, false,
                        (uint)NodeClass.ObjectType | (uint)NodeClass.VariableType, out var _, out var references);
                    if (references.Any())
                    {
                        refd.TypeDefinition = references.First().NodeId;
                    }
                }
                catch (ServiceResultException ex)
                {
                    throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadRootNode);
                }
            }

            refd.ReferenceTypeId = null;
            refd.IsForward = true;
            return refd;
        }
        /// <summary>
        /// Retrieve a representation of the server node
        /// </summary>
        /// <returns></returns>
        public UANode GetServerNode(CancellationToken token)
        {
            var desc = GetRootNode(ObjectIds.Server);
            if (desc == null) throw new ExtractorFailureException("Server node is null. Invalid server configuration");
            var node = new UANode(ObjectIds.Server, desc.DisplayName.Text, NodeId.Null, NodeClass.Object);
            ReadNodeData(new[] { node }, token);
            return node;
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
        /// Remove all externalId overrides
        /// </summary>
        public void ClearNodeOverrides()
        {
            nodeOverrides.Clear();
        }
        /// <summary>
        /// Get all children of the given list of parents as a map from parentId to list of children descriptions
        /// </summary>
        /// <param name="parents">List of parents to browse</param>
        /// <param name="referenceTypes">Referencetype to browse, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes, as specified in the OPC-UA specification</param>
        /// <param name="direction">BrowseDirection, default forward</param>
        /// <returns>Dictionary from parent nodeId to collection of children as ReferenceDescriptions</returns>
        public Dictionary<NodeId, ReferenceDescriptionCollection> GetNodeChildren(
            IEnumerable<NodeId> parents,
            NodeId? referenceTypes,
            uint nodeClassMask,
            CancellationToken token,
            BrowseDirection direction = BrowseDirection.Forward)
        {
            if (Session == null) throw new InvalidOperationException("Requires open session");
            var finalResults = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            IncOperations();
            var tobrowse = new BrowseDescriptionCollection(parents.Select(id =>
                new BrowseDescription
                {
                    NodeId = id,
                    ReferenceTypeId = referenceTypes ?? ReferenceTypeIds.HierarchicalReferences,
                    IncludeSubtypes = true,
                    NodeClassMask = nodeClassMask,
                    BrowseDirection = direction,
                    ResultMask = (uint)BrowseResultMask.NodeClass | (uint)BrowseResultMask.DisplayName | (uint)BrowseResultMask.IsForward
                        | (uint)BrowseResultMask.ReferenceTypeId | (uint)BrowseResultMask.TypeDefinition | (uint)BrowseResultMask.BrowseName
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
                        (uint)config.Source.BrowseChunk,
                        tobrowse,
                        out results,
                        out diagnostics
                    );
                }
                catch (ServiceResultException ex)
                {
                    throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.Browse);
                }

                var indexMap = new NodeId[parents.Count()];
                var continuationPoints = new ByteStringCollection();
                int index = 0;
                int bindex = 0;
                foreach (var result in results)
                {
                    if (StatusCode.IsBad(result.StatusCode))
                    {
                        throw new ServiceResultException(result.StatusCode);
                    }
                    var nodeId = parents.ElementAt(bindex++);
                    log.Verbose("GetNodeChildren Browse result {nodeId}: {cnt}", nodeId, result.References.Count);
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
                        throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.BrowseNext);
                    }

                    int nindex = 0;
                    int pindex = 0;
                    continuationPoints.Clear();
                    foreach (var result in results)
                    {
                        if (StatusCode.IsBad(result.StatusCode))
                        {
                            throw new ServiceResultException(result.StatusCode);
                        }
                        var nodeId = indexMap[pindex++];
                        log.Verbose("GetNodeChildren BrowseNext result {nodeId}", nodeId);
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
        /// <summary>
        /// Clear internal list of visited nodes, allowing callbacks to be called for visited nodes again.
        /// </summary>
        public void ResetVisitedNodes()
        {
            lock (visitedNodesLock)
            {
                VisitedNodes.Clear();
            }
        }
        /// <summary>
        /// Apply ignore filters, if any are set.
        /// </summary>
        /// <param name="displayName">DisplayName of node to filter</param>
        /// <param name="id">NodeId of node to filter</param>
        /// <param name="typeDefinition">TypeDefinition of node to filter</param>
        /// <param name="nc">NodeClass of node to filter</param>
        /// <returns>True if the node should be kept</returns>
        public bool NodeFilter(string displayName, NodeId id, NodeId typeDefinition, NodeClass nc)
        {
            if (IgnoreFilters == null) return true;
            if (IgnoreFilters.Any(filter => filter.IsBasicMatch(displayName, id, typeDefinition, NamespaceTable!, nc))) return false;
            return true;
        }

        /// <summary>
        /// Get all children of root nodes recursively and invoke the callback for each.
        /// </summary>
        /// <param name="roots">Root nodes to browse</param>
        /// <param name="callback">Callback for each node</param>
        /// <param name="referenceTypes">Permitted reference types, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes as described in the OPC-UA specification</param>
        /// <param name="doFilter">True to apply the node filter to discovered nodes</param>
        /// <param name="ignoreVisited">True to not call callback on already visited nodes.</param>
        /// <param name="readVariableChildren">Read the children of variables.</param>
        public void BrowseDirectory(
            IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            NodeId? referenceTypes = null,
            uint nodeClassMask = (uint)NodeClass.Variable | (uint)NodeClass.Object,
            bool ignoreVisited = true,
            bool doFilter = true,
            bool readVariableChildren = false)
        {
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
                var references = new Dictionary<NodeId, ReferenceDescriptionCollection>();
                var total = nextIds.Count;
                int count = 0;
                int countChildren = 0;
                foreach (var chunk in nextIds.ChunkBy(config.Source.BrowseNodesChunk))
                {
                    if (token.IsCancellationRequested) return;
                    var result = GetNodeChildren(chunk, referenceTypes, nodeClassMask, token);
                    foreach (var res in result)
                    {
                        references[res.Key] = res.Value;
                        countChildren += res.Value.Count;
                    }
                    count += result.Count;
                    log.Debug("Read node children {cnt} / {total}. Children: {childcnt}", count, total, countChildren);
                }

                nextIds.Clear();
                levelCnt++;
                foreach (var (parentId, children) in references)
                {
                    nodeCnt += children.Count;
                    foreach (var rd in children)
                    {
                        var nodeId = ToNodeId(rd.NodeId);
                        if (rd.NodeId == ObjectIds.Server || rd.NodeId == ObjectIds.Aliases) continue;
                        if (doFilter && !NodeFilter(rd.DisplayName.Text, ToNodeId(rd.TypeDefinition), ToNodeId(rd.NodeId), rd.NodeClass))
                        {
                            log.Verbose("Ignoring filtered {nodeId}", nodeId);
                            continue;
                        }

                        bool docb = true;
                        lock (visitedNodesLock)
                        {
                            if (ignoreVisited && !VisitedNodes.Add(nodeId))
                            {
                                docb = false;
                                log.Verbose("Ignoring visited {nodeId}", nodeId);
                            }
                        }
                        if (docb)
                        {
                            log.Verbose("Discovered new node {nodeid}", nodeId);
                            callback?.Invoke(rd, parentId);
                        }
                        if (!readVariableChildren && rd.NodeClass == NodeClass.Variable) continue;
                        if (readVariableChildren && rd.TypeDefinition == VariableTypeIds.PropertyType) continue;
                        if (localVisitedNodes.Add(nodeId))
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
        /// Call the Read service with the given <paramref name="readValueIds"/>.
        /// </summary>
        /// <param name="readValueIds">Attributes to read.</param>
        /// <param name="distinctNodeCount">Number of distinct nodes</param>
        /// <returns>List of retrieved datavalues,
        /// if the server is compliant this will have length equal to <paramref name="readValueIds"/></returns>
        public IList<DataValue> ReadAttributes(ReadValueIdCollection readValueIds, int distinctNodeCount, CancellationToken token)
        {
            if (Session == null) throw new InvalidOperationException("Requires open session");
            var values = new List<DataValue>();
            if (readValueIds == null || !readValueIds.Any()) return values;
            IncOperations();
            int total = readValueIds.Count;
            int attrCount = 0;
            try
            {
                int count = 0;
                foreach (var nextValues in readValueIds.ChunkBy(config.Source.AttributesChunk))
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
                    values.AddRange(lvalues);
                    attrCount += lvalues.Count;
                    log.Debug("Read {NumAttributesRead} / {total} attributes", attrCount, total);
                }
                log.Information("Read {TotalAttributesRead} attributes with {NumAttributeReadOperations} operations for {nodeCount} nodes",
                    values.Count, count, distinctNodeCount);
            }
            catch (ServiceResultException ex)
            {
                attributeRequestFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadAttributes);
            }
            finally
            {
                DecOperations();
            }
            return values;
        }

        /// <summary>
        /// Gets attributes for the given list of nodes. The attributes retrieved for each node depends on its NodeClass.
        /// </summary>
        /// <param name="nodes">Nodes to be updated with data from the opcua server</param>
        public void ReadNodeData(IEnumerable<UANode> nodes, CancellationToken token)
        {
            nodes = nodes.Where(node => (!(node is UAVariable variable) || variable.Index == -1) && !node.DataRead).ToList();

            int expected = 0;
            var readValueIds = new ReadValueIdCollection();
            foreach (var node in nodes)
            {
                var attributes = node.Attributes.GetAttributeIds(config);
                readValueIds.AddRange(attributes.Select(attr => new ReadValueId { AttributeId = attr, NodeId = node.Id }));
                expected += attributes.Count();
            }

            IList<DataValue> values;
            try
            {
                values = ReadAttributes(readValueIds, nodes.Count(), token);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadAttributes);
            }
            int total = values.Count;

            log.Information("Retrieved {total}/{expected} attributes", total, expected);
            if (total < expected && !token.IsCancellationRequested)
            {
                throw new ExtractorFailureException(
                    $"Too few results in ReadNodeData, this is a bug in the OPC-UA server implementation, total : {total}, expected: {expected}");
            }

            int idx = 0;
            foreach (var node in nodes)
            {
                idx = node.Attributes.HandleAttributeRead(config, values, idx, this);
            }
        }

        /// <summary>
        /// Get the raw values for each given node id.
        /// Nodes must be variables
        /// </summary>
        /// <param name="ids">Nodes to get values for</param>
        /// <returns>A map from given nodeId to DataValue</returns>
        public Dictionary<NodeId, DataValue> ReadRawValues(IEnumerable<NodeId> ids, CancellationToken token)
        {
            var readValueIds = ids.Distinct().Select(id => new ReadValueId { AttributeId = Attributes.Value, NodeId = id }).ToList();
            var values = ReadAttributes(new ReadValueIdCollection(readValueIds), ids.Count(), token);
            return values.Select((dv, index) => (ids.ElementAt(index), dv)).ToDictionary(pair => pair.Item1, pair => pair.dv);
        }

        /// <summary>
        /// Gets the values of the given list of variables, then updates each variable with a BufferedDataPoint
        /// </summary>
        /// <remarks>
        /// Note that there is a fixed maximum message size, and we here fetch a large number of values at the same time.
        /// To avoid complications, avoid fetching data of unknown large size here.
        /// </remarks>
        /// <param name="nodes">List of variables to be updated</param>
        public void ReadNodeValues(IEnumerable<UAVariable> nodes, CancellationToken token)
        {
            nodes = nodes.Where(node => !node.ValueRead && node.Index == -1).ToList();
            var readValueIds = new ReadValueIdCollection(
                nodes.Select(node => new ReadValueId { AttributeId = Attributes.Value, NodeId = node.Id }));
            IEnumerable<DataValue> values;
            try
            {
                var attributes = new List<uint> { Attributes.Value };
                values = ReadAttributes(readValueIds, nodes.Count(), token);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadAttributes);
            }

            var enumerator = values.GetEnumerator();
            foreach (var node in nodes)
            {
                node.ValueRead = true;
                enumerator.MoveNext();
                node.SetDataPoint(enumerator.Current?.WrappedValue ?? Variant.Null);
            }
            enumerator.Dispose();
        }

        /// <summary>
        /// Gets properties for variables in nodes given, then updates all properties in given list of nodes with relevant data and values.
        /// </summary>
        /// <param name="nodes">Nodes to be updated with properties</param>
        public async Task GetNodeProperties(IEnumerable<UANode> nodes, CancellationToken token)
        {
            if (nodes == null || !nodes.Any()) return;

            var properties = new HashSet<UAVariable>();
            log.Information("Get properties for {NumNodesToPropertyRead} nodes", nodes.Count());
            var idsToCheck = new HashSet<NodeId>();
            var nodeDict = new Dictionary<NodeId, UANode>();
            foreach (var node in nodes)
            {
                if ((node is UAVariable variable) && !node.PropertiesRead && variable.Index <= 0)
                {
                    idsToCheck.Add(node.Id);
                    nodeDict[node.Id] = node;
                }
                if (node.Properties != null)
                {
                    foreach (var property in node.GetAllProperties())
                    {
                        if (property is UAVariable propertyVar)
                        {
                            properties.Add(propertyVar);
                        }
                        if (!property.PropertiesRead)
                        {
                            idsToCheck.Add(property.Id);
                            nodeDict[property.Id] = property;
                        }
                    }
                }
            }

            void cb(ReferenceDescription desc, NodeId parentId)
            {
                var id = ToNodeId(desc.NodeId);
                var parent = nodeDict[parentId];
                if (nodeDict.ContainsKey(id)) return;

                UANode prop;

                if (desc.NodeClass == NodeClass.Object || desc.NodeClass == NodeClass.ObjectType)
                {
                    prop = new UANode(id, desc.DisplayName.Text, parentId, desc.NodeClass);
                }
                else if (desc.NodeClass == NodeClass.Variable || desc.NodeClass == NodeClass.VariableType)
                {
                    var varProp = new UAVariable(id, desc.DisplayName.Text, parentId, desc.NodeClass);
                    properties.Add(varProp);
                    prop = varProp;
                }
                else return;

                prop.SetNodeType(this, desc.TypeDefinition);

                prop.Attributes.IsProperty = true;
                prop.Attributes.PropertiesRead = true;

                parent.AddProperty(prop);
                if (desc.TypeDefinition != VariableTypeIds.PropertyType)
                {
                    nodeDict[id] = prop;
                }
            }

            BrowseDirectory(idsToCheck, cb, token, ReferenceTypeIds.HierarchicalReferences,
                (uint)NodeClass.Object | (uint)NodeClass.Variable, false, true, true);

            log.Information("Read attributes for {cnt} properties", properties.Count);
            ReadNodeData(properties, token);
            var toGetValue = properties.Where(node => DataTypeManager.AllowTSMap(node, 10, true)).ToList();
            await DataTypeManager.GetDataTypeMetadataAsync(toGetValue.SelectNonNull(prop => prop.DataType?.Raw), token);
            ReadNodeValues(toGetValue, token);
        }
        #endregion

        #region Synchronization
        /// <summary>
        /// Modifies passed HistoryReadParams while doing a single config-limited iteration of history read.
        /// </summary>
        /// <param name="readParams"></param>
        /// <returns>Pairs of NodeId and history read results as IEncodable</returns>
        public IEnumerable<(HistoryReadNode Node, IEncodeable RawData)> DoHistoryRead(HistoryReadParams readParams)
        {
            if (Session == null) throw new InvalidOperationException("Requires open session");
            IncOperations();
            var ids = new HistoryReadValueIdCollection();
            foreach (var node in readParams.Nodes)
            {
                ids.Add(new HistoryReadValueId
                {
                    NodeId = node.Id,
                    ContinuationPoint = node.ContinuationPoint
                });
            }

            var result = new List<(HistoryReadNode, IEncodeable)>();
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
                for (int i = 0; i < readParams.Nodes.Count; i++)
                {
                    var data = results[i];
                    var node = readParams.Nodes[i];
                    if (StatusCode.IsBad(data.StatusCode))
                    {
                        throw new ServiceResultException(data.StatusCode);
                    }
                    result.Add((node, ExtensionObject.ToEncodeable(data.HistoryData)));
                    if (data.ContinuationPoint == null)
                    {
                        node.Completed = true;
                    }
                    else
                    {
                        node.ContinuationPoint = data.ContinuationPoint;
                    }
                }

                log.Debug("Fetched historical "
                          + (readParams.Details is ReadEventDetails ? "events" : "datapoints")
                          + " for {nodeCount} nodes", readParams.Nodes.Count);
            }
            catch (ServiceResultException ex)
            {
                historyReadFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, readParams.Details is ReadEventDetails
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
        /// Add MonitoredItems to the given list of states.
        /// </summary>
        /// <param name="nodeList">States to subscribe to</param>
        /// <param name="subName">Name of subscription</param>
        /// <param name="handler">Callback for the items</param>
        /// <param name="builder">Method to build monitoredItems from states</param>
        /// <returns>Constructed subscription</returns>
        public Subscription AddSubscriptions(
            IEnumerable<UAHistoryExtractionState> nodeList,
            string subName,
            MonitoredItemNotificationEventHandler handler,
            Func<UAHistoryExtractionState, MonitoredItem> builder,
            CancellationToken token)
        {
            if (Session == null) throw new InvalidOperationException("Requires open session");
            lock (subscriptionLock)
            {
                var subscription = Session.Subscriptions.FirstOrDefault(sub =>
                                       sub.DisplayName.StartsWith(subName, StringComparison.InvariantCulture));
                if (subscription == null)
                {
#pragma warning disable CA2000 // Dispose objects before losing scope
                    subscription = new Subscription(Session.DefaultSubscription)
                    {
                        PublishingInterval = config.Source.PublishingInterval,
                        DisplayName = subName
                    };
#pragma warning restore CA2000 // Dispose objects before losing scope
                }
                int count = 0;
                var hasSubscription = subscription.MonitoredItems.Select(sub => sub.ResolvedNodeId).ToHashSet();
                int total = nodeList.Count();

                IncOperations();
                try
                {
                    foreach (var chunk in nodeList.ChunkBy(config.Source.SubscriptionChunk))
                    {
                        if (token.IsCancellationRequested) break;
                        int lcount = 0;
                        subscription.AddItems(chunk
                            .Where(node => !hasSubscription.Contains(node.SourceId))
                            .Select(node =>
                            {
                                var monitor = builder(node);
                                monitor.Notification += handler;
                                lcount++;
                                return monitor;
                            })
                        );
                        log.Debug("Add subscriptions for {numnodes} nodes, {subscribed} / {total} done.", lcount, count, total);
                        count += lcount;

                        if (lcount > 0 && subscription.Created)
                        {
                            try
                            {
                                subscription.CreateItems();
                            }
                            catch (ServiceResultException ex)
                            {
                                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateMonitoredItems);
                            }
                        }
                        else if (lcount > 0)
                        {
                            try
                            {
                                Session.AddSubscription(subscription);
                                subscription.Create();
                            }
                            catch (ServiceResultException ex)
                            {
                                throw ExtractorUtils.HandleServiceResult(log, ex,
                                    ExtractorUtils.SourceOp.CreateSubscription);
                            }
                        }
                    }
                }
                finally
                {
                    DecOperations();
                    if (!subscription.Created)
                    {
                        subscription.Dispose();
                    }
                }
                log.Information("Added {TotalAddedSubscriptions} / {total} subscriptions to {sub}", count, total, subscription.DisplayName);
                return subscription;
            }
        }


        /// <summary>
        /// Create datapoint subscriptions for given list of nodes
        /// </summary>
        /// <param name="nodeList">List of buffered variables to synchronize</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        public void SubscribeToNodes(IEnumerable<VariableExtractionState> nodeList,
            MonitoredItemNotificationEventHandler subscriptionHandler,
            CancellationToken token)
        {
            if (!nodeList.Any()) return;

#pragma warning disable CA2000 // Dispose objects before losing scope
            var sub = AddSubscriptions(
                nodeList,
                "DataChangeListener",
                subscriptionHandler,
                node => new MonitoredItem
                {
                    StartNodeId = node.SourceId,
                    DisplayName = "Value: " + (node as VariableExtractionState)?.DisplayName,
                    SamplingInterval = config.Source.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, config.Source.QueueLength),
                    AttributeId = Attributes.Value,
                    NodeClass = NodeClass.Variable,
                    CacheQueueSize = Math.Max(0, config.Source.QueueLength),
                    Filter = config.Subscriptions.DataChangeFilter?.Filter
                }, token);
#pragma warning restore CA2000 // Dispose objects before losing scope

            numSubscriptions.Set(sub.MonitoredItemCount);
        }
        /// <summary>
        /// Subscribe to events from the given list of emitters.
        /// </summary>
        /// <param name="emitters">List of emitters. These are the actual targets of the subscription.</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        /// <returns>Map of fields, EventTypeId->(SourceTypeId, BrowseName)</returns>
        public void SubscribeToEvents(IEnumerable<EventExtractionState> emitters,
            MonitoredItemNotificationEventHandler subscriptionHandler,
            CancellationToken token)
        {
            var filter = BuildEventFilter();

#pragma warning disable CA2000 // Dispose objects before losing scope
            AddSubscriptions(
                emitters,
                "EventListener",
                subscriptionHandler,
                node => new MonitoredItem
                {
                    StartNodeId = node.SourceId,
                    AttributeId = Attributes.EventNotifier,
                    DisplayName = "Events: " + node.Id,
                    SamplingInterval = config.Source.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, config.Source.QueueLength),
                    Filter = filter,
                    NodeClass = NodeClass.Object
                },
                token);
#pragma warning restore CA2000 // Dispose objects before losing scope
        }

        /// <summary>
        /// Deletes a subscription starting with the given name.
        /// The client manages three subscriptions: EventListener, DataChangeListener and AuditListener,
        /// if the subscription does not exist, nothing happens.
        /// </summary>
        /// <param name="name"></param>
        public void RemoveSubscription(string name)
        {
            if (Session == null || Session.Subscriptions == null) return;
            var subscription = Session.Subscriptions.FirstOrDefault(sub =>
                                       sub.DisplayName.StartsWith(name, StringComparison.InvariantCulture));
            if (subscription == null || !subscription.Created) return;
            Session.RemoveSubscription(subscription);
        }
        #endregion

        #region Events
        /// <summary>
        /// Return systemContext. Can be used by SDK-tools for converting events.
        /// </summary>
        public ISystemContext? SystemContext => Session?.SystemContext;
        /// <summary>
        /// Return MessageContext, used for serialization
        /// </summary>
        public ServiceMessageContext? MessageContext => Session?.MessageContext;
        /// <summary>
        /// Fetch event fields from the server and store them on the client
        /// </summary>
        /// <param name="token"></param>
        /// <returns>The collected event fields</returns>
        public Dictionary<NodeId, HashSet<EventField>> GetEventFields(CancellationToken token)
        {
            if (eventFields != null) return eventFields;
            var collector = new EventFieldCollector(this, config.Events);
            eventFields = collector.GetEventIdFields(token);
            foreach (var pair in eventFields)
            {
                log.Verbose("Collected event field: {id}", pair.Key);
                foreach (var fields in pair.Value)
                {
                    log.Verbose("    {browse}", fields.Name);
                }
            }
            return eventFields;
        }
        /// <summary>
        /// Remove collected event fields
        /// </summary>
        public void ClearEventFields()
        {
            eventFields = null;
        }
        /// <summary>
        /// Constructs a filter from the given list of permitted eventids, the already constructed field map and an optional receivedAfter property.
        /// </summary>
        /// <param name="nodeIds">Permitted SourceNode ids</param>
        /// <param name="receivedAfter">Optional, if defined, attempt to filter out events with [ReceiveTimeProperty] > receivedAfter</param>
        /// <returns>The final event filter</returns>
        public EventFilter BuildEventFilter()
        {
            /*
             * Essentially equivalent to SELECT Message, EventId, SourceNode, Time FROM [source] WHERE EventId IN eventIds;
             * using the internal query language in OPC-UA
             */
            var whereClause = new ContentFilter();

            if (eventFields == null) eventFields = new Dictionary<NodeId, HashSet<EventField>>();

            if (eventFields.Keys.Any() && ((config.Events.EventIds?.Any() ?? false) || !config.Events.AllEvents))
            {
                log.Debug("Limit event results to the following ids: {ids}", string.Join(", ", eventFields.Keys));
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

                whereClause.Push(FilterOperator.InList, eventOperands.Prepend(eventListOperand).ToArray<object>());
            }


            var fieldList = eventFields
                .Aggregate((IEnumerable<EventField>)new List<EventField>(), (agg, kvp) => agg.Concat(kvp.Value))
                .Distinct();

            if (!fieldList.Any())
            {
                log.Warning("Missing valid event fields, no results will be returned");
            }
            var selectClauses = new SimpleAttributeOperandCollection();
            foreach (var field in fieldList)
            {
                if (config.Events.ExcludeProperties.Contains(field.Name)
                    || config.Events.BaseExcludeProperties.Contains(field.Name)) continue;
                var operand = new SimpleAttributeOperand
                {
                    AttributeId = Attributes.Value,
                    TypeDefinitionId = ObjectTypeIds.BaseEventType
                };
                operand.BrowsePath = field.BrowsePath;
                selectClauses.Add(operand);
            }
            return new EventFilter
            {
                WhereClause = whereClause,
                SelectClauses = selectClauses
            };
        }
        /// <summary>
        /// Build ContentFilter to be used when subscribing to audit events.
        /// </summary>
        /// <returns>Final EventFilter</returns>
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
        /// <summary>
        /// Subscribe to audit events on the server node
        /// </summary>
        /// <param name="callback">Callback to use for subscriptions</param>
        public void SubscribeToAuditEvents(MonitoredItemNotificationEventHandler callback)
        {
            if (Session == null) throw new InvalidOperationException("Requires open session");
            var filter = BuildAuditFilter();
            lock (subscriptionLock)
            {
                var subscription = Session.Subscriptions.FirstOrDefault(sub => sub.DisplayName.StartsWith("AuditListener", StringComparison.InvariantCulture))
#pragma warning disable CA2000 // Dispose objects before losing scope
                               ?? new Subscription(Session.DefaultSubscription)
                               {
                                   PublishingInterval = config.Source.PublishingInterval,
                                   DisplayName = "AuditListener"
                               };
#pragma warning restore CA2000 // Dispose objects before losing scope
                if (subscription.MonitoredItemCount != 0) return;
                var item = new MonitoredItem
                {
                    StartNodeId = ObjectIds.Server,
                    Filter = filter,
                    AttributeId = Attributes.EventNotifier,
                    SamplingInterval = config.Source.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, config.Source.QueueLength),
                    NodeClass = NodeClass.Object
                };
                item.Notification += callback;
                subscription.AddItem(item);
                log.Information("Subscribe to auditing events on the server node");

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

        #endregion

        #region Utils

        public NamespaceTable? NamespaceTable => Session?.NamespaceUris;
        /// <summary>
        /// Converts an ExpandedNodeId into a NodeId using the Session
        /// </summary>
        /// <param name="nodeid"></param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(ExpandedNodeId nodeid)
        {
            if (nodeid == null || nodeid.IsNull || Session == null) return NodeId.Null;
            return ExpandedNodeId.ToNodeId(nodeid, Session.NamespaceUris);
        }
        /// <summary>
        /// Converts identifier string and namespaceUri into NodeId. Identifier will be on form i=123 or s=abc etc.
        /// </summary>
        /// <param name="identifier">Full identifier on form i=123 or s=abc etc.</param>
        /// <param name="namespaceUri">Full namespaceUri</param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(string? identifier, string? namespaceUri)
        {
            if (identifier == null || namespaceUri == null || Session == null) return NodeId.Null;
            int idx = Session.NamespaceUris.GetIndex(namespaceUri);
            if (idx < 0)
            {
                if (config.Extraction.NamespaceMap.ContainsValue(namespaceUri))
                {
                    string readNs = config.Extraction.NamespaceMap.First(kvp => kvp.Value == namespaceUri).Key;
                    idx = Session.NamespaceUris.GetIndex(readNs);
                    if (idx < 0) return NodeId.Null;
                }
                else
                {
                    return NodeId.Null;
                }
            }

            string nsString = "ns=" + idx;
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
            if (datavalue is Variant variant) return ConvertToDouble(variant.Value);
            // Check if the value is somehow an array
            if (datavalue is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                enumerator.MoveNext();
                return ConvertToDouble(enumerator.Current);
            }
            // Give up if there is no clear way to convert it
            if (!typeof(IConvertible).IsAssignableFrom(datavalue.GetType())) return 0;
            try
            {
                return Convert.ToDouble(datavalue, CultureInfo.InvariantCulture);
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>
        /// Returns consistent unique string representation of a <see cref="NodeId"/> given its namespaceUri
        /// </summary>
        /// <remarks>
        /// NodeId is, according to spec, unique in combination with its namespaceUri. We use this to generate a consistent, unique string
        /// to be used for mapping assets and timeseries in CDF to opcua nodes.
        /// To avoid having to send the entire namespaceUri to CDF, we allow mapping Uris to prefixes in the config file.
        /// </remarks>
        /// <param name="id">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        public string? GetUniqueId(ExpandedNodeId id, int index = -1)
        {
            var nodeId = ToNodeId(id);
            if (nodeId.IsNullNodeId) return null;
            if (nodeOverrides.TryGetValue(nodeId, out var nodeOverride))
            {
                if (index <= -1) return nodeOverride;
                return $"{nodeOverride}[{index}]";
            }

            // ExternalIds shorter than 32 chars are unlikely, this will generally avoid at least 1 re-allocation of the buffer,
            // and usually boost performance.
            var buffer = new StringBuilder(config.Extraction.IdPrefix, 32);

            if (!nsPrefixMap.TryGetValue(nodeId.NamespaceIndex, out var prefix))
            {
                var namespaceUri = id.NamespaceUri ?? Session!.NamespaceUris.GetString(nodeId.NamespaceIndex);
                string newPrefix = config.Extraction.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");
                nsPrefixMap[nodeId.NamespaceIndex] = prefix = newPrefix;
            }

            buffer.Append(prefix);
            // Use 0 as namespace-index. This means that the namespace is not appended, as the string representation
            // of a base namespace nodeId does not include the namespace-index, which fits our use-case.
            NodeId.Format(buffer, nodeId.Identifier, nodeId.IdType, 0);

            TrimEnd(buffer);

            if (index > -1)
            {
                // Modifying buffer.Length effectively removes the last few elements, but more efficiently than modifying strings,
                // StringBuilder is just a char array.
                // 255 is max length, Log10(Max(1, index)) + 3 is the length of the index suffix ("[123]").
                buffer.Length = Math.Min(buffer.Length, 255 - ((int)Math.Log10(Math.Max(1, index)) + 3));
                buffer.AppendFormat(CultureInfo.InvariantCulture, "[{0}]", index);
            }
            else
            {
                buffer.Length = Math.Min(buffer.Length, 255);
            }
            return buffer.ToString();
        }
        /// <summary>
        /// Used to trim the whitespace off the end of a StringBuilder
        /// </summary> 
        private static void TrimEnd(StringBuilder sb)
        {
            if (sb == null || sb.Length == 0) return;

            int i = sb.Length - 1;
            for (; i >= 0; i--)
                if (!char.IsWhiteSpace(sb[i]))
                    break;

            if (i < sb.Length - 1)
                sb.Length = i + 1;

            return;
        }

        /// <summary>
        /// Append NodeId and namespace to the given StringBuilder.
        /// </summary>
        /// <param name="buffer">Builder to append to</param>
        /// <param name="nodeId">NodeId to append</param>
        private void AppendNodeId(StringBuilder buffer, NodeId nodeId)
        {
            if (nodeOverrides.TryGetValue(nodeId, out var nodeOverride))
            {
                buffer.Append(nodeOverride);
                return;
            }

            if (!nsPrefixMap.TryGetValue(nodeId.NamespaceIndex, out var prefix))
            {
                var namespaceUri = Session!.NamespaceUris.GetString(nodeId.NamespaceIndex);
                string newPrefix = config.Extraction.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");
                nsPrefixMap[nodeId.NamespaceIndex] = prefix = newPrefix;
            }

            buffer.Append(prefix);

            NodeId.Format(buffer, nodeId.Identifier, nodeId.IdType, 0);

            TrimEnd(buffer);
        }

        /// <summary>
        /// Get string representation of NodeId on the form i=123 or s=string, etc.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private string GetNodeIdString(NodeId id)
        {
            var buffer = new StringBuilder();
            AppendNodeId(buffer, id);
            return buffer.ToString();
        }

        /// <summary>
        /// Get the unique reference id, on the form [prefix][reference-name];[sourceId];[targetId]
        /// </summary>
        /// <param name="reference">Reference to get id for</param>
        /// <returns>String reference id</returns>
        public string GetRelationshipId(UAReference reference)
        {
            var buffer = new StringBuilder(config.Extraction.IdPrefix, 64);
            buffer.Append(reference.GetName());
            buffer.Append(';');
            AppendNodeId(buffer, reference.Source.Id);
            buffer.Append(';');
            AppendNodeId(buffer, reference.Target.Id);

            if (buffer.Length > 255)
            {
                // This is an edge-case. If the id overflows, it is most sensible to cut from the
                // start of the id, as long ids are likely (from experience) to be similar to
                // system.subsystem.sensor.measurement...
                // so cutting from the start is less likely to cause conflicts
                var overflow = (int)Math.Ceiling((buffer.Length - 255) / 2.0);
                buffer = new StringBuilder(config.Extraction.IdPrefix, 255);
                buffer.Append(reference.GetName());
                buffer.Append(';');
                buffer.Append(GetNodeIdString(reference.Source.Id).AsSpan(overflow));
                buffer.Append(';');
                buffer.Append(GetNodeIdString(reference.Target.Id).AsSpan(overflow));
            }
            return buffer.ToString();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            try
            {
                Close();
            }
            catch (Exception ex)
            {
                log.Warning("Failed to close UAClient: {msg}", ex.Message);
            }
            reconnectHandler?.Dispose();
            reverseConnectManager?.Dispose();
            if (AppConfig != null)
            {
                AppConfig.CertificateValidator.CertificateValidation -= CertificateValidationHandler;
            }
            if (Session != null)
            {
                Session.KeepAlive -= ClientKeepAlive;
            }
        }
        #endregion
    }
}
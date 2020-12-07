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

using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using System.Collections.ObjectModel;
using System.Globalization;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System.Linq;
using Prometheus;
using System.Threading;
using Serilog;
using Cognite.Extractor.Common;
using System.Text;
using System.Collections;

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
        public UAExtractor Extractor { get; set; }
        public DataTypeManager DataTypeManager { get; }
        private readonly object visitedNodesLock = new object();
        protected ISet<NodeId> VisitedNodes { get; }= new HashSet<NodeId>();
        private readonly object subscriptionLock = new object();
        private readonly Dictionary<NodeId, string> nodeOverrides = new Dictionary<NodeId, string>();
        public bool Started { get; private set; }
        private CancellationToken liveToken;
        private Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> eventFields;

        private Dictionary<ushort, string> nsPrefixMap = new Dictionary<ushort, string>();

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

        private readonly ILogger log = Log.Logger.ForContext(typeof(UAClient));

        /// <summary>
        /// Constructor, does not start the client.
        /// </summary>
        /// <param name="config">Full configuartion object</param>
        public UAClient(FullConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            this.config = config.Source;
            DataTypeManager = new DataTypeManager(this, config.Extraction.DataTypes);
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
            reconnectHandler?.Dispose();
            reconnectHandler = null;
            if (Session != null && !Session.Disposed)
            {
                Session.Close(1000);
                Session.Dispose();
                Session = null;
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

            var application = new ApplicationInstance
            {
                ApplicationName = ".NET OPC-UA Extractor",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "opc.ua.net.extractor"
            };
            log.Information("Load OPC-UA Configuration from {root}/opc.ua.net.extractor.Config.xml", config.ConfigRoot);
            try
            {
                Appconfig = await application.LoadApplicationConfiguration($"{config.ConfigRoot}/opc.ua.net.extractor.Config.xml", false);
            }
            catch (ServiceResultException exc)
            {
                throw new ExtractorFailureException("Failed to load OPC-UA xml configuration file", exc);
            }
            string certificateDir = Environment.GetEnvironmentVariable("OPCUA_CERTIFICATE_DIR");
            if (!string.IsNullOrEmpty(certificateDir))
            {
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
                Appconfig.ApplicationUri = Utils.GetApplicationUriFromCertificate(
                    Appconfig.SecurityConfiguration.ApplicationCertificate.Certificate);
                config.AutoAccept |= Appconfig.SecurityConfiguration.AutoAcceptUntrustedCertificates;
                Appconfig.CertificateValidator.CertificateValidation += CertificateValidationHandler;
            }
            log.Information("Attempt to select endpoint from: {EndpointURL}", config.EndpointUrl);
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(config.EndpointUrl, config.Secure);
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
                Session.KeepAliveInterval = config.KeepAliveInterval;
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.CreateSession);
            }


            Session.KeepAlive += ClientKeepAlive;
            Started = true;
            connects.Inc();
            connected.Set(1);
            log.Information("Successfully connected to server at {EndpointURL}", config.EndpointUrl);
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
            if (config.RestartOnReconnect)
            {
                Extractor?.DataTypeManager?.Configure();
                nodeOverrides?.Clear();
                eventFields?.Clear();
                Task.Run(() => Extractor?.RestartExtractor());
                lock (visitedNodesLock)
                {
                    VisitedNodes.Clear();
                }
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
                }
                catch
                {
                    log.Warning("Client failed to close, quitting");
                }
                finally
                {
                    Extractor?.Close();
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
        /// <summary>
        /// Browse node hierarchy for single root node
        /// </summary>
        /// <param name="root">Root node to browse for</param>
        /// <param name="callback">Callback to call for each found node</param>
        public Task BrowseNodeHierarchy(NodeId root,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            bool ignoreVisited = true)
        {
            return BrowseNodeHierarchy(new[] {root}, callback, token, ignoreVisited);
        }
        /// <summary>
        /// Browse an opcua directory, calling callback for all relevant nodes found.
        /// </summary>
        /// <param name="roots">Initial nodes to start mapping.</param>
        /// <param name="callback">Callback for each mapped node, takes a description of a single node, and its parent id</param>
        public async Task BrowseNodeHierarchy(IEnumerable<NodeId> roots,
            Action<ReferenceDescription, NodeId> callback,
            CancellationToken token,
            bool ignoreVisited = true)
        {
            if (roots == null) throw new ArgumentNullException(nameof(roots));
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
                    callback?.Invoke(rootNode, null);
                }
            }
            await Task.Run(() => BrowseDirectory(roots, callback, token, null,
                (uint)NodeClass.Variable | (uint)NodeClass.Object, ignoreVisited), token);
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
            refd.NodeId = results[0].GetValue(NodeId.Null);
            if (refd.NodeId == NodeId.Null) return null;
            refd.BrowseName = results[1].GetValue(QualifiedName.Null);
            refd.DisplayName = results[2].GetValue(LocalizedText.Null);
            refd.NodeClass = (NodeClass)results[3].GetValue(0);
            refd.ReferenceTypeId = null;
            refd.IsForward = true;
            return refd;
        }
        /// <summary>
        /// Retrieve a representation of the server node
        /// </summary>
        /// <returns></returns>
        public BufferedNode GetServerNode(CancellationToken token)
        {
            var desc = GetRootNode(ObjectIds.Server);
            var node = new BufferedNode(ObjectIds.Server, desc.DisplayName.Text, NodeId.Null);
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
        /// <returns>Dictionary from parent nodeId to collection of children as ReferenceDescriptions</returns>
        public Dictionary<NodeId, ReferenceDescriptionCollection> GetNodeChildren(
            IEnumerable<NodeId> parents,
            NodeId referenceTypes,
            uint nodeClassMask,
            CancellationToken token,
            BrowseDirection direction = BrowseDirection.Forward)
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
                foreach (var result in results)
                {
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
                        throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.BrowseNext);
                    }

                    int nindex = 0;
                    int pindex = 0;
                    continuationPoints.Clear();
                    foreach (var result in results)
                    {
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
        /// Get all children of root nodes recursively and invoke the callback for each.
        /// </summary>
        /// <param name="roots">Root nodes to browse</param>
        /// <param name="callback">Callback for each node</param>
        /// <param name="referenceTypes">Permitted reference types, defaults to HierarchicalReferences</param>
        /// <param name="nodeClassMask">Mask for node classes as described in the OPC-UA specification</param>
        public void BrowseDirectory(
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
                var references = new Dictionary<NodeId, ReferenceDescriptionCollection>();
                var total = nextIds.Count;
                int count = 0;
                int countChildren = 0;
                foreach (var chunk in nextIds.ChunkBy(config.BrowseNodesChunk))
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
                            log.Verbose("Discovered new node {nodeid}", nodeId);
                            callback?.Invoke(rd, parentId);
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
        public IList<DataValue> ReadAttributes(ReadValueIdCollection readValueIds, int distinctNodeCount, CancellationToken token)
        {
            var values = new List<DataValue>();
            if (readValueIds == null || !readValueIds.Any()) return values;
            IncOperations();
            int total = readValueIds.Count;
            int attrCount = 0;
            try
            {
                int count = 0;
                foreach (var nextValues in readValueIds.ChunkBy(config.AttributesChunk))
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
            catch (Exception ex)
            {
                attributeRequestFailures.Inc();
                if (ex is ServiceResultException serviceEx)
                {
                    throw ExtractorUtils.HandleServiceResult(serviceEx, ExtractorUtils.SourceOp.ReadAttributes);
                }
                throw;
            }
            finally
            {
                DecOperations();
            }
            return values;
        }


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
            IEnumerable<uint> properties,
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
                    if (node is BufferedVariable variable && variable.IsProperty)
                    {
                        readValueIds.AddRange(properties.Select(attribute => new ReadValueId { AttributeId = attribute, NodeId = node.Id }));
                    }
                    else
                    {
                        readValueIds.AddRange(variables.Select(attribute => new ReadValueId { AttributeId = attribute, NodeId = node.Id }));
                    }
                }
            }
            var values = ReadAttributes(readValueIds, nodes.Count(), token);
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
                Attributes.ValueRank
            };
            var commonAttributes = new List<uint>
            {
                Attributes.Description
            };
            var propertyAttributes = new List<uint>
            {
                Attributes.DataType,
                Attributes.ValueRank,
                Attributes.ArrayDimensions
            };

            bool arraysEnabled = extractionConfig.DataTypes.MaxArraySize != 0;

            if (historyConfig.Enabled && historyConfig.Data)
            {
                variableAttributes.Add(Attributes.Historizing);
            }
            if (arraysEnabled)
            {
                variableAttributes.Add(Attributes.ArrayDimensions);
            }
            if (eventConfig.Enabled)
            {
                commonAttributes.Add(Attributes.EventNotifier);
            }


            IEnumerable<DataValue> values;
            try
            {
                values = GetNodeAttributes(nodes, commonAttributes, variableAttributes, propertyAttributes, token);
            }
            catch (ServiceResultException ex)
            {
                throw ExtractorUtils.HandleServiceResult(ex, ExtractorUtils.SourceOp.ReadAttributes);
            }
            int total = values.Count();
            int expected = nodes.Aggregate(0, (seed, node) =>
            {
                if (node.IsVariable && node is BufferedVariable variable)
                {
                    if (variable.IsProperty)
                    {
                        return seed + propertyAttributes.Count + 1;
                    }
                    else
                    {
                        return seed + variableAttributes.Count + 1;
                    }
                }
                return seed + 1;
            });
            if (total < expected)
            {
                throw new ExtractorFailureException(
                    $"Too few results in ReadNodeData, this is a bug in the OPC-UA server implementation, total : {total}, expected: {expected}");
            }

            var enumerator = values.GetEnumerator();
            foreach (var node in nodes)
            {
                if (token.IsCancellationRequested) return;
                enumerator.MoveNext();
                node.Description = enumerator.Current.GetValue(new LocalizedText("")).Text;
                if (eventConfig.Enabled)
                {
                    enumerator.MoveNext();
                    node.EventNotifier = enumerator.Current.GetValue(EventNotifiers.None);
                }
                if (node.IsVariable && node is BufferedVariable vnode)
                {
                    enumerator.MoveNext();
                    NodeId dataType = enumerator.Current.GetValue(NodeId.Null);
                    vnode.DataType = Extractor?.DataTypeManager?.GetDataType(dataType) ?? new BufferedDataType(dataType);

                    enumerator.MoveNext();
                    vnode.ValueRank = enumerator.Current.GetValue(0);
                    if (historyConfig.Enabled && historyConfig.Data && !vnode.IsProperty)
                    {
                        enumerator.MoveNext();
                        vnode.Historizing = enumerator.Current.GetValue(false);
                    }
                    if (vnode.IsProperty || arraysEnabled)
                    {
                        enumerator.MoveNext();
                        var dimVal = enumerator.Current.GetValue(typeof(int[])) as int[];
                        if (dimVal != null)
                        {
                            vnode.ArrayDimensions = new Collection<int>((int[])enumerator.Current.GetValue(typeof(int[])));
                        }
                    }
                }
            }
            enumerator.Dispose();
        }
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
        public void ReadNodeValues(IEnumerable<BufferedVariable> nodes, CancellationToken token)
        {
            nodes = nodes.Where(node => !node.DataRead && node.Index == -1).ToList();
            IEnumerable<DataValue> values;
            try
            {
                var attributes = new List<uint> { Attributes.Value };
                values = GetNodeAttributes(nodes,
                    new List<uint>(),
                    attributes,
                    attributes,
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
                enumerator.MoveNext();
                node.SetDataPoint(enumerator.Current?.Value,
                    enumerator.Current?.SourceTimestamp ?? DateTime.MinValue,
                    this);
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

            var nodeList = nodes.ToList();

            var properties = new HashSet<BufferedVariable>();
            log.Information("Get properties for {NumNodesToPropertyRead} nodes", nodes.Count());
            var idsToCheck = new List<NodeId>();
            foreach (var node in nodes)
            {
                if (node.IsVariable)
                {
                    if (node is BufferedVariable variable && variable.Index <= 0)
                    {
                        idsToCheck.Add(node.Id);
                    }
                }
                if (node.Properties != null)
                {
                    foreach (var property in node.Properties)
                    {
                        if (!node.IsVariable)
                        {
                            properties.Add(property);
                        }
                        if (!property.PropertiesRead)
                        {
                            idsToCheck.Add(property.Id);
                            nodeList.Add(property);
                        }
                    }
                }
            }
            idsToCheck = idsToCheck.Distinct().ToList();

            var result = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            var total = idsToCheck.Count;
            int found = 0;
            int readCount = 0;
            foreach (var chunk in idsToCheck.ChunkBy(config.BrowseNodesChunk))
            {
                var read = GetNodeChildren(chunk, ReferenceTypeIds.HasProperty, (uint)NodeClass.Variable, token);
                foreach (var kvp in read)
                {
                    result[kvp.Key] = kvp.Value;
                    found += kvp.Value.Count;
                }
                readCount += chunk.Count();
                log.Debug("Read properties for {cnt} / {total} nodes. Found: {found}", readCount, total, found);
            }

            nodeList = nodeList.DistinctBy(node => node.Id).ToList();

            foreach (var parent in nodeList)
            {
                if (!result.TryGetValue(parent.Id, out var children)) continue;
                foreach (var child in children)
                {
                    var property = new BufferedVariable(ToNodeId(child.NodeId), child.DisplayName.Text, parent.Id) { IsProperty = true };
                    properties.Add(property);
                    if (parent.Properties == null)
                    {
                        parent.Properties = new List<BufferedVariable>();
                    }
                    parent.Properties.Add(property);
                }
                if (parent.IsVariable && parent is BufferedVariable variable)
                {
                    if (variable.IsProperty) continue;
                    BufferedVariable arrayParent = variable.Index == -1 ? variable : variable.ArrayParent;

                    if (arrayParent != null && arrayParent.Index == -1 && arrayParent.ArrayDimensions != null
                        && arrayParent.ArrayDimensions.Count == 1 && arrayParent.ArrayDimensions[0] > 0)
                    {
                        foreach (var child in arrayParent.ArrayChildren)
                        {
                            child.PropertiesRead = true;
                            child.Properties = parent.Properties;
                        }
                        arrayParent.PropertiesRead = true;
                        arrayParent.Properties = parent.Properties;
                    }
                }
            }

            ReadNodeData(properties, token);
            var toGetValue = properties.Where(node => DataTypeManager.AllowTSMap(node, 10, true));
            ReadNodeValues(toGetValue, token);
        }
        #endregion

        #region Synchronization
        /// <summary>
        /// Modifies passed HistoryReadParams while doing a single config-limited iteration of history read.
        /// </summary>
        /// <param name="readParams"></param>
        /// <returns>Pairs of NodeId and history read results as IEncodable</returns>
        public IEnumerable<(NodeId Id, IEncodeable RawData)> DoHistoryRead(HistoryReadParams readParams)
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

        private Subscription AddSubscriptions(
            IEnumerable<UAHistoryExtractionState> nodeList,
            string subName,
            MonitoredItemNotificationEventHandler handler,
            Func<UAHistoryExtractionState, MonitoredItem> builder,
            CancellationToken token)
        {
            lock (subscriptionLock)
            {
                var subscription = Session.Subscriptions.FirstOrDefault(sub =>
                                       sub.DisplayName.StartsWith(subName, StringComparison.InvariantCulture));
                if (subscription == null)
                {
#pragma warning disable CA2000 // Dispose objects before losing scope. The subscription is disposed properly or added to the client.
                    subscription = new Subscription(Session.DefaultSubscription)
                    {
                        PublishingInterval = config.PublishingInterval,
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
                    foreach (var chunk in nodeList.ChunkBy(config.SubscriptionChunk))
                    {
                        if (token.IsCancellationRequested) break;
                        int lcount = 0;
                        subscription.AddItems(chunk
                            .Where(node => !hasSubscription.Contains(node.SourceId))
                            .Select(node =>
                            {
                                var monitor = builder(node);
                                monitor.Notification += handler;
                                count++;
                                lcount++;
                                return monitor;
                            })
                        );
                        log.Debug("Add subscriptions for {numnodes} nodes, {subscribed} / {total} done.", lcount, count, total);

                        if (lcount > 0 && subscription.Created)
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
                        else if (lcount > 0)
                        {
                            try
                            {
                                Session.AddSubscription(subscription);
                                subscription.Create();
                            }
                            catch (ServiceResultException ex)
                            {
                                throw ExtractorUtils.HandleServiceResult(ex,
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
                log.Information("Added {TotalAddedSubscriptions} / {total} subscriptions", count, total);
                return subscription;
            }
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

#pragma warning disable CA2000 // Dispose objects before losing scope
            var sub = AddSubscriptions(
                nodeList,
                "DataChangeListener",
                subscriptionHandler,
                node => new MonitoredItem
                {
                    StartNodeId = node.SourceId,
                    DisplayName = "Value: " + (node as NodeExtractionState).DisplayName,
                    SamplingInterval = config.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, config.QueueLength),
                    AttributeId = Attributes.Value
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
            if (emitters == null) throw new ArgumentNullException(nameof(emitters));

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
                    SamplingInterval = config.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, config.QueueLength),
                    Filter = filter
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
        /// <returns>ISystemContext for given session, or null if no session exists</returns>
        public ISystemContext SystemContext => Session?.SystemContext;
        public Dictionary<NodeId, IEnumerable<(NodeId root, QualifiedName browseName)>> GetEventFields(CancellationToken token)
        {
            if (eventFields != null) return eventFields;
            var collector = new EventFieldCollector(this, eventConfig);
            eventFields = collector.GetEventIdFields(token);
            foreach (var pair in eventFields)
            {
                log.Verbose("Collected event field: {id}", pair.Key);
                foreach (var fields in pair.Value)
                {
                    log.Verbose("    {root}: {browse}", fields.Item1, fields.Item2);
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

            if (eventFields.Keys.Any() && ((eventConfig.EventIds?.Any() ?? false) || !eventConfig.AllEvents))
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
                .Aggregate((IEnumerable<(NodeId Root, QualifiedName BrowseName)>)new List<(NodeId, QualifiedName)>(), (agg, kvp) => agg.Concat(kvp.Value))
                .GroupBy(variable => variable.BrowseName)
                .Select(items => items.FirstOrDefault());

            if (!fieldList.Any())
            {
                log.Warning("Missing valid event fields, no results will be returned");
            }
            var selectClauses = new SimpleAttributeOperandCollection();
            foreach (var (root, browseName) in fieldList)
            {
                if (eventConfig.ExcludeProperties.Contains(browseName.Name)
                    || eventConfig.BaseExcludeProperties.Contains(browseName.Name) && root == ObjectTypeIds.BaseEventType) continue;
                var operand = new SimpleAttributeOperand
                {
                    AttributeId = Attributes.Value,
                    TypeDefinitionId = root
                };
                operand.BrowsePath.Add(browseName);
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
            var filter = BuildAuditFilter();
            lock (subscriptionLock)
            {
                var subscription = Session.Subscriptions.FirstOrDefault(sub => sub.DisplayName.StartsWith("AuditListener", StringComparison.InvariantCulture))
#pragma warning disable CA2000 // Dispose objects before losing scope
                               ?? new Subscription(Session.DefaultSubscription)
                {
                    PublishingInterval = config.PublishingInterval,
                    DisplayName = "AuditListener"
                };
#pragma warning restore CA2000 // Dispose objects before losing scope
                if (subscription.MonitoredItemCount != 0) return;
                var item = new MonitoredItem
                {
                    StartNodeId = ObjectIds.Server,
                    Filter = filter,
                    AttributeId = Attributes.EventNotifier,
                    SamplingInterval = config.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, config.QueueLength)
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

        public NamespaceTable NamespaceTable => Session.NamespaceUris;
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
            int idx = Session.NamespaceUris.GetIndex(namespaceUri);
            if (idx < 0)
            {
                if (extractionConfig.NamespaceMap.ContainsValue(namespaceUri))
                {
                    string readNs = extractionConfig.NamespaceMap.First(kvp => kvp.Value == namespaceUri).Key;
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
            // Check if the value is somehow an array
            if (typeof(IEnumerable).IsAssignableFrom(datavalue.GetType()))
            {
                var enumerator = (datavalue as IEnumerable).GetEnumerator();
                enumerator.MoveNext();
                return ConvertToDouble(enumerator.Current);
            }
            // Give up if there is no clear way to convert it
            if (!typeof(IConvertible).IsAssignableFrom(datavalue.GetType())) return 0;
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
            if (value is string strValue)
            {
                return strValue;
            }
            if (typeof(IEnumerable).IsAssignableFrom(value.GetType()))
            {
                var builder = new StringBuilder("[");
                int count = 0;
                foreach (var dvalue in value as IEnumerable)
                {
                    builder.Append(((count++ > 0) ? ", " : "") + ConvertToString(dvalue));
                }
                builder.Append(']');
                return builder.ToString();
            }
            if (value.GetType() == typeof(NodeId))
            {
                return GetUniqueId((NodeId)value);
            }
            if (value.GetType() == typeof(ExpandedNodeId))
            {
                return GetUniqueId(ToNodeId((ExpandedNodeId)value));
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
        /// <param name="rNodeId">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        public string GetUniqueId(ExpandedNodeId rNodeId, int index = -1)
        {
            if (rNodeId == null || rNodeId.IsNull) return null;
            var nodeId = ToNodeId(rNodeId);
            if (nodeId == null || nodeId.IsNullNodeId) return null;
            if (nodeOverrides.TryGetValue(nodeId, out var nodeOverride))
            {
                if (index <= -1) return nodeOverride;
                return $"{nodeOverride}[{index}]";
            }

            // ExternalIds shorter than 32 chars are unlikely, this will generally avoid at least 1 re-allocation of the buffer,
            // and usually boost performance.
            var buffer = new StringBuilder(extractionConfig.IdPrefix, 32);

            if (!nsPrefixMap.TryGetValue(nodeId.NamespaceIndex, out var prefix))
            {
                var namespaceUri = rNodeId.NamespaceUri ?? Session.NamespaceUris.GetString(nodeId.NamespaceIndex);
                string newPrefix = extractionConfig.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");
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
        // Used to trim the whitespace off the end of a StringBuilder
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

        private void AppendNodeId(StringBuilder buffer, NodeId nodeId)
        {
            if (nodeOverrides.TryGetValue(nodeId, out var nodeOverride))
            {
                buffer.Append(nodeOverride);
                return;
            }

            if (!nsPrefixMap.TryGetValue(nodeId.NamespaceIndex, out var prefix))
            {
                var namespaceUri = Session.NamespaceUris.GetString(nodeId.NamespaceIndex);
                string newPrefix = extractionConfig.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");
                nsPrefixMap[nodeId.NamespaceIndex] = prefix = newPrefix;
            }

            buffer.Append(prefix);

            NodeId.Format(buffer, nodeId.Identifier, nodeId.IdType, 0);

            TrimEnd(buffer);
        }

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
        public string GetRelationshipId(BufferedReference reference)
        {
            if (reference == null) throw new ArgumentNullException(nameof(reference));
            var buffer = new StringBuilder(extractionConfig.IdPrefix, 64);
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
                buffer = new StringBuilder(extractionConfig.IdPrefix, 255);
                buffer.Append(reference.GetName());
                buffer.Append(';');
                buffer.Append(GetNodeIdString(reference.Source.Id).Substring(overflow));
                buffer.Append(';');
                buffer.Append(GetNodeIdString(reference.Target.Id).Substring(overflow));
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
            if (Appconfig != null)
            {
                Appconfig.CertificateValidator.CertificateValidation -= CertificateValidationHandler;
            }
            if (Session != null)
            {
                Session.KeepAlive -= ClientKeepAlive;
            }
        }
        #endregion
    }
    /// <summary>
    /// Parameter class containing the state of a single history read operation.
    /// </summary>
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
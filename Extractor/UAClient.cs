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
        private CancellationToken liveToken;

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
            this.config = config.UAConfig;
            extractionConfig = config.ExtractionConfig;
            historyGranularity = config.UAConfig.HistoryGranularity <= 0 ? TimeSpan.Zero
                : TimeSpan.FromSeconds(config.UAConfig.HistoryGranularity);
        }
        #region Session management
        /// <summary>
        /// Entrypoint for starting the opcua session. Must be called before any further requests can be made.
        /// </summary>
        public async Task Run(CancellationToken token)
        {
            liveToken = token;
            try
            {
                await StartSession();
            }
            catch (Exception e)
            {
                Log.Error(e, "Error starting client");
            }
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
            var root = session.ReadNode(nodeId);
            if (root == null || root.NodeId == NodeId.Null) return null;
            return new ReferenceDescription
            {
                NodeId = root.NodeId,
                BrowseName = root.BrowseName,
                DisplayName = root.DisplayName,
                NodeClass = root.NodeClass,
                ReferenceTypeId = null,
                IsForward = false,
                TypeDefinition = root.TypeDefinitionId
            };
        }
        public void AddNodeOverride(NodeId nodeId, string externalId)
        {
            nodeOverrides[nodeId] = externalId;
        }
        /// <summary>
        /// Get all children of the given list of parents as a map from parentId to list of children descriptions
        /// </summary>
        /// <param name="parents">List of parents to browse</param>
        /// <param name="referenceTypes">Referencetype to browse, defaults to HierarchicalReferences</param>
        /// <returns></returns>
        private Dictionary<NodeId, ReferenceDescriptionCollection> GetNodeChildren(IEnumerable<NodeId> parents, CancellationToken token, NodeId referenceTypes = null)
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
                        NodeClassMask = (uint)NodeClass.Variable | (uint)NodeClass.Object,
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
                        out _
                    );
                    var indexMap = new NodeId[lparents.Count()];
                    var continuationPoints = new ByteStringCollection();
                    int index = 0;
                    int bindex = 0;
                    foreach (var result in results)
                    {
                        NodeId nodeId = lparents.ElementAt(bindex++);
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
                            out _
                        );
                        int nindex = 0;
                        int pindex = 0;
                        continuationPoints.Clear();
                        foreach (var result in nextResults)
                        {
                            NodeId nodeId = indexMap[pindex++];
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
        private void BrowseDirectory(IEnumerable<NodeId> roots, Action<ReferenceDescription, NodeId> callback, CancellationToken token)
        {
            var nextIds = roots.ToList();
            int levelCnt = 0;
            int nodeCnt = 0;
            do
            {
                var references = Utils.ChunkBy(nextIds, config.BrowseNodesChunk)
                    .Select(ids => GetNodeChildren(nextIds, token))
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
                        if (rd.NodeId == ObjectIds.Server) continue;
                        if (extractionConfig.IgnorePrefix != null && extractionConfig.IgnorePrefix.Any(prefix =>
                            rd.DisplayName.Text.StartsWith(prefix, StringComparison.CurrentCulture))
                            || extractionConfig.IgnoreName != null && extractionConfig.IgnoreName.Contains(rd.DisplayName.Text)) continue;
                        lock (visitedNodesLock)
                        {
                            if (!visitedNodes.Add(ToNodeId(rd.NodeId))) continue;
                        }
                        callback(rd, parentId);
                        if (rd.NodeClass == NodeClass.Variable) continue;
                        nextIds.Add(ToNodeId(rd.NodeId));
                    }
                }
            } while (nextIds.Any());
            Log.Information("Found {NumUANodes} nodes in {NumNodeLevels} levels", nodeCnt, levelCnt);
            depth.Set(levelCnt);
        }
        #endregion

        #region Get data
        /// <summary>
        /// Read historydata for the requested nodes and call the callback after each call to HistoryRead
        /// </summary>
        /// <param name="toRead">Variables to read for</param>
        /// <param name="callback">Callback, takes a <see cref="HistoryReadResultCollection"/>,
        /// a bool indicating that this is the final callback for this node, and the id of the node in question</param>
        private void DoHistoryRead(IEnumerable<BufferedVariable> toRead,
            Action<HistoryData, bool, NodeId> callback,
            CancellationToken token)
        {
            DateTime lowest = DateTime.MinValue;
            lowest = toRead.Select((bvar) => { return bvar.LatestTimestamp; }).Min();
            var details = new ReadRawModifiedDetails
            {
                StartTime = lowest,
                EndTime = DateTime.Now.AddDays(1),
                NumValuesPerNode = (uint)config.HistoryReadChunk
            };
            int opCnt = 0;
            int ptCnt = 0;
            IncOperations();
            var ids = new HistoryReadValueIdCollection();
            var indexMap = new NodeId[toRead.Count()];
            int index = 0;
            foreach (var node in toRead)
            {
                ids.Add(new HistoryReadValueId
                {
                    NodeId = node.Id,
                });
                indexMap[index] = node.Id;
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
                        var hdata = ExtensionObject.ToEncodeable(data.HistoryData) as HistoryData;
                        ptCnt += hdata?.DataValues?.Count ?? 0;
                        callback(hdata, data == null || hdata == null || data.ContinuationPoint == null, indexMap[prevIndex]);
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
        public async Task DoHistoryRead(IEnumerable<BufferedVariable> toRead,
            Action<HistoryData, bool, NodeId> callback,
            TimeSpan granularity,
            CancellationToken token)
        {
            var tasks = new List<Task>();
            if (granularity == TimeSpan.Zero)
            {
                foreach (var variable in toRead)
                {
                    if (variable.Historizing)
                    {
                        tasks.Add(Task.Run(() => DoHistoryRead(new List<BufferedVariable> { variable }, callback, token), token));
                    }
                    else
                    {
                        callback(null, true, variable.Id);
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
            var groupedVariables = new Dictionary<long, IList<BufferedVariable>>();
            foreach (var variable in toRead)
            {
                if (variable.Historizing)
                {
                    cnt++;
                    long group = variable.LatestTimestamp.Ticks / granularity.Ticks;
                    if (!groupedVariables.ContainsKey(group))
                    {
                        groupedVariables[group] = new List<BufferedVariable>();
                    }
                    groupedVariables[group].Add(variable);
                }
                else
                {
                    callback(null, true, variable.Id);
                }
            }
            if (!groupedVariables.Any()) return;
            foreach (var nodes in groupedVariables.Values)
            {
                foreach (var nextNodes in Utils.ChunkBy(nodes, config.HistoryReadNodesChunk))
                {
                    tasks.Add(Task.Run(() => DoHistoryRead(nextNodes, callback, token)));
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
        /// Synchronizes a list of nodes with the server, creating subscriptions and reading historical data where necessary.
        /// </summary>
        /// <param name="nodeList">List of buffered variables to synchronize</param>
        /// <param name="callback">Callback used for DoHistoryRead. Takes a <see cref="HistoryReadResultCollection"/>,
        /// a bool indicating that this is the final callback for this node, and the id of the node in question</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        public async Task SynchronizeNodes(IEnumerable<BufferedVariable> nodeList,
            Action<HistoryData, bool, NodeId> callback,
            MonitoredItemNotificationEventHandler subscriptionHandler,
            CancellationToken token)
        {
            if (!nodeList.Any()) return;
            var subscription = session.Subscriptions.FirstOrDefault(sub => sub.DisplayName != "NodeChangeListener");
            if (subscription == null)
            {
                subscription = new Subscription(session.DefaultSubscription) { PublishingInterval = config.PollingInterval };
            }
            int count = 0;
            var hasSubscription = subscription.MonitoredItems
                .Select(sub => sub.ResolvedNodeId)
                .ToHashSet();

            foreach (var chunk in Utils.ChunkBy(nodeList, 1000))
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
            await DoHistoryRead(nodeList, callback, historyGranularity, token);
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
                    vnode.Historizing = enumerator.Current.GetValue(false);
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
            var result = GetNodeChildren(idsToCheck, token, ReferenceTypeIds.HasProperty);
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
        public static string ConvertToString(object value)
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
            if (value.GetType() == typeof(LocalizedText))
            {
                return ((LocalizedText)value).Text;
            }
            if (value.GetType() == typeof(ExtensionObject))
            {
                return ConvertToString(((ExtensionObject)value).Body);
            }
            if (value.GetType() == typeof(Range))
            {
                return $"({((Range)value).Low}, {((Range)value).High})";
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
        public static string ConvertToString(DataValue datavalue)
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
            if (extractionConfig.NSMaps.TryGetValue(namespaceUri, out string prefixNode))
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
            string extId = $"{extractionConfig.GlobalPrefix}.{prefix}:{nodeidstr}".Replace("\n", "");

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
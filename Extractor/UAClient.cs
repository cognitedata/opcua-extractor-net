using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System.Linq;
using Prometheus.Client;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Client managing the connection to the opcua server, and providing wrapper methods to simplify interaction with the server.
    /// </summary>
    public class UAClient
    {
        private readonly UAClientConfig config;
        private readonly BulkSizes bulkConfig;
        private Session session;
        private SessionReconnectHandler reconnectHandler;
        public Extractor Extractor { get; set; }
        private readonly object visitedNodesLock = new object();
        private readonly ISet<NodeId> visitedNodes = new HashSet<NodeId>();
        private readonly object subscriptionLock = new object();
        private readonly Dictionary<string, string> nsmaps = new Dictionary<string, string>();
        private bool clientReconnecting;
        public bool Started { get; private set; }
        private readonly TimeSpan historyGranularity;

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
            bulkConfig = config.BulkSizes;
            nsmaps = config.NSMaps;
            historyGranularity = config.UAConfig.HistoryGranularity <= 0 ? TimeSpan.Zero
                : TimeSpan.FromSeconds(config.UAConfig.HistoryGranularity);
        }
        #region Session management
        /// <summary>
        /// Entrypoint for starting the opcua session. Must be called before any further requests can be made.
        /// </summary>
        public async Task Run()
        {
            try
            {
                await StartSession();
            }
            catch (Exception e)
            {
                Logger.LogError("Error starting client");
                Logger.LogException(e);
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
            var application = new ApplicationInstance
            {
                ApplicationName = ".NET OPC-UA Extractor",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "opc.ua.net.extractor"
            };
            var appconfig = await application.LoadApplicationConfiguration(false);
            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!validAppCert)
            {
                Logger.LogWarning("Missing application certificate, using insecure connection.");
            }
            else
            {
                appconfig.ApplicationUri = Opc.Ua.Utils.GetApplicationUriFromCertificate(
                    appconfig.SecurityConfiguration.ApplicationCertificate.Certificate);
                config.Autoaccept |= appconfig.SecurityConfiguration.AutoAcceptUntrustedCertificates;
                appconfig.CertificateValidator.CertificateValidation += CertificateValidationHandler;
            }
            Logger.LogInfo("Attempt to select endpoint from: " + config.EndpointURL);
            var selectedEndpoint = CoreClientUtils.SelectEndpoint(config.EndpointURL, validAppCert && config.Secure);
            var endpointConfiguration = EndpointConfiguration.Create(appconfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            Logger.LogInfo("Attempt to connect to endpoint: " + endpoint.Description.SecurityPolicyUri);
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
            Logger.LogInfo("Successfully connected to server at " + config.EndpointURL);
        }
        /// <summary>
        /// Event triggered after a succesfull reconnect.
        /// </summary>
        private void ClientReconnectComplete(object sender, EventArgs eventArgs)
        {
            if (!clientReconnecting) return;
            if (!ReferenceEquals(sender, reconnectHandler)) return;
            session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            clientReconnecting = false;
            Logger.LogWarning("--- RECONNECTED ---");
            Task.Run(() => Extractor?.RestartExtractor());
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
                Logger.LogWarning(eventArgs.Status.ToString());
                if (reconnectHandler == null)
                {
                    connected.Set(0);
                    Logger.LogWarning("--- RECONNECTING ---");
                    clientReconnecting = true;
                    reconnectHandler = new SessionReconnectHandler();
                    reconnectHandler.BeginReconnect(sender, 5000, ClientReconnectComplete);
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
                eventArgs.Accept = config.Autoaccept;
                // TODO Verify client acceptance here somehow?
                if (config.Autoaccept)
                {
                    Logger.LogWarning("Accepted Bad Certificate " + eventArgs.Certificate.Subject);
                }
                else
                {
                    Logger.LogInfo("Rejected Bad Certificate " + eventArgs.Certificate.Subject);
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
        public async Task BrowseDirectoryAsync(NodeId root, Action<ReferenceDescription, NodeId> callback)
        {
            lock (visitedNodesLock)
            {
                visitedNodes.Clear();
                visitedNodes.Add(root);
            }
            try
            {
                await Task.Run(() => BrowseDirectory(new List<NodeId> { root }, callback));
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to browse directory");
                Logger.LogException(e);
            }
        }
        /// <summary>
        /// Get all children of the given list of parents as a map from parentId to list of children descriptions
        /// </summary>
        /// <param name="parents">List of parents to browse</param>
        /// <param name="referenceTypes">Referencetype to browse, defaults to HierarchicalReferences</param>
        /// <returns></returns>
        private Dictionary<NodeId, ReferenceDescriptionCollection> GetNodeChildren(IEnumerable<NodeId> parents, NodeId referenceTypes = null)
        {
            var finalResults = new Dictionary<NodeId, ReferenceDescriptionCollection>();
            foreach (var lparents in Utils.ChunkBy(parents, bulkConfig.UABrowse))
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
                    while (continuationPoints.Any())
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
                catch (Exception e)
                {
                    browseFailures.Inc();
                    Logger.LogError("Failed during browse session");
                    throw e;
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
        private void BrowseDirectory(IEnumerable<NodeId> roots, Action<ReferenceDescription, NodeId> callback)
        {
            var nextIds = roots.ToList();
            int levelCnt = 0;
            int nodeCnt = 0;
            do
            {
                if (clientReconnecting) return;
                var references = GetNodeChildren(nextIds);
                nextIds.Clear();
                levelCnt++;
                foreach (var rdlist in references)
                {
                    NodeId parentId = rdlist.Key;
                    nodeCnt += rdlist.Value.Count;
                    foreach (var rd in rdlist.Value)
                    {
                        if (rd.NodeId == ObjectIds.Server) continue;
                        if (!string.IsNullOrWhiteSpace(config.IgnorePrefix) && rd.DisplayName.Text
                            .StartsWith(config.IgnorePrefix, StringComparison.CurrentCulture)) continue;
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
            Logger.LogInfo("Found " + nodeCnt + " nodes in " + levelCnt + " levels");
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
            Action<HistoryData, bool, NodeId> callback)
        {
            DateTime lowest = DateTime.MinValue;
            lowest = toRead.Select((bvar) => { return bvar.LatestTimestamp; }).Min();
            var details = new ReadRawModifiedDetails
            {
                StartTime = lowest,
                EndTime = DateTime.Now.AddDays(1),
                NumValuesPerNode = (uint)bulkConfig.UAHistoryReadPoints
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
                } while (ids.Any());
            }
            catch (Exception e)
            {
                historyReadFailures.Inc();
                Logger.LogError("Failed during HistoryRead");
                throw e;
            }
            finally
            {
                DecOperations();
                Logger.LogInfo("Fetched " + ptCnt + " historical datapoints with " + opCnt + " operations for " + index + " nodes");
            }
        }
        public void DoHistoryRead(IEnumerable<BufferedVariable> toRead,
            Action<HistoryData, bool, NodeId> callback,
            TimeSpan granularity)
        {
            if (granularity == TimeSpan.Zero)
            {
                foreach (var variable in toRead)
                {
                    if (variable.Historizing)
                    {
                        Task.Run(() => DoHistoryRead(new List<BufferedVariable> { variable }, callback));
                    }
                    else
                    {
                        callback(null, true, variable.Id);
                    }
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
                foreach (var nextNodes in Utils.ChunkBy(nodes, bulkConfig.UAHistoryReadNodes))
                {
                    Task.Run(() => DoHistoryRead(nextNodes, callback));
                }
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
        public void SynchronizeNodes(IEnumerable<BufferedVariable> nodeList,
            Action<HistoryData, bool, NodeId> callback,
            MonitoredItemNotificationEventHandler subscriptionHandler)
        {
            var toSynch = nodeList.Where(node => IsNumericType(node.DataType) && node.ValueRank == ValueRanks.Scalar);
            if (!toSynch.Any()) return;
            var subscription = session.Subscriptions.FirstOrDefault(sub => sub.DisplayName != "NodeChangeListener");
            if (subscription == null)
            {
                subscription = new Subscription(session.DefaultSubscription) { PublishingInterval = config.PollingInterval };
            }
            int count = 0;
            var hasSubscription = subscription.MonitoredItems
                .Select(sub => sub.ResolvedNodeId)
                .ToHashSet();

            subscription.AddItems(toSynch
                .Where(node => !hasSubscription.Contains(node.Id))
                .Select(node =>
                {
                    var monitor = new MonitoredItem(subscription.DefaultItem)
                    {
                        StartNodeId = node.Id,
                        DisplayName = "Value: " + node.DisplayName
                    };
                    monitor.Notification += subscriptionHandler;
                    count++;
                    return monitor;
                })
            );

            Logger.LogInfo("Add " + count + " subscriptions");
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
                catch (Exception e)
                {
                    Logger.LogError("Failed to create subscriptions");
                    throw e;
                }
                finally
                {
                    DecOperations();
                }
                numSubscriptions.Set(subscription.MonitoredItemCount);
            }
            DoHistoryRead(toSynch, callback, historyGranularity);
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
            IEnumerable<uint> variables)
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
                int total = readValueIds.Count;
                foreach (var nextValues in Utils.ChunkBy(readValueIds, bulkConfig.UAAttributes))
                {
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
                }
                Logger.LogInfo("Read " + total + " attributes with " + count + " operations");
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to fetch attributes from opcua");
                attributeRequestFailures.Inc();
                throw e;
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
        public void ReadNodeData(IEnumerable<BufferedNode> nodes)
        {
            var values = GetNodeAttributes(nodes, new List<uint>
            {
                Attributes.Description
            }, new List<uint>
            {
                Attributes.DataType,
                Attributes.Historizing,
                Attributes.ValueRank
            });
            var enumerator = values.GetEnumerator();
            foreach (BufferedNode node in nodes)
            {
                enumerator.MoveNext();
                node.Description = enumerator.Current.GetValue("");
                if (node.IsVariable && node is BufferedVariable vnode)
                {
                    enumerator.MoveNext();
                    NodeId dataType = enumerator.Current.GetValue(NodeId.Null);
                    if (dataType.IdType == IdType.Numeric)
                    {
                        vnode.DataType = (uint)dataType.Identifier;
                    }
                    enumerator.MoveNext();
                    vnode.Historizing = enumerator.Current.GetValue(false);
                    enumerator.MoveNext();
                    vnode.ValueRank = enumerator.Current.GetValue(0);
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
        public void ReadNodeValues(IEnumerable<BufferedVariable> nodes)
        {
            var values = GetNodeAttributes(nodes.Where((BufferedVariable buff) => buff.ValueRank == ValueRanks.Scalar),
                new List<uint>(),
                new List<uint> { Attributes.Value }
            );
            var enumerator = values.GetEnumerator();
            foreach (var node in nodes)
            {
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
        public void GetNodeProperties(IEnumerable<BufferedNode> nodes)
        {
            var properties = new HashSet<BufferedVariable>();
            Logger.LogInfo("Get properties for " + nodes.Count() + " nodes");
            var idsToCheck = new List<NodeId>();
            foreach (var node in nodes)
            {
                if (node.IsVariable)
                {
                    idsToCheck.Add(node.Id);
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
            var result = GetNodeChildren(idsToCheck, ReferenceTypeIds.HasProperty);
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
            ReadNodeData(properties);
            ReadNodeValues(properties);
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
                return "(" + ((Range)value).Low + ", " + ((Range)value).High + ")";
            }
            if (value.GetType() == typeof(EUInformation))
            {
                return ((EUInformation)value).DisplayName + ": " + ((EUInformation)value).Description;
            }
            if (value.GetType() == typeof(EnumValueType))
            {
                return ((EnumValueType)value).DisplayName + ": " + ((EnumValueType)value).Value;
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
        public string GetUniqueId(ExpandedNodeId nodeid)
        {
            string namespaceUri = nodeid.NamespaceUri;
            if (namespaceUri == null)
            {
                namespaceUri = session.NamespaceUris.GetString(nodeid.NamespaceIndex);
            }
            string prefix;
            if (nsmaps.TryGetValue(namespaceUri, out string prefixNode))
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
            string nsstr = "ns=" + nodeid.NamespaceIndex + ";";
            int pos = nodeidstr.IndexOf(nsstr, StringComparison.CurrentCulture);
            if (pos == 0)
            {
                nodeidstr = nodeidstr.Substring(0, pos) + nodeidstr.Substring(pos + nsstr.Length);
            }
            string extId = config.GlobalPrefix + "." + prefix + ":" + nodeidstr;
            // ExternalId is limited to 128 characters
            extId = extId.Trim();
            if (extId.Length > 255)
            {
                return extId.Substring(0, 255);
            }
            return extId;
        }
        /// <summary>
        /// Check datatype is numeric and allowed to be mapped to CDF
        /// </summary>
        /// <param name="dataType">Datatype to be tested</param>
        /// <returns>True if datatype is numeric</returns>
        public bool IsNumericType(uint dataType)
        {
            return dataType >= DataTypes.Boolean && dataType <= DataTypes.Double;
        }
        #endregion
    }
}
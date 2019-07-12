
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
        private Session session;
        private SessionReconnectHandler reconnectHandler;
        public Extractor Extractor { get; set; }
        private readonly ISet<NodeId> visitedNodes = new HashSet<NodeId>();
        private readonly object subscriptionLock = new object();
        private bool clientReconnecting;
        public bool Started { get; private set; }

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

        /// <summary>
        /// Constructor, does not start the client.
        /// </summary>
        /// <param name="config">Full configuartion object</param>
        public UAClient(FullConfig config)
        {
            this.config = config.UAConfig;
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
                Logger.LogError("Erorr starting client");
                Logger.LogException(e);
            }
        }
        /// <summary>
        /// Close the session, cleaning up any client data on the server
        /// </summary>
        public void Close()
        {
            session.CloseSession(null, true);
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
            visitedNodes.Clear();
            Task.Run(() => Extractor?.RestartExtractor());
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
                    reconnectHandler.BeginReconnect(sender, config.ReconnectPeriod, ClientReconnectComplete);
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
        private void IncOperations()
        {
            lock (pendingOpLock)
            {
                pendingOperations++;
            }
        }
        private void DecOperations()
        {
            lock (pendingOpLock)
            {
                pendingOperations--;
            }
        }
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
            visitedNodes.Clear();
            visitedNodes.Add(root);
            try
            {
                await Task.Run(() => BrowseDirectory(root, callback));
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to browse directory");
                Logger.LogException(e);
            }
        }
        /// <summary>
        /// Returns a list of all nodes with a hierarchical reference to the parent
        /// </summary>
        /// <param name="parent">Parent of returned nodes</param>
        /// <returns><see cref="ReferenceDescriptionCollection"/> containing descriptions of all found children</returns>
        private ReferenceDescriptionCollection GetNodeChildren(NodeId parent, NodeId referenceTypes = null)
        {
            IncOperations();
            ReferenceDescriptionCollection references = null;
            try
            {
                session.Browse(
                    null,
                    null,
                    parent,
                    0,
                    BrowseDirection.Forward,
                    referenceTypes ?? ReferenceTypeIds.HierarchicalReferences,
                    true,
                    (uint)NodeClass.Variable | (uint)NodeClass.Object,
                    out byte[] continuationPoint,
                    out references
                );
                numBrowse.Inc();
                while (continuationPoint != null)
                {
                    session.BrowseNext(
                        null,
                        false,
                        continuationPoint,
                        out continuationPoint,
                        out ReferenceDescriptionCollection tmpReferences
                    );
                    references.AddRange(tmpReferences);
                    numBrowse.Inc();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                DecOperations();
            }
            return references;
        }
        /// <summary>
        /// The main browse method, recursively calls itself on all object children
        /// </summary>
        /// <param name="root">Root node. Will not be sent to callback</param>
        /// <param name="callback">Callback for each mapped node, takes a description of a single node, and its parent id</param>
        private void BrowseDirectory(NodeId root, Action<ReferenceDescription, NodeId> callback)
        {
            if (clientReconnecting) return;
            var references = GetNodeChildren(root);
            List<Task> tasks = new List<Task>();
            foreach (var rd in references)
            {
                if (rd.NodeId == ObjectIds.Server) continue;
                if (!string.IsNullOrWhiteSpace(config.IgnorePrefix) && rd.DisplayName.Text
                    .StartsWith(config.IgnorePrefix, StringComparison.CurrentCulture)) continue;
                if (!visitedNodes.Add(ToNodeId(rd.NodeId))) continue;
                callback(rd, root);
                if (rd.NodeClass == NodeClass.Variable) continue;
                tasks.Add(Task.Run(() => BrowseDirectory(ToNodeId(rd.NodeId), callback)));
            }
            Task.WhenAll(tasks).Wait();
        }
        #endregion

        #region Get data
        /// <summary>
        /// Read historydata for the requested node and call the callback after each call to HistoryRead
        /// </summary>
        /// <param name="toRead">Variable to read for</param>
        /// <param name="callback">Callback, takes a <see cref="HistoryReadResultCollection"/>,
        /// a bool indicating that this is the final callback for this node, and the id of the node in question</param>
        public void DoHistoryRead(BufferedVariable toRead,
            Action<HistoryReadResultCollection, bool, NodeId> callback)
        {
            var details = new ReadRawModifiedDetails
            {
                StartTime = toRead.LatestTimestamp,
                EndTime = DateTime.Now.AddDays(1),
                NumValuesPerNode = config.MaxResults
            };
            HistoryReadResultCollection results = null;
            IncOperations();
            try
            {
                do
                {
                    session.HistoryRead(
                        null,
                        new ExtensionObject(details),
                        TimestampsToReturn.Neither,
                        false,
                        new HistoryReadValueIdCollection
                        {
                        new HistoryReadValueId
                        {
                            NodeId = FromUniqueId(toRead.Id),
                            ContinuationPoint = results ? [0].ContinuationPoint
                        },

                        },
                        out results,
                        out _
                    );
                    numHistoryReads.Inc();
                    callback(results, results[0].ContinuationPoint == null, FromUniqueId(toRead.Id));
                } while (results != null && results[0].ContinuationPoint != null);
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                DecOperations();
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
            Action<HistoryReadResultCollection, bool, NodeId> callback,
            MonitoredItemNotificationEventHandler subscriptionHandler)
        {
            int count = 0;
            var toSynch = new List<BufferedVariable>();
            var toHistoryRead = new List<BufferedVariable>();

            foreach (var node in nodeList)
            {
                if (node != null
                    && node.DataType >= DataTypes.Boolean
                    && node.DataType <= DataTypes.Double
                    && node.IsVariable
                    && node.ValueRank == ValueRanks.Scalar)
                {
                    count++;
                    toSynch.Add(node);
                    if (node.Historizing)
                    {
                        toHistoryRead.Add(node);
                    }
                }
            }
            if (count == 0) return;
            Subscription subscription = null;
            foreach (var sub in session.Subscriptions)
            {
                if (sub.DisplayName != "NodeChangeListener")
                {
                    subscription = sub;
                    break;
                }
            }
            if (subscription == null)
            {
                subscription = new Subscription(session.DefaultSubscription) { PublishingInterval = config.PollingInterval };
            }
            count = 0;
            var hasSubscription = new HashSet<NodeId>();
            foreach (var item in subscription.MonitoredItems)
            {
                hasSubscription.Add(item.ResolvedNodeId);
            }
            foreach (BufferedNode node in toSynch)
            {
                if (!hasSubscription.Contains(FromUniqueId(node.Id)))
                {
                    var monitor = new MonitoredItem(subscription.DefaultItem)
                    {
                        StartNodeId = FromUniqueId(node.Id),
                        DisplayName = "Value: " + node.DisplayName
                    };
                    monitor.Notification += subscriptionHandler;
                    Logger.LogInfo("Add subscription to " + node.DisplayName);
                    subscription.AddItem(monitor);
                    count++;
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
                catch (Exception e)
                {
                    throw e;
                }
                finally
                {
                    DecOperations();
                }
                numSubscriptions.Set(subscription.MonitoredItemCount);
            }
            foreach (BufferedVariable node in toHistoryRead)
            {
                Task.Run(() => DoHistoryRead(node, callback));
            }
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
                foreach (var attribute in common)
                {
                    readValueIds.Add(new ReadValueId
                    {
                        AttributeId = attribute,
                        NodeId = FromUniqueId(node.Id)
                    });
                }
                if (node.IsVariable)
                {
                    foreach (var attribute in variables)
                    {
                        readValueIds.Add(new ReadValueId
                        {
                            AttributeId = attribute,
                            NodeId = FromUniqueId(node.Id)
                        });
                    }
                }
            }
            IEnumerable<DataValue> values = new DataValueCollection();
            IncOperations();
            try
            {
                var enumerator = readValueIds.GetEnumerator();
                int remaining = readValueIds.Count;
                int per = 1000;
                while (remaining > 0)
                {
                    Logger.LogInfo("Read " + Math.Min(remaining, per) + " attributes");
                    ReadValueIdCollection nextValues = new ReadValueIdCollection();
                    for (int i = 0; i < Math.Min(remaining, per); i++)
                    {
                        enumerator.MoveNext();
                        nextValues.Add(enumerator.Current);
                    }
                    session.Read(
                        null,
                        0,
                        TimestampsToReturn.Source,
                        nextValues,
                        out DataValueCollection lvalues,
                        out _
                    );
                    attributeRequests.Inc(Math.Min(remaining, per));
                    values = values.Concat(lvalues);
                    remaining -= per;
                }
            }
            catch (Exception e)
            {
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
                if (node.IsVariable)
                {
                    BufferedVariable vnode = node as BufferedVariable;
                    if (node == null) continue;
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
            var tasks = new List<Task>();
            var properties = new HashSet<BufferedVariable>();
            foreach (var node in nodes)
            {
                if (node.IsVariable)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        var children = GetNodeChildren(FromUniqueId(node.Id), ReferenceTypeIds.HasProperty);
                        foreach (var child in children)
                        {
                            var property = new BufferedVariable(GetUniqueId(child.NodeId),
                                child.DisplayName.Text, node.Id)
                            { IsProperty = true };
                            properties.Add(property);
                            if (node.properties == null)
                            {
                                node.properties = new List<BufferedVariable>();
                            }
                            node.properties.Add(property);
                        }
                    }));
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
            Task.WhenAll(tasks).Wait();
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
        /// Returns consistent unique string representation of an <see cref="ExpandedNodeId"/> or <see cref="NodeId"/>
        /// </summary>
        /// <param name="nodeid">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        public UniqueId GetUniqueId(ExpandedNodeId nodeid)
        {
            string namespaceUri = nodeid.NamespaceUri;
            if (namespaceUri == null)
            {
                namespaceUri = session.NamespaceUris.GetString(nodeid.NamespaceIndex);
            }
            return new UniqueId(namespaceUri, GetIdTypeChar(nodeid.IdType), nodeid.Identifier);
        }
        private char GetIdTypeChar(IdType idType)
        {
            switch (idType)
            {
                case IdType.Guid:
                    return 'g';
                case IdType.Numeric:
                    return 'i';
                case IdType.Opaque:
                    return 'o';
                case IdType.String:
                    return 's';
            }
            return 'i';
        }
        /// <summary>
        /// Returns consistent unique string representation of a <see cref="NodeId"/> given its namespaceUri
        /// </summary>
        /// <remarks>
        /// NodeId is, according to spec, unique in combination with its namespaceUri. We use this to generate a consistent, unique string
        /// to be used for mapping assets and timeseries in CDF to opcua nodes.
        /// To avoid having to send the entire namespaceUri to CDF, we allow mapping Uris to prefixes in the config file.
        /// </remarks>
        /// <param name="namespaceUri">NamespaceUri of given node</param>
        /// <param name="nodeid">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        private UniqueId GetUniqueId(string namespaceUri, NodeId nodeid)
        {
            return new UniqueId(namespaceUri, GetIdTypeChar(nodeid.IdType), nodeid.Identifier);
        }
        public bool IsNumericType(uint dataType)
        {
            return dataType >= DataTypes.Boolean && dataType <= DataTypes.Double;
        }
        public NodeId FromUniqueId(UniqueId uniqueId)
        {
            return new NodeId(uniqueId.value, (ushort)session.NamespaceUris.GetIndex(uniqueId.namespaceUri));
        }
        #endregion
    }
}
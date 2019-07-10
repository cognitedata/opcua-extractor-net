
using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using YamlDotNet.RepresentationModel;
using System.Linq;

namespace Cognite.OpcUa
{
    public class UAClient
    {
        static UAClientConfig config;
        Session session;
        SessionReconnectHandler reconnectHandler;
        readonly Dictionary<string, string> nsmaps = new Dictionary<string, string>();
        readonly Extractor extractor;
        readonly ISet<NodeId> visitedNodes = new HashSet<NodeId>();
        readonly object subscriptionLock = new object();
        bool clientReconnecting;
        public bool Started { get; private set; }

        public UAClient(UAClientConfig config, YamlMappingNode namespaces, Extractor extractor = null)
        {
            foreach (var node in namespaces.Children)
            {
                nsmaps.Add(((YamlScalarNode)node.Key).Value, ((YamlScalarNode)node.Value).Value);
            }
            this.extractor = extractor;
            UAClient.config = config;
        }
        #region Session management

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
        public void Close()
        {
            session.CloseSession(null, true);
        }
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
                appconfig.ApplicationUri = Utils.GetApplicationUriFromCertificate(
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
            Logger.LogInfo("Successfully connected to server at " + config.EndpointURL);
        }
        private void ClientReconnectComplete(object sender, EventArgs eventArgs)
        {
            if (!clientReconnecting) return;
            if (!ReferenceEquals(sender, reconnectHandler)) return;
            session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            clientReconnecting = false;
            Logger.LogWarning("--- RECONNECTED ---");
            visitedNodes.Clear();
            Task.Run(() => extractor?.RestartExtractor());
            reconnectHandler = null;
        }
        private void ClientKeepAlive(Session sender, KeepAliveEventArgs eventArgs)
        {
            if (eventArgs.Status != null && ServiceResult.IsNotGood(eventArgs.Status))
            {
                Logger.LogWarning(eventArgs.Status.ToString());
                if (reconnectHandler == null)
                {
                    Logger.LogWarning("--- RECONNECTING ---");
                    clientReconnecting = true;
                    extractor.Blocking = true;
                    reconnectHandler = new SessionReconnectHandler();
                    reconnectHandler.BeginReconnect(sender, config.ReconnectPeriod, ClientReconnectComplete);
                }
            }
        }
        private static void CertificateValidationHandler(CertificateValidator validator,
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
        #endregion

        #region Browse

        public async Task BrowseDirectoryAsync(NodeId root, Action<ReferenceDescription, NodeId> callback)
        {
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
        private ReferenceDescriptionCollection GetNodeChildren(NodeId parent)
        {
            session.Browse(
                null,
                null,
                parent,
                0,
                BrowseDirection.Forward,
                ReferenceTypeIds.HierarchicalReferences,
                true,
                (uint)NodeClass.Variable | (uint)NodeClass.Object,
                out byte[] continuationPoint,
                out ReferenceDescriptionCollection references
            );
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
            }
            return references;
        }
        private void BrowseDirectory(NodeId root, Action<ReferenceDescription, NodeId> callback)
        {
            if (!visitedNodes.Add(root)) return;
            if (clientReconnecting) return;
            var references = GetNodeChildren(root);
            List<Task> tasks = new List<Task>();
            foreach (var rd in references)
            {
                if (rd.NodeId == ObjectIds.Server) continue;
                callback(rd, root);
                if (rd.NodeClass == NodeClass.Variable) continue;
                tasks.Add(Task.Run(() => BrowseDirectory(ToNodeId(rd.NodeId), callback)));
            }
            Task.WhenAll(tasks.ToArray()).Wait();
        }
        #endregion

        #region Get data

        public void DoHistoryRead(BufferedVariable toRead,
            Action<HistoryReadResultCollection, bool, NodeId> callback)
        {
            var details = new ReadRawModifiedDetails
            {
                StartTime = toRead.LatestTimestamp,
                EndTime = DateTime.MaxValue,
                NumValuesPerNode = config.MaxResults
            };
            HistoryReadResultCollection results = null;
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
                            NodeId = toRead.Id,
                            ContinuationPoint = results ? [0].ContinuationPoint
                        },

                   },
                   out results,
                   out _
               );
               callback(results, results[0].ContinuationPoint == null, toRead.Id);
            } while (results != null && results[0].ContinuationPoint != null);
        }
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
                if (!hasSubscription.Contains(node.Id))
                {
                    var monitor = new MonitoredItem(subscription.DefaultItem)
                    {
                        StartNodeId = node.Id,
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
            foreach (BufferedVariable node in toHistoryRead)
            {
                Task.Run(() => DoHistoryRead(node, callback));
            }
        }
        private IEnumerable<DataValue> GetNodeAttributes(IEnumerable<BufferedNode> nodes,
            IEnumerable<uint> common,
            IEnumerable<uint> variables)
        {
            if (!nodes.Any()) return null;
            var readValueIds = new ReadValueIdCollection();
            foreach (var node in nodes)
            {
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
                values = values.Concat(lvalues);
                remaining -= per;
            }
            return values;
        }
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
        public void ReadNodeValues(IEnumerable<BufferedVariable> nodes)
        {
            var values = GetNodeAttributes(nodes.Where((BufferedVariable buff) => buff.ValueRank == -1),
                new List<uint>(),
                new List<uint> { Attributes.Value }
            );
            var enumerator = values.GetEnumerator();
            foreach (var node in nodes)
            {
                if (node.ValueRank == -1)
                {
                    enumerator.MoveNext();
                    node.SetDataPoint(enumerator.Current);
                }
            }
        }
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
                        var children = GetNodeChildren(node.Id);
                        foreach (var child in children)
                        {
                            var property = new BufferedVariable(ToNodeId(child.NodeId), child.DisplayName.Text, node.Id) { IsProperty = true };
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

        public NodeId ToNodeId(ExpandedNodeId nodeid)
        {
            return ExpandedNodeId.ToNodeId(nodeid, session.NamespaceUris);
        }
        public NodeId ToNodeId(string identifier, string namespaceUri)
        {
            string nsString = "ns=" + session.NamespaceUris.GetIndex(namespaceUri);
            if (session.NamespaceUris.GetIndex(namespaceUri) == -1)
            {
                return NodeId.Null;
            }
            return new NodeId(nsString + ";" + identifier);
        }
        public static double ConvertToDouble(DataValue datavalue)
        {
            if (datavalue == null || datavalue.Value == null) return 0;
            if (datavalue.Value.GetType().IsArray)
            {
                return Convert.ToDouble((datavalue.Value as IEnumerable<object>)?.First());
            }
            return Convert.ToDouble(datavalue.Value);
        }
        public static string ConvertToString(object value)
        {
            if (value == null) return "";
            if (value.GetType().IsArray)
            {
                string result = "[";
                if (value is object[]values)
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
        public static string ConvertToString(DataValue datavalue)
        {
            if (datavalue == null) return "";
            return ConvertToString(datavalue.Value);
        }
        public string GetUniqueId(ExpandedNodeId nodeid)
        {
            string namespaceUri = nodeid.NamespaceUri;
            if (namespaceUri == null)
            {
                namespaceUri = session.NamespaceUris.GetString(nodeid.NamespaceIndex);
            }
            return GetUniqueId(namespaceUri, ExpandedNodeId.ToNodeId(nodeid, session.NamespaceUris));
        }
        private string GetUniqueId(string namespaceUri, NodeId nodeid)
        {
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
            if (pos >= 0)
            {
                nodeidstr = nodeidstr.Substring(0, pos) + nodeidstr.Substring(pos + nsstr.Length);
            }
            return config.GlobalPrefix + "." + prefix + ":" + nodeidstr;
        }
        #endregion
    }
}
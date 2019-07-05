
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
    class UAClient
    {
        static UAClientConfig config;
        Session session;
        SessionReconnectHandler reconnectHandler;
        readonly YamlMappingNode nsmaps;
        readonly Extractor extractor;
        readonly ISet<NodeId> visitedNodes = new HashSet<NodeId>();
        readonly object subscriptionLock = new object();
        bool clientReconnecting;

        public UAClient(UAClientConfig config, YamlMappingNode nsmaps, Extractor extractor = null)
        {
            this.nsmaps = nsmaps;
            this.extractor = extractor;
            UAClient.config = config;
        }
        public async Task Run()
        {
            try
            {
                await StartSession();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error starting client: " + e.Message);
                throw e;
            }
        }
        public void Close()
        {
            session.CloseSession(null, true);
        }
        public async Task BrowseDirectoryAsync(NodeId root, Action<ReferenceDescription, NodeId> callback)
        {
            try
            {
                await Task.Run(() => BrowseDirectory(root, callback));
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to browse directory: " + e.Message);
            }
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
        public void ClearSubscriptions()
        {
            Console.WriteLine("Begin clear subscriptions");
            if (!session.RemoveSubscriptions(session.Subscriptions))
            {
                Console.WriteLine("Failed to remove subscriptions, retrying");
                session.RemoveSubscriptions(session.Subscriptions);
            }
            Console.WriteLine("End clear subscriptions");
        }
        public double ConvertToDouble(DataValue datavalue)
        {
            if (datavalue == null || datavalue.Value == null) return 0;
            if (datavalue.Value.GetType().IsArray)
            {
                return Convert.ToDouble((datavalue.Value as IEnumerable<object>)?.First());
            }
            return Convert.ToDouble(datavalue.Value);
        }
        public void DoHistoryRead(BufferedVariable toRead,
            Action<HistoryReadResultCollection, bool, NodeId> callback)
        {
            ReadRawModifiedDetails details = new ReadRawModifiedDetails
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
            } while (results[0].ContinuationPoint != null);
        }
        public void SynchronizeNodes(List<BufferedVariable> nodeList,
            Action<HistoryReadResultCollection, bool, NodeId> callback,
            MonitoredItemNotificationEventHandler subscriptionHandler)
        {
            int count = 0;
            List<BufferedVariable> toSynch = new List<BufferedVariable>();
            List<BufferedVariable> toHistoryRead = new List<BufferedVariable>();

            foreach (BufferedVariable node in nodeList)
            {
                if (node != null
                    && node.DataType >= DataTypes.SByte
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
            Subscription subscription = session.SubscriptionCount == 0
                ? new Subscription(session.DefaultSubscription) { PublishingInterval = config.PollingInterval }
                : session.Subscriptions.First();
            count = 0;
            ISet<NodeId> hasSubscription = new HashSet<NodeId>();
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
        public void ReadNodeData(IEnumerable<BufferedNode> nodes)
        {
            IEnumerable<uint> variableAttributes = new List<uint>
            {
                Attributes.DataType,
                Attributes.Historizing,
                Attributes.ValueRank
            };
            var readValueIds = new ReadValueIdCollection();
            foreach (BufferedNode node in nodes)
            {
                readValueIds.Add(new ReadValueId
                {
                    AttributeId = Attributes.Description,
                    NodeId = node.Id
                });
                if (node.IsVariable)
                {
                    foreach (uint attribute in variableAttributes)
                    {
                        readValueIds.Add(new ReadValueId
                        {
                            AttributeId = attribute,
                            NodeId = node.Id
                        });
                    }
                }
            }
            session.Read(
                null,
                0,
                TimestampsToReturn.Neither,
                readValueIds,
                out DataValueCollection values,
                out _
            );
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
        private async Task StartSession()
        {
            ApplicationInstance application = new ApplicationInstance
            {
                ApplicationName = ".NET OPC-UA Extractor",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "opc.ua.net.extractor"
            };
            ApplicationConfiguration appconfig = await application.LoadApplicationConfiguration(false);
            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!validAppCert)
            {
                Console.WriteLine("Missing application certificate, using insecure connection.");
            }
            else
            {
                appconfig.ApplicationUri = Utils.GetApplicationUriFromCertificate(
                    appconfig.SecurityConfiguration.ApplicationCertificate.Certificate);
                config.Autoaccept |= appconfig.SecurityConfiguration.AutoAcceptUntrustedCertificates;
                appconfig.CertificateValidator.CertificateValidation += CertificateValidationHandler;
            }
            var selectedEndpoint = CoreClientUtils.SelectEndpoint(config.EndpointURL, validAppCert && false);
            var endpointConfiguration = EndpointConfiguration.Create(appconfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            Console.WriteLine("Attempt to connect to endpoint: " + endpoint.Description.SecurityPolicyUri);
            session = await Session.Create(
                appconfig,
                endpoint,
                false,
                ".NET OPC-UA Extractor Client",
                0,
                new UserIdentity(new AnonymousIdentityToken()),
                // new UserIdentity(config.Username, config.Password),
                null
            );

            session.KeepAlive += ClientKeepAlive;
            Console.WriteLine("Successfully connected to server {0}", config.EndpointURL);
        }
        private void ClientReconnectComplete(object sender, EventArgs eventArgs)
        {
            if (!clientReconnecting) return;
            if (!ReferenceEquals(sender, reconnectHandler)) return;
            session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            clientReconnecting = false;
            Console.WriteLine("--- RECONNECTED ---");
            visitedNodes.Clear();
            Task.Run(() => extractor?.RestartExtractor());
        }
        private void ClientKeepAlive(Session sender, KeepAliveEventArgs eventArgs)
        {
            if (eventArgs.Status != null && ServiceResult.IsNotGood(eventArgs.Status))
            {
                Console.WriteLine("{0} {1}/{2}", eventArgs.Status, sender.OutstandingRequestCount, sender.DefunctRequestCount);

                if (reconnectHandler == null)
                {
                    Console.WriteLine("--- RECONNECTING ---");
                    clientReconnecting = true;
                    extractor?.SetBlocking();
                    reconnectHandler = new SessionReconnectHandler();
                    reconnectHandler.BeginReconnect(sender, config.ReconnectPeriod, ClientReconnectComplete);
                }
            }
        }
        private static void CertificateValidationHandler(CertificateValidator validator, CertificateValidationEventArgs eventArgs)
        {
            if (eventArgs.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                eventArgs.Accept = config.Autoaccept;
                // TODO Verify client acceptance here somehow?
                if (config.Autoaccept)
                {
                    Console.WriteLine("Accepted Bad Certificate {0}", eventArgs.Certificate.Subject);
                }
                else
                {
                    Console.WriteLine("Rejected Bad Certificate {0}", eventArgs.Certificate.Subject);
                }
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
                Task.Run(() => BrowseDirectory(ToNodeId(rd.NodeId), callback));
            }
        }
        private string GetUniqueId(string namespaceUri, NodeId nodeid)
        {
            string prefix;
            if (nsmaps.Children.TryGetValue(new YamlScalarNode(namespaceUri), out YamlNode prefixNode))
            {
                prefix = prefixNode.ToString();
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
    }
}
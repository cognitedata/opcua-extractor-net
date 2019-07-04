
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
        object subscriptionLock = new object();
        bool clientReconnecting = false;

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
        public async Task BrowseDirectory(NodeId root, Func<ReferenceDescription, long, Task<long>> callback, long initial)
        {
            try
            {
                await BrowseDirectory(root, initial, callback);
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
        public LocalizedText GetDescription(NodeId nodeId)
        {
            session.Read(
                null,
                0,
                TimestampsToReturn.Neither,
                new ReadValueIdCollection
                {
                    new ReadValueId
                    {
                        AttributeId = Attributes.Description,
                        NodeId = nodeId
                    }
                },
                out DataValueCollection values,
                out _
            );
            return values[0].GetValue<LocalizedText>("");
        }
        public uint GetDatatype(NodeId nodeId)
        {
            session.Read(
                null,
                0,
                TimestampsToReturn.Neither,
                new ReadValueIdCollection
                {
                    new ReadValueId
                    {
                        AttributeId = Attributes.DataType,
                        NodeId = nodeId
                    }
                },
                out DataValueCollection values,
                out _
            );
            if (values[0].GetValue(NodeId.Null).IdType != IdType.Numeric) return 0;
            return (uint)values[0].GetValue(NodeId.Null).Identifier;
        }
        public void SynchronizeDataNode(NodeId nodeid,
            DateTime startTime,
            Action<HistoryReadResultCollection, bool, NodeId> callback,
            MonitoredItemNotificationEventHandler subscriptionHandler)
        {
            if (clientReconnecting) return;
            // First get necessary node data
            SortedDictionary<uint, DataValue> attributes = new SortedDictionary<uint, DataValue>
            {
                { Attributes.DataType, null },
                { Attributes.Historizing, null },
                { Attributes.NodeClass, null },
                { Attributes.DisplayName, null },
                { Attributes.ValueRank, null },
                { Attributes.Value, null }
            };

            ReadValueIdCollection itemsToRead = new ReadValueIdCollection();
            foreach (uint attributeId in attributes.Keys)
            {
                ReadValueId itemToRead = new ReadValueId
                {
                    AttributeId = attributeId,
                    NodeId = nodeid
                };
                itemsToRead.Add(itemToRead);
            }
            session.Read(
                null,
                0,
                TimestampsToReturn.Source,
                itemsToRead,
                out DataValueCollection values,
                out _
            );

            for (int i = 0; i < itemsToRead.Count; i++)
            {
                attributes[itemsToRead[i].AttributeId] = values[i];
            }

            if ((NodeClass)attributes[Attributes.NodeClass].Value != NodeClass.Variable)
            {
                throw new Exception("Node not a variable");
            }
            // Filter out data we can't or won't parse
            if (((NodeId)attributes[Attributes.DataType].Value).IdType != IdType.Numeric
                || (uint)((NodeId)attributes[Attributes.DataType].Value).Identifier < DataTypes.SByte
                || (uint)((NodeId)attributes[Attributes.DataType].Value).Identifier > DataTypes.Double
                || (int)attributes[Attributes.ValueRank].Value != ValueRanks.Scalar) return;
            Subscription subscription;
            if (session.SubscriptionCount == 0)
            {
                subscription = new Subscription(session.DefaultSubscription) { PublishingInterval = config.PollingInterval };
            }
            else
            {
                subscription = session.Subscriptions.First();
            }
            bool contains = false;
            foreach (var item in subscription.MonitoredItems)
            {
                if (item.StartNodeId == nodeid)
                {
                    Console.WriteLine("Duplicate sub found" + nodeid);
                    contains = true;
                    break;
                }
            }
            if (!contains)
            {
                Console.WriteLine("Add subscription for " + attributes[Attributes.DisplayName] + ", " + nodeid);
                var monitor = new MonitoredItem(subscription.DefaultItem)
                {
                    DisplayName = "Value: " + attributes[Attributes.DisplayName],
                    StartNodeId = nodeid
                };

                monitor.Notification += subscriptionHandler;
                subscription.AddItem(monitor);
                lock (subscriptionLock)
                {
                    if (!subscription.Created)
                    {
                        session.AddSubscription(subscription);
                        subscription.Create();
                    }
                    else
                    {
                        lock (subscriptionLock)
                        {
                            subscription.CreateItems();
                        }
                    }
                }
            }

            if (!(bool)attributes[Attributes.Historizing].Value)
            {
                DataValue value = attributes[Attributes.Value];
                extractor.AddSingleDataPoint(new BufferedDataPoint(
                    (long)value.SourceTimestamp.Subtract(extractor.epoch).TotalMilliseconds,
                    nodeid,
                    ConvertToDouble(value)
                ));
                callback(null, true, nodeid);
                return;
            }
            // Thread.Sleep(1000);
            HistoryReadResultCollection results = null;
            do
            {
                ReadRawModifiedDetails details = new ReadRawModifiedDetails
                {
                    StartTime = startTime,
                    EndTime = DateTime.MaxValue,
                    NumValuesPerNode = config.MaxResults,
                };
                session.HistoryRead(
                    null,
                    new ExtensionObject(details),
                    TimestampsToReturn.Neither,
                    false,
                    new HistoryReadValueIdCollection
                    {
                        new HistoryReadValueId
                        {
                            NodeId = nodeid,
                            ContinuationPoint = results ? [0].ContinuationPoint
                        },

                    },
                    out results,
                    out _
                );
                callback(results, results[0].ContinuationPoint == null, nodeid);
            } while (results[0].ContinuationPoint != null);
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
        private async Task BrowseDirectory(NodeId root, long last, Func<ReferenceDescription, long, Task<long>> callback)
        {
            if (!visitedNodes.Add(root)) return;
            if (clientReconnecting) return;
            var references = GetNodeChildren(root);
            List<Task> tasks = new List<Task>();
            // Thread.Sleep(1000);
            foreach (var rd in references)
            {
                // Console.WriteLine("Call cb for parent " + last);
                if (rd.NodeId == ObjectIds.Server) continue;
                await Task.Run(async () =>
                {
                    long cbresult = await callback(rd, last);
                    if (cbresult > 0)
                    {
                        await BrowseDirectory(ToNodeId(rd.NodeId), cbresult, callback);
                    }
                }).ConfigureAwait(false);
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
        // Fetch data for synchronizing with cdf, also establishing a subscription. This does require that the node is a variable, or it will fail.
    }
    public class BufferedDataPoint
    {
        public readonly long timestamp;
        public readonly NodeId nodeId;
        public readonly double doubleValue;
        public readonly string stringValue;
        public readonly bool isString;
        public BufferedDataPoint(long timestamp, NodeId nodeId, double value)
        {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
            doubleValue = value;
            isString = false;
        }
        public BufferedDataPoint(long timestamp, NodeId nodeId, string value)
        {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
            stringValue = value;
            isString = true;
        }
    }
}
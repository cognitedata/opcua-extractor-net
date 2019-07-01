
using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using YamlDotNet.RepresentationModel;

namespace Cognite.OpcUa
{
    class UAClient
    {
        static UAClientConfig config;
        Session session;
        SessionReconnectHandler reconnectHandler;
        readonly YamlMappingNode nsmaps;
        Extractor extractor;

        public UAClient(UAClientConfig config, YamlMappingNode nsmaps, Extractor extractor = null)
        {
            this.nsmaps = nsmaps;
            this.extractor = extractor;
            UAClient.config = config;
            if (config.GlobalPrefix == null)
            {
                throw new Exception("Missing glboal prefix");
            }
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
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.InnerException.StackTrace);
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
            var selectedEndpoint = CoreClientUtils.SelectEndpoint(config.EndpointURL, validAppCert, 15000);
            var endpointConfiguration = EndpointConfiguration.Create(appconfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);

            session = await Session.Create(
                appconfig,
                endpoint,
                false,
                ".NET OPC-UA Extractor Client",
                0,
                new UserIdentity(new AnonymousIdentityToken()), null
            );

            session.KeepAlive += ClientKeepAlive;
            Console.WriteLine("Successfully connected to server {0}", config.EndpointURL);
        }
        private void ClientReconnectComplete(Object sender, EventArgs eventArgs)
        {
            if (!Object.ReferenceEquals(sender, reconnectHandler)) return;
            session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            Console.WriteLine("--- RECONNECTED ---");
            extractor?.RestartExtractor();
            // TODO Here we need to synch, as the server may have been alive while we were reconnecting.
        }
        private void ClientKeepAlive(Session sender, KeepAliveEventArgs eventArgs)
        {
            if (eventArgs.Status != null && ServiceResult.IsNotGood(eventArgs.Status))
            {
                Console.WriteLine("{0} {1}/{2}", eventArgs.Status, sender.OutstandingRequestCount, sender.DefunctRequestCount);

                if (reconnectHandler == null)
                {
                    Console.WriteLine("--- RECONNECTING ---");
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
            if (root == ObjectIds.Server) return;
            var references = GetNodeChildren(root);
            List<Task> tasks = new List<Task>();
            foreach (var rd in references)
            {
                tasks.Add(BrowseDirectory(ExpandedNodeId.ToNodeId(rd.NodeId, session.NamespaceUris), await callback(rd, last), callback));
            }
            await Task.WhenAll(tasks.ToArray());
        }
        public async Task BrowseDirectory(NodeId root, Func<ReferenceDescription, long, Task<long>> callback, long initial)
        {
            if (root != ObjectIds.ObjectsFolder)
            {
                Node rootNode = session.ReadNode(root);
            }
            await BrowseDirectory(root, initial, callback);
        }
        public void DebugBrowseDirectory(NodeId root)
        {
            Console.WriteLine(" Browsename, DisplayName, NodeClass");
            BrowseDirectory(root, 0, async (ReferenceDescription rd, long level) =>
            {
                Console.WriteLine(new String(' ', (int)(level * 4 + 1)) + "{0}, {1}, {2}", rd.BrowseName, rd.DisplayName, rd.NodeClass);
                Console.WriteLine(GetUniqueId(rd.NodeId));
                if (rd.NodeClass == NodeClass.Variable)
                {
                    SynchronizeDataNode(
                        ExpandedNodeId.ToNodeId(rd.NodeId, session.NamespaceUris),
                        new DateTime(1970, 1, 1), // TODO find a solution to this
                        (HistoryReadResultCollection val, bool final, NodeId nodeid) => {
                            if (val == null) return;
                            foreach (HistoryReadResult res in val)
                            {
                                HistoryData data = ExtensionObject.ToEncodeable(res.HistoryData) as HistoryData;
                                Console.WriteLine("Found {0} results", data.DataValues.Count);
                                foreach (var item in data.DataValues)
                                {
                                    Console.WriteLine("{0}: {1}", item.SourceTimestamp, item.Value);
                                }
                            }
                        },
                        (MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs) =>
                        {
                            foreach (var j in item.DequeueValues())
                            {
                                Console.WriteLine("{0}: {1}, {2}, {3}", item.DisplayName, j.Value, j.SourceTimestamp, j.StatusCode);
                            }
                        }

                    );
                }
                return level + 1;
            }).Wait();
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
        public string GetUniqueId(NodeId nodeid)
        {
            return GetUniqueId(session.NamespaceUris.GetString(nodeid.NamespaceIndex), nodeid);
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
        // Fetch data for synchronizing with cdf, also establishing a subscription. This does require that the node is a variable, or it will fail.
        public void SynchronizeDataNode(NodeId nodeid,
            DateTime startTime,
            Action<HistoryReadResultCollection, bool, NodeId> callback,
            MonitoredItemNotificationEventHandler subscriptionHandler)
        {
            // First get necessary node data
            SortedDictionary<uint, DataValue> attributes = new SortedDictionary<uint, DataValue>
            {
                { Attributes.DataType, null },
                { Attributes.Historizing, null },
                { Attributes.NodeClass, null },
                { Attributes.DisplayName, null }
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
                TimestampsToReturn.Neither,
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

            if ((uint)((NodeId)attributes[Attributes.DataType].Value).Identifier < DataTypes.SByte
                || (uint)((NodeId)attributes[Attributes.DataType].Value).Identifier > DataTypes.Double) return;

            Subscription subscription;
            if (session.SubscriptionCount == 0)
            {
                subscription = new Subscription(session.DefaultSubscription) { PublishingInterval = config.PollingInterval };
            }
            else
            {
                var enumerator = session.Subscriptions.GetEnumerator();
                enumerator.MoveNext();
                subscription = enumerator.Current;
            }
            var monitor = new MonitoredItem(subscription.DefaultItem)
            {
                DisplayName = "Value: " + attributes[Attributes.DisplayName],
                StartNodeId = nodeid
            };
            Console.WriteLine("Add subscription to {0}", attributes[Attributes.DisplayName]);
            // TODO, it might be more efficient to register all items as a single subscription? Does it matter?
            // It will require a more complicated subscription handler, but will probably result in less overhead overall.
            // The handlers can be reused if viable
            monitor.Notification += subscriptionHandler;
            subscription.AddItem(monitor);
            // This is thread safe, see implementation
            if (!subscription.Created)
            {
                session.AddSubscription(subscription);
                subscription.Create();
            }
            else
            {
                subscription.CreateItems();
            }
            if (!((bool)attributes[Attributes.Historizing].Value))
            {
                callback(null, true, nodeid);
                return;
            }
            HistoryReadResultCollection results = null;
            do
            {
                ReadRawModifiedDetails details = new ReadRawModifiedDetails()
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
                    new HistoryReadValueIdCollection()
                    {
                        new HistoryReadValueId()
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
            return new NodeId(nsString + ";" + identifier);
        }
        public void ClearSubscriptions()
        {
            session.RemoveSubscriptions(session.Subscriptions);
        }
    }
}
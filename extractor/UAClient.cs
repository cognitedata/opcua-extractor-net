
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;

namespace opcua_extractor_net
{
    class UAClient
    {
        const int ReconnectPeriod = 10;
        readonly string endpointURL;
        static bool autoaccept;
        Session session;
        SessionReconnectHandler reconnectHandler;
        readonly uint maxResults;
        readonly int pollingInterval;
        public UAClient(string endpointURL, bool autoaccept, int pollingInterval, uint maxResults)
        {
            this.endpointURL = endpointURL;
            UAClient.autoaccept = autoaccept;
            this.pollingInterval = pollingInterval;
            this.maxResults = maxResults;
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
            Console.WriteLine(application == null);
            ApplicationConfiguration config = await application.LoadApplicationConfiguration(false);
            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!validAppCert)
            {
                Console.WriteLine("Missing application certificate, using insecure connection.");
            }
            else
            {
                config.ApplicationUri = Utils.GetApplicationUriFromCertificate(config.SecurityConfiguration.ApplicationCertificate.Certificate);
                autoaccept |= config.SecurityConfiguration.AutoAcceptUntrustedCertificates;
                config.CertificateValidator.CertificateValidation += new CertificateValidationEventHandler(CertificateValidationHandler);
            }
            var selectedEndpoint = CoreClientUtils.SelectEndpoint(endpointURL, validAppCert, 15000);
            var endpointConfiguration = EndpointConfiguration.Create(config);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);

            session = await Session.Create(
                config,
                endpoint,
                false,
                ".NET OPC-UA Extractor Client",
                0,
                new UserIdentity(new AnonymousIdentityToken()), null
            );

            session.KeepAlive += ClientKeepAlive;
            Console.WriteLine("Successfully connected to server {0}", endpointURL);
        }
        private void ClientReconnectComplete(Object sender, EventArgs eventArgs)
        {
            if (!Object.ReferenceEquals(sender, reconnectHandler)) return;
            session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            Console.WriteLine("--- RECONNECTED ---");
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
                    reconnectHandler.BeginReconnect(sender, ReconnectPeriod * 1000, ClientReconnectComplete);
                }
            }
        }
        private static void CertificateValidationHandler(CertificateValidator validator, CertificateValidationEventArgs eventArgs)
        {
            if (eventArgs.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                eventArgs.Accept = autoaccept;
                if (autoaccept)
                {
                    Console.WriteLine("Accepted Bad Certificate {0}", eventArgs.Certificate.Subject);
                }
                else
                {
                    Console.WriteLine("Rejected Bad Certificate {0}", eventArgs.Certificate.Subject);
                }
            }
        }
        public Node GetServerNode()
        {
            if (session == null) return null;
            return session.ReadNode(ObjectIds.Server);
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
        private void BrowseDirectory(NodeId root, int level, Action<ReferenceDescription, int> callback)
        {
            if (root == ObjectIds.Server) return;
            var references = GetNodeChildren(root);
            foreach (var rd in references)
            {
                callback?.Invoke(rd, level);
                
                BrowseDirectory(ExpandedNodeId.ToNodeId(rd.NodeId, session.NamespaceUris), level + 1, callback);
            }
        }
        public void BrowseDirectory(NodeId root, Action<ReferenceDescription, int> callback)
        {
            BrowseDirectory(root, 0, callback);
        }
        public void DebugBrowseDirectory(NodeId root)
        {
            Console.WriteLine(" Browsename, DisplayName, NodeClass");
            BrowseDirectory(root, 0, (ReferenceDescription rd, int level) =>
            {
                Console.WriteLine(new String(' ', level * 4 + 1) + "{0}, {1}, {2}", rd.BrowseName, rd.DisplayName, rd.NodeClass);
                Console.WriteLine(GetUniqueId(rd.NodeId));
                if (rd.NodeClass == NodeClass.Variable)
                {
                    SynchronizeDataNode(
                        ExpandedNodeId.ToNodeId(rd.NodeId, session.NamespaceUris),
                        new DateTime(1970, 1, 1), // TODO find a solution to this
                        (HistoryReadResultCollection val) => {
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
            });
        }
        private string GetUniqueId(string namespaceUri, NodeId nodeid)
        {
            string prefix = ConfigurationManager.AppSettings[namespaceUri];
            if (prefix == null)
            {
                prefix = ConfigurationManager.AppSettings["defaultPrefix"];
            }
            if (prefix == null)
            {
                prefix = "opcua";
            }
            // Strip the ns=namespaceIndex; part, as it may be inconsistent between sessions
            // We still want the identifierType part of the id, so we just remove the first ocurrence of ns=..
            string nodeidstr = nodeid.ToString();
            string nsstr = "ns=" + nodeid.NamespaceIndex + ";";
            int pos = nodeidstr.IndexOf(nsstr, StringComparison.CurrentCulture);
            if (pos >= 0)
            {
                nodeidstr = nodeidstr.Substring(0, pos) + nodeidstr.Substring(pos + nsstr.Length);
            }
            return prefix + ":" + nodeidstr;

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
        // Fetch data for synchronizing with cdf, also establishing a subscription. This does require that the node is a variable, or it will fail.
        public void SynchronizeDataNode(NodeId nodeid,
            DateTime startTime,
            Action<HistoryReadResultCollection> callback,
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
                TimestampsToReturn.Both,
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
            Subscription subscription = new Subscription(session.DefaultSubscription) { PublishingInterval = pollingInterval };

            var monitor = new MonitoredItem(subscription.DefaultItem)
            {
                DisplayName = "Value: " + attributes[Attributes.DisplayName],
                StartNodeId = nodeid
            };
            // TODO, it might be more efficient to register all items as a single subscription? Does it matter?
            // It will require a more complicated subscription handler, but will probably result in less overhead overall.
            // The handlers can be reused if viable
            monitor.Notification += subscriptionHandler;
            subscription.AddItem(monitor);
            // This is thread safe, see implementation
            session.AddSubscription(subscription);
            subscription.Create();
            if (!((bool)attributes[Attributes.Historizing].Value)) return;
            // Store this date now, at this point the subscription should be created, so combined they should cover all timestamps.
            DateTime endTime = DateTime.UtcNow;
            HistoryReadResultCollection results = null;
            do
            {
                ReadRawModifiedDetails details = new ReadRawModifiedDetails()
                {
                    StartTime = startTime,
                    EndTime = endTime,
                    NumValuesPerNode = maxResults,
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
                callback(results);
            } while (results[0].ContinuationPoint != null);
        }
    }
}
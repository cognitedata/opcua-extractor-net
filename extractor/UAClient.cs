
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;
using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;

namespace opcua_extractor_net
{
    class UAClient
    {
        const int ReconnectPeriod = 10;
        string endpointURL;
        static bool autoaccept;
        Session session = null;
        SessionReconnectHandler reconnectHandler;
        public UAClient(string _endpointURL, bool _autoaccept)
        {
            endpointURL = _endpointURL;
            autoaccept = _autoaccept;
        }
        public async Task run()
        {
            try {
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
            ApplicationInstance application = new ApplicationInstance {
                ApplicationName = ".NET OPC-UA Extractor",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "opc.ua.net.extractor"
            };
            ApplicationConfiguration config = await application.LoadApplicationConfiguration(false);
            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!validAppCert)
            {
                Console.WriteLine("Missing application certificate, using insecure connection.");
            }
            else
            {
                config.ApplicationUri = Utils.GetApplicationUriFromCertificate(config.SecurityConfiguration.ApplicationCertificate.Certificate);
                if (config.SecurityConfiguration.AutoAcceptUntrustedCertificates)
                {
                    autoaccept = true;
                }
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
        public Node getServerNode() {
            if (session == null) return null;
            return session.ReadNode(ObjectIds.Server);
        }
        private ReferenceDescriptionCollection getNodeChildren(NodeId parent)
        {
            Byte[] continuationPoint;
            ReferenceDescriptionCollection references;
            session.Browse(
                null,
                null,
                parent,
                0,
                BrowseDirection.Forward,
                ReferenceTypeIds.HierarchicalReferences,
                true,
                (uint)NodeClass.Variable | (uint)NodeClass.Object,
                out continuationPoint,
                out references
            );
            return references;
        }
        private void _browseDirectory(NodeId root, int level)
        {
            if (root == ObjectIds.Server) return;
            var references = getNodeChildren(root);
            foreach (var rd in references)
            {
                Console.WriteLine(new String(' ', level * 4 + 1) + "{0}, {1}, {2}", rd.BrowseName, rd.DisplayName, rd.NodeClass);
                Console.WriteLine(getUniqueId(rd.NodeId));
                _browseDirectory(ExpandedNodeId.ToNodeId(rd.NodeId, session.NamespaceUris), level + 1);
            }
        }
        public void browseDirectory(NodeId root)
        {
            Console.WriteLine(" BrowseName, DisplayName, NodeClass");
            _browseDirectory(root, 0);
        }
        private string getUniqueId(string namespaceUri, NodeId nodeid)
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
            string nsstr = "ns=" + nodeid.NamespaceIndex+";";
            int pos = nodeidstr.IndexOf(nsstr);
            if (pos >= 0) {
                nodeidstr = nodeidstr.Substring(0, pos) + nodeidstr.Substring(pos + nsstr.Length);
            }
            return prefix + ":" + nodeidstr;
 
        }
        public string getUniqueId(NodeId nodeid)
        {
            return getUniqueId(session.NamespaceUris.GetString(nodeid.NamespaceIndex), nodeid);
        }
        public string getUniqueId(ExpandedNodeId nodeid)
        {
            string namespaceUri = nodeid.NamespaceUri;
            if (namespaceUri == null)
            {
                namespaceUri = session.NamespaceUris.GetString(nodeid.NamespaceIndex);
            }
            return getUniqueId(namespaceUri, ExpandedNodeId.ToNodeId(nodeid, session.NamespaceUris));
        }
    }
}
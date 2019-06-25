
using System.Threading;
using System.Threading.Tasks;
using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;

namespace opcua_extractor_net
{
    class OPCUAClient
    {
        const int ReconnectPeriod = 10;
        bool running = false;
        string endpointURL;
        static bool autoaccept;
        int stopTimeout;
        Session session;
        SessionReconnectHandler reconnectHandler;
        public OPCUAClient(string _endpointURL, bool _autoaccept, int _stopTimeout)
        {
            endpointURL = _endpointURL;
            autoaccept = _autoaccept;
            stopTimeout = _stopTimeout <= 0 ? Timeout.Infinite : _stopTimeout;
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
                60000,
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
    }
}
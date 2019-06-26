using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System.Threading;
using System.Configuration;

namespace opcua_extractor_net
{
    class Program
    {
        static void Main(string[] args)
        {
            string clientURL = ConfigurationManager.AppSettings["clientURL"];
            bool autoaccept = ConfigurationManager.AppSettings["autoaccept"] == "true";
            UAClient client = new UAClient(clientURL, autoaccept);
            client.run().Wait();

            ManualResetEvent quitEvent = new ManualResetEvent(false);
            try
            {
                Console.CancelKeyPress += (sender, eArgs) =>
                {
                    quitEvent.Set();
                    eArgs.Cancel = true;
                };
            }
            catch
            {
            }

            quitEvent.WaitOne(-1);
        }
    }
}

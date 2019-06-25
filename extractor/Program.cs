using System;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System.Threading;

namespace opcua_extractor_net
{
    class Program
    {
        static void Main(string[] args)
        {
            OPCUAClient client = new OPCUAClient("opc.tcp://localhost:4840", false, 0);
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

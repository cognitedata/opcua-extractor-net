using System.ServiceProcess;

namespace OpcUaService
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new OpcUaWindowsService(args)
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}

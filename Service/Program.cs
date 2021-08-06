using System;
using System.IO;
using System.Reflection;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Service
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    string path;
                    if (args.Length > 0)
                    {
                        path = args[0];
                        if (!Directory.Exists(path))
                        {
                            throw new ConfigurationException($"Target directory does not exist: {path}");
                        }
                    }
                    else
                    {
                        path = Directory.GetParent(AppContext.BaseDirectory).Parent.FullName;
                    }
                    Directory.SetCurrentDirectory(path);
                    var configFile = "config/config.yml";
                    var config = services.AddConfig<FullConfig>(configFile, 1);
                    config.Source.ConfigRoot = "config/";
                    services.AddMetrics();
                    services.AddLogger();
                    if (config.Cognite != null)
                    {
                        services.AddCogniteClient("OPC-UA Extractor", $"CogniteOPCUAExtractor/{Version.GetVersion()}", true, true, true);
                    }
                    services.AddStateStore();
                    services.AddHostedService<Worker>();
                })
                .ConfigureLogging(loggerFactory => loggerFactory.AddEventLog())
                .UseWindowsService()
                .UseSystemd();
    }
}

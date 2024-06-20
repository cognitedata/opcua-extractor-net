/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils.CommandLine;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Parsing;
using System.Threading;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Cognite.OpcUa
{


    public class BaseExtractorParams
    {
        [CommandLineOption("Override configured OPC-UA endpoint", true, "-e")]
        public string? EndpointUrl { get; set; }
        [CommandLineOption("Override configured OPC-UA user", true, "-u")]
        public string? User { get; set; }
        [CommandLineOption("Override configured OPC-UA password", true, "-p")]
        public string? Password { get; set; }
        [CommandLineOption("Auto accept server certificates")]
        public bool AutoAccept { get; set; }
        [CommandLineOption("Try to use a secured OPC-UA endpoint")]
        public bool Secure { get; set; }
        [CommandLineOption("Set path to .yml configuration file", true, "-f")]
        public string? ConfigFile { get; set; }
        [CommandLineOption("Set path to config directory", true, "-d")]
        public string? ConfigDir { get; set; }
        [CommandLineOption("Set path to log files, enables logging to file")]
        public string? LogDir { get; set; }
        [CommandLineOption("Set the console log-level [fatal/error/warning/information/debug/verbose]", true, "-l")]
        public string? LogLevel { get; set; }
        [CommandLineOption("Set the working directory of the extractor. Defaults to current directory for standalone," +
                " or one level above for service version", true, "-w")]
        public string? WorkingDir { get; set; }
        [CommandLineOption("Run extractor without a yml config file. The .xml config file is still needed", true, "-n")]
        public bool NoConfig { get; set; }
        [CommandLineOption("Run the extractor in dry-run mode. In this mode, it will not push anything to destinations")]
        public bool DryRun { get; set; }

        public bool ConfigTool { get; set; }
        public FullConfig? Config { get; set; }
        public FullConfig? BaseConfig { get; set; }
    }

    public class ExtractorParams : BaseExtractorParams
    {
        [CommandLineOption("Required flag when starting the extractor as a service", true, "-s")]
        public bool Service { get; set; }
        [CommandLineOption("Exit the extractor on failure. Equivalent to source.exit-on-failure", true, "-x")]
        public bool Exit { get; set; }

    }

    public class ConfigToolParams : BaseExtractorParams
    {
        [CommandLineOption("Path to output of config tool. Defaults to [config-dir]/config.config-tool-output.yml", true, "-o")]
        public string? ConfigTarget { get; set; }

    }

    /// <summary>
    /// Console launcher for the OPC-UA extractor and Configuration tool. Includes basic setup of logging/config/metrics and
    /// parsing of command-line arguments
    /// </summary>
    public static class Program
    {
        // For testing
        public static bool CommandDryRun { get; set; }
        public static Action<ServiceCollection, BaseExtractorParams>? OnLaunch { get; set; }
        public static CancellationToken? RootToken { get; set; }
        public static async Task<int> Main(string[] args)
        {
            try
            {
                ConfigurationUtils.AddTypeConverter(new FieldFilterConverter());
            }
            catch { }

            return await GetCommandLineOptions().InvokeAsync(args);
        }

        private static Parser GetCommandLineOptions()
        {
            var services = new ServiceCollection();

            var rootCommand = new RootCommand
            {
                Description = "Cognite OPC-UA Extractor"
            };

            var toolCmd = new Command("tool", "Run the configuration tool");
            rootCommand.Add(toolCmd);

            bool OnLaunchCommon<T>(T setup) where T : BaseExtractorParams
            {
                services.AddSingleton(setup);
                OnLaunch?.Invoke(services, setup);
                if (CommandDryRun) return false;
                return true;
            }

            var rootBinder = new AttributeBinder<ExtractorParams>();
            rootBinder.AddOptionsToCommand(rootCommand);

            rootCommand.SetHandler<ExtractorParams>(async setup =>
            {
                if (!OnLaunchCommon(setup)) return;
                if (setup.Service)
                {
                    await RunService(services, setup);
                }
                else
                {
                    await ExtractorStarter.RunExtractor(null, setup, services, RootToken ?? CancellationToken.None);
                }
            }, rootBinder);

            var toolBinder = new AttributeBinder<ConfigToolParams>();
            toolBinder.AddOptionsToCommand(toolCmd);

            toolCmd.SetHandler<ConfigToolParams>(async setup =>
            {
                setup.ConfigTool = true;
                if (!OnLaunchCommon(setup)) return;
                await ExtractorStarter.RunConfigTool(null, setup, services, RootToken ?? CancellationToken.None);
            }, toolBinder);

            return new CommandLineBuilder(rootCommand)
                .UseVersionOption()
                .UseHelp()
                .Build();
        }

        private static async Task RunService(ServiceCollection extServices, ExtractorParams setup)
        {
            var builder = Host.CreateDefaultBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService(provider =>
                        new Worker(provider.GetRequiredService<ILogger<Worker>>(), extServices, setup));
                });
            if (OperatingSystem.IsWindows())
            {
                builder = builder.ConfigureLogging(loggerFactory =>
                {
                    if (OperatingSystem.IsWindows())
                    {
                        loggerFactory.AddEventLog();
                    }
                }).UseWindowsService(options => options.ServiceName = "OpcuaExtractor");
            }
            else if (OperatingSystem.IsLinux())
            {
                builder = builder.UseSystemd();
            }
            await builder.Build().RunAsync();
        }
    }
}

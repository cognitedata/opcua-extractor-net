﻿/* Cognite Extractor for OPC-UA
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

using Cognite.OpcUa.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.Threading;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Cognite.OpcUa
{
    public class ExtractorParams
    {
        public string EndpointUrl { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public bool AutoAccept { get; set; }
        public bool Secure { get; set; }
        public string ConfigFile { get; set; }
        public string ConfigDir { get; set; }
        public string LogDir { get; set; }
        public string ConfigTarget { get; set; }
        public bool NoConfig { get; set; }
        public string LogLevel { get; set; }
        public bool Service { get; set; }
        public string WorkingDir { get; set; }
        public bool Exit { get; set; }
        public bool ConfigTool { get; set; }
        public FullConfig Config { get; set; }
        public FullConfig BaseConfig { get; set; }
    }
    /// <summary>
    /// Console launcher for the OPC-UA extractor and Configuration tool. Includes basic setup of logging/config/metrics and
    /// parsing of command-line arguments
    /// </summary>
    public static class Program
    {
        // For testing
        public static bool CommandDryRun { get; set; }
        public static Action<ServiceCollection, ExtractorParams> OnLaunch { get; set; }
        public static async Task<int> Main(string[] args)
        {
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

            var option = new Option<string>("--endpoint-url", "Override configured OPC-UA endpoint");
            option.AddAlias("-e");
            rootCommand.AddGlobalOption(option);

            option = new Option<string>("--user", "Override configured OPC-UA user");
            option.AddAlias("-u");
            rootCommand.AddGlobalOption(option);

            option = new Option<string>("--password", "Override configured OPC-UA password");
            option.AddAlias("-p");
            rootCommand.AddGlobalOption(option);

            var flag = new Option("--auto-accept", "Auto accept server certificates");
            rootCommand.AddGlobalOption(flag);

            flag = new Option("--secure", "Try to use a secured OPC-UA endpoint");
            rootCommand.AddGlobalOption(flag);

            option = new Option<string>("--config-file", "Set path to .yml configuration file");
            option.AddAlias("-f");
            rootCommand.AddGlobalOption(option);

            option = new Option<string>("--config-dir", "Set path to config directory");
            option.AddAlias("-d");
            rootCommand.AddGlobalOption(option);

            option = new Option<string>("--log-dir", "Set path to log files, enables logging to file");
            rootCommand.AddGlobalOption(option);

            option = new Option<string>("--config-target", "Path to output of config tool. Defaults to [config-dir]/config.config-tool-output.yml");
            option.AddAlias("-o");
            toolCmd.AddOption(option);

            flag = new Option("--no-config", "Run extractor without a yml config file. The .xml config file is still needed");
            flag.AddAlias("-n");
            rootCommand.AddGlobalOption(flag);

            option = new Option<string>("--log-level", "Set the console log-level [fatal/error/warning/information/debug/verbose]");
            option.AddAlias("-l");
            rootCommand.AddGlobalOption(option);

            flag = new Option("--service", "Required flag when starting the extractor as a service");
            flag.AddAlias("-s");
            rootCommand.AddOption(flag);

            option = new Option<string>("--working-dir", "Set the working directory of the extractor. Defaults to current directory for standalone," +
                " or one level above for service version");
            option.AddAlias("-w");
            rootCommand.AddGlobalOption(option);

            flag = new Option("--exit", "Exit the extractor on failure. Equivalent to source.exit-on-failure");
            flag.AddAlias("-x");
            rootCommand.AddOption(flag);

            bool OnLaunchCommon(ExtractorParams setup)
            {
                services.AddSingleton(setup);
                OnLaunch?.Invoke(services, setup);
                if (CommandDryRun) return false;
                return true;
            }

            rootCommand.Handler = CommandHandler.Create(async (ExtractorParams setup) =>
            {
                if (!OnLaunchCommon(setup)) return;
                if (setup.Service)
                {
                    await RunService(services, setup);
                }
                else
                {
                    await RunStandalone(services, setup);
                }
            });
            toolCmd.Handler = CommandHandler.Create(async (ExtractorParams setup) =>
            {
                setup.ConfigTool = true;
                if (!OnLaunchCommon(setup)) return;
                await RunStandalone(services, setup);
            });

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
                builder = builder.ConfigureLogging(loggerFactory => loggerFactory.AddEventLog())
                    .UseWindowsService(options => options.ServiceName = "OpcuaExtractor");
            }
            else if (OperatingSystem.IsLinux())
            {
                builder = builder.UseSystemd();
            }
            await builder.Build().RunAsync();
        }
        private static async Task RunStandalone(ServiceCollection services, ExtractorParams setup)
        {
            if (setup.ConfigTool)
            {
                await ExtractorStarter.RunConfigTool(null, setup, services, CancellationToken.None);
            }
            else
            {
                await ExtractorStarter.RunExtractor(null, setup, services, CancellationToken.None);
            }
        }
    }
}

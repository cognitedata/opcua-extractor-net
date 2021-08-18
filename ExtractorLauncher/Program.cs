/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;
using Serilog;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Cognite.OpcUa
{
    /// <summary>
    /// Console launcher for the OPC-UA extractor and Configuration tool. Includes basic setup of logging/config/metrics and
    /// parsing of command-line arguments
    /// </summary>
    class Program
    {
        private static readonly Gauge version =
            Metrics.CreateGauge("opcua_version", $"version: {Version.GetVersion()}, status: {Version.Status()}");
        private static Serilog.ILogger log;
        static int Main(string[] args)
        {
            // Temporary logger config for capturing logs during configuration.
            Log.Logger = LoggingUtils.GetSerilogDefault();

            return GetCommandLineOptions().InvokeAsync(args).Result;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1812:Avoid uninstantiated internal classes", Justification = "Late instantiation")]
        private class ExtractorParams
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

        private static Parser GetCommandLineOptions()
        {
            var rootCommand = new RootCommand();
            rootCommand.Description = "Cognite OPC-UA Extractor";
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

            rootCommand.Handler = CommandHandler.Create((ExtractorParams setup) =>
            {
                if (setup.Service)
                {
                    RunService(setup);
                }
                else
                {
                    RunStandalone(setup);
                }
            });
            toolCmd.Handler = CommandHandler.Create((ExtractorParams setup) =>
            {
                setup.ConfigTool = true;
                RunStandalone(setup);
            });

            return new CommandLineBuilder(rootCommand)
                .UseVersionOption()
                .UseHelp()
                .Build();
        }

        private static void RunService(ExtractorParams setup)
        {
            Host.CreateDefaultBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    Configure(setup, services);
                    services.AddHostedService<Worker>();
                })
                .ConfigureLogging(loggerFactory => loggerFactory.AddEventLog())
                .UseWindowsService(options => options.ServiceName = "OpcuaExtractor")
                .UseSystemd()
                .Build()
                .Run();
        }
        private static void RunStandalone(ExtractorParams setup)
        {
            var services = new ServiceCollection();
            Configure(setup, services);
            var provider = services.BuildServiceProvider();
            Log.Logger = provider.GetRequiredService<Serilog.ILogger>();
            log = Log.Logger.ForContext<Program>();

            log.Information("Starting OPC UA Extractor version {version}", Version.GetVersion());
            log.Information("Revision information: {status}", Version.Status());

            version.Set(0);

            if (setup.ConfigTool)
            {
                string configOutput = setup.ConfigTarget ?? Path.Combine(setup.Config.Source.ConfigRoot, "config.config-tool-output.yml");
                RunConfigTool(setup.Config, setup.BaseConfig, configOutput);
            }
            else
            {
                var metrics = provider.GetRequiredService<MetricsService>();
                metrics.Start();
                RunExtractor(setup.Config, provider);
            }
        }
        private static void Configure(ExtractorParams setup, IServiceCollection services)
        {
            string path = null;
            if (setup.WorkingDir != null)
            {
                path = setup.WorkingDir;
            }
            else if (setup.Service)
            {
                path = Directory.GetParent(AppContext.BaseDirectory).Parent.FullName;
            }
            if (path != null)
            {
                if (!Directory.Exists(path))
                {
                    throw new ConfigurationException($"Target directory does not exist: {path}");
                }
                Directory.SetCurrentDirectory(path);
            }

            string configDir = setup.ConfigDir ?? Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR") ?? "config/";

            setup.Config = new FullConfig();
            setup.BaseConfig = null;
            if (!setup.NoConfig)
            {
                try
                {
                    string configFile = setup.ConfigFile ?? System.IO.Path.Combine(configDir, setup.ConfigTool ? "config.config-tool.yml" : "config.yml");
                    Log.Information("Loading config file from {path}", configFile);
                    setup.Config = services.AddConfig<FullConfig>(configFile, 1);
                    if (setup.ConfigTool)
                    {
                        setup.BaseConfig = ConfigurationUtils.TryReadConfigFromFile<FullConfig>(configFile, 1);
                    }
                }
                catch (Extractor.Configuration.ConfigurationException e)
                {
                    Log.Error("Failed to load configuration: {msg}", e.Message);
                    throw;
                }
                setup.Config.Source.ConfigRoot = configDir;
            }
            else
            {
                setup.Config.GenerateDefaults();
                if (setup.ConfigTool)
                {
                    setup.BaseConfig = new FullConfig();
                    setup.BaseConfig.GenerateDefaults();
                    setup.BaseConfig.Version = 1;
                    setup.Config.Version = 1;
                }
            }

            if (!string.IsNullOrEmpty(setup.EndpointUrl)) setup.Config.Source.EndpointUrl = setup.EndpointUrl;
            if (!string.IsNullOrEmpty(setup.User)) setup.Config.Source.Username = setup.User;
            if (!string.IsNullOrEmpty(setup.Password)) setup.Config.Source.Password = setup.Password;
            setup.Config.Source.Secure |= setup.Secure;
            if (!string.IsNullOrEmpty(setup.LogLevel)) setup.Config.Logger.Console = new LogConfig() { Level = setup.LogLevel };
            else if (setup.NoConfig) setup.Config.Logger.Console = new LogConfig { Level = "information" };
            if (!string.IsNullOrEmpty(setup.LogDir))
            {
                if (setup.Config.Logger.File == null)
                {
                    setup.Config.Logger.File = new FileConfig { Level = "information", Path = setup.LogDir };
                }
                else
                {
                    setup.Config.Logger.File.Path = setup.LogDir;
                }
            }
            setup.Config.Source.AutoAccept |= setup.AutoAccept;
            setup.Config.Source.ExitOnFailure |= setup.Exit;

            if (setup.NoConfig)
            {
                services.AddConfig(setup.Config,
                    typeof(CogniteConfig),
                    typeof(LoggerConfig),
                    typeof(MetricsConfig),
                    typeof(StateStoreConfig));
                services.AddSingleton(setup.Config);
            }

            services.AddMetrics();
            services.AddLogger();

            if (!setup.ConfigTool)
            {
                if (setup.Config.Cognite != null)
                {
                    services.AddCogniteClient("OPC-UA Extractor", $"CogniteOPCUAExtractor/{Version.GetVersion()}", true, true, true);
                }
                services.AddStateStore();
            }
        }

        /// <summary>
        /// Start the extractor and keep it running until canceled, restarting on crashes
        /// </summary>
        /// <param name="config"></param>
        private static void RunExtractor(FullConfig config, IServiceProvider provider)
        {
            var runTime = new ExtractorRuntime(config, provider);

            int waitRepeats = 0;

            using var source = new CancellationTokenSource();
            using var quitEvent = new ManualResetEvent(false);
            bool canceled = false;
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                quitEvent?.Set();
                eArgs.Cancel = true;
                source?.Cancel();
                canceled = true;
            };
            while (true)
            {
                if (canceled)
                {
                    log.Warning("Extractor stopped manually");
                    break;
                }

                DateTime startTime = DateTime.UtcNow;
                try
                {
                    log.Information("Starting extractor");
                    runTime.Run(source.Token).Wait();
                    log.Information("Extractor closed without error");
                }
                catch (TaskCanceledException)
                {
                }
                catch (AggregateException aex)
                {
                    if (ExtractorUtils.GetRootExceptionOfType<ConfigurationException>(aex) != null)
                    {
                        log.Error("Invalid configuration, stopping: {msg}", aex.InnerException.Message);
                        break;
                    }
                    if (ExtractorUtils.GetRootExceptionOfType<TaskCanceledException>(aex) != null)
                    {
                        log.Error("Extractor halted due to cancelled task");
                    }
                    else if (ExtractorUtils.GetRootExceptionOfType<SilentServiceException>(aex) == null)
                    {
                        log.Error(aex, "Unexpected failure in extractor: {msg}", aex.Message);
                    }
                }
                catch (ConfigurationException)
                {
                    log.Error("Invalid configuration, stopping");
                    break;
                }
                catch (Exception ex)
                {
                    log.Error(ex, "Unexpected failure in extractor: {msg}", ex.Message);
                }

                if (config.Source.ExitOnFailure)
                {
                    break;
                }

                if (startTime > DateTime.UtcNow - TimeSpan.FromSeconds(600))
                {
                    waitRepeats++;
                }
                else
                {
                    waitRepeats = 0;
                }

                if (source.IsCancellationRequested)
                {
                    log.Warning("Extractor stopped manually");
                    break;
                }

                try
                {
                    var sleepTime = TimeSpan.FromSeconds(Math.Pow(2, Math.Min(waitRepeats, 9)));
                    log.Information("Sleeping for {time}", sleepTime);
                    Task.Delay(sleepTime, source.Token).Wait();
                }
                catch (Exception)
                {
                    log.Warning("Extractor stopped manually");
                    break;
                }
            }
            Log.CloseAndFlush();
        }
        /// <summary>
        /// Run the config tool
        /// </summary>
        /// <param name="config">Basic configuration for the config tool</param>
        /// <param name="baseConfig">Configuration that will be modified and returned by the config tool</param>
        /// <param name="output">Path to output config file</param>
        private static void RunConfigTool(FullConfig config, FullConfig baseConfig, string output)
        {
            var runTime = new ConfigToolRuntime(config, baseConfig, output);
            runTime.Run().Wait();
        }
    }
}

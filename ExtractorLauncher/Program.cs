﻿/* Cognite Extractor for OPC-UA
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
using Microsoft.Extensions.DependencyInjection;
using Prometheus;
using Serilog;
using System;
using System.Collections.Generic;
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
        private static ILogger log;
        static int Main(string[] args)
        {
            // Temporary logger config for capturing logs during configuration.
            Log.Logger = LoggingUtils.GetSerilogDefault();

            ExtractorParams setup;

            try
            {
                setup = ParseCommandLineArguments(args);
            }
            catch (Exception ex)
            {
                if (!(ex is ArgumentOutOfRangeException))
                {
                    Log.Error(ex.Message);
                }
                Log.Warning("Bad command-line arguments passed. Usage:\n" +
                            "    -t|--tool                  - Run the configuration tool\n" +
                            "    -h|--host [host]           - Override configured OPC-UA endpoint\n" +
                            "    -u|--user [user]           - Override configured OPC-UA username\n" +
                            "    -p|--password [pass]       - Override configured OPC-UA password\n" +
                            "    -s|--secure                - Use a secure connection to OPC-UA\n" +
                            "    -f|--config-file [path]    - Set the config-file path. Overrides config-dir for .yml config files\n" +
                            "    -a|--auto-accept           - Auto-accept server certificates\n" +
                            "    -d|--config-dir [path]     - Set the path to the config directory\n" +
                            "    -ct|--config-target [path] - Set the path to the output file for the config tool. " +
                            "By default [config-dir]/config.config-tool-output.yml. This file is overwritten.\n" +
                            "    -nc|--no-config            - Don't attempt to load yml config files. " +
                            "The OPC-UA XML config file will still be needed.\n" +
                            "    -l|--log-level             - Set the console log-level [fatal/error/warning/information/debug/verbose].\n" +
                            "    -x|--exit                  - Exit the extractor on failure, equivalent to Source.ExitOnFailure.");
                return -1;
            }


            string configDir = setup.ConfigDir ?? Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR") ?? "config/";

            var services = new ServiceCollection();

            var config = new FullConfig();
            FullConfig baseConfig = null;
            if (!setup.NoConfig)
            {
                try
                {
                    string configFile = setup.ConfigFile ?? System.IO.Path.Combine(configDir, setup.ConfigTool ? "config.config-tool.yml" : "config.yml");
                    Log.Information("Loading config file from {path}", configFile);
                    config = services.AddConfig<FullConfig>(configFile, 1);
                    if (setup.ConfigTool)
                    {
                        baseConfig = ConfigurationUtils.TryReadConfigFromFile<FullConfig>(configFile, 1);
                    }
                }
                catch (Extractor.Configuration.ConfigurationException e)
                {
                    Log.Error("Failed to load configuration: {msg}", e.Message);
                    throw;
                }
                config.Source.ConfigRoot = configDir;
            }
            else
            {
                config.GenerateDefaults();
                if (setup.ConfigTool)
                {
                    baseConfig = new FullConfig();
                    baseConfig.GenerateDefaults();
                    baseConfig.Version = 1;
                    config.Version = 1;
                }
            }

            if (!string.IsNullOrEmpty(setup.Host)) config.Source.EndpointUrl = setup.Host;
            if (!string.IsNullOrEmpty(setup.Username)) config.Source.Username = setup.Username;
            if (!string.IsNullOrEmpty(setup.Password)) config.Source.Password = setup.Password;
            config.Source.Secure |= setup.Secure;
            if (!string.IsNullOrEmpty(setup.LogLevel)) config.Logger.Console = new LogConfig() { Level = setup.LogLevel };
            else if (setup.NoConfig) config.Logger.Console = new LogConfig { Level = "information" };
            config.Source.AutoAccept |= setup.AutoAccept;
            config.Source.ExitOnFailure |= setup.ExitOnFailure;

            if (setup.NoConfig)
            {
                services.AddConfig(config,
                    typeof(CogniteConfig), typeof(LoggerConfig), typeof(MetricsConfig), typeof(StateStoreConfig));
            }

            services.AddMetrics();
            services.AddLogger();

            if (!setup.ConfigTool)
            {
                if (config.Cognite != null)
                {
                    services.AddCogniteClient("OPC-UA Extractor", $"CogniteOPCUAExtractor/{Version.GetVersion()}", true, true, true);
                }
                services.AddStateStore();
            }

            var provider = services.BuildServiceProvider();
            Log.Logger = provider.GetRequiredService<ILogger>();
            log = Log.Logger.ForContext<Program>();

            log.Information("Starting OPC UA Extractor version {version}", Version.GetVersion());
            log.Information("Revision information: {status}", Version.Status());

            version.Set(0);

            if (setup.ConfigTool)
            {
                string configOutput = setup.ConfigTarget ?? System.IO.Path.Combine(configDir, "config.config-tool-output.yml");
                RunConfigTool(config, baseConfig, configOutput);
            }
            else
            {
                var metrics = provider.GetRequiredService<MetricsService>();
                metrics.Start();
                RunExtractor(config, provider);
            }

            return 0;
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
        /// <summary>
        /// Parse list of command line arguments to produce parameter object
        /// </summary>
        /// <param name="args">Raw parameter list</param>
        /// <returns>Final ExtractorParams struct</returns>
        private static ExtractorParams ParseCommandLineArguments(IReadOnlyList<string> args)
        {
            var result = new ExtractorParams();
            for (int i = 0; i < args.Count; i++)
            {
                switch (args[i])
                {
                    case "-t":
                    case "--tool":
                        result.ConfigTool = true;
                        break;
                    case "-h":
                    case "--host":
                        result.Host = args[++i];
                        break;
                    case "-u":
                    case "--user":
                        result.Username = args[++i];
                        break;
                    case "-p":
                    case "--password":
                        result.Password = args[++i];
                        break;
                    case "-s":
                    case "--secure":
                        result.Secure = true;
                        break;
                    case "-f":
                    case "--config-file":
                        result.ConfigFile = args[++i];
                        break;
                    case "-d":
                    case "--config-dir":
                        result.ConfigDir = args[++i];
                        break;
                    case "-ct":
                    case "--config-target":
                        result.ConfigTarget = args[++i];
                        break;
                    case "-nc":
                    case "--no-config":
                        result.NoConfig = true;
                        break;
                    case "-l":
                    case "--log-level":
                        result.LogLevel = args[++i];
                        break;
                    case "-a":
                    case "--auto-accept":
                        result.AutoAccept = true;
                        break;
                    case "--x":
                    case "--exit":
                        result.ExitOnFailure = true;
                        break;
                    default:
                        throw new InvalidOperationException($"Unrecognized parameter: {args[i]}");
                }
            }

            return result;
        }

        private struct ExtractorParams
        {
            public bool ConfigTool;
            public string Host;
            public string Username;
            public string Password;
            public bool Secure;
            public string ConfigFile;
            public string ConfigTarget;
            public string LogLevel;
            public string ConfigDir;
            public bool NoConfig;
            public bool AutoAccept;
            public bool ExitOnFailure;
        }
    }
}

/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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

using System;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Prometheus.Client;
using Serilog;

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
        private static readonly ILogger log = Log.Logger.ForContext(typeof(Program));
        static int Main(string[] args)
        {
            // Temporary logger config for capturing logs during configuration.
            Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();

            ExtractorParams setup;

            try
            {
                setup = ParseCommandLineArguments(args);
            }
            catch (Exception ex)
            {
                if (!(ex is ArgumentOutOfRangeException))
                {
                    log.Error(ex.Message);
                }
                log.Warning("Bad command-line arguments passed. Usage:\n" +
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

            FullConfig config = new FullConfig();
            FullConfig baseConfig = null;
            if (!setup.NoConfig)
            {
                try
                {
                    string configFile = setup.ConfigFile ?? System.IO.Path.Combine(configDir, setup.ConfigTool ? "config.config-tool.yml" : "config.yml");
                    log.Information($"Loading config from {configFile}");
                    config = ExtractorUtils.GetConfig(configFile);
                    if (setup.ConfigTool)
                    {
                        baseConfig = ExtractorUtils.GetConfig(configFile);
                    }
                }
                catch (YamlDotNet.Core.YamlException e)
                {
                    log.Error("Failed to load config at {start}: {msg}", e.Start, e.InnerException?.Message ?? e.Message);
                    throw;
                }
                config.Source.ConfigRoot = configDir;
            }

            if (!string.IsNullOrEmpty(setup.Host)) config.Source.EndpointURL = setup.Host;
            if (!string.IsNullOrEmpty(setup.Username)) config.Source.Username = setup.Username;
            if (!string.IsNullOrEmpty(setup.Password)) config.Source.Password = setup.Password;
            config.Source.Secure |= setup.Secure;
            if (!string.IsNullOrEmpty(setup.LogLevel)) config.Logging.ConsoleLevel = setup.LogLevel;
            config.Source.AutoAccept |= setup.AutoAccept;
            config.Source.ExitOnFailure |= setup.ExitOnFailure;

            Logger.Configure(config.Logging);

            log.Information("Starting OPC UA Extractor version {version}", Version.GetVersion());
            log.Information("Revision information: {status}", Version.Status());

            version.Set(0);



            try
            {
                MetricsManager.Setup(config.Metrics);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to start metrics pusher");
            }

            if (setup.ConfigTool)
            {
                string configOutput = setup.ConfigTarget ?? System.IO.Path.Combine(configDir, "config.config-tool-output.yml");
                RunConfigTool(config, baseConfig, configOutput);
            }
            else
            {
                RunExtractor(config);
            }

            return 0;
        }

        private static void RunExtractor(FullConfig config)
        {
            var runTime = new ExtractorRuntime(config);
            runTime.Configure();

            int waitRepeats = 0;

            using var manualSource = new CancellationTokenSource();
            CancellationTokenSource source = null;
            using (var quitEvent = new ManualResetEvent(false))
            {
                bool canceled = false;
                Console.CancelKeyPress += (sender, eArgs) =>
                {
                    quitEvent?.Set();
                    eArgs.Cancel = true;
                    source?.Cancel();
                    canceled = true;
                    manualSource?.Cancel();
                };
                while (true)
                {
                    using (source = new CancellationTokenSource())
                    {
                        if (canceled)
                        {
                            log.Warning("Extractor stopped manually");
                            break;
                        }

                        DateTime startTime = DateTime.Now;
                        try
                        {
                            log.Information("Starting extractor");
                            runTime.Run(source).Wait();
                        }
                        catch (TaskCanceledException)
                        {
                            log.Warning("Extractor stopped manually");
                            break;
                        }
                        catch (AggregateException aex)
                        {
                            if (ExtractorUtils.GetRootExceptionOfType<ConfigurationException>(aex) != null)
                            {
                                log.Error("Invalid configuration, stopping");
                                break;
                            }
                            if (ExtractorUtils.GetRootExceptionOfType<TaskCanceledException>(aex) != null)
                            {
                                log.Warning("Extractor stopped manually");
                                break;
                            }
                        }
                        catch (ConfigurationException)
                        {
                            log.Error("Invalid configuration, stopping");
                            break;
                        }
                        catch
                        {
                            log.Error("Extractor crashed, restarting");
                        }

                        if (config.Source.ExitOnFailure)
                        {
                            break;
                        }

                        if (startTime > DateTime.Now - TimeSpan.FromSeconds(600))
                        {
                            waitRepeats++;
                        }
                        else
                        {
                            waitRepeats = 0;
                        }



                        try
                        {
                            TimeSpan sleepTime = TimeSpan.FromSeconds(Math.Pow(2, Math.Min(waitRepeats, 9)));
                            log.Information("Sleeping for {time}", sleepTime);
                            Task.Delay(sleepTime, manualSource.Token).Wait();
                        }
                        catch (TaskCanceledException)
                        {
                            log.Warning("Extractor stopped manually");
                            break;
                        }
                    }
                }
            }
            Log.CloseAndFlush();
        }

        private static void RunConfigTool(FullConfig config, FullConfig baseConfig, string output)
        {
            var runTime = new ConfigToolRuntime(config, baseConfig, output);
            runTime.Run().Wait();
        }

        private static ExtractorParams ParseCommandLineArguments(string[] args)
        {
            var result = new ExtractorParams();
            for (int i = 0; i < args.Length; i++)
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

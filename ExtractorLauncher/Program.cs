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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Prometheus.Client.MetricPusher;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Console launcher for the OPC-UA extractor and Configuration tool. Includes basic setup of logging/config/metrics and
    /// parsing of command-line arguments
    /// </summary>
    class Program
    {
        private static MetricPushServer _worker;

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
                            "    -l|--log-level             - Set the console log-level [fatal/error/warning/information/debug/verbose]");
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
                    Log.Information($"Loading config from {configFile}");
                    config = Utils.GetConfig(configFile);
                    if (setup.ConfigTool)
                    {
                        baseConfig = Utils.GetConfig(configFile);
                    }
                }
                catch (YamlDotNet.Core.YamlException e)
                {
                    Log.Error("Failed to load config at {start}: {msg}", e.Start, e.InnerException?.Message ?? e.Message);
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

            Logger.Configure(config.Logging);

            Log.Information("Starting OPC UA Extractor version {version}", Version.GetVersion());
            Log.Information("Revision information: {status}", Version.Status());

            try
            {
                SetupMetrics(config.Metrics);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to start metrics pusher");
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
                };
                while (true)
                {
                    using (source = new CancellationTokenSource())
                    {
                        if (canceled)
                        {
                            Log.Warning("Extractor stopped manually");
                            break;
                        }

                        try
                        {
                            runTime.Run(source).Wait();
                        }
                        catch (TaskCanceledException)
                        {
                            Log.Warning("Extractor stopped manually");
                            break;
                        }
                        catch
                        {
                            Log.Error("Extractor crashed, restarting");
                        }
                        try
                        {
                            Task.Delay(1000, source.IsCancellationRequested ? CancellationToken.None : source.Token).Wait();
                        }
                        catch (TaskCanceledException)
                        {
                            Log.Warning("Extractor stopped manually");
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

        /// <summary>
        /// Starts prometheus pushgateway client
        /// </summary>
        /// <param name="config">The metrics config object</param>
        private static void SetupMetrics(MetricsConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.URL) || string.IsNullOrWhiteSpace(config.Job))
            {
                Log.Information("Not pushing metrics, missing URL or Job");
                return;
            }
            var additionalHeaders = new Dictionary<string, string>();
            if (!string.IsNullOrWhiteSpace(config.Username) && !string.IsNullOrWhiteSpace(config.Password))
            {
                string encoded = Convert.ToBase64String(
                    System.Text.Encoding
                        .GetEncoding("ISO-8859-1")
                        .GetBytes($"{config.Username}:{config.Password}")
                );
                additionalHeaders.Add("Authorization", $"Basic {encoded}");
            }
            var pusher = new MetricPusher(config.URL, config.Job, config.Instance, additionalHeaders);
            _worker = new MetricPushServer(pusher, TimeSpan.FromMilliseconds(config.PushInterval));
            _worker.Start();
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
                    default:
                        throw new Exception($"Unrecognized parameter: {args[i]}");
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
        }
    } 
}

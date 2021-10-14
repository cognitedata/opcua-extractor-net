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
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public static class ExtractorStarter
    {
        private static readonly Gauge version =
            Metrics.CreateGauge("opcua_version",
                $"version: {Extractor.Metrics.Version.GetVersion(Assembly.GetExecutingAssembly())}"
                + $", status: {Extractor.Metrics.Version.GetDescription(Assembly.GetExecutingAssembly())}");

        private static string VerifyConfig(ILogger log, FullConfig config)
        {
            if (string.IsNullOrEmpty(config.Source.EndpointUrl)) return "Missing endpoint-url";
            try
            {
                var uri = new Uri(config.Source.EndpointUrl);
            }
            catch
            {
                return "EndpointUrl is not a valid URI";
            }
            if (string.IsNullOrEmpty(config.Extraction.IdPrefix)) log.LogWarning("No id-prefix specified in config file");
            if (config.Cognite == null && config.Influx == null && config.Mqtt == null) log.LogWarning("No destination system specified");
            if (config.Extraction.IdPrefix == "events.") return "Do not use events. as id-prefix, as it is used internally";
            return null;
        }

        private static void VerifyAndBuildConfig(ILogger log, FullConfig config, ExtractorParams setup, string configRoot)
        {
            config.Source.ConfigRoot = configRoot;
            if (!string.IsNullOrEmpty(setup.EndpointUrl)) config.Source.EndpointUrl = setup.EndpointUrl;
            if (!string.IsNullOrEmpty(setup.User)) config.Source.Username = setup.User;
            if (!string.IsNullOrEmpty(setup.Password)) config.Source.Password = setup.Password;
            config.Source.Secure |= setup.Secure;
            if (!string.IsNullOrEmpty(setup.LogLevel)) config.Logger.Console = new ConsoleConfig { Level = setup.LogLevel };
            else if (setup.NoConfig) config.Logger.Console = new ConsoleConfig { Level = "information" };
            if (!string.IsNullOrEmpty(setup.LogDir))
            {
                if (config.Logger.File == null)
                {
                    config.Logger.File = new FileConfig { Level = "information", Path = setup.LogDir };
                }
                else
                {
                    config.Logger.File.Path = setup.LogDir;
                }
            }
            config.Source.AutoAccept |= setup.AutoAccept;
            config.Source.ExitOnFailure |= setup.Exit;

            string configResult = VerifyConfig(log, config);
            if (configResult != null)
            {
                throw new ConfigurationException($"Invalid config: {configResult}");
            }

            if (!File.Exists($"{config.Source.ConfigRoot}/opc.ua.net.extractor.Config.xml"))
            {
                throw new ConfigurationException($"Missing opc.ua.net.extractor.Config.xml in config folder {config.Source.ConfigRoot}");
            }
        }

        public static async Task RunConfigTool(ILogger log, ExtractorParams setup, CancellationToken token)
        {
            var services = new ServiceCollection();
            string configDir = setup.ConfigDir ?? Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR") ?? "config/";

            if (log == null)
            {
                log = LoggingUtils.GetDefault();
            }

            if (setup.NoConfig)
            {
                setup.Config = new FullConfig();
                setup.Config.GenerateDefaults();
                setup.Config.Version = 1;
                setup.BaseConfig = new FullConfig();
                setup.BaseConfig.GenerateDefaults();
                setup.BaseConfig.Version = 1;
            }
            else
            {
                string configFile = setup.ConfigFile ?? Path.Combine(configDir, "config.config-tool.yml");
                setup.Config = services.AddConfig<FullConfig>(configFile, 1);
                setup.BaseConfig = ConfigurationUtils.TryReadConfigFromFile<FullConfig>(configFile, 1);
            }

            VerifyAndBuildConfig(log, setup.Config, setup, configDir);

            if (setup.NoConfig)
            {
                services.AddConfig(setup.Config, typeof(LoggerConfig));
                services.AddSingleton(setup.Config);
            }

            services.AddLogger();

            using var provider = services.BuildServiceProvider();

            Serilog.Log.Logger = provider.GetRequiredService<Serilog.ILogger>();

            string configOutput = setup.ConfigTarget ?? Path.Combine(setup.Config.Source.ConfigRoot, "config.config-tool-output.yml");

            var runTime = new ConfigToolRuntime(setup.Config, setup.BaseConfig, configOutput);
            await runTime.Run(token);
        }

        public static async Task RunExtractor(ILogger log, ExtractorParams setup, CancellationToken token)
        {
            var services = new ServiceCollection();
            string configDir = setup.ConfigDir ?? Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR") ?? "config/";

            if (log == null)
            {
                log = LoggingUtils.GetDefault();
            }

            version.Set(0);
            var ver = Extractor.Metrics.Version.GetVersion(Assembly.GetExecutingAssembly());
            

            FullConfig config;
            if (setup.NoConfig)
            {
                config = new FullConfig();
                config.GenerateDefaults();
            }
            else
            {
                string configFile = setup.ConfigFile ?? Path.Join(configDir, "config.yml");
                config = ConfigurationUtils.TryReadConfigFromFile<FullConfig>(configFile, 1);
                config.GenerateDefaults();
            }

            if (config.Cognite == null)
            {
                config.Cognite = new CognitePusherConfig();
            }

            services.AddSingleton<IPusher, CDFPusher>(provider =>
            {
                var conf = provider.GetService<FullConfig>();
                var dest = provider.GetService<CogniteDestination>();
                if (conf.Cognite == null || dest == null || dest.CogniteClient == null) return null;
                return new CDFPusher(conf.Extraction, conf.Cognite, dest);
            });
            services.AddSingleton<IPusher, InfluxPusher>(provider =>
            {
                var conf = provider.GetService<FullConfig>();
                if (conf.Influx == null) return null;
                return new InfluxPusher(conf.Influx);
            });
            services.AddSingleton<IPusher, MQTTPusher>(provider =>
            {
                var conf = provider.GetService<FullConfig>();
                if (conf.Mqtt == null) return null;
                return new MQTTPusher(provider, conf.Mqtt);
            });

            services.AddSingleton<UAClient>();

            await ExtractorRunner.Run<FullConfig, UAExtractor>(
                setup.ConfigFile ?? Path.Join(configDir, "config.yml"),
                new[] { 1 },
                $"OPC-UA Extractor:{ver}",
                $"CogniteOPCUAExtractor/{ver}",
                true,
                true,
                true,
                !(setup.Exit || config.Source.ExitOnFailure),
                token,
                configCallback: config => VerifyAndBuildConfig(log, config, setup, configDir),
                extServices: services,
                startupLogger: log,
                config: config,
                requireDestination: false);
        }
    }
}

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

using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Cognite.Extractor.Utils.Unstable.Runtime;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Pushers.Writers;
using Cognite.OpcUa.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Prometheus;
using Serilog;
using Serilog.Events;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Cognite.OpcUa
{
    public static class ExtractorStarter
    {
        private static readonly Gauge version =
            Metrics.CreateGauge("opcua_version",
                $"version: {Extractor.Metrics.Version.GetVersion(Assembly.GetExecutingAssembly())}"
                + $", status: {Extractor.Metrics.Version.GetDescription(Assembly.GetExecutingAssembly())}");

        public static Action<CogniteDestination?, UAExtractor>? OnCreateExtractor { get; set; }

        private static string? VerifyConfig(ILogger log, FullConfig config)
        {
            if (string.IsNullOrEmpty(config.Source.EndpointUrl))
            {
                if (config.Source.NodeSetSource == null || !(config.Source.NodeSetSource.NodeSets?.Any() ?? false))
                {
                    return "Missing endpoint-url";
                }
            }
            else
            {
                try
                {
                    var uri = new Uri(config.Source.EndpointUrl);
                }
                catch
                {
                    return "EndpointUrl is not a valid URI";
                }
            }

            if (string.IsNullOrEmpty(config.Extraction.IdPrefix)) log.LogWarning("No id-prefix specified in config file");
            if (config.Cognite == null) log.LogWarning("No destination system specified");
            if (config.Extraction.IdPrefix == "events.") return "Do not use events. as id-prefix, as it is used internally";

            if (config.Subscriptions.LifetimeCount <= 0 || config.Subscriptions.LifetimeCount < 3 * config.Subscriptions.KeepAliveCount)
            {
                return "subscriptions.lifetime-count must be greater than 0 and at least 3 * subscriptions.keep-alive-count";
            }
            if (config.Subscriptions.KeepAliveCount <= 0)
            {
                return "subscriptions.keep-alive-count must be greater than 0";
            }
            if (config.Cognite?.MetadataTargets == null)
            {
                log.LogWarning("The extractor has not been configured with any metadata target. No metadata will be written to CDF.");
            }

            if (config.Cognite?.MetadataTargets?.Raw != null)
            {
                var rawMetaTarget = config.Cognite.MetadataTargets.Raw;
                if (rawMetaTarget.Database == null)
                {
                    return "cognite.metadata-targets.raw.database is required when setting raw";
                }
                if (rawMetaTarget.AssetsTable == null || rawMetaTarget.RelationshipsTable == null || rawMetaTarget.TimeseriesTable == null)
                {
                    return "At least one of assets-table, relationships-table or timeseries-table is required when setting cognite.metadata-targets.raw";
                }
            }

            if (config.Cognite?.MetadataTargets?.DataModels?.Enabled ?? false)
            {
                log.LogWarning("Data modeling support is enabled. This feature is in Alpha, any may change at any time.");
            }

            if (config.Cognite?.Records != null)
            {
                log.LogWarning("Writing events to data modeling records is enabled. This feature is in Beta, and may change in the future.");
                if (string.IsNullOrWhiteSpace(config.Cognite.Records.Stream))
                {
                    return "Missing required field cognite.records.stream";
                }
                if (string.IsNullOrWhiteSpace(config.Cognite.Records.LogSpace))
                {
                    return "Missing required field cognite.records.log-space";
                }
                if (string.IsNullOrWhiteSpace(config.Cognite.Records.ModelSpace))
                {
                    return "Missing required field cognite.records.model-space";
                }
            }

            if (config.FailureBuffer?.EventsBatch <= 0)
            {
                return "events-batch must be greater than 0";
            }
            if (config.FailureBuffer?.DatapointsBatch <= 0)
            {
                return "datapoints-batch must be greater than 0";
            }

            if (config.StateStorage.Database != Extractor.StateStorage.StateStoreConfig.StorageType.None
                && string.IsNullOrWhiteSpace(config.StateStorage.Location))
            {
                return "When state-storage.database is set to something other than None, state-storage.location must be specified";
            }

            if ((config.History?.Enabled ?? false) && !config.StateStorage.IsEnabled)
            {
                return "When history is enabled, you must configure state-storage.";
            }

            return null;
        }

        private static void VerifyAndBuildConfig(
            ILogger log,
            FullConfig config,
            BaseExtractorParams setup,
            ExtractorRuntimeBuilder<FullConfig, UAExtractor>? options,
            string configRoot,
            ServiceCollection services)
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
            services.AddWriters(config);
            config.Source.AutoAccept |= setup.AutoAccept;
            config.Source.ExitOnFailure |= setup is ExtractorParams p2 && p2.Exit;
            config.DryRun |= setup.DryRun;

            if (options != null && config.Source.ExitOnFailure)
            {
                options.RestartPolicy = ExtractorRestartPolicy.Never;
            }

            string? configResult = VerifyConfig(log, config);
            if (configResult != null)
            {
                throw new ConfigurationException($"Invalid config: {configResult}");
            }

            if (!File.Exists($"{config.Source.ConfigRoot}/opc.ua.net.extractor.Config.xml"))
            {
                throw new ConfigurationException($"Missing opc.ua.net.extractor.Config.xml in config folder {config.Source.ConfigRoot}");
            }
        }

        private static void SetWorkingDir(BaseExtractorParams setup)
        {
            string? path = null;
            if (setup.WorkingDir != null)
            {
                path = setup.WorkingDir;
            }
            else if (setup is ExtractorParams p2 && p2.Service)
            {
                path = Directory.GetParent(AppContext.BaseDirectory)?.Parent?.FullName;
            }
            if (path != null)
            {
                if (!Directory.Exists(path))
                {
                    throw new ConfigurationException($"Target directory does not exist: {path}");
                }
                Directory.SetCurrentDirectory(path);
            }
        }

        public static async Task RunConfigTool(ILogger? log, ConfigToolParams setup, ServiceCollection services, CancellationToken token)
        {
            string configDir = setup.ConfigDir ?? Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR") ?? "config/";

            SetWorkingDir(setup);

            log ??= LoggingUtils.GetDefault();

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

            VerifyAndBuildConfig(log, setup.Config, setup, null, configDir, services);

            if (setup.NoConfig)
            {
                services.AddConfig(setup.Config, typeof(LoggerConfig));
                services.AddSingleton(setup.Config);
            }

            services.AddLogger(BuildConfigToolLogger);

            using var provider = services.BuildServiceProvider();

            string configOutput = setup.ConfigTarget ?? Path.Combine(setup.Config.Source.ConfigRoot, "config.config-tool-output.yml");

            var runTime = new ConfigToolRuntime(provider, setup.Config, setup.BaseConfig, configOutput);
            await runTime.Run(token);
        }

        private static Serilog.ILogger BuildConfigToolLogger(LoggerConfig config)
        {
            config ??= new LoggerConfig();
            config.Console ??= new ConsoleConfig
            {
                Level = "information"
            };
            var path = $"config-tool-{DateTime.Now:yyyy-MM-dd-HHmmss}.log";
            return LoggingUtils.GetConfiguration(config)
                .WriteTo.Async(p => p.File(
                    path: path,
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    outputTemplate: LoggingUtils.LogTemplateWithContext)
                ).CreateLogger();
        }

        public static async Task RunExtractor(ILogger? log, ExtractorParams setup, ServiceCollection services, CancellationToken token)
        {
            string configDir = setup.ConfigDir ?? Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR") ?? "config/";

            SetWorkingDir(setup);

            log ??= LoggingUtils.GetDefault();

            version.Set(0);
            var ver = Extractor.Metrics.Version.GetVersion(Assembly.GetExecutingAssembly());

            FullConfig? config = null;
            if (setup.NoConfig)
            {
                config = new FullConfig();
                config.GenerateDefaults();
            }

            services.AddSingleton<IPusher>(provider =>
            {
                var conf = provider.GetRequiredService<FullConfig>();
                var dest = provider.GetService<CogniteDestinationWithIDM>();
                var log = provider.GetRequiredService<ILogger<CDFPusher>>();
                var connectionConfig = provider.GetRequiredService<ConnectionConfig>();
                if (conf.Cognite == null || dest == null || dest.CogniteClient == null)
                    return null!;
                return new CDFPusher(log, conf, conf.Cognite, dest, connectionConfig, provider);
            });

            services.AddSingleton<UAClient>();

            var builder = new ExtractorRuntimeBuilder<FullConfig, UAExtractor>($"OPC-UA Extractor:{ver}", $"CogniteOPCUAExtractor/{ver}")
            {
                ConfigFolder = configDir,
                AcceptedConfigVersions = new[] { 1 },
                OverrideConfigFile = setup.ConfigFile != null ? setup.ConfigFile : null,
                AddStateStore = true,
                AddLogger = true,
                AddMetrics = true,
                RestartPolicy = ExtractorRestartPolicy.OnError,
                OnConfigure = (config, builder, services) => VerifyAndBuildConfig(log, config, setup, builder, configDir, services),
                ExternalServices = services,
                StartupLogger = log,
                ConfigSource = setup.ConfigFile != null ? ConfigSourceType.Local : ConfigSourceType.Remote,
                ExternalConfig = config,
                RetryStartupRequest = true,
                BufferRemoteConfig = true,
                LogException = (log, e, msg) => ExtractorUtils.LogException(log, e, msg, msg),
                OnCreateExtractor = OnCreateExtractor,
            };

            var runtime = await builder.MakeRuntime(token);
            await runtime.Run();
        }
    }
}

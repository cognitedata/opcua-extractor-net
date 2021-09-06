using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    internal class ConfigLoader
    {
        private readonly ExtractorParams setup;
        private readonly IServiceCollection services;
        private bool loaded;
        public ConfigLoader(ExtractorParams setup)
        {
            this.setup = setup;
            services = new ServiceCollection();

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
        }

        public async Task<IServiceProvider> WaitForConfig(CancellationToken token)
        {
            while (!token.IsCancellationRequested && !LoadConfig())
            {
                await Task.Delay(10_000, token);
            }
            if (!loaded) throw new ConfigurationException("Failed to load config");
            Configure();
            return services.BuildServiceProvider();
        }

        public IServiceProvider TryLoadConfig()
        {
            if (!LoadConfig()) throw new ConfigurationException("Failed to load config");
            Configure();
            return services.BuildServiceProvider();
        }

        private static string VerifyConfig(FullConfig config)
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
            if (string.IsNullOrEmpty(config.Extraction.IdPrefix)) Log.Warning("No id-prefix specified in config file");
            if (config.Cognite == null && config.Influx == null && config.Mqtt == null) Log.Warning("No destination system specified");
            return null;
        }

        private bool LoadConfig()
        {
            services.Clear();
            string configDir = setup.ConfigDir ?? Environment.GetEnvironmentVariable("OPCUA_CONFIG_DIR") ?? "config/";

            setup.Config = new FullConfig();
            setup.BaseConfig = null;

            if (!setup.NoConfig)
            {
                try
                {
                    string configFile = setup.ConfigFile ?? Path.Combine(configDir, setup.ConfigTool ? "config.config-tool.yml" : "config.yml");
                    Log.Information("Loading config file from {path}", configFile);
                    setup.Config = services.AddConfig<FullConfig>(configFile, 1);
                    if (setup.ConfigTool)
                    {
                        setup.BaseConfig = ConfigurationUtils.TryReadConfigFromFile<FullConfig>(configFile, 1);
                    }
                }
                catch (Exception e)
                {
                    Log.Error("Failed to load configuration: {msg}", e.Message);
                    return false;
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

            string configResult = VerifyConfig(setup.Config);
            if (configResult != null)
            {
                Log.Error("Invalid config: {err}", configResult);
                return false;
            }

            if (!File.Exists($"{setup.Config.Source.ConfigRoot}/opc.ua.net.extractor.Config.xml"))
            {
                Log.Error("Missing opc.ua.net.extractor.Config.xml in config folder {root}", setup.Config.Source.ConfigRoot);
                return false;
            }

            if (setup.NoConfig)
            {
                services.AddConfig(setup.Config,
                    typeof(CogniteConfig),
                    typeof(LoggerConfig),
                    typeof(MetricsConfig),
                    typeof(StateStoreConfig));
                services.AddSingleton(setup.Config);
            }
            loaded = true;
            return true;
        }


        private void Configure()
        {
            services.AddMetrics();
            services.AddLogger();

            if (!setup.ConfigTool)
            {
                if (setup.Config.Cognite != null)
                {
                    services.AddCogniteClient($"OPC-UA Extractor:{Version.GetVersion()}",
                        $"CogniteOPCUAExtractor/{Version.GetVersion()}", true, true, true);
                }
                services.AddStateStore();
            }
        }
    }
}

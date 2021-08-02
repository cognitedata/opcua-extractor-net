using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;
using Cognite.OpcUa;
using Microsoft.Extensions.DependencyInjection;
using Prometheus;
using Serilog;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace OpcUaService
{
    /// <summary>
    /// Contains the code to start the extractor, validate configuration file etc
    /// </summary>
    public static class ExtractorServiceStarter
    {
        private static CancellationTokenSource _sourceProgram;
        private static bool _isRunning;
        private static FullConfig _config;

        private static ILogger _log;
        private static readonly Gauge version = Metrics.CreateGauge("opcua_version", $"version: {Version.GetVersion()}, status: {Version.Status()}");

        /// <summary>
        /// Funtion called by the service to start the extractor
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static int Start(string configFile)
        {
            // Temporary logger config for capturing logs during configuration.
            //Log.Logger = new LoggerConfiguration().WriteTo.File(@"c:\opcua\logs\servicestart.log").CreateLogger();

            _sourceProgram = new CancellationTokenSource();
            _isRunning = true;

            // Validation has already been done so no need for try catch here now
            var services = new ServiceCollection();
            _config = services.AddConfig<FullConfig>(configFile, 1);
            _config.Source.ConfigRoot = "config/";
            _config.Logger.Console.Level = "none";

            services.AddLogger();
            services.AddMetrics();

            if (_config.Cognite != null)
            {
                services.AddCogniteClient("OPC-UA Extractor", $"CogniteOPCUAExtractor/{Version.GetVersion()}", true, true, true);
            }

            using var provider = services.BuildServiceProvider();

            Log.Logger = provider.GetRequiredService<ILogger>();
            _log = Log.Logger.ForContext(typeof(ExtractorServiceStarter));

            _log.Information("Using configuration file: {configFile}", configFile);
            _log.Information("Starting OPC UA Extractor version {version}", Version.GetVersion());
            _log.Information("Revision information: {status}", Version.Status());

            version.Set(0);

            var metrics = provider.GetRequiredService<MetricsService>();
            metrics.Start();

            RunExtractorService(_config, provider);

            return 0;
        }

        /// <summary>
        /// Own function used when starting up from the windows service layer.
        /// </summary>
        /// <param name="config"></param>
        private static void RunExtractorService(FullConfig config, IServiceProvider provider)
        {
            var runTime = new ExtractorRuntime(config, provider);

            try
            {
                _log.Information("Starting extractor");
                runTime.Run(_sourceProgram.Token).Wait();
            }
            catch (TaskCanceledException)
            {
            }
            catch (AggregateException aex)
            {
                if (ExtractorUtils.GetRootExceptionOfType<Cognite.OpcUa.ConfigurationException>(aex) != null)
                {
                    _log.Error("Invalid configuration: {msg}", aex.InnerException.Message);
                }
                else if (ExtractorUtils.GetRootExceptionOfType<TaskCanceledException>(aex) != null)
                {
                    _log.Error("Extractor halted due to cancelled task");
                }
                else if (ExtractorUtils.GetRootExceptionOfType<SilentServiceException>(aex) == null)
                {
                    _log.Error(aex, "Unexpected failure in extractor: {msg}", aex.Message);
                }
            }
            catch (Cognite.OpcUa.ConfigurationException ex)
            {
                _log.Error("Invalid configuration: {msg}", ex.Message);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Unexpected failure in extractor: {msg}", ex.Message);
            }

            _log.Information("Stopping extractor");
            Log.CloseAndFlush();
            _isRunning = false;
        }

        /// <summary>
        /// Stop method for use by the windows service.
        /// </summary>
        public static void Stop()
        {
            _sourceProgram.Cancel();
            Thread.Sleep(2000);
        }

        /// <summary>
        /// Returns extractor status, used by the windows service  
        /// </summary>
        /// <returns></returns>
        public static bool RunningStatus()
        {
            return _isRunning;
        }

        /// <summary>
        /// Does check that the config file exists and can be used, does not check for errors on the values itself.
        /// </summary>
        /// <param name="configFile">Full path to a OpcUa config file, example: c:\tmp\config.yml</param>
        /// <returns>OK or an error string to be logged</returns>
        public static string ValidateConfigurationFile(string configFile)
        {
            string result = "OK";
            FullConfig testConfig;

            try
            {
                testConfig = ConfigurationUtils.Read<FullConfig>(configFile);
                // More checks can be added here on user provided values, for example url validation numeric and string tests etc.
            }
            catch (System.IO.FileNotFoundException)
            {
                return $"Configuration file not found. {configFile}";
            }
            catch (YamlDotNet.Core.YamlException e)
            {
                return $"Failed to load config at {e.Start}: {e.InnerException?.Message ?? e.Message}";
            }

            return result;

        }

    }
}


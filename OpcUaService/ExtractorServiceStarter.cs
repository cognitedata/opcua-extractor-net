using System;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Configuration;
using Prometheus.Client;
using Serilog;
using Cognite.OpcUa;
using Cognite.Extractor.Logging;

namespace OpcUaService
{
    /// <summary>
    /// Contains the code to start the extractor, validate configuration file etc
    /// </summary>
    public static class ExtractorServiceStarter
    {
        private static CancellationTokenSource _sourceProgram;
        private static bool _isRunning = false;
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
            _config = ConfigurationUtils.Read<FullConfig>(configFile);

            _config.Source.ConfigRoot = "config/";

            _config.Logger.Console.Level = "NoConsoleLogging";
            LoggingUtils.Configure(_config.Logger);
            _log = Log.Logger.ForContext(typeof(ExtractorServiceStarter));

            _log.Information($"Using configuration file: {configFile}");
            _log.Information("Starting OPC UA Extractor version {version}", Version.GetVersion());
            _log.Information("Revision information: {status}", Version.Status());

            version.Set(0);

            try
            {
                MetricsManager.Setup(_config.Metrics);
            }
            catch (Exception e)
            {
                _log.Error(e, "Failed to start metrics pusher");
            }

            RunExtractorService(_config);

            return 0;
        }

        /// <summary>
        /// Own function used when starting up from the windows service layer.
        /// </summary>
        /// <param name="config"></param>
        private static void RunExtractorService(FullConfig config)
        {
            var runTime = new ExtractorRuntime(config);
            runTime.Configure();

            try
            {
                _log.Information("Starting extractor");
                runTime.Run(_sourceProgram).Wait();
            }
            catch (TaskCanceledException)
            {
                _log.Warning("Extractor stopped manually");
            }
            catch (AggregateException aex)
            {
                if (ExtractorUtils.GetRootExceptionOfType<Cognite.OpcUa.ConfigurationException>(aex) != null)
                {
                    _log.Error("Invalid configuration, stopping");
                }
                if (ExtractorUtils.GetRootExceptionOfType<TaskCanceledException>(aex) != null)
                {
                    _log.Warning("Extractor stopped manually");
                }
            }
            catch
            {
                _log.Error("Extractor crashed, restarting");
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


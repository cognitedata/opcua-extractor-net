using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.GoogleCloudLogging;

namespace Cognite.OpcUa
{
    public static class Logger
    {
        private static ILogger logger;

        public static ILogger Current()
        {
            if (logger == null)
            {
                throw new InvalidOperationException("Logger has not been configured.");
            }
            return logger;
        }

        public static ILogger Configure(LoggerConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            bool logToConsole = Enum.TryParse(config.ConsoleLevel, true, out LogEventLevel consoleLevel);
            bool logToFile = Enum.TryParse(config.FileLevel, true, out LogEventLevel fileLevel);
            bool logToStackdriver = config.StackdriverCredentials != null;

            var logConfig = new LoggerConfiguration();
            logConfig.MinimumLevel.Verbose();

            if (logToConsole)
            {
                logConfig.WriteTo.Console(consoleLevel);
            }

            if (logToFile && config.LogFolder != null)
            {
                string path = $"{config.LogFolder}{Path.DirectorySeparatorChar}log.log";
                logConfig.WriteTo.Async(p => p.File(
                    path,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: config.RetentionLimit,
                    restrictedToMinimumLevel: fileLevel));
            }

            if (logToStackdriver)
            {
                using StreamReader r = new StreamReader(config.StackdriverCredentials);
                string json = r.ReadToEnd();
                var jsonObj = JsonConvert.DeserializeObject<GpcCredentials>(json);

                var resourceLabels = new Dictionary<string, string>
                    {
                        { "email_id", jsonObj.ClientEmail },
                        { "unique_id", jsonObj.ClientId }
                    };

                var gcConfig = new GoogleCloudLoggingSinkOptions(
                    jsonObj.ProjectId,
                    jsonObj.ResourceType,
                    config.StackdriverLogName,
                    resourceLabels: resourceLabels,
                    useJsonOutput: true,
                    googleCredentialJson: json);
                logConfig.WriteTo.GoogleCloudLogging(gcConfig);
            }

            logger = logConfig.CreateLogger();
            Log.Logger = logger;
            return logger;

        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1812:Uninstantiated internal class",
            Justification = "Late initialization")]
        private class GpcCredentials
        {
            [JsonProperty("project_id")]
            public string ProjectId { get; set; }

            [JsonProperty("type")]
            public string ResourceType { get; set; }

            [JsonProperty("client_email")]
            public string ClientEmail { get; set; }

            [JsonProperty("client_id")]
            public string ClientId { get; set; }
        }
    }
}

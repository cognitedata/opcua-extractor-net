using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
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
            var logToConsole = Enum.TryParse(config.ConsoleLevel, true, out LogEventLevel consoleLevel);
            var logToFile = Enum.TryParse(config.FileLevel, true, out LogEventLevel fileLevel);
            var logToStackdriver = config.StackdriverCredentials != null;

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
                using (StreamReader r = new StreamReader(config.StackdriverCredentials))
                {
                    string json = r.ReadToEnd();
                    var jsonObj = JsonConvert.DeserializeObject<GpcCredentials>(json);

                    var resourceLabels = new Dictionary<string, string>();
                    resourceLabels.Add("email_id", jsonObj.ClientEmail);
                    resourceLabels.Add("unique_id", jsonObj.ClientId);

                    var gcConfig = new GoogleCloudLoggingSinkOptions(
                        jsonObj.ProjectId,
                        jsonObj.ResourceType,
                        config.StackdriverLogName,
                        resourceLabels: resourceLabels,
                        useJsonOutput: true,
                        googleCredentialJson: json);
                    logConfig.WriteTo.GoogleCloudLogging(gcConfig);
                }
            }

            logger = logConfig.CreateLogger();
            Log.Logger = logger;
            return logger;

        }
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

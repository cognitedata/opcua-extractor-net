using Cognite.Extractor.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;
using Serilog;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Service
{
    public class Worker : BackgroundService
    {
        private static readonly Gauge version =
            Metrics.CreateGauge("opcua_version", $"version: {Version.GetVersion()}, status: {Version.Status()}");
        private FullConfig config;
        private readonly IServiceProvider provider;
        private Microsoft.Extensions.Logging.ILogger eventLog;
        public Worker(ILogger<Worker> eventLog, IServiceProvider provider)
        {
            this.provider = provider;
            this.eventLog = eventLog;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Log.Logger = provider.GetRequiredService<Serilog.ILogger>();
            config = provider.GetRequiredService<FullConfig>();
            eventLog.LogInformation("Starting OPC UA Extractor service version {version}", Version.GetVersion());
            eventLog.LogInformation("Revision information: {status}", Version.Status());

            var metrics = provider.GetRequiredService<MetricsService>();
            metrics.Start();

            var runTime = new ExtractorRuntime(config, provider);

            int waitRepeats = 0;

            version.Set(0);

            while (!stoppingToken.IsCancellationRequested)
            {

                DateTime startTime = DateTime.UtcNow;
                try
                {
                    await runTime.Run(stoppingToken);
                }
                catch (TaskCanceledException)
                {
                }
                catch (AggregateException aex)
                {
                    if (ExtractorUtils.GetRootExceptionOfType<ConfigurationException>(aex) != null)
                    {
                        eventLog.LogError("Invalid configuration, stopping: {msg}", aex.InnerException.Message);
                        break;
                    }
                    if (ExtractorUtils.GetRootExceptionOfType<TaskCanceledException>(aex) != null)
                    {
                        eventLog.LogError("Extractor halted due to cancelled task");
                    }
                    else if (ExtractorUtils.GetRootExceptionOfType<SilentServiceException>(aex) == null)
                    {
                        eventLog.LogError(aex, "Unexpected failure in extractor: {msg}", aex.Message);
                    }
                }
                catch (ConfigurationException)
                {
                    eventLog.LogError("Invalid configuration, stopping");
                    break;
                }
                catch (Exception ex)
                {
                    eventLog.LogError(ex, "Unexpected failure in extractor: {msg}", ex.Message);
                }

                if (startTime > DateTime.UtcNow - TimeSpan.FromSeconds(600))
                {
                    waitRepeats++;
                }
                else
                {
                    waitRepeats = 0;
                }

                try
                {
                    var sleepTime = TimeSpan.FromSeconds(Math.Pow(2, Math.Min(waitRepeats, 9)));
                    eventLog.LogInformation("Sleeping for {time}", sleepTime);
                    await Task.Delay(sleepTime, stoppingToken);
                }
                catch (Exception)
                {
                    eventLog.LogWarning("Extractor stopped manually");
                    break;
                }

                if (waitRepeats > 5) break;
            }
        }
    }
}

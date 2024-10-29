using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.FDM;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.ILA
{
    public class ILAWriter
    {
        private readonly ILogger<ILAWriter> log;

        private readonly string logSpace;

        private readonly CogniteDestination destination;

        private readonly LogAnalyticsConfig logAnalyticsConfig;
        private readonly LogContainerCache containerCache;
        private readonly string stream;

        private DMSValueConverter? converter;
        private INodeIdConverter? context;

        public ILAWriter(FullConfig config, CogniteDestination destination, ILogger<ILAWriter> log)
        {
            // this.config = config;
            this.destination = destination;
            this.log = log;
            logAnalyticsConfig = config.Cognite!.LogAnalytics!;
            // modelSpace = logAnalyticsConfig.ModelSpace ?? throw new ConfigurationException("log-analytics.model-space is required when writing to log analytics is enabled");
            logSpace = logAnalyticsConfig.LogSpace ?? throw new ConfigurationException("log-analytics.log-space is required when writing to log analytics is enabled");
            stream = logAnalyticsConfig.Stream ?? throw new ConfigurationException("log-analytics.stream is required when writing to log analytics is enabled");
            containerCache = new LogContainerCache(destination, logAnalyticsConfig, log);
        }

        private async Task Initialize(UAExtractor extractor, CancellationToken token)
        {
            await containerCache.Initialize(token);
            converter = new DMSValueConverter(extractor.StringConverter, logSpace);
            context = new NodeIdExternalIdConverter(extractor);
        }

        public async Task<bool?> PushEvents(UAExtractor extractor, List<UAEvent> events, CancellationToken token)
        {
            if (converter == null)
            {
                try
                {
                    await Initialize(extractor, token);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to initialize log analytics writer: {Message}", ex.Message);
                    return false;
                }
            }

            await containerCache.EnsureContainers(events.SelectNonNull(e => e.EventType), token);

            log.LogDebug("Writing {Count} events to industrial log analytics", events.Count);

            var logs = new List<LogItem>();

            foreach (var evt in events)
            {
                if (evt.EventType == null)
                {
                    log.LogWarning("Cannot ingest event to ILA, event type is not set");
                    continue;
                }

                var type = containerCache.ResolveEventType(evt.EventType, token);
                logs.Add(type.InstantiateFromEvent(evt, logSpace, converter!, context!));
            }

            var generators = logs.ChunkBy(10_000).Select<IEnumerable<LogItem>, Func<Task>>(c => async () =>
            {
                log.LogInformation("Send request to ILA");
                await destination.CogniteClient.Alpha.LogAnalytics.IngestAsync(stream, c, token);
                log.LogInformation("Finished writing logs to ILA");
            });
            try
            {
                await generators.RunThrottled(5, token);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push events to log analytics: {Message}", ex.Message);
                return ex is ResponseException rex && (rex.Code == 400 || rex.Code == 409);
            }

            return true;
        }
    }
}
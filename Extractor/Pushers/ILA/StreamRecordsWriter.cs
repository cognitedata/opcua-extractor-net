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
using CogniteSdk.Beta;
using CogniteSdk.DataModels;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.ILA
{
    public class StreamRecordsWriter
    {
        private readonly ILogger<StreamRecordsWriter> log;

        private readonly string logSpace;

        private readonly CogniteDestination destination;

        private readonly StreamRecordsConfig logAnalyticsConfig;
        private readonly LogContainerCache containerCache;
        private readonly string stream;

        private DMSValueConverter? converter;
        private INodeIdConverter? context;

        public StreamRecordsWriter(FullConfig config, CogniteDestination destination, ILogger<StreamRecordsWriter> log)
        {
            // this.config = config;
            this.destination = destination;
            this.log = log;
            logAnalyticsConfig = config.Cognite!.StreamRecords!;
            // modelSpace = logAnalyticsConfig.ModelSpace ?? throw new ConfigurationException("log-analytics.model-space is required when writing to log analytics is enabled");
            logSpace = logAnalyticsConfig.LogSpace ?? throw new ConfigurationException("log-analytics.log-space is required when writing to log analytics is enabled");
            stream = logAnalyticsConfig.Stream ?? throw new ConfigurationException("log-analytics.stream is required when writing to log analytics is enabled");
            containerCache = new LogContainerCache(destination, logAnalyticsConfig, log);
        }

        private async Task Initialize(UAExtractor extractor, CancellationToken token)
        {
            var spaces = new[] { logAnalyticsConfig.ModelSpace, logAnalyticsConfig.LogSpace }.Distinct();

            await destination.CogniteClient.DataModels.UpsertSpaces(spaces.Select(s => new SpaceCreate { Space = s, Name = s }), token);

            await containerCache.Initialize(token);
            converter = new DMSValueConverter(extractor.StringConverter, logSpace);
            if (logAnalyticsConfig.UseRawNodeId)
            {
                context = new NodeIdDirectStringConverter();
            }
            else
            {
                context = new NodeIdExternalIdConverter(extractor);
            }
            await destination.GetOrCreateStreamAsync(new StreamWrite
            {
                ExternalId = stream
            }, token);
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
                    log.LogError(ex, "Failed to initialize stream records writer: {Message}", ex.Message);
                    return false;
                }
            }

            await containerCache.EnsureContainers(events.SelectNonNull(e => e.EventType), logAnalyticsConfig.UseRawNodeId, token);

            log.LogDebug("Writing {Count} events to stream records", events.Count);

            var logs = new List<StreamRecordWrite>();

            foreach (var evt in events)
            {
                if (evt.EventType == null)
                {
                    log.LogWarning("Cannot ingest event to stream records, event type is not set");
                    continue;
                }

                var type = containerCache.ResolveEventType(evt.EventType, token);
                logs.Add(type.InstantiateFromEvent(evt, logSpace, converter!, context!, logAnalyticsConfig.UseReversibleJson));
            }

            try
            {
                await destination.InsertRecordsAsync(stream, logs, token);
                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push events to log analytics: {Message}", ex.Message);
                return ex is ResponseException rex && (rex.Code == 400 || rex.Code == 409);
            }
        }
    }
}
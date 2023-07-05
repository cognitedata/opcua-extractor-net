using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class TimeseriesWriter : ITimeseriesWriter
    {
        private ILogger<TimeseriesWriter> log;
        private readonly FullConfig config;
        private readonly CogniteDestination destination;
        private readonly CancellationToken token;
        private bool pushCleanTimeseries =>
            string.IsNullOrWhiteSpace(config.Cognite?.RawMetadata?.Database)
            && string.IsNullOrWhiteSpace(config.Cognite?.RawMetadata?.TimeseriesTable);

        public TimeseriesWriter(
            ILogger<TimeseriesWriter> logger,
            CancellationToken token,
            CogniteDestination destination,
            FullConfig config) 
        {
            this.log = logger;
            this.config = config;
            this.destination = destination;
            this.token = token;
        }

        public async Task<Result> PushVariables(
            UAExtractor extractor,
            ConcurrentDictionary<string, UAVariable> timeseriesMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            HashSet<string> mismatchedTimeseries,
            TypeUpdateConfig update
        )
        {
            var result = new Result { Created = 0, Updated = 0 };
            var skipMeta = config.Cognite?.SkipMetadata;
            var timeseries = await CreateTimeseries(
                extractor,
                timeseriesMap,
                nodeToAssetIds,
                mismatchedTimeseries,
                result,
                !pushCleanTimeseries || (skipMeta.HasValue ? skipMeta.Value : false)
            );

            var toPushMeta = timeseriesMap
                .Where(kvp => kvp.Value.Source != NodeSource.CDF)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            if (update.AnyUpdate && toPushMeta.Any() && pushCleanTimeseries)
            {
                await UpdateTimeseries(extractor, toPushMeta, timeseries, nodeToAssetIds, update, result);
            }
            return result;
        }
        
        private async Task<IEnumerable<TimeSeries>> CreateTimeseries(
            UAExtractor extractor,
            IDictionary<string, UAVariable> tsMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            HashSet<string> mismatchedTimeseries,
            Result result,
            bool createMinimalTimeseries
        )
        {
            var timeseries = await destination.GetOrCreateTimeSeriesAsync(
                tsMap.Keys,
                ids =>
                {
                    var tss = ids.Select(id => tsMap[id]);
                    var creates = tss.Select(
                            ts =>
                                ts.ToTimeseries(
                                    config,
                                    extractor,
                                    extractor,
                                    config.Cognite?.DataSet?.Id,
                                    nodeToAssetIds,
                                    config.Cognite?.MetadataMapping?.Timeseries,
                                    createMinimalTimeseries
                                )
                        )
                        .Where(ts => ts != null);
                    if (createMinimalTimeseries)
                    {
                        result.Created += creates.Count();
                    }
                    else
                    {
                        result.Created += creates.Count();
                    }
                    return creates;
                },
                RetryMode.None,
                SanitationMode.Clean,
                token
            );

            log.LogResult(timeseries, RequestType.CreateTimeSeries, true);

            timeseries.ThrowOnFatal();

            if (timeseries.Results == null)
                return Array.Empty<TimeSeries>();

            var foundBadTimeseries = new List<string>();
            foreach (var ts in timeseries.Results)
            {
                var loc = tsMap[ts.ExternalId];
                if (nodeToAssetIds.TryGetValue(loc.ParentId, out var parentId))
                {
                    nodeToAssetIds[loc.Id] = parentId;
                }
                if (ts.IsString != loc.FullAttributes.DataType.IsString)
                {
                    mismatchedTimeseries.Add(ts.ExternalId);
                    foundBadTimeseries.Add(ts.ExternalId);
                }
            }
            if (foundBadTimeseries.Any())
            {
                log.LogDebug(
                    "Found mismatched timeseries when ensuring: {TimeSeries}",
                    string.Join(", ", foundBadTimeseries)
                );
            }

            return timeseries.Results;
        }

        private async Task UpdateTimeseries(
            UAExtractor extractor,
            IDictionary<string, UAVariable> tsMap,
            IEnumerable<TimeSeries> timeseries,
            IDictionary<NodeId, long> nodeToAssetIds,
            TypeUpdateConfig update,
            Result result)
        {
            var updates = new List<TimeSeriesUpdateItem>();
            var existing = timeseries.ToDictionary(asset => asset.ExternalId);
            foreach (var kvp in tsMap)
            {
                if (existing.TryGetValue(kvp.Key, out var ts))
                {
                    var tsUpdate = PusherUtils.GetTSUpdate(config, extractor, ts, kvp.Value, update, nodeToAssetIds);
                    if (tsUpdate == null) continue;
                    if (tsUpdate.AssetId != null || tsUpdate.Description != null
                        || tsUpdate.Name != null || tsUpdate.Metadata != null)
                    {
                        updates.Add(new TimeSeriesUpdateItem(ts.ExternalId) { Update = tsUpdate });
                    }
                }
            }

            if (updates.Any())
            {
                var res = await destination.UpdateTimeSeriesAsync(updates, RetryMode.OnError, SanitationMode.Clean, token);

                log.LogResult(res, RequestType.UpdateTimeSeries, false);
                res.ThrowOnFatal();

                result.Updated += res.Results?.Count() ?? 0;
            }
        }
    }
}

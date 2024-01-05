using System;
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
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers
{
    public abstract class BaseTimeseriesWriter
    {
        protected readonly ILogger logger;
        protected readonly FullConfig config;
        protected readonly CogniteDestination destination;

        public BaseTimeseriesWriter(ILogger logger, CogniteDestination destination, FullConfig config)
        {
            this.logger = logger;
            this.config = config;
            this.destination = destination;
        }


        /// <summary>
        /// Synchronizes all BaseUANode to CDF Timeseries
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="timeseriesMap">Dictionary of mapping of variables to keys</param>
        /// <param name="nodeToAssetIds">Node to assets to ids</param>
        /// <param name="mismatchedTimeseries">Mismatched timeseries</param>
        /// <param name="update">Type update configuration</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Operation result</returns>
        public async Task<bool> PushVariables(
            UAExtractor extractor,
            IDictionary<string, UAVariable> timeseriesMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            HashSet<string> mismatchedTimeseries,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token)
        {
            try
            {
                var result = new Result { Created = 0, Updated = 0 };
                var timeseries = await CreateTimeseries(
                    extractor,
                    timeseriesMap,
                    nodeToAssetIds,
                    mismatchedTimeseries,
                    result,
                    token
                );

                var toPushMeta = timeseriesMap
                    .Where(kvp => kvp.Value.Source != NodeSource.CDF)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                if (update.AnyUpdate && toPushMeta.Count != 0)
                {
                    await UpdateTimeseries(extractor, toPushMeta, timeseries, nodeToAssetIds, update, result, token);
                }
                if (this is MinimalTimeseriesWriter)
                {
                    report.MinimalTimeSeriesCreated += result.Created;
                }
                else
                {
                    report.TimeSeriesCreated += result.Created;
                }
                report.TimeSeriesUpdated += result.Updated;
                return true;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to push timeseries to CDF Clean: {Message}", ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Create BaseUANode to CDF Timeseries
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="tsMap">Dictionary of mapping of variables to keys</param>
        /// <param name="nodeToAssetIds">Node to assets to ids</param>
        /// <param name="mismatchedTimeseries">Mismatched timeseries</param>
        /// <param name="result">Operation result</param>
        /// <param name="update">Type update configuration</param>
        /// <param name="createMinimalTimeseries">Indicate if to create minimal timeseries</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Operation result</returns>
        private async Task<IEnumerable<TimeSeries>> CreateTimeseries(
            UAExtractor extractor,
            IDictionary<string, UAVariable> tsMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            HashSet<string> mismatchedTimeseries,
            Result result,
            CancellationToken token)
        {
            var timeseries = await destination.GetOrCreateTimeSeriesAsync(
                tsMap.Keys,
                ids => BuildTimeseries(tsMap, ids, extractor, nodeToAssetIds, result),
                RetryMode.None,
                SanitationMode.Clean,
                token
            );

            logger.LogResult(timeseries, RequestType.CreateTimeSeries, true);

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
            if (foundBadTimeseries.Count != 0)
            {
                logger.LogDebug(
                    "Found mismatched timeseries when ensuring: {TimeSeries}",
                    string.Join(", ", foundBadTimeseries)
                );
            }

            return timeseries.Results;
        }

        protected abstract IEnumerable<TimeSeriesCreate> BuildTimeseries(
            IDictionary<string, UAVariable> tsMap,
            IEnumerable<string> ids,
            UAExtractor extractor,
            IDictionary<NodeId, long> nodeToAssetIds,
            Result result);

        protected abstract Task UpdateTimeseries(
            UAExtractor extractor,
            IDictionary<string, UAVariable> tsMap,
            IEnumerable<TimeSeries> timeseries,
            IDictionary<NodeId, long> nodeToAssetIds,
            TypeUpdateConfig update,
            Result result,
            CancellationToken token);

        public async Task MarkTimeseriesDeleted(IEnumerable<string> externalIds, CancellationToken token)
        {
            var updates = externalIds.Select(
                extId =>
                    new TimeSeriesUpdateItem(extId)
                    {
                        Update = new TimeSeriesUpdate
                        {
                            Metadata = new UpdateDictionary<string>(
                                new Dictionary<string, string>
                                {
                                    { config.Extraction.Deletes.DeleteMarker, "true" }
                                },
                                Enumerable.Empty<string>()
                            )
                        }
                    }
            );
            var result = await destination.UpdateTimeSeriesAsync(
                updates,
                RetryMode.OnError,
                SanitationMode.Clean,
                token
            );
            logger.LogResult(result, RequestType.UpdateAssets, true);
            result.ThrowOnFatal();
        }
    }
}

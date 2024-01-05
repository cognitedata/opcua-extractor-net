/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class TimeseriesWriter : BaseTimeseriesWriter
    {
        public TimeseriesWriter(ILogger<TimeseriesWriter> logger, CogniteDestination destination, FullConfig config)
            : base(logger, destination, config)
        { }

        protected override IEnumerable<TimeSeriesCreate> BuildTimeseries(IDictionary<string, UAVariable> tsMap,
                IEnumerable<string> ids, UAExtractor extractor, IDictionary<NodeId, long> nodeToAssetIds, Result result)
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
                            config.Cognite?.MetadataMapping?.Timeseries
                        )
                )
                .Where(ts => ts != null);
            result.Created += creates.Count();
            return creates;
        }

        /// <summary>
        /// Update BaseUANode to CDF Timeseries
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="tsMap">Dictionary of mapping of variables to keys</param>
        /// <param name="nodeToAssetIds">Node to assets to ids</param>
        /// <param name="update">Type update configuration</param>
        /// <param name="result">Operation result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Operation result</returns>
        protected override async Task UpdateTimeseries(UAExtractor extractor, IDictionary<string, UAVariable> tsMap,
                IEnumerable<TimeSeries> timeseries, IDictionary<NodeId, long> nodeToAssetIds, TypeUpdateConfig update, Result result, CancellationToken token)
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

            if (updates.Count != 0)
            {
                var res = await destination.UpdateTimeSeriesAsync(updates, RetryMode.OnError, SanitationMode.Clean, token);

                logger.LogResult(res, RequestType.UpdateTimeSeries, false);
                res.ThrowOnFatal();

                result.Updated += res.Results?.Count() ?? 0;
            }
        }
    }
}

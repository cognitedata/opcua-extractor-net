using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Text.Json;
using CogniteSdk;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Com.Cognite.V1.Timeseries.Proto;
using MQTTnet;
using Serilog;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;

namespace Cognite.Bridge
{
    public class Destination
    {
        private readonly IServiceProvider provider;
        private readonly CogniteConfig config;

        private readonly ConcurrentDictionary<string, long?> assetIds = new ConcurrentDictionary<string, long?>();
        private readonly ConcurrentDictionary<string, bool> tsIsString = new ConcurrentDictionary<string, bool>();

        private readonly ILogger log = Log.Logger.ForContext(typeof(Destination));
        public Destination(CogniteConfig config, IServiceProvider provider)
        {
            this.config = config;
            this.provider = provider;
        }

        private CogniteDestination GetDestination()
        {
            return provider.GetRequiredService<CogniteDestination>();
        }
        /// <summary>
        /// Retrieve assets from CDF, push any that do not exist.
        /// </summary>
        /// <param name="msg">Raw message from MQTT</param>
        /// <returns>True on success</returns>
        public async Task<bool> PushAssets(MqttApplicationMessage msg, CancellationToken token)
        {
            if (msg == null) return true;
            if (msg.Payload == null)
            {
                log.Warning("Null payload in assets");
                return true;
            }
            var assets = JsonSerializer.Deserialize<IEnumerable<AssetCreate>>(Encoding.UTF8.GetString(msg.Payload));
            if (!assets.Any()) return true;
            var destination = GetDestination();

            var idsToTest = assets.Select(asset => asset.ExternalId).ToList();

            try
            {
                var created = await destination.GetOrCreateAssetsAsync(idsToTest, ids =>
                {
                    var idsSet = new HashSet<string>(ids);
                    return assets.Where(asset => idsSet.Contains(asset.ExternalId));
                }, token);
                foreach (var asset in created)
                {
                    assetIds[asset.ExternalId] = asset.Id;
                }
            }
            catch (ResponseException ex)
            {
                log.Error(ex, "Failed to ensure assets: {msg}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }

            return true;
        }
        private async Task<bool> RetrieveMissingAssets(IEnumerable<string> ids, CancellationToken token)
        {
            if (!ids.Any()) return true;
            var destination = GetDestination();

            try
            {
                var assets = await destination.CogniteClient.Assets.RetrieveAsync(ids.Distinct().Select(Identity.Create), true, token);
                foreach (var asset in assets)
                {
                    assetIds[asset.ExternalId] = asset.Id;
                }
            }
            catch (ResponseException ex)
            {
                log.Error(ex, "Failed to retrieve missing assets: {msg}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }
            return true;
        }

        private async Task<bool> RetrieveMissingTimeSeries(IEnumerable<string> ids, CancellationToken token)
        {
            if (!ids.Any()) return true;
            var destination = GetDestination();

            try
            {
                var tss = await destination.CogniteClient.TimeSeries.RetrieveAsync(ids.Distinct().Select(Identity.Create), true, token);
                foreach (var ts in tss)
                {
                    tsIsString[ts.ExternalId] = ts.IsString;
                }
            }
            catch (ResponseException ex)
            {
                log.Error(ex, "Failed to retrieve missing timeseries: {msg}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }
            return true;
        }

        /// <summary>
        /// Retrieve list of timeseries from mqtt message, pushing any that are not found and registering
        /// all in the state, whether they existed before or not.
        /// </summary>
        /// <param name="msg">Raw message from MQTT with timeseries payload</param>
        /// <returns>True on success</returns>
        public async Task<bool> PushTimeseries(MqttApplicationMessage msg, CancellationToken token)
        {
            if (msg == null) return true;
            if (msg.Payload == null)
            {
                log.Warning("Null payload in timeseries");
                return true;
            }
            var timeseries = JsonSerializer.Deserialize<IEnumerable<StatelessTimeSeriesCreate>>(Encoding.UTF8.GetString(msg.Payload));

            var assetExternalIds = timeseries.Select(ts => ts.AssetExternalId).Where(id => id != null).ToHashSet();

            var missingAssetIds = assetExternalIds.Except(assetIds.Keys);

            var destination = GetDestination();

            if (missingAssetIds.Any())
            {
                if (!await RetrieveMissingAssets(missingAssetIds, token)) return false;
            }

            var testSeries = new List<TimeSeriesCreate>();

            foreach (var ts in timeseries)
            {
                if (ts.AssetExternalId == null)
                {
                    testSeries.Add(ts);
                }
                else if (assetIds.ContainsKey(ts.AssetExternalId))
                {
                    var id = assetIds[ts.AssetExternalId];
                    if (id != null)
                    {
                        testSeries.Add(ts);
                        ts.AssetId = id;
                    }
                }
            }

            try
            {
                var created = await destination.GetOrCreateTimeSeriesAsync(testSeries.Select(ts => ts.ExternalId), ids =>
                {
                    var idsSet = new HashSet<string>(ids);
                    return testSeries.Where(ts => idsSet.Contains(ts.ExternalId));
                }, token);
                foreach (var ts in created)
                {
                    tsIsString[ts.ExternalId] = ts.IsString;
                }
            }
            catch (ResponseException ex)
            {
                log.Error(ex, "Failed to create missing time series: {msg}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }

            return true;
        }
        /// <summary>
        /// Find unknown timeseries, then push datapoints with matching, existing, timeseries to CDF.
        /// </summary>
        /// <param name="msg">Raw message with protobuf datapoint payload</param>
        /// <returns>True on success</returns>
        public async Task<bool> PushDatapoints(MqttApplicationMessage msg, CancellationToken token)
        {
            if (msg == null) return true;
            if (msg.Payload == null)
            {
                log.Warning("Null payload in datapoints");
                return true;
            }
            var datapoints = DataPointInsertionRequest.Parser.ParseFrom(msg.Payload);

            var ids = datapoints.Items.Select(pts => pts.ExternalId).ToList();

            var missingTsIds = ids
                .Where(id => !tsIsString.ContainsKey(id))
                .ToList();

            if (missingTsIds.Any())
            {
                if (!await RetrieveMissingTimeSeries(missingTsIds, token)) return false;
            }

            var destination = GetDestination();

            var req = new DataPointInsertionRequest();
            req.Items.AddRange(datapoints.Items.Where(
                pts => tsIsString.ContainsKey(pts.ExternalId)
                && !(pts.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.NumericDatapoints && tsIsString[pts.ExternalId])
                && !(pts.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.StringDatapoints && !tsIsString[pts.ExternalId])));

            log.Verbose("Push datapoints for {cnt} out of {cnt2} timeseries", req.Items.Count, datapoints.Items.Count);

            var missingIds = new HashSet<string>();

            try
            {
                await destination.CogniteClient.DataPoints.CreateAsync(req, token);
                log.Debug("Push datapoints for {cnt} timeseries to CDF", req.Items.Count);
            }
            catch (ResponseException ex)
            {
                log.Warning("Failed to push datapoints to CDF: {msg}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }

            return true;
        }
        /// <summary>
        /// Find unknown assets, then push events with any assetIds remaining (or none to begin with) to CDF.
        /// </summary>
        /// <param name="msg">Raw message with event payload</param>
        /// <returns>True on success</returns>
        public async Task<bool> PushEvents(MqttApplicationMessage msg, CancellationToken token)
        {
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (msg.Payload == null)
            {
                log.Warning("Null payload in events");
                return true;
            }
            var events = JsonSerializer.Deserialize<IEnumerable<StatelessEventCreate>>(Encoding.UTF8.GetString(msg.Payload));

            var assetExternalIds = events.SelectMany(evt => evt.AssetExternalIds).Where(id => id != null);
            var missingAssetIds = assetExternalIds.Except(assetIds.Keys);

            var destination = GetDestination();

            if (missingAssetIds.Any())
            {
                if (!await RetrieveMissingAssets(missingAssetIds, token))
                {
                    return false;
                }
            }

            int ignoreCount = 0;
            foreach (var evt in events)
            {
                evt.AssetIds = evt.AssetExternalIds.Where(id => assetIds.ContainsKey(id) && assetIds[id] != null)
                    .Select(id => assetIds[id] ?? 0);
                if (!evt.AssetIds.Any() && evt.AssetExternalIds.Any())
                {
                    ignoreCount++;
                }
            }

            if (ignoreCount > 0)
            {
                log.Warning("Ignoring {cnt} events due to missing assetIds", ignoreCount);
            }

            events = events.Where(evt => evt.AssetIds.Any() || !evt.AssetExternalIds.Any());

            if (!events.Any()) return true;

            try
            {
                await destination.EnsureEventsExistsAsync(events, true, token);
            }
            catch (ResponseException ex)
            {
                return ex.Code == 400 || ex.Code == 409;
            }

            return true;
        }
        [SuppressMessage("Microsoft.Performance", "CA1812")]
        internal class StatelessEventCreate : EventCreate
        {
            public IEnumerable<string> AssetExternalIds { get; set; }
        }
        [SuppressMessage("Microsoft.Performance", "CA1812")]
        internal class StatelessTimeSeriesCreate : TimeSeriesCreate
        {
            public string AssetExternalId { get; set; }
        }
    }
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Text.Json;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Com.Cognite.V1.Timeseries.Proto;
using MQTTnet;
using Serilog;

namespace Cognite.Bridge
{
    public class Destination
    {
        private readonly IHttpClientFactory factory;
        private readonly CDFConfig config;

        private readonly ConcurrentDictionary<string, long?> assetIds = new ConcurrentDictionary<string, long?>();
        private readonly ConcurrentDictionary<string, bool?> tsIsString = new ConcurrentDictionary<string, bool?>();

        private readonly ILogger log = Log.ForContext(typeof(Destination));
        public Destination(CDFConfig config, IServiceProvider provider)
        {
            this.config = config;
            factory = provider.GetRequiredService<IHttpClientFactory>();
        }

        private Client GetClient(string name = "Context")
        {
            return new Client.Builder()
                .SetHttpClient(factory.CreateClient(name))
                .AddHeader("api-key", config.ApiKey)
                .SetAppId("MQTT Bridge")
                .SetProject(config.Project)
                .SetBaseUrl(new Uri(config.Host, UriKind.Absolute))
                .Build();
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
            var client = GetClient();

            var idsToTest = assets.Select(asset => asset.ExternalId).Select(Identity.Create).ToList();

            var missingAssets = new HashSet<string>();

            try
            {
                var retrievedAssets = await client.Assets.RetrieveAsync(idsToTest, token);

                foreach (var asset in retrievedAssets)
                {
                    assetIds[asset.ExternalId] = asset.Id;
                }
                log.Information("Sucessfully retrieved: {cnt} assets", retrievedAssets.Count());
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing != null && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out MultiValue raw))
                        {
                            if (!(raw is MultiValue.String value)) continue;
                            missingAssets.Add(value.Value);
                        }
                    }

                    log.Information("Found {NumMissingAssets} missing assets", ex.Missing.Count());
                }
                else
                {
                    log.Warning("Failed to retrieve assets from CDF: {msg}", ex.Message);
                    return ex.Code < 500;
                }
            }

            if (!missingAssets.Any()) return true;

            var toCreate = assets.Where(asset => missingAssets.Contains(asset.ExternalId)).ToList();

            try
            {
                var createdAssets = await client.Assets.CreateAsync(toCreate);
                foreach (var asset in createdAssets)
                {
                    assetIds[asset.ExternalId] = asset.Id;
                }
                log.Information("Created {cnt} assets", createdAssets.Count());
            }
            catch (ResponseException ex)
            {
                log.Warning("Failed to create assets in CDF: {msg}", ex.Message);
                return ex.Code < 500;
            }

            var lastIds = assets
                .Where(asset => !missingAssets.Contains(asset.ExternalId))
                .Select(asset => Identity.Create(asset.ExternalId))
                .ToList();

            if (!lastIds.Any()) return true;

            try
            {
                var retrievedAssets = await client.Assets.RetrieveAsync(lastIds);
                foreach (var asset in retrievedAssets)
                {
                    assetIds[asset.ExternalId] = asset.Id;
                }
                log.Information("Retrieved {cnt} missing asset ids", retrievedAssets.Count());
            }
            catch (ResponseException ex)
            {
                log.Warning("Failed to create assets in CDF: {msg}", ex.Message);
                return ex.Code < 500;
            }

            return true;
        }
        /// <summary>
        /// Retrieve given list of assets, registering them in the state.
        /// If any are missing, remove them and try again.
        /// </summary>
        /// <param name="ids">Ids to fetch</param>
        /// <returns>True on success</returns>
        private async Task<bool> RetrieveMissingAssetIds(IEnumerable<string> ids, CancellationToken token)
        {
            if (!ids.Any()) return true;
            var secondMissingIds = new List<string>();
            var client = GetClient();
            try
            {
                var retrievedAssets =
                    await client.Assets.RetrieveAsync(ids.Select(Identity.Create), token);

                foreach (var asset in retrievedAssets)
                {
                    assetIds[asset.ExternalId] = asset.Id;
                }
                log.Information("Retrieved {cnt} missing asset ids", retrievedAssets.Count());
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing != null && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out MultiValue raw))
                        {
                            if (!(raw is MultiValue.String value)) continue;
                            assetIds[value.Value] = null;
                            secondMissingIds.Add(value.Value);
                        }
                    }

                    log.Information("Found {NumMissingAssets} missing assets", ex.Missing.Count());
                }
                else
                {
                    log.Warning("Failed to retrieve missing assets: {msg}", ex.Message);
                    return ex.Code < 500;
                }
            }

            if (secondMissingIds.Any())
            {
                var lastMissing = ids.Except(secondMissingIds);
                try
                {
                    var retrievedAssets = await client.Assets.RetrieveAsync(lastMissing.Select(Identity.Create));
                    foreach (var asset in retrievedAssets)
                    {
                        assetIds[asset.ExternalId] = asset.Id;
                    }
                    log.Information("Retrieved {cnt} missing asset ids", retrievedAssets.Count());
                }
                catch (ResponseException ex)
                {
                    log.Warning("Failed to retrieve missing assets: {msg}", ex.Message);
                    return ex.Code < 500;
                }
            }

            return true;
        }
        /// <summary>
        /// Retrieve given list of timeseries, registering them with their type in the state.
        /// If any are missing, remove them and try again.
        /// </summary>
        /// <param name="ids">Ids to fetch</param>
        /// <returns>True on success</returns>
        private async Task<bool> RetrieveMissingTimeseries(IEnumerable<string> ids, CancellationToken token)
        {
            if (!ids.Any()) return true;
            var secondMissingIds = new List<string>();
            var client = GetClient();
            try
            {
                var retrievedTimeseries =
                    await client.TimeSeries.RetrieveAsync(ids.Select(Identity.Create), token);

                foreach (var ts in retrievedTimeseries)
                {
                    tsIsString[ts.ExternalId] = ts.IsString;
                }
                log.Information("Retrieved {cnt} missing timeseries", retrievedTimeseries.Count());
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing != null && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out MultiValue raw))
                        {
                            if (!(raw is MultiValue.String value)) continue;
                            secondMissingIds.Add(value.Value);
                            tsIsString[value.Value] = null;
                        }
                    }

                    log.Information("Found {NumMissingAssets} missing timeseries", ex.Missing.Count());
                }
                else
                {
                    log.Warning("Failed to retrieve missing timeseries: {msg}", ex.Message);
                    return ex.Code < 500;
                }
            }

            if (secondMissingIds.Any())
            {
                var lastMissing = ids.Except(secondMissingIds);
                try
                {
                    var retrievedTimeseries = await client.TimeSeries.RetrieveAsync(lastMissing.Select(Identity.Create));
                    foreach (var ts in retrievedTimeseries)
                    {
                        tsIsString[ts.ExternalId] = ts.IsString;
                    }
                    log.Information("Retrieved {cnt} missing timeseries", retrievedTimeseries.Count());
                }
                catch (ResponseException ex)
                {
                    log.Warning("Failed to retrieve missing timeseries: {msg}", ex.Message);
                    return ex.Code < 500;
                }
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

            var client = GetClient();

            if (missingAssetIds.Any())
            {
                if (!await RetrieveMissingAssetIds(missingAssetIds, token))
                {
                    return false;
                }
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

            var ids = testSeries.Select(ts => Identity.Create(ts.ExternalId));

            var missingIds = new HashSet<string>();

            try
            {
                var retrievedTimeseries = await client.TimeSeries.RetrieveAsync(ids, token);
                foreach (var ts in retrievedTimeseries)
                {
                    tsIsString[ts.ExternalId] = ts.IsString;
                }
                log.Information("Retrieved {cnt} missing timeseries", retrievedTimeseries.Count());
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing != null && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out MultiValue raw))
                        {
                            if (!(raw is MultiValue.String value)) continue;
                            missingIds.Add(value.Value);
                        }
                    }

                    log.Information("Found {NumMissingTimeseries} missing timeseries", ex.Missing.Count());
                }
                else
                {
                    log.Warning("Failed to retrieve missing timeseries: {msg}", ex.Message);
                    return ex.Code < 500;
                }
            }

            if (!missingIds.Any()) return true;

            var toCreate = testSeries.Where(ts => missingIds.Contains(ts.ExternalId));

            try
            {
                var retrievedTimeseries = await client.TimeSeries.CreateAsync(toCreate, token);
                foreach (var ts in retrievedTimeseries)
                {
                    tsIsString[ts.ExternalId] = ts.IsString;
                }
                log.Information("Created {cnt} missing timeseries", retrievedTimeseries.Count());
            }
            catch (ResponseException ex)
            {
                log.Warning("Failed to create missing timeseries: {msg}", ex.Message);
                return ex.Code < 500;
            }

            var lastIds = testSeries
                .Where(ts => !missingIds.Contains(ts.ExternalId))
                .Select(ts => Identity.Create(ts.ExternalId))
                .ToList();

            if (!lastIds.Any()) return true;

            try
            {
                var retrievedTimeseries = await client.TimeSeries.RetrieveAsync(lastIds, token);
                foreach (var ts in retrievedTimeseries)
                {
                    tsIsString[ts.ExternalId] = ts.IsString;
                }
                log.Information("Retrieved {cnt} final timeseries", retrievedTimeseries.Count());
            }
            catch (ResponseException ex)
            {
                log.Warning("Failed to retrieve final missing timeseries: {msg}", ex.Message);
                return ex.Code < 500;
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
                if (!await RetrieveMissingTimeseries(missingTsIds, token))
                {
                    return false;
                }
            }

            var req = new DataPointInsertionRequest();
            req.Items.AddRange(datapoints.Items.Where(
                pts => tsIsString[pts.ExternalId] != null
                && !(pts.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.NumericDatapoints && tsIsString[pts.ExternalId].Value)
                && !(pts.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.StringDatapoints && !tsIsString[pts.ExternalId].Value)));

            log.Verbose("Push datapoints for {cnt} out of {cnt2} timeseries", req.Items.Count, datapoints.Items.Count);

            var client = GetClient("Data");

            var missingIds = new HashSet<string>();

            try
            {
                await client.DataPoints.CreateAsync(req, token);
                log.Debug("Push datapoints for {cnt} timeseries to CDF", req.Items.Count);
            }
            catch (ResponseException ex)
            {
                if (ex.Code == 400 && ex.Missing != null && ex.Missing.Any())
                {
                    foreach (var missing in ex.Missing)
                    {
                        if (missing.TryGetValue("externalId", out MultiValue raw))
                        {
                            if (!(raw is MultiValue.String value)) continue;
                            missingIds.Add(value.Value);
                        }
                    }

                    log.Information("Found {NumMissingTimeseries} missing timeseries", ex.Missing.Count());
                }
                else
                {
                    log.Warning("Failed to push datapoints to CDF: {msg}", ex.Message);
                    return false;
                }
            }

            if (!missingIds.Any()) return true;

            var finalReq = new DataPointInsertionRequest();
            finalReq.Items.AddRange(req.Items.Where(pts => !missingIds.Contains(pts.ExternalId)));

            try
            {
                await client.DataPoints.CreateAsync(finalReq, token);
                log.Debug("Push datapoints for {cnt} timeseries to CDF", finalReq.Items.Count);
            }
            catch (ResponseException ex)
            {
                log.Warning("Failed to push datapoints to CDF: {msg}", ex.Message);
                return false;
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
            log.Information("Events push");
            var events = JsonSerializer.Deserialize<IEnumerable<StatelessEventCreate>>(Encoding.UTF8.GetString(msg.Payload));

            var assetExternalIds = events.SelectMany(evt => evt.AssetExternalIds).Where(id => id != null);
            var missingAssetIds = assetExternalIds.Except(assetIds.Keys);

            var client = GetClient();

            if (missingAssetIds.Any())
            {
                if (!await RetrieveMissingAssetIds(missingAssetIds, token))
                {
                    return false;
                }
            }

            foreach (var evt in events)
            {
                evt.AssetIds = evt.AssetExternalIds.Where(id => assetIds.ContainsKey(id) && assetIds[id] != null)
                    .Select(id => assetIds[id] ?? 0);
                if (!evt.AssetIds.Any() && evt.AssetExternalIds.Any())
                {
                    log.Verbose("Ignoring event with assetIds: {ids}", evt.AssetExternalIds.Aggregate((id, cr) => id + ", " + cr));
                }
            }

            events = events.Where(evt => evt.AssetIds.Any() || !evt.AssetExternalIds.Any());

            if (!events.Any()) return true;

            try
            {
                await client.Events.CreateAsync(events, token);
                log.Debug("Push {cnt} events to CDF", events.Count());
            }
            catch (ResponseException ex)
            {
                if (ex.Duplicated.Any())
                {
                    var duplicates = ex.Duplicated.Where(dict => dict.ContainsKey("externalId")).Select(dict => dict["externalId"].ToString())
                        .ToHashSet();
                    log.Warning("{numduplicates} duplicated event ids, retrying", duplicates.Count);
                    events = events.Where(evt => !duplicates.Contains(evt.ExternalId));
                    try
                    {
                        await client.Events.CreateAsync(events, token);
                        log.Debug("Push {cnt} events to CDF", events.Count());
                    }
                    catch (ResponseException exc)
                    {
                        log.Error("Failed to push {NumFailedEvents} events to CDF: {msg}",
                            events.Count(), exc.Message);
                        return exc.Code < 500;
                    }
                }
                else
                {
                    log.Error("Failed to push {NumFailedEvents} events to CDF: {msg}",
                        events.Count(), ex.Message);
                    return ex.Code < 500;
                }
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

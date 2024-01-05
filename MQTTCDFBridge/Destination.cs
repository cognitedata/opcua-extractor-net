using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Bridge
{
    /// <summary>
    /// Contains methods for pushing contents of MQTT payload to CDF.
    /// </summary>
    public sealed class Destination
    {
        private readonly CogniteDestConfig config;

        private readonly ConcurrentDictionary<string, long?> assetIds = new ConcurrentDictionary<string, long?>();
        private readonly ConcurrentDictionary<string, bool> tsIsString = new ConcurrentDictionary<string, bool>();

        private readonly ILogger log;

        private readonly CogniteDestination destination;

        public Destination(CogniteDestConfig config, IServiceProvider provider)
        {
            this.config = config;
            destination = provider.GetRequiredService<CogniteDestination>();
            log = provider.GetRequiredService<ILogger<Destination>>();
        }

        /// <summary>
        /// Create an asset update from a new asset and an old.
        /// This is conservative, meaning that if new fields are null, they will be ignored and the old will be kept.
        /// </summary>
        /// <param name="update">New asset</param>
        /// <param name="old">Old asset</param>
        /// <returns>Asset update or null if no updates are necessary</returns>
        private static AssetUpdateItem? GetAssetUpdate(AssetCreate? update, Asset? old)
        {
            if (update == null || old == null) return null;

            var upd = new AssetUpdate();
            if (update.DataSetId != null && update.DataSetId != old.DataSetId) upd.DataSetId = new UpdateNullable<long?>(update.DataSetId);
            if (update.Description != null && update.Description != old.Description) upd.Description = new UpdateNullable<string>(update.Description);

            if (update.Metadata != null && update.Metadata.Count != 0)
                upd.Metadata = new UpdateDictionary<string>(update.Metadata, Enumerable.Empty<string>());

            if (update.Name != null && update.Name != old.Name) upd.Name = new Update<string>(update.Name);
            if (update.ParentExternalId != null && update.ParentExternalId != old.ParentExternalId)
                upd.ParentExternalId = new Update<string>(update.ParentExternalId);

            if (upd.DataSetId == null && upd.Description == null && upd.Metadata == null && upd.Name == null
                && upd.ParentExternalId == null) return null;

            return new AssetUpdateItem(update.ExternalId) { Update = upd };
        }
        /// <summary>
        /// Convert an arbitrarily complex JsonDocument assumed to be a JSON object into a string, string dictionary.
        /// </summary>
        /// <param name="doc">Document to convert</param>
        /// <returns>A dictionary</returns>
        private static Dictionary<string, string>? JsonDocumentToDictionary(JsonDocument? doc)
        {
            if (doc == null || doc.RootElement.ValueKind != JsonValueKind.Object) return null;
            var result = new Dictionary<string, string>();
            foreach (var elem in doc.RootElement.EnumerateObject())
            {
                result[elem.Name] = elem.Value.ToString();
            }
            return result;
        }

        /// <summary>
        /// Create an timeseries update from a new timeseries and an old.
        /// This is conservative, meaning that if new fields are null, they will be ignored and the old will be kept.
        /// </summary>
        /// <param name="update">New timeseries</param>
        /// <param name="old">Old timeseries</param>
        /// <returns>Asset update or null if no updates are necessary</returns>
        private static TimeSeriesUpdateItem? GetTimeSeriesUpdate(StatelessTimeSeriesCreate? update, TimeSeries? old)
        {
            if (update == null || old == null) return null;

            var upd = new TimeSeriesUpdate();
            if (update.DataSetId != null && update.DataSetId != old.DataSetId) upd.DataSetId = new UpdateNullable<long?>(update.DataSetId);
            if (update.Description != null && update.Description != old.Description) upd.Description = new UpdateNullable<string>(update.Description);

            var meta = JsonDocumentToDictionary(update.Metadata);
            if (meta != null && meta
                .Any(kvp => old.Metadata == null || !old.Metadata.TryGetValue(kvp.Key, out string? value) || value != kvp.Value))
                upd.Metadata = new UpdateDictionary<string>(meta, Enumerable.Empty<string>());

            if (update.Name != null && update.Name != old.Name) upd.Name = new UpdateNullable<string>(update.Name);
            if (update.AssetId != null && update.AssetId != old.AssetId)
                upd.AssetId = new UpdateNullable<long?>(update.AssetId);

            if (update.Unit != null && update.Unit != old.Unit) upd.Unit = new UpdateNullable<string>(update.Unit);

            if (upd.DataSetId == null && upd.Description == null && upd.Metadata == null && upd.Name == null
                && upd.Unit == null && upd.AssetId == null) return null;

            return new TimeSeriesUpdateItem(update.ExternalId) { Update = upd };
        }

        /// <summary>
        /// Retrieve assets from CDF, push any that do not exist. If updates are enabled,
        /// conservatively update existing assets with any changes.
        /// </summary>
        /// <param name="msg">Raw message from MQTT</param>
        /// <returns>True on success</returns>
        public async Task<bool> PushAssets(MqttApplicationMessage msg, CancellationToken token)
        {
            if (msg == null)
            {
                log.LogWarning("Null payload in assets");
                return true;
            }
            var assets = JsonSerializer.Deserialize<IEnumerable<AssetCreate>>(Encoding.UTF8.GetString(msg.PayloadSegment));
            if (assets == null || !assets.Any()) return true;

            var idsToTest = assets.Select(asset => asset.ExternalId).ToList();

            var createdIds = new ConcurrentBag<string>();

            CogniteResult<Asset, AssetCreate> found;
            try
            {
                found = await destination.GetOrCreateAssetsAsync(idsToTest, ids =>
                {
                    var idsSet = new HashSet<string>(ids);
                    foreach (var id in ids) createdIds.Add(id);
                    return assets.Where(asset => idsSet.Contains(asset.ExternalId));
                }, RetryMode.OnError, SanitationMode.Clean, token);

                if (found.Errors != null && found.Errors.Any(err => err.Type == ErrorType.FatalFailure)) return false;

                foreach (var asset in found.Results!)
                {
                    assetIds[asset.ExternalId] = asset.Id;
                }
            }
            catch (ResponseException ex)
            {
                log.LogError(ex, "Failed to ensure assets: {Message}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }
            if (config.Update)
            {
                var createdIdsSet = new HashSet<string>(createdIds);

                var newAssetsMap = assets.ToDictionary(asset => asset.ExternalId);
                var toUpdate = found.Results!
                    .Where(asset => !createdIdsSet.Contains(asset.ExternalId))
                    .Select(old => GetAssetUpdate(newAssetsMap.GetValueOrDefault(old.ExternalId), old))
                    .Where(update => update != null)
                    .ToList();

                if (toUpdate.Count == 0) return true;

                try
                {
                    await destination.CogniteClient.Assets.UpdateAsync(toUpdate, token);
                }
                catch (ResponseException ex)
                {
                    log.LogError(ex, "Failed to update assets: {Message}", ex.Message);
                    return ex.Code == 400 || ex.Code == 409;
                }
            }

            return true;
        }
        /// <summary>
        /// Retrieve a list of assets by externalId.
        /// </summary>
        /// <param name="ids">Asset externalIds to retrieve</param>
        /// <returns>True on success</returns>
        private async Task<bool> RetrieveMissingAssets(IEnumerable<string> ids, CancellationToken token)
        {
            if (!ids.Any()) return true;
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
                log.LogError(ex, "Failed to retrieve missing assets: {Message}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }
            return true;
        }

        /// <summary>
        /// Retrieve a list of timeseries by externalId.
        /// </summary>
        /// <param name="ids">Timeseries externalIds to retrieve</param>
        /// <returns>True on success</returns>
        private async Task<bool> RetrieveMissingTimeSeries(IEnumerable<string> ids, CancellationToken token)
        {
            if (!ids.Any()) return true;
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
                log.LogError(ex, "Failed to retrieve missing timeseries: {Message}", ex.Message);
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
            var timeseries = JsonSerializer.Deserialize<IEnumerable<StatelessTimeSeriesCreate>>(Encoding.UTF8.GetString(msg.PayloadSegment));

            if (timeseries == null || !timeseries.Any()) return true;

            var assetExternalIds = timeseries.Select(ts => ts.AssetExternalId!).Where(id => id != null).ToHashSet();

            var missingAssetIds = assetExternalIds.Except(assetIds.Keys);

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
                else
                {
                    var id = assetIds.GetValueOrDefault(ts.AssetExternalId);
                    if (id != null)
                    {
                        ts.AssetId = id;
                    }
                    testSeries.Add(ts);
                }
            }

            var createdIds = new ConcurrentBag<string>();

            CogniteResult<TimeSeries, TimeSeriesCreate> found;

            try
            {
                found = await destination.GetOrCreateTimeSeriesAsync(testSeries.Select(ts => ts.ExternalId), ids =>
                {
                    var idsSet = new HashSet<string>(ids);
                    foreach (var id in ids) createdIds.Add(id);
                    return testSeries.Where(ts => idsSet.Contains(ts.ExternalId));
                }, RetryMode.OnError, SanitationMode.Clean, token);

                if (found.Errors != null && found.Errors.Any(err => err.Type == ErrorType.FatalFailure)) return false;

                foreach (var ts in found.Results!)
                {
                    tsIsString[ts.ExternalId] = ts.IsString;
                }
            }
            catch (ResponseException ex)
            {
                log.LogError(ex, "Failed to create missing time series: {Message}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }

            if (config.Update)
            {
                var createdIdsSet = new HashSet<string>(createdIds);

                var newTsMap = timeseries.ToDictionary(ts => ts.ExternalId);
                var toUpdate = found.Results
                    .Where(asset => !createdIdsSet.Contains(asset.ExternalId))
                    .Select(old => GetTimeSeriesUpdate(newTsMap.GetValueOrDefault(old.ExternalId), old))
                    .Where(update => update != null)
                    .ToList();

                if (toUpdate.Count == 0) return true;

                try
                {
                    await destination.CogniteClient.TimeSeries.UpdateAsync(toUpdate, token);
                }
                catch (ResponseException ex)
                {
                    log.LogError(ex, "Failed to update timeseries: {Message}", ex.Message);
                    return ex.Code == 400 || ex.Code == 409;
                }
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
            if (msg == null)
            {
                log.LogWarning("Null payload in datapoints");
                return true;
            }
            var datapoints = DataPointInsertionRequest.Parser.ParseFrom(msg.PayloadSegment);

            var ids = datapoints.Items.Select(pts => pts.ExternalId).ToList();

            var missingTsIds = ids
                .Where(id => !tsIsString.ContainsKey(id))
                .ToList();

            if (missingTsIds.Count != 0)
            {
                if (!await RetrieveMissingTimeSeries(missingTsIds, token))
                {
                    log.LogDebug("Failed to retrieve {Count} missing ids from CDF", missingTsIds.Count);
                    return false;
                }
            }

            var req = new DataPointInsertionRequest();
            req.Items.AddRange(datapoints.Items.Where(
                pts => tsIsString.ContainsKey(pts.ExternalId)
                && !(pts.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.NumericDatapoints && tsIsString[pts.ExternalId])
                && !(pts.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.StringDatapoints && !tsIsString[pts.ExternalId])));

            log.LogTrace("Push datapoints for {Count} out of {DpCount} timeseries", req.Items.Count, datapoints.Items.Count);

            var missingIds = new HashSet<string>();

            try
            {
                await destination.CogniteClient.DataPoints.CreateAsync(req, token);
                log.LogDebug("Push datapoints for {Count} timeseries to CDF", req.Items.Count);
            }
            catch (ResponseException ex)
            {
                log.LogWarning("Failed to push datapoints to CDF: {Message}", ex.Message);
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
            ArgumentNullException.ThrowIfNull(msg);
            var events = JsonSerializer.Deserialize<IEnumerable<StatelessEventCreate>>(Encoding.UTF8.GetString(msg.PayloadSegment));

            if (events == null || !events.Any()) return true;

            var assetExternalIds = events.SelectMany(evt => evt?.AssetExternalIds ?? Enumerable.Empty<string>()).Where(id => id != null);
            var missingAssetIds = assetExternalIds.Except(assetIds.Keys);

            if (missingAssetIds.Any())
            {
                if (!await RetrieveMissingAssets(missingAssetIds, token))
                {
                    return false;
                }
            }

            foreach (var evt in events)
            {
                if (evt.AssetExternalIds == null) continue;
                evt.AssetIds = evt.AssetExternalIds.Where(id => id != null && assetIds.ContainsKey(id) && assetIds[id] != null)
                    .Select(id => assetIds[id] ?? 0);
            }

            if (!events.Any()) return true;

            try
            {
                var result = await destination.EnsureEventsExistsAsync(events, RetryMode.OnError, SanitationMode.Clean, token);

                if (result.Errors != null && result.Errors.Any(err => err.Type == ErrorType.FatalFailure)) return false;
            }
            catch (ResponseException ex)
            {
                log.LogError("Failed to push events to CDF: {Message}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }

            return true;
        }

        /// <summary>
        /// Try to push relationships to CDF, then remove duplicates. 
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<bool> PushRelationships(MqttApplicationMessage msg, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(msg);
            var relationships = JsonSerializer.Deserialize<IEnumerable<RelationshipCreate>>(Encoding.UTF8.GetString(msg.PayloadSegment));

            if (relationships == null || !relationships.Any()) return true;

            var tasks = relationships.ChunkBy(1000).Select(chunk => PushRelationshipsChunk(chunk, token));
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (ResponseException ex)
            {
                log.LogError("Failed to push relationships to CDF: {Message}", ex.Message);

                return ex.Code == 400 || ex.Code == 409;
            }
            catch (AggregateException aex)
            {
                return aex.InnerException is ResponseException ex && (ex.Code == 400 || ex.Code == 409);
            }
            return true;
        }

        /// <summary>
        /// Push chunk of relationships to CDF, retries on duplicates.
        /// </summary>
        /// <param name="relationships">Relationships to create</param>
        private async Task PushRelationshipsChunk(IEnumerable<RelationshipCreate> relationships, CancellationToken token)
        {
            try
            {
                await destination.CogniteClient.Relationships.CreateAsync(relationships, token);
            }
            catch (ResponseException ex)
            {
                if (ex.Duplicated.Any())
                {
                    var existing = new HashSet<string>();
                    foreach (var dict in ex.Duplicated)
                    {
                        if (dict.TryGetValue("externalId", out var value))
                        {
                            existing.Add((value as MultiValue.String)!.Value);
                        }
                    }
                    if (existing.Count == 0) throw;

                    relationships = relationships.Where(rel => !existing.Contains(rel.ExternalId)).ToList();
                    await PushRelationshipsChunk(relationships, token);
                }
                else
                {
                    throw;
                }
            }
        }

        private static readonly JsonSerializerOptions rawJsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        /// <summary>
        /// Push contents of message to raw, the message should contain both the Raw database and table.
        /// If update is enabled, existing rows are updated.
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<bool> PushRaw(MqttApplicationMessage msg, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(msg);
            var rows = JsonSerializer.Deserialize<RawRequestWrapper>(Encoding.UTF8.GetString(msg.PayloadSegment), rawJsonOptions);

            if (rows?.Rows == null || !rows.Rows.Any() || rows.Database == null || rows.Table == null) return true;

            try
            {
                if (config.Update)
                {
                    await UpsertRawRows(rows.Database, rows.Table,
                        rows.Rows.DistinctBy(row => row.Key).ToDictionary(row => row.Key, row => row.Columns), token);
                }
                else
                {
                    await destination.InsertRawRowsAsync(rows.Database, rows.Table,
                        rows.Rows.DistinctBy(row => row.Key).ToDictionary(row => row.Key, row => row.Columns), token);
                }
            }
            catch (ResponseException ex)
            {
                log.LogError("Failed to push raw rows to CDF: {Message}", ex.Message);
                return ex.Code == 400 || ex.Code == 409;
            }

            return true;
        }

        /// <summary>
        /// Method to create or update a collection of raw rows.
        /// </summary>
        /// <param name="dbName">Raw database</param>
        /// <param name="tableName">Raw table</param>
        /// <param name="toUpsert">Rows to update, by key</param>
        private async Task UpsertRawRows(
            string dbName,
            string tableName,
            Dictionary<string, JsonElement> toUpsert,
            CancellationToken token)
        {
            if (toUpsert.Count == 0) return;
            string? cursor = null;
            var existing = new List<RawRow<Dictionary<string, JsonElement>>>();
            do
            {
                try
                {
                    var result = await destination.CogniteClient.Raw.ListRowsAsync<Dictionary<string, JsonElement>>(dbName, tableName,
                        new RawRowQuery { Cursor = cursor, Limit = 10_000 }, null, token);
                    foreach (var item in result.Items)
                    {
                        existing.Add(item);
                    }
                    cursor = result.NextCursor;
                }
                catch (ResponseException ex) when (ex.Code == 404)
                {
                    log.LogWarning("Table or database not found: {Message}", ex.Message);
                    break;
                }
            } while (cursor != null);

            foreach (var row in existing)
            {
                if (toUpsert.TryGetValue(row.Key, out var newRow))
                {
                    var oldRow = JsonDocument.Parse(JsonSerializer.Serialize(row.Columns)).RootElement;
                    toUpsert[row.Key] = JsonDocument.Parse(Merge(oldRow, newRow)).RootElement;

                }
            }
            log.LogInformation("Creating or updating {Count} raw rows in CDF", toUpsert.Count);

            await destination.InsertRawRowsAsync(dbName, tableName, toUpsert, token);
        }
        /// <summary>
        /// Adapted from https://stackoverflow.com/a/59574030/9946909. 
        /// Merge two JsonElements, producing a json encoded string with the merged contents.
        /// </summary>
        private static string Merge(JsonElement r1, JsonElement r2)
        {
            var outputBuffer = new ArrayBufferWriter<byte>();

            using (var jsonWriter = new Utf8JsonWriter(outputBuffer, new JsonWriterOptions { Indented = true }))
            {
                if (r1.ValueKind != JsonValueKind.Array && r1.ValueKind != JsonValueKind.Object || r1.ValueKind != r2.ValueKind)
                {
                    r2.WriteTo(jsonWriter);
                }
                else if (r1.ValueKind == JsonValueKind.Array)
                {
                    MergeArrays(jsonWriter, r1, r2);
                }
                else
                {
                    MergeObjects(jsonWriter, r1, r2);
                }
            }

            return Encoding.UTF8.GetString(outputBuffer.WrittenSpan);
        }

        /// <summary>
        /// Merge two json objects, write the result to <paramref name="jsonWriter"/>.
        /// </summary>
        /// <param name="jsonWriter">Output writer</param>
        /// <param name="root1">First object</param>
        /// <param name="root2">Second object</param>
        private static void MergeObjects(Utf8JsonWriter jsonWriter, JsonElement root1, JsonElement root2)
        {
            jsonWriter.WriteStartObject();
            foreach (JsonProperty property in root1.EnumerateObject())
            {
                string propertyName = property.Name;

                JsonValueKind newValueKind;

                if (root2.TryGetProperty(propertyName, out JsonElement newValue) && (newValueKind = newValue.ValueKind) != JsonValueKind.Null)
                {
                    jsonWriter.WritePropertyName(propertyName);

                    JsonElement originalValue = property.Value;
                    JsonValueKind originalValueKind = originalValue.ValueKind;

                    if (newValueKind == JsonValueKind.Object && originalValueKind == JsonValueKind.Object)
                    {
                        MergeObjects(jsonWriter, originalValue, newValue);
                    }
                    else if (newValueKind == JsonValueKind.Array && originalValueKind == JsonValueKind.Array)
                    {
                        MergeArrays(jsonWriter, originalValue, newValue);
                    }
                    else
                    {
                        newValue.WriteTo(jsonWriter);
                    }
                }
                else
                {
                    property.WriteTo(jsonWriter);
                }
            }

            // Write all the properties of the second document that are unique to it.
            foreach (JsonProperty property in root2.EnumerateObject())
            {
                if (!root1.TryGetProperty(property.Name, out _))
                {
                    property.WriteTo(jsonWriter);
                }
            }

            jsonWriter.WriteEndObject();
        }

        /// <summary>
        /// Merge two JSON arrays, write the result to <paramref name="jsonWriter"/>.
        /// </summary>
        /// <param name="jsonWriter">Output writer</param>
        /// <param name="root1">First array</param>
        /// <param name="root2">Second array</param>
        private static void MergeArrays(Utf8JsonWriter jsonWriter, JsonElement root1, JsonElement root2)
        {
            jsonWriter.WriteStartArray();

            foreach (JsonElement element in root1.EnumerateArray())
            {
                element.WriteTo(jsonWriter);
            }
            foreach (JsonElement element in root2.EnumerateArray())
            {
                element.WriteTo(jsonWriter);
            }

            jsonWriter.WriteEndArray();
        }

        [SuppressMessage("Microsoft.Performance", "CA1812")]
        internal sealed class StatelessEventCreate : EventCreate
        {
            public IEnumerable<string>? AssetExternalIds { get; set; }
        }
        [SuppressMessage("Microsoft.Performance", "CA1812")]
        internal sealed class StatelessTimeSeriesCreate : TimeSeriesCreate
        {
            public string? AssetExternalId { get; set; }
            public new JsonDocument? Metadata { get; set; }
        }
        [SuppressMessage("Microsoft.Performance", "CA1812")]
        internal sealed class RawRequestWrapper
        {
            public string? Database { get; set; }
            public string? Table { get; set; }
            public IEnumerable<RawRow<JsonElement>>? Rows { get; set; }
        }
    }
}

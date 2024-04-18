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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class RawWriter
    {
        private readonly ILogger<RawWriter> log;
        private readonly FullConfig config;
        private readonly CogniteDestination destination;

        private readonly RawMetadataTargetConfig rawConfig;

        public RawWriter(ILogger<RawWriter> log, CogniteDestination destination, FullConfig config)
        {
            this.log = log;
            this.config = config;
            this.destination = destination;
            rawConfig = config.Cognite?.MetadataTargets?.Raw ?? throw new ArgumentException("Attempted to create RawWriter without valid raw config");
        }

        public async Task<bool> PushAssets(UAExtractor extractor, IReadOnlyDictionary<string, BaseUANode> assets, TypeUpdateConfig update, BrowseReport report, CancellationToken token)
        {
            if (rawConfig.AssetsTable == null || rawConfig.Database == null) return true;

            try
            {
                var result = await PushRows(extractor, rawConfig.Database, rawConfig.AssetsTable,
                                assets, ConverterType.Node, update.AnyUpdate, token);

                report.RawAssetsCreated += result.Created;
                report.RawAssetsUpdated += result.Updated;
                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push assets to CDF Raw: {Message}", ex.Message);
                return false;
            }
        }

        public async Task<bool> PushTimeseries(UAExtractor extractor, IReadOnlyDictionary<string, UAVariable> timeseries,
            TypeUpdateConfig update, BrowseReport report, CancellationToken token)
        {
            if (rawConfig.TimeseriesTable == null || rawConfig.Database == null) return true;

            try
            {
                var result = await PushRows(extractor, rawConfig.Database, rawConfig.TimeseriesTable,
                    timeseries, ConverterType.Variable, update.AnyUpdate, token);

                report.RawTimeseriesCreated += result.Created;
                report.RawTimeseriesUpdated += result.Updated;
                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push timeseries to CDF Raw: {Message}", ex.Message);
                return false;
            }
        }


        public async Task<bool> PushReferences(IEnumerable<RelationshipCreate> relationships, BrowseReport report, CancellationToken token)
        {
            if (rawConfig.RelationshipsTable == null || rawConfig.Database == null) return true;

            try
            {
                var result = new Result { Created = 0, Updated = 0 };
                await EnsureRows(
                    rawConfig.Database,
                    rawConfig.RelationshipsTable,
                    relationships.Select(rel => rel.ExternalId),
                    ids =>
                    {
                        var idSet = ids.ToHashSet();
                        var creates = relationships
                            .Where(rel => idSet.Contains(rel.ExternalId))
                            .ToDictionary(rel => rel.ExternalId);
                        result.Created += creates.Count;
                        return creates;
                    },
                    new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase },
                    token
                );
                report.RawRelationshipsCreated += result.Created;
                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push timeseries to CDF Raw: {Message}", ex.Message);
                return false;
            }
        }


        public async Task MarkDeleted(DeletedNodes deletes, CancellationToken token)
        {
            if (rawConfig.Database == null) return;

            var tasks = new List<Task>();
            if (rawConfig.AssetsTable != null)
            {
                tasks.Add(MarkRawRowsAsDeleted(rawConfig.Database, rawConfig.AssetsTable, deletes.Objects.Select(d => d.Id), token));
            }
            if (rawConfig.TimeseriesTable != null)
            {
                tasks.Add(MarkRawRowsAsDeleted(rawConfig.Database, rawConfig.TimeseriesTable, deletes.Variables.Select(d => d.Id), token));
            }
            if (rawConfig.RelationshipsTable != null)
            {
                tasks.Add(MarkRawRowsAsDeleted(rawConfig.Database, rawConfig.RelationshipsTable, deletes.References.Select(d => d.Id), token));
            }
            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Synchronizes all BaseUANode to CDF raw
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="database">Name of metadata database in CDF</param>
        /// <param name="table">Name of metadata table in CDF</param>
        /// <param name="rows">Dictionary map of BaseUANode of their keys</param>
        /// <param name="converter">Converter</param>
        /// <param name="shouldUpdate">Indicates if it is an update operation</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Operation result</returns>
        private async Task<Result> PushRows<T>(UAExtractor extractor, string database, string table,
                IReadOnlyDictionary<string, T> rows, ConverterType converter, bool shouldUpdate, CancellationToken token) where T : BaseUANode
        {
            var result = new Result { Created = 0, Updated = 0 };

            if (shouldUpdate)
            {
                await UpdateRows(extractor, database, table, rows, converter, result, token);
            }
            else
            {
                await CreateRows(extractor, database, table, rows, converter, result, token);
            }
            return result;
        }

        /// <summary>
        /// Updates all BaseUANode to CDF raw
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="database">Name of metadata database in CDF</param>
        /// <param name="table">Name of metadata table in CDF</param>
        /// <param name="rowsToUpsert">Dictionary map of BaseUANode of their keys</param>
        /// <param name="converter">Converter</param>
        /// <param name="result">Operation result</param>
        /// <param name="shouldUpdate">Indicates if it is an update operation</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task UpdateRows<T>(UAExtractor extractor, string database, string table,
                IReadOnlyDictionary<string, T> rowsToUpsert, ConverterType converter, Result result, CancellationToken token) where T : BaseUANode
        {
            await UpsertRows(
                database,
                table,
                rows =>
                {
                    if (rows == null)
                    {
                        return rowsToUpsert
                            .Select(
                                kvp =>
                                    (kvp.Key, update: PusherUtils.CreateRawUpdate(log, extractor.StringConverter, kvp.Value, null, converter))
                            )
                            .Where(elem => elem.update != null)
                            .ToDictionary(pair => pair.Key, pair => pair.update!.Value);
                    }

                    var toWrite =
                        new List<(string key, RawRow<Dictionary<string, JsonElement>> row, T node)>();

                    foreach (var row in rows)
                    {
                        if (rowsToUpsert.TryGetValue(row.Key, out var node))
                        {
                            toWrite.Add((row.Key, row, node));
                        }
                    }

                    var updates = new Dictionary<string, JsonElement>();

                    foreach (var (key, row, node) in toWrite)
                    {
                        var update = PusherUtils.CreateRawUpdate(log, extractor.StringConverter, node, row, converter);

                        if (update != null)
                        {
                            updates[key] = update.Value;
                            if (row == null)
                            {
                                result.Created++;
                            }
                            else
                            {
                                result.Updated++;
                            }
                        }
                    }

                    return updates;
                },
                null,
                token
            );
        }

        /// <summary>
        /// Creates all BaseUANode to CDF raw
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="database">Name of metadata database in CDF</param>
        /// <param name="table">Name of metadata table in CDF</param>
        /// <param name="dataSet">Dictionary map of BaseUANode of their keys</param>
        /// <param name="converter">Converter</param>
        /// <param name="result">Operation result</param>
        /// <param name="shouldUpdate">Indicates if it is an update operation</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task CreateRows<T>(UAExtractor extractor, string database, string table,
                IReadOnlyDictionary<string, T> dataMap, ConverterType converter, Result result, CancellationToken token) where T : BaseUANode
        {
            await EnsureRows(
                database,
                table,
                dataMap.Keys,
                ids =>
                {
                    var rows = ids.Select(id => (dataMap[id], id));
                    var creates = rows
                        .Select(pair => (pair.Item1.ToJson(log, extractor.StringConverter, converter), pair.id))
                        .Where(pair => pair.Item1 != null)
                        .ToDictionary(pair => pair.id, pair => pair.Item1!.RootElement);
                    result.Created += creates.Count;
                    return creates;
                },
                new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase },
                token
            );
        }

        /// <summary>
        /// Upserts all BaseUANode to CDF raw
        /// </summary>
        /// <param name="dbName">Name of metadata database in CDF</param>
        /// <param name="tableName">Name of metadata table in CDF</param>
        /// <param name="dtoBuilder">Callback to build the dto</param>
        /// <param name="options">Json serialization options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task UpsertRows<T>(
            string dbName,
            string tableName,
            Func<
                IEnumerable<RawRow<Dictionary<string, JsonElement>>>?,
                IDictionary<string, T>
            > dtoBuilder,
            JsonSerializerOptions? options,
            CancellationToken token
        )
        {
            int count = 0;
            async Task CallAndCreate(IEnumerable<RawRow<Dictionary<string, JsonElement>>>? rows)
            {
                var toUpsert = dtoBuilder(rows);
                count += toUpsert.Count;
                await destination.InsertRawRowsAsync(dbName, tableName, toUpsert, options, token);
            }

            string? cursor = null;
            do
            {
                try
                {
                    var result = await destination.CogniteClient.Raw.ListRowsAsync<
                        Dictionary<string, JsonElement>
                    >(
                        dbName,
                        tableName,
                        new RawRowQuery { Cursor = cursor, Limit = 10_000 },
                        null,
                        token
                    );
                    cursor = result.NextCursor;

                    await CallAndCreate(result.Items);
                }
                catch (ResponseException ex) when (ex.Code == 404)
                {
                    log.LogWarning("Table or database not found: {Message}", ex.Message);
                    break;
                }
            } while (cursor != null);

            await CallAndCreate(null);

            log.LogInformation("Updated or created {Count} rows in CDF Raw", count);
        }

        /// <summary>
        /// Ensure all rows in CDF
        /// </summary>
        /// <param name="dbName">Name of metadata database in CDF</param>
        /// <param name="tableName">Name of metadata table in CDF</param>
        /// <param name="keys">keys</param>
        /// <param name="dtoBuilder">Callback to build the dto</param>
        /// <param name="options">Json serialization options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task EnsureRows<T>(string dbName, string tableName, IEnumerable<string> keys,
                Func<IEnumerable<string>, IDictionary<string, T>> dtoBuilder, JsonSerializerOptions options, CancellationToken token)
        {
            var rows = await WriterUtils.GetRawRows(dbName, tableName, destination, new[] { "," }, log, token);
            var existing = rows.Select(row => row.Key);

            var toCreate = keys.Except(existing);
            if (!toCreate.Any())
                return;
            log.LogInformation("Creating {Count} raw rows in CDF", toCreate.Count());

            var createDtos = dtoBuilder(toCreate);

            await destination.InsertRawRowsAsync(dbName, tableName, createDtos, options, token);
        }

        private async Task MarkRawRowsAsDeleted(
            string dbName,
            string tableName,
            IEnumerable<string> keys,
            CancellationToken token
        )
        {
            if (!keys.Any()) return;
            var keySet = new HashSet<string>(keys);
            var rows = await WriterUtils.GetRawRows(dbName, tableName, destination, null, log, token);
            var trueElem = JsonDocument.Parse("true").RootElement;
            var toMark = rows.Where(r => keySet.Contains(r.Key)).ToList();
            foreach (var row in toMark)
            {
                row.Columns[config.Extraction.Deletes.DeleteMarker] = trueElem;
            }
            await destination.InsertRawRowsAsync(
                dbName,
                tableName,
                toMark.ToDictionary(e => e.Key, e => e.Columns),
                token
            );
        }
    }
}

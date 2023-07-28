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
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class RawWriter : IRawWriter
    {
        private readonly ILogger<RawWriter> log;
        private FullConfig config { get; }
        private CogniteDestination destination { get; }

        public RawWriter(ILogger<RawWriter> log, CogniteDestination destination, FullConfig config)
        {
            this.log = log;
            this.config = config;
            this.destination = destination;
        }

        /// <summary>
        /// Get all rows from CDF
        /// </summary>
        /// <param name="dbName">Name of metadata database in CDF</param>
        /// <param name="tableName"> Name of metadata table in CDF</param>
        /// <param name="columns">Columns</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A dictionary of JsonElement</returns>
        public async Task<IEnumerable<RawRow<Dictionary<string, JsonElement>>>> GetRows(
            string dbName,
            string tableName,
            IEnumerable<string>? columns,
            CancellationToken token
        )
        {
            string? cursor = null;
            var rows = new List<RawRow<Dictionary<string, JsonElement>>>();
            do
            {
                try
                {
                    var result = await destination.CogniteClient.Raw.ListRowsAsync<
                        Dictionary<string, JsonElement>
                    >(
                        dbName,
                        tableName,
                        new RawRowQuery { Cursor = cursor, Limit = 10_000, Columns = columns },
                        null,
                        token
                    );
                    rows.AddRange(result.Items);
                    cursor = result.NextCursor;
                }
                catch (ResponseException ex) when (ex.Code == 404)
                {
                    log.LogWarning("Table or database not found: {Message}", ex.Message);
                    break;
                }
            } while (cursor != null);
            return rows;
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
        public async Task<Result> PushNodes<T>(UAExtractor extractor, string database, string table,
                IDictionary<string, T> rows, ConverterType converter, bool shouldUpdate, CancellationToken token) where T : BaseUANode
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
        /// <param name="dataSet">Dictionary map of BaseUANode of their keys</param>
        /// <param name="converter">Converter</param>
        /// <param name="result">Operation result</param>
        /// <param name="shouldUpdate">Indicates if it is an update operation</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task UpdateRows<T>(UAExtractor extractor, string database, string table,
                IDictionary<string, T> dataSet, ConverterType converter, Result result, CancellationToken token) where T : BaseUANode
        {
            await UpsertRows<JsonElement>(
                database,
                table,
                rows =>
                {
                    if (rows == null)
                    {
                        return dataSet
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
                        if (dataSet.TryGetValue(row.Key, out var node))
                        {
                            toWrite.Add((row.Key, row, node));
                            dataSet.Remove(row.Key);
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
                IDictionary<string, T> dataMap, ConverterType converter,  Result result, CancellationToken token) where T : BaseUANode
        {
            await EnsureRows<JsonElement>(
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
            var rows = await GetRows(dbName, tableName, new[] { "," }, token);
            var existing = rows.Select(row => row.Key);

            var toCreate = keys.Except(existing);
            if (!toCreate.Any())
                return;
            log.LogInformation("Creating {Count} raw rows in CDF", toCreate.Count());

            var createDtos = dtoBuilder(toCreate);

            await destination.InsertRawRowsAsync(dbName, tableName, createDtos, options, token);
        }

        public async Task<Result> PushReferences(string database, string table, IEnumerable<RelationshipCreate> relationships, CancellationToken token)
        {
            var result = new Result { Created = 0, Updated = 0 };
            await EnsureRows(
                database,
                table,
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
            return result;
        }
    }
}

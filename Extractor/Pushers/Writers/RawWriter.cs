using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class RawWriter : IRawWriter
    {
        private readonly ILogger<RawWriter> log;

        private CancellationToken token { get; }

        private FullConfig config { get; }
        private CogniteDestination destination { get; }

        public RawWriter(
            ILogger<RawWriter> log,
            CancellationToken token,
            CogniteDestination destination,
            FullConfig config
        )
        {
            this.log = log;
            this.token = token;
            this.config = config;
            this.destination = destination;
        }

        public async Task<IEnumerable<RawRow<Dictionary<string, JsonElement>>>> GetRawRows(
            string dbName,
            string tableName,
            IEnumerable<string>? columns
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
                        new RawRowQuery
                        {
                            Cursor = cursor,
                            Limit = 10_000,
                            Columns = columns
                        },
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

        public async Task PushNodes<T>(
            UAExtractor extractor,
            string database,
            string table,
            ConcurrentDictionary<string, T> rows,
            bool shouldUpdate,
            BrowseReport report
        )
            where T : BaseUANode
        {
            if (shouldUpdate)
            {
                await UpdateRawAssets(extractor, database, table, rows, report);
            }
            else
            {
                await CreateRawAssets(extractor, database, table, rows, report);
            }
        }

        private async Task UpdateRawAssets<T>(
            UAExtractor extractor,
            string database,
            string table,
            IDictionary<string, T> dataSet,
            BrowseReport report
        )
            where T : BaseUANode
        {
            if (database == null || table == null)
                return;
            await UpsertRawRows<JsonElement>(
                database,
                table,
                rows =>
                {
                    if (rows == null)
                    {
                        return dataSet
                            .Select(
                                kvp =>
                                    (
                                        kvp.Key,
                                        update: PusherUtils.CreateRawUpdate(
                                            log,
                                            extractor.StringConverter,
                                            kvp.Value,
                                            null,
                                            ConverterType.Node
                                        )
                                    )
                            )
                            .Where(elem => elem.update != null)
                            .ToDictionary(pair => pair.Key, pair => pair.update!.Value);
                    }

                    var toWrite =
                        new List<(
                            string key,
                            RawRow<Dictionary<string, JsonElement>> row,
                            T node
                        )>();

                    foreach (var row in rows)
                    {
                        if (dataSet.TryGetValue(row.Key, out var ts))
                        {
                            toWrite.Add((row.Key, row, ts));
                            dataSet.Remove(row.Key);
                        }
                    }

                    var updates = new Dictionary<string, JsonElement>();

                    foreach (var (key, row, node) in toWrite)
                    {
                        var update = PusherUtils.CreateRawUpdate(
                            log,
                            extractor.StringConverter,
                            node,
                            row,
                            ConverterType.Node
                        );

                        if (update != null)
                        {
                            updates[key] = update.Value;
                            if (row == null)
                            {
                                report.AssetsCreated++;
                            }
                            else
                            {
                                report.AssetsUpdated++;
                            }
                        }
                    }

                    return updates;
                },
                null,
                token
            );
        }

        private async Task CreateRawAssets<T>(
            UAExtractor extractor,
            string database,
            string table,
            IDictionary<string, T> assetMap,
            BrowseReport report
        )
            where T : BaseUANode
        {
            if (database == null || table == null)
                return;

            await EnsureRawRows<JsonElement>(
                database,
                table,
                assetMap.Keys,
                ids =>
                {
                    var assets = ids.Select(id => (assetMap[id], id));
                    var creates = assets
                        .Select(
                            pair =>
                                (
                                    pair.Item1.ToJson(
                                        log,
                                        extractor.StringConverter,
                                        ConverterType.Node
                                    ),
                                    pair.id
                                )
                        )
                        .Where(pair => pair.Item1 != null)
                        .ToDictionary(pair => pair.id, pair => pair.Item1!.RootElement);
                    report.AssetsCreated += creates.Count;
                    return creates;
                },
                new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }
            );
        }

        private async Task UpsertRawRows<T>(
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

        private async Task EnsureRawRows<T>(
            string dbName,
            string tableName,
            IEnumerable<string> keys,
            Func<IEnumerable<string>, IDictionary<string, T>> dtoBuilder,
            JsonSerializerOptions options
        )
        {
            var rows = await GetRawRows(dbName, tableName, new[] { "," });
            var existing = rows.Select(row => row.Key);

            var toCreate = keys.Except(existing);
            if (!toCreate.Any())
                return;
            log.LogInformation("Creating {Count} raw rows in CDF", toCreate.Count());

            var createDtos = dtoBuilder(toCreate);

            await destination.InsertRawRowsAsync(dbName, tableName, createDtos, options, token);
        }

        public async Task PushReferences(
            string database,
            string table,
            IEnumerable<RelationshipCreate> relationships,
            BrowseReport report
        )
        {
            await EnsureRawRows(
                database,
                table,
                relationships.Select(rel => rel.ExternalId),
                ids =>
                {
                    var idSet = ids.ToHashSet();
                    var creates = relationships
                        .Where(rel => idSet.Contains(rel.ExternalId))
                        .ToDictionary(rel => rel.ExternalId);
                    report.RelationshipsCreated += creates.Count;
                    return creates;
                },
                new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }
            );
        }
    }
}

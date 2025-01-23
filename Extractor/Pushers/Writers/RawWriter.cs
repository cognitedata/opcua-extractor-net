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
using Cognite.Extractor.Common;
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

        public bool Assets => rawConfig.AssetsTable != null;
        public bool Timeseries => rawConfig.TimeseriesTable != null;
        public bool Relationships => rawConfig.RelationshipsTable != null;

        public RawWriter(ILogger<RawWriter> log, CogniteDestination destination, FullConfig config)
        {
            this.log = log;
            this.config = config;
            this.destination = destination;
            rawConfig = config.Cognite?.MetadataTargets?.Raw ?? throw new ArgumentException("Attempted to create RawWriter without valid raw config");
        }

        public async Task<bool> PushAssets(UAExtractor extractor, IDictionary<string, BaseUANode> assets, BrowseReport report, CancellationToken token)
        {
            if (rawConfig.AssetsTable == null || rawConfig.Database == null) return true;

            try
            {
                var result = await PushRows(extractor, rawConfig.Database, rawConfig.AssetsTable,
                                assets, ConverterType.Node, token);

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

        public async Task<bool> PushTimeseries(UAExtractor extractor, IDictionary<string, UAVariable> timeseries,
            BrowseReport report, CancellationToken token)
        {
            if (rawConfig.TimeseriesTable == null || rawConfig.Database == null) return true;

            try
            {
                var result = await PushRows(extractor, rawConfig.Database, rawConfig.TimeseriesTable,
                    timeseries, ConverterType.Variable, token);

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


        public async Task<bool> PushReferences(
            IUAClientAccess client,
            IEnumerable<UAReference> references,
            BrowseReport report,
            CancellationToken token)
        {
            if (rawConfig.RelationshipsTable == null || rawConfig.Database == null) return true;

            var relationships = references
                    .Select(rf => rf.ToRelationship(config.Cognite?.DataSet?.Id, client))
                    .DistinctBy(rel => rel.ExternalId);

            try
            {
                var json = relationships.ToDictionary(rel => rel.ExternalId);
                await destination.InsertRawRowsAsync(rawConfig.Database, rawConfig.RelationshipsTable, json, token);
                report.RawRelationshipsCreated += json.Count;
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
        /// <param name="token">Cancellation token</param>
        /// <returns>Operation result</returns>
        private async Task<Result> PushRows<T>(UAExtractor extractor, string database, string table,
                IDictionary<string, T> rows, ConverterType converter, CancellationToken token) where T : BaseUANode
        {
            var result = new Result { Created = 0, Updated = 0 };

            await UpsertRows(extractor, database, table, rows, converter, result, token);
            return result;
        }

        /// <summary>
        /// Creates or updates the given BaseUANodes in CDF Raw.
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="database">Name of metadata database in CDF</param>
        /// <param name="table">Name of metadata table in CDF</param>
        /// <param name="dataSet">Dictionary map of BaseUANode of their keys</param>
        /// <param name="converter">Converter</param>
        /// <param name="result">Operation result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task UpsertRows<T>(UAExtractor extractor, string database, string table,
                IDictionary<string, T> dataMap, ConverterType converter, Result result, CancellationToken token) where T : BaseUANode
        {
            var json = dataMap
                .Where(pair => pair.Value.Source != NodeSources.NodeSource.CDF)
                .Select(pair => (pair.Key, pair.Value.ToJson(log, extractor.StringConverter, converter)))
                .Where(pair => pair.Item2 != null)
                .ToDictionary(pair => pair.Key, pair => pair.Item2!.RootElement);

            await destination.InsertRawRowsAsync(database, table, json, token);
            result.Created += dataMap.Count;
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

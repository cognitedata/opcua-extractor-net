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

using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers.FDM;
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Prometheus;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TimeRange = Cognite.Extractor.Common.TimeRange;

namespace Cognite.OpcUa.Pushers
{
    /// <summary>
    /// Pusher against CDF
    /// </summary>
    public sealed class CDFPusher : IPusher
    {
        private readonly CognitePusherConfig config;
        private readonly FullConfig fullConfig;
        private readonly ICDFWriter cdfWriter;
        private readonly IDictionary<NodeId, long> nodeToAssetIds = new Dictionary<NodeId, long>();

        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }

        public PusherInput? PendingNodes { get; set; }

        private UAExtractor extractor;
        public UAExtractor Extractor { get => extractor; set {
            extractor = value;
            if (fdmDestination != null)
            {
                fdmDestination.Extractor = value;
            }
        } }
        public IPusherConfig BaseConfig { get; }

        private readonly HashSet<string> mismatchedTimeseries = new HashSet<string>();
        private readonly HashSet<string> missingTimeseries = new HashSet<string>();
        private readonly CogniteDestination destination;

        private readonly BrowseCallback? callback;
        private readonly FDMWriter? fdmDestination;
        private bool pushCleanAssets =>
            string.IsNullOrWhiteSpace(config.RawMetadata?.Database)
            && string.IsNullOrWhiteSpace(config.RawMetadata?.AssetsTable);
        private bool pushCleanTimeseries =>
            string.IsNullOrWhiteSpace(config.RawMetadata?.Database)
            && string.IsNullOrWhiteSpace(config.RawMetadata?.TimeseriesTable);


        public CDFPusher(
            ILogger<CDFPusher> log,
            FullConfig fullConfig,
            CognitePusherConfig config,
            CogniteDestination destination,
            IServiceProvider provider)
        {
            extractor = null!;
            this.log = log;
            this.config = config;
            BaseConfig = config;
            this.destination = destination;
            this.fullConfig = fullConfig;
            cdfWriter =  provider.GetRequiredService<ICDFWriter>();
            if (config.BrowseCallback != null && (config.BrowseCallback.Id.HasValue || !string.IsNullOrEmpty(config.BrowseCallback.ExternalId)))
            {
                callback = new BrowseCallback(destination, config.BrowseCallback, log);
            }
            if (config.FlexibleDataModels != null && config.FlexibleDataModels.Enabled)
            {
                fdmDestination = new FDMWriter(provider.GetRequiredService<FullConfig>(), destination,
                    provider.GetRequiredService<ILogger<FDMWriter>>());
            }
        }

        private static readonly Counter dataPointsCounter = Metrics
            .CreateCounter("opcua_datapoints_pushed_cdf", "Number of datapoints pushed to CDF");
        private static readonly Counter dataPointPushes = Metrics
            .CreateCounter("opcua_datapoint_pushes_cdf", "Number of times datapoints have been pushed to CDF");
        private static readonly Counter dataPointPushFailures = Metrics
            .CreateCounter("opcua_datapoint_push_failures_cdf", "Number of completely failed pushes of datapoints to CDF");
        private static readonly Counter eventCounter = Metrics
            .CreateCounter("opcua_events_pushed_cdf", "Number of events pushed to CDF");
        private static readonly Counter eventPushCounter = Metrics
            .CreateCounter("opcua_event_pushes_cdf", "Number of times events have been pushed to CDF");
        private static readonly Counter eventPushFailures = Metrics
            .CreateCounter("opcua_event_push_failures_cdf", "Number of times events have been pushed to CDF");
        private static readonly Counter nodeEnsuringFailures = Metrics
            .CreateCounter("opcua_node_ensure_failures_cdf",
            "Number of completely failed requests to CDF when ensuring assets/timeseries exist");
        private static readonly Counter skippedEvents = Metrics
            .CreateCounter("opcua_skipped_events_cdf", "Number of events skipped by CDF pusher");
        private static readonly Gauge missingTimeseriesCnt = Metrics
            .CreateGauge("opcua_missing_timeseries", "Number of distinct timeseries that have been found to be missing in CDF");
        private static readonly Gauge mismatchedTimeseriesCnt = Metrics
            .CreateGauge("opcua_mismatched_timeseries", "Number of distinct timeseries that have been found to have different types in OPC-UA and in CDF");

        private readonly ILogger<CDFPusher> log;
        #region Interface


        /// <summary>
        /// Attempts to push the given list of datapoints to CDF.
        /// </summary>'
        /// <returns>True if push succeeded, false if it failed, null if there were no points to push.</returns>
        public async Task<bool?> PushDataPoints(IEnumerable<UADataPoint> points, CancellationToken token)
        {
            if (points == null) return null;
            Dictionary<string, List<UADataPoint>> dataPointList = points
                .Where(dp => dp.Timestamp > DateTime.UnixEpoch)
                .GroupBy(dp => dp.Id)
                .Where(group => !mismatchedTimeseries.Contains(group.Key)
                    && !missingTimeseries.Contains(group.Key))
                .ToDictionary(group => group.Key, group => group.ToList());

            int count = dataPointList.Aggregate(0, (seed, points) => seed + points.Value.Count);

            if (count == 0) return null;

            var inserts = dataPointList.ToDictionary(kvp =>
                Identity.Create(kvp.Key),
                kvp => kvp.Value.Select(
                    dp => dp.IsString ? new Datapoint(dp.Timestamp, dp.StringValue) : new Datapoint(dp.Timestamp, dp.DoubleValue.Value))
                );

            if (fullConfig.DryRun)
            {
                log.LogInformation("Dry run enabled. Would insert {Count} datapoints over {C2} timeseries to CDF", count, inserts.Count);
                return null;
            }

            try
            {
                var result = await destination.InsertDataPointsAsync(inserts, SanitationMode.Clean, RetryMode.OnError, token);
                int realCount = count;

                log.LogResult(result, RequestType.CreateDatapoints, false, LogLevel.Debug);

                if (result.Errors != null)
                {
                    var missing = result.Errors.FirstOrDefault(err => err.Type == ErrorType.ItemMissing);
                    if (missing?.Skipped != null)
                    {
                        log.LogError("Failed to push datapoints to CDF, missing ids: {Ids}", missing.Skipped.Select(ms => ms.Id));
                        foreach (var skipped in missing.Skipped)
                        {
                            missingTimeseries.Add(skipped.Id.ExternalId);
                        }
                        missingTimeseriesCnt.Set(missing.Skipped.Count());
                    }

                    var mismatched = result.Errors.FirstOrDefault(err => err.Type == ErrorType.MismatchedType);
                    if (mismatched?.Skipped != null)
                    {
                        log.LogError("Failed to push datapoints to CDF, mismatched timeseries: {Ids}", mismatched.Skipped.Select(ms => ms.Id));
                        foreach (var skipped in mismatched.Skipped)
                        {
                            mismatchedTimeseries.Add(skipped.Id.ExternalId);
                        }
                        mismatchedTimeseriesCnt.Set(mismatched.Skipped.Count());
                    }

                    foreach (var err in result.Errors)
                    {
                        if (err.Skipped != null)
                        {
                            foreach (var skipped in err.Skipped)
                            {
                                realCount -= skipped.DataPoints.Count();
                            }
                        }
                    }
                }

                

                result.ThrowOnFatal();
                log.LogDebug("Successfully pushed {Real} / {Total} points to CDF", realCount, count);

                dataPointPushes.Inc();
                dataPointsCounter.Inc(realCount);

                if (realCount == 0) return null;
            }
            catch (Exception e)
            {
                log.LogError("Failed to push {Count} points to CDF: {Message}", count, e.Message);
                dataPointPushFailures.Inc();
                // Return false indicating unexpected failure if we want to buffer.
                return false;
            }

            return true;
        }
        /// <summary>
        /// Attempts to push the given list of events to CDF.
        /// </summary>
        /// <returns>True if push succeeded, false if it failed, null if there were no events to push.</returns>
        public async Task<bool?> PushEvents(IEnumerable<UAEvent> events, CancellationToken token)
        {
            if (events == null) return null;
            var eventList = new List<UAEvent>();
            int count = 0;
            foreach (var buffEvent in events)
            {
                if (buffEvent.Time < PusherUtils.CogniteMinTime || buffEvent.Time > PusherUtils.CogniteMaxTime)
                {
                    skippedEvents.Inc();
                    continue;
                }
                eventList.Add(buffEvent);
                count++;
            }

            if (count == 0) return null;

            if (fullConfig.DryRun)
            {
                log.LogInformation("Dry run enabled. Would insert {Count} events to CDF", count);
                return null;
            }

            try
            {
                var result = await destination.EnsureEventsExistsAsync(eventList
                    .Select(evt => evt.ToCDFEvent(Extractor, config.DataSet?.Id, nodeToAssetIds))
                    .Where(evt => evt != null), RetryMode.OnError, SanitationMode.Clean, token);

                log.LogResult(result, RequestType.CreateEvents, false, LogLevel.Debug);

                int skipped = 0;
                if (result.Errors != null)
                {
                    skipped = result.Errors.Aggregate(0, (seed, err) =>
                        seed + (err.Skipped?.Count() ?? 0));

                    var fatalError = result.Errors.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
                    if (fatalError != null)
                    {
                        log.LogError("Failed to push {NumFailedEvents} events to CDF: {Message}", count, fatalError.Exception?.Message);
                        eventPushFailures.Inc();
                        return fatalError.Exception is ResponseException rex && (rex.Code == 400 || rex.Code == 409);
                    }
                }

                eventCounter.Inc(count - skipped);
                eventPushCounter.Inc();
                log.LogDebug("Successfully pushed {Count} events to CDF", count - skipped);
                skippedEvents.Inc(skipped);
            }
            catch (Exception exc)
            {
                log.LogError(exc, "Failed to push {NumFailedEvents} events to CDF: {Message}", count, exc.Message);
                eventPushFailures.Inc();
                return exc is ResponseException rex && (rex.Code == 400 || rex.Code == 409);
            }

            return true;
        }

        /// <summary>
        /// Attempts to ensure that the given list of objects and variables are correctly reflected in CDF.
        /// </summary>
        /// <param name="objects">List of objects to be synchronized</param>
        /// <param name="variables">List of variables to be synchronized</param>
        /// <param name="update">Configuration of what fields, if any, should be updated.</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        public async Task<PushResult> PushNodes(
            IEnumerable<BaseUANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            UpdateConfig update,
            CancellationToken token
        )
        {
            var result = new PushResult();
            var report = new BrowseReport
            {
                IdPrefix = fullConfig.Extraction.IdPrefix,
                RawDatabase = config.RawMetadata?.Database,
                AssetsTable = config.RawMetadata?.AssetsTable,
                TimeSeriesTable = config.RawMetadata?.TimeseriesTable,
                RelationshipsTable = config.RawMetadata?.RelationshipsTable
            };

            if (!variables.Any() && !objects.Any() && !references.Any())
            {
                if (callback != null && !fullConfig.DryRun)
                {
                    await callback.Call(report, token);
                }
                log.LogDebug("Testing 0 nodes against CDF");
                return result;
            }

            log.LogInformation(
                "Testing {TotalNodesToTest} nodes against CDF",
                variables.Count() + objects.Count()
            );

            if (fullConfig.DryRun)
            {
                if (fdmDestination != null)
                {
                    await fdmDestination.PushNodes(
                        objects,
                        variables,
                        references,
                        Extractor,
                        token
                    );
                }
                return result;
            }

            try
            {
                await EnsureConfigInit(token);
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to initialize config");
                result.Objects = false;
                result.References = false;
                result.Variables = false;
                nodeEnsuringFailures.Inc();
                return result;
            }

            var assetsMap = MapAssets(objects);
            var timeseriesMap = MapTimeseries(variables);
            bool isTimeseriesPushed = true;

            if (pushCleanAssets && assetsMap.Any())
            {
                await PushCleanAssets(assetsMap, update.Objects, report, result);
            }

            isTimeseriesPushed = await PushCleanTimeseries(
                timeseriesMap,
                update.Variables,
                report,
                result
            );

            var tasks = new List<Task>();

            if (isTimeseriesPushed && fdmDestination != null)
            {
                tasks.Add(PushFdm(objects, variables, references, result, token));
            }

            if (!pushCleanAssets && assetsMap.Any())
            {
                tasks.Add(PushRawAssets(assetsMap, update.Objects, report, result));
            }

            if (!pushCleanTimeseries && timeseriesMap.Any())
            {
                tasks.Add(PushRawTimeseries(timeseriesMap, update.Variables, report, result));
            }

            tasks.Add(PushReferences(references, report, result, token));

            await Task.WhenAll(tasks);

            log.LogInformation("Finish pushing nodes to CDF");

            if (result.Objects && result.References && result.Variables)
            {
                if (callback != null)
                {
                    await callback.Call(report, token);
                }
            }
            else
            {
                nodeEnsuringFailures.Inc();
            }

            return result;
        }

        private async Task PushFdm(
            IEnumerable<BaseUANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            PushResult result,
            CancellationToken token
        )
        {
            bool pushResult = true;
            try
            {
                pushResult = await fdmDestination!.PushNodes(objects, variables, references, Extractor, token);
            }
            catch
            {
                pushResult = false;
            }
            result.Variables = pushResult;
            result.Objects = pushResult;
            result.References = pushResult;
        }

        private ConcurrentDictionary<string, BaseUANode> MapAssets(IEnumerable<BaseUANode> objects)
        {
            return config.SkipMetadata ? new ConcurrentDictionary<string, BaseUANode>() : new ConcurrentDictionary<string, BaseUANode>(
                objects
                    .Where(node => node.Source != NodeSource.CDF)
                    .ToDictionary(obj => Extractor.GetUniqueId(obj.Id)!)
            );
        }

        private ConcurrentDictionary<string, UAVariable> MapTimeseries(
            IEnumerable<UAVariable> variables
        )
        {
            return new ConcurrentDictionary<string, UAVariable>(
                variables.ToDictionary(ts => ts.GetUniqueId(Extractor)!)
            );
        }

        private async Task<bool> PushCleanAssets(
            ConcurrentDictionary<string, BaseUANode> assetsMap,
            TypeUpdateConfig update,
            BrowseReport report,
            PushResult result
        )
        {
            try
            {
                await cdfWriter.assets.PushNodes(Extractor, assetsMap, nodeToAssetIds, update, report);
            }
            catch
            {
                result.Objects = false;
            }
            return result.Objects;
        }

        private async Task PushCleanAssets(
            ConcurrentDictionary<string, BaseUANode> assetsMap,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token
        )
        {
            var assets = await CreateAssets(assetsMap, report, token);

            if (update.AnyUpdate)
            {
                await UpdateAssets(assetsMap, assets, update, report, token);
            }
        }

        private async Task<bool> PushCleanTimeseries(
            ConcurrentDictionary<string, UAVariable> timeseriesMap,
            TypeUpdateConfig update,
            BrowseReport report,
            PushResult result
        )
        {
            try
            {
                var _result = await cdfWriter.timeseries.PushVariables(Extractor, timeseriesMap, nodeToAssetIds, mismatchedTimeseries, update);
                var skipMetadata = config.SkipMetadata;
                var createMinimal = !pushCleanTimeseries || skipMetadata; 
                if (createMinimal)
                {
                    report.MinimalTimeSeriesCreated += _result.Created;
                }
                else
                {
                    report.TimeSeriesCreated += _result.Created;
                }
            }
            catch
            {
                result.Variables = false;
            }

            return result.Variables;
        }

        private async Task PushCleanTimeseries(
            ConcurrentDictionary<string, UAVariable> timeseriesMap,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token
        )
        {
            var timeseries = await CreateTimeseries(
                timeseriesMap,
                report,
                !pushCleanTimeseries || config.SkipMetadata,
                token
            );

            var toPushMeta = timeseriesMap
                .Where(kvp => kvp.Value.Source != NodeSource.CDF)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            if (update.AnyUpdate && toPushMeta.Any() && pushCleanTimeseries)
            {
                await UpdateTimeseries(toPushMeta, timeseries, update, report, token);
            }
        }

        /// <summary>
        /// Reset the pusher, preparing it to be restarted
        /// </summary>
        public void Reset()
        {
            missingTimeseries.Clear();
            mismatchedTimeseries.Clear();
        }

        /// <summary>
        /// Initialize extracted datapoint ranges on the given list of states.
        /// </summary>
        /// <param name="states">List of states representing timeseries in CDF.
        /// These must all exist.</param>
        /// <param name="backfillEnabled">True if backfill is enabled and start points should be read.</param>
        /// <returns>True if nothing failed unexpectedly.</returns>
        public async Task<bool> InitExtractedRanges(
            IEnumerable<VariableExtractionState> states,
            bool backfillEnabled,
            CancellationToken token
        )
        {
            if (!states.Any() || !config.ReadExtractedRanges || fullConfig.DryRun)
                return true;
            var ids = new List<string>();
            foreach (var state in states)
            {
                if (state.IsArray)
                {
                    for (int i = 0; i < state.ArrayDimensions[0]; i++)
                    {
                        var id = Extractor.GetUniqueId(state.SourceId, i);
                        if (id == null)
                            break;
                        ids.Add(id);
                    }
                }
                else
                {
                    ids.Add(state.Id);
                }
            }
            log.LogInformation("Getting extracted ranges from CDF for {Count} states", ids.Count);

            Dictionary<string, TimeRange> ranges;
            try
            {
                var dict = await destination.GetExtractedRanges(
                    ids.Select(Identity.Create).ToList(),
                    token,
                    backfillEnabled
                );
                ranges = dict.ToDictionary(kvp => kvp.Key.ExternalId, kvp => kvp.Value);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to get extracted ranges");
                return false;
            }

            foreach (var state in states)
            {
                if (state.IsArray)
                {
                    for (int i = 0; i < state.ArrayDimensions[0]; i++)
                    {
                        var id = Extractor.GetUniqueId(state.SourceId, i);
                        if (id == null)
                            break;
                        if (ranges.TryGetValue(id, out var range))
                        {
                            if (range == TimeRange.Empty)
                            {
                                state.InitToEmpty();
                            }
                            else
                            {
                                state.InitExtractedRange(range.First, range.Last);
                            }
                        }
                    }
                }
                else
                {
                    if (ranges.TryGetValue(state.Id, out var range))
                    {
                        if (range == TimeRange.Empty)
                        {
                            state.InitToEmpty();
                        }
                        else
                        {
                            state.InitExtractedRange(range.First, range.Last);
                        }
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Test that the extractor is capable of pushing to CDF.
        /// Also fetches DataSet externalId.
        /// </summary>
        /// <param name="fullConfig">Configuration in use</param>
        /// <returns>True if pushing is possible, false if not.</returns>
        public async Task<bool?> TestConnection(FullConfig fullConfig, CancellationToken token)
        {
            if (fullConfig.DryRun)
                return true;

            try
            {
                await destination.TestCogniteConfig(token);
            }
            catch (Exception ex)
            {
                log.LogError(
                    "Failed to get CDF login status, this is likely a problem with the network or configuration. Project {Project} at {Url}: {Message}",
                    config.Project,
                    config.Host,
                    ex.Message
                );
                return false;
            }

            try
            {
                await destination.CogniteClient.TimeSeries.ListAsync(
                    new TimeSeriesQuery { Limit = 1 },
                    token
                );
            }
            catch (ResponseException ex)
            {
                log.LogError(
                    "Could not access CDF Time Series - most likely due "
                        + "to insufficient access rights on API key. Project {Project} at {Host}: {Message}",
                    config.Project,
                    config.Host,
                    ex.Message
                );
                return false;
            }

            if (fullConfig.Events.Enabled)
            {
                try
                {
                    await destination.CogniteClient.Events.ListAsync(
                        new EventQuery { Limit = 1 },
                        token
                    );
                }
                catch (ResponseException ex)
                {
                    log.LogError(
                        "Could not access CDF Events, though event emitters are specified - most likely due "
                            + "to insufficient access rights on API key. Project {Project} at {Host}: {Message}",
                        config.Project,
                        config.Host,
                        ex.Message
                    );
                    return false;
                }
            }

            try
            {
                await EnsureConfigInit(token);
            }
            catch
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Push list of references as relationships to CDF.
        /// </summary>
        /// <param name="references">List of references to push</param>
        /// <returns>True if nothing failed unexpectedly</returns>
        private async Task PushReferences(
            IEnumerable<UAReference> references,
            BrowseReport report,
            PushResult result,
            CancellationToken token
        )
        {
            try
            {
                await PushReferences(references, report, token);
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to ensure references");
                result.References = false;
            }
        }

        private async Task PushReferences(
            IEnumerable<UAReference> references,
            BrowseReport report,
            CancellationToken token
        )
        {
            if (references == null || !references.Any())
                return;

            var relationships = references
                .Select(reference => reference.ToRelationship(config.DataSet?.Id, Extractor))
                .DistinctBy(rel => rel.ExternalId);

            bool useRawRelationships =
                config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.RelationshipsTable);

            log.LogInformation("Test {Count} relationships against CDF", references.Count());

            if (useRawRelationships)
            {
                var _result = await cdfWriter.raw.PushReferences(config.RawMetadata!.Database!, config.RawMetadata!.RelationshipsTable!, relationships);
                report.RelationshipsCreated += _result.Created;
            }
            else
            {
                await cdfWriter.relationships.PushReferences(relationships, report);
            }

            log.LogInformation("Sucessfully pushed relationships to CDF");
        }

        public async Task<bool> ExecuteDeletes(DeletedNodes deletes, CancellationToken token)
        {
            if (fullConfig.DryRun)
                return true;

            var tasks = new List<Task>();
            if (deletes.Objects.Any())
            {
                tasks.Add(MarkAssetsAsDeleted(deletes.Objects, token));
            }
            if (deletes.Variables.Any())
            {
                tasks.Add(MarkTimeSeriesAsDeleted(deletes.Variables, token));
            }
            if (deletes.References.Any())
            {
                tasks.Add(MarkReferencesAsDeleted(deletes.References, token));
            }
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to mark resources as deleted: {Message}", ex.Message);
                return false;
            }

            return true;
        }
        #endregion

        #region assets
        /// <summary>
        /// Update list of nodes as assets in CDF Raw.
        /// </summary>
        /// <param name="assetMap">Id, node map for the assets that should be pushed.</param>
        private async Task UpdateRawAssets(IDictionary<string, BaseUANode> assetMap, BrowseReport report, CancellationToken token)
        {
            if (config.RawMetadata?.Database == null || config.RawMetadata?.AssetsTable == null) return;
            await UpsertRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.AssetsTable, rows =>
            {
                if (rows == null)
                {
                    return assetMap.Select(kvp => (
                        kvp.Key,
                        update: PusherUtils.CreateRawUpdate(log, Extractor.StringConverter, kvp.Value, null, ConverterType.Node)
                    )).Where(elem => elem.update != null)
                    .ToDictionary(pair => pair.Key, pair => pair.update!.Value);
                }

                var toWrite = new List<(string key, RawRow<Dictionary<string, JsonElement>> row, BaseUANode node)>();

                foreach (var row in rows)
                {
                    if (assetMap.TryGetValue(row.Key, out var ts))
                    {
                        toWrite.Add((row.Key, row, ts));
                        assetMap.Remove(row.Key);
                    }
                }

                var updates = new Dictionary<string, JsonElement>();

                foreach (var (key, row, node) in toWrite)
                {
                    var update = PusherUtils.CreateRawUpdate(log, Extractor.StringConverter, node, row, ConverterType.Node);

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
            }, null, token);
        }

        /// <summary>
        /// Create list of nodes as assets in CDF Raw.
        /// This does not create rows if they already exist.
        /// </summary>
        /// <param name="assetMap">Id, node map for the assets that should be pushed.</param>
        private async Task CreateRawAssets(IDictionary<string, BaseUANode> assetMap, BrowseReport report, CancellationToken token)
        {
            if (config.RawMetadata?.Database == null || config.RawMetadata?.AssetsTable == null) return;

            await EnsureRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.AssetsTable, assetMap.Keys, ids =>
            {
                var assets = ids.Select(id => (assetMap[id], id));
                var creates = assets.Select(pair => (pair.Item1.ToJson(log, Extractor.StringConverter, ConverterType.Node), pair.id))
                    .Where(pair => pair.Item1 != null)
                    .ToDictionary(pair => pair.id, pair => pair.Item1!.RootElement);
                report.AssetsCreated += creates.Count;
                return creates;
            }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, token);
        }

        /// <summary>
        /// Create assets in CDF Clean.
        /// </summary>
        /// <param name="assetMap">Id, node map for the assets that should be pushed.</param>
        private async Task<IEnumerable<Asset>> CreateAssets(IDictionary<string, BaseUANode> assetMap, BrowseReport report, CancellationToken token)
        {
            var assets = new List<Asset>();
            foreach (var chunk in Chunking.ChunkByHierarchy(assetMap.Values, config.CdfChunking.Assets, node => node.Id, node => node.ParentId))
            {
                var assetChunk = await destination.GetOrCreateAssetsAsync(chunk.Select(node => Extractor.GetUniqueId(node.Id)!), ids =>
                {
                    var assets = ids.Select(id => assetMap[id]);
                    var creates = assets
                        .Select(node => node.ToCDFAsset(fullConfig, Extractor, config.DataSet?.Id, config.MetadataMapping?.Assets))
                        .Where(asset => asset != null);
                    report.AssetsCreated += creates.Count();
                    return creates;
                }, RetryMode.None, SanitationMode.Clean, token);

                log.LogResult(assetChunk, RequestType.CreateAssets, true);

                assetChunk.ThrowOnFatal();

                if (assetChunk.Results == null) continue;

                foreach (var asset in assetChunk.Results)
                {
                    nodeToAssetIds[assetMap[asset.ExternalId].Id] = asset.Id;
                }
                assets.AddRange(assetChunk.Results);
            }
            return assets;
        }

        /// <summary>
        /// Update assets in CDF Clean.
        /// </summary>
        /// <param name="assetMap">Id, node map for the assets that should be pushed.</param>
        /// <param name="assets">List of existing assets in CDF.</param>
        /// <param name="update">Configuration for which fields should be updated.</param>
        private async Task UpdateAssets(
            IDictionary<string, BaseUANode> assetMap,
            IEnumerable<Asset> assets,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token
        )
        {
            var updates = new List<AssetUpdateItem>();
            var existing = assets.ToDictionary(asset => asset.ExternalId);
            foreach (var kvp in assetMap)
            {
                if (existing.TryGetValue(kvp.Key, out var asset))
                {
                    var assetUpdate = PusherUtils.GetAssetUpdate(
                        fullConfig,
                        asset,
                        kvp.Value,
                        Extractor,
                        update
                    );

                    if (assetUpdate == null)
                        continue;
                    if (
                        assetUpdate.ParentExternalId != null
                        || assetUpdate.Description != null
                        || assetUpdate.Name != null
                        || assetUpdate.Metadata != null
                    )
                    {
                        updates.Add(new AssetUpdateItem(asset.ExternalId) { Update = assetUpdate });
                    }
                }
            }
            if (updates.Any())
            {
                var res = await destination.UpdateAssetsAsync(
                    updates,
                    RetryMode.OnError,
                    SanitationMode.Clean,
                    token
                );

                log.LogResult(res, RequestType.UpdateAssets, false);

                res.ThrowOnFatal();

                report.AssetsUpdated += res.Results?.Count() ?? 0;
            }
        }

        /// <summary>
        /// Master method for pushing assets to CDF raw.
        /// </summary>
        /// <param name="objects">Assets to push</param>
        /// <param name="update">Configuration for which fields, if any, to update in CDF</param>
        private async Task PushRawAssets(
            ConcurrentDictionary<string, BaseUANode> assetsMap,
            TypeUpdateConfig update,
            BrowseReport report,
            PushResult result
        )
        {
            try
            {
                var _result = await cdfWriter.raw.PushNodes(
                    Extractor, 
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.AssetsTable!,
                    assetsMap,
                    ConverterType.Node,
                    update.AnyUpdate
                );
                report.AssetsCreated += _result.Created;
                report.AssetsUpdated += _result.Updated;
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to ensure assets");
                result.Objects = false;
            }
        }

        private async Task PushRawAssets(
            ConcurrentDictionary<string, BaseUANode> assetsMap,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token
        )
        {
            if (update.AnyUpdate)
            {
                await UpdateRawAssets(assetsMap, report, token);
            }
            else
            {
                await CreateRawAssets(assetsMap, report, token);
            }
        }

        private async Task MarkAssetsAsDeleted(
            IEnumerable<string> externalIds,
            CancellationToken token
        )
        {
            bool useRawAssets =
                config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.AssetsTable);

            if (useRawAssets)
            {
                await MarkRawRowsAsDeleted(
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.AssetsTable!,
                    externalIds,
                    token
                );
            }
            else
            {
                var updates = externalIds.Select(
                    extId =>
                        new AssetUpdateItem(extId)
                        {
                            Update = new AssetUpdate
                            {
                                Metadata = new UpdateDictionary<string>(
                                    new Dictionary<string, string>
                                    {
                                        { fullConfig.Extraction.Deletes.DeleteMarker, "true" }
                                    },
                                    Enumerable.Empty<string>()
                                )
                            }
                        }
                );
                var result = await destination.UpdateAssetsAsync(
                    updates,
                    RetryMode.OnError,
                    SanitationMode.Clean,
                    token
                );
                log.LogResult(result, RequestType.UpdateAssets, true);
                result.ThrowOnFatal();
            }
        }

        #endregion

        #region timeseries
        /// <summary>
        /// Update list of nodes as timeseries in CDF Raw.
        /// </summary>
        /// <param name="tsMap">Id, node map for the timeseries that should be pushed.</param>
        private async Task UpdateRawTimeseries(IDictionary<string, UAVariable> tsMap, BrowseReport report, CancellationToken token)
        {
            if (config.RawMetadata?.Database == null || config.RawMetadata.TimeseriesTable == null) return;
            await UpsertRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.TimeseriesTable, rows =>
            {
                if (rows == null)
                {
                    return tsMap.Select(kvp => (
                        kvp.Key,
                        update: PusherUtils.CreateRawUpdate(log, Extractor.StringConverter, kvp.Value, null, ConverterType.Variable)
                    )).Where(elem => elem.update != null)
                    .ToDictionary(pair => pair.Key, pair => pair.update!.Value);
                }

                var toWrite = new List<(string key, RawRow<Dictionary<string, JsonElement>> row, UAVariable node)>();

                foreach (var row in rows)
                {
                    if (tsMap.TryGetValue(row.Key, out var ts))
                    {
                        toWrite.Add((row.Key, row, ts));
                        tsMap.Remove(row.Key);
                    }
                }

                var updates = new Dictionary<string, JsonElement>();

                foreach (var (key, row, node) in toWrite)
                {
                    var update = PusherUtils.CreateRawUpdate(log, Extractor.StringConverter, node, row, ConverterType.Variable);

                    if (update != null)
                    {
                        updates[key] = update.Value;
                        if (row == null)
                        {
                            report.TimeSeriesCreated++;
                        }
                        else
                        {
                            report.TimeSeriesUpdated++;
                        }
                    }
                }

                return updates;
            }, null, token);
        }

        /// <summary>
        /// Create list of nodes as timeseries in CDF Raw.
        /// This does not create rows if they already exist.
        /// </summary>
        /// <param name="tsMap">Id, node map for the timeseries that should be pushed.</param>
        private async Task CreateRawTimeseries(IDictionary<string, UAVariable> tsMap, BrowseReport report, CancellationToken token)
        {
            if (config.RawMetadata?.Database == null || config.RawMetadata.TimeseriesTable == null) return;

            await EnsureRawRows<JsonElement>(config.RawMetadata.Database, config.RawMetadata.TimeseriesTable, tsMap.Keys, ids =>
            {
                var timeseries = ids.Select(id => (tsMap[id], id));
                var creates = timeseries.Select(pair => (pair.Item1.ToJson(log, Extractor.StringConverter, ConverterType.Variable), pair.id))
                    .Where(pair => pair.Item1 != null)
                    .ToDictionary(pair => pair.id, pair => pair.Item1!.RootElement);

                report.TimeSeriesCreated += creates.Count;
                return creates;
            }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, token);
        }

        /// <summary>
        /// Create timeseries in CDF Clean, optionally creates only minimal timeseries with no metadata or context.
        /// </summary>
        /// <param name="tsMap">Id, node map for the timeseries that should be pushed.</param>
        /// <param name="createMinimalTimeseries">True to create timeseries with no metadata.</param>
        private async Task<IEnumerable<TimeSeries>> CreateTimeseries(
            IDictionary<string, UAVariable> tsMap,
            BrowseReport report,
            bool createMinimalTimeseries,
            CancellationToken token
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
                                    fullConfig,
                                    Extractor,
                                    Extractor,
                                    config.DataSet?.Id,
                                    nodeToAssetIds,
                                    config.MetadataMapping?.Timeseries,
                                    createMinimalTimeseries
                                )
                        )
                        .Where(ts => ts != null);
                    if (createMinimalTimeseries)
                    {
                        report.MinimalTimeSeriesCreated += creates.Count();
                    }
                    else
                    {
                        report.TimeSeriesCreated += creates.Count();
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

        /// <summary>
        /// Update timeseries in CDF Clean.
        /// </summary>
        /// <param name="tsMap">Id, node map for the timeseries that should be pushed.</param>
        /// <param name="timeseries">List of existing timeseries in CDF.</param>
        /// <param name="update">Configuration for which fields should be updated.</param>
        private async Task UpdateTimeseries(
            IDictionary<string, UAVariable> tsMap,
            IEnumerable<TimeSeries> timeseries,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token)
        {
            var updates = new List<TimeSeriesUpdateItem>();
            var existing = timeseries.ToDictionary(asset => asset.ExternalId);
            foreach (var kvp in tsMap)
            {
                if (existing.TryGetValue(kvp.Key, out var ts))
                {
                    var tsUpdate = PusherUtils.GetTSUpdate(fullConfig, Extractor, ts, kvp.Value, update, nodeToAssetIds);
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

                report.TimeSeriesUpdated += res.Results?.Count() ?? 0;
            }
        }

        /// <summary>
        /// Master method for pushing timeseries to CDF raw or clean.
        /// </summary>
        /// <param name="tsList">Timeseries to push</param>
        /// <param name="update">Configuration for which fields, if any, to update in CDF</param>
        private async Task PushRawTimeseries(ConcurrentDictionary<string, UAVariable> tsIds, TypeUpdateConfig update, BrowseReport report, PushResult result)
        {
            try
            {
                var toPushMeta = tsIds
                    .Where(kvp => kvp.Value.Source != NodeSource.CDF)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                var _result = await cdfWriter.raw.PushNodes(
                    Extractor, 
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.TimeseriesTable!,
                    toPushMeta,
                    ConverterType.Variable,
                    update.AnyUpdate && !config.SkipMetadata
                );
                report.TimeSeriesCreated += _result.Created;
                report.TimeSeriesUpdated += _result.Updated;
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to ensure timeseries");
                result.Variables = false;
            }
        }

        private async Task PushRawTimeseries(
            ConcurrentDictionary<string, UAVariable> tsIds,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token
        )
        {
            var toPushMeta = tsIds
                .Where(kvp => kvp.Value.Source != NodeSource.CDF)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            if (update.AnyUpdate && !config.SkipMetadata)
            {
                await UpdateRawTimeseries(toPushMeta, report, token);
            }
            else
            {
                await CreateRawTimeseries(toPushMeta, report, token);
            }
        }

        private async Task MarkTimeSeriesAsDeleted(
            IEnumerable<string> externalIds,
            CancellationToken token
        )
        {
            bool useRawTss =
                config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.TimeseriesTable);

            if (useRawTss)
            {
                await MarkRawRowsAsDeleted(
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.TimeseriesTable!,
                    externalIds,
                    token
                );
            }

            var updates = externalIds.Select(
                extId =>
                    new TimeSeriesUpdateItem(extId)
                    {
                        Update = new TimeSeriesUpdate
                        {
                            Metadata = new UpdateDictionary<string>(
                                new Dictionary<string, string>
                                {
                                    { fullConfig.Extraction.Deletes.DeleteMarker, "true" }
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
            log.LogResult(result, RequestType.UpdateAssets, true);
            result.ThrowOnFatal();
        }
        #endregion

        #region raw-utils
        /// <summary>
        /// Ensure that raw rows given by <paramref name="keys"/> exist in the table given by
        /// <paramref name="dbName"/> and <paramref name="tableName"/>.
        /// Keys that do not exist are built into DTOs by <paramref name="dtoBuilder"/>.
        /// </summary>
        /// <typeparam name="T">Type of DTO to build</typeparam>
        /// <param name="dbName">Name of database in CDF Raw</param>
        /// <param name="tableName">Name of table in CDF Raw</param>
        /// <param name="keys">Keys of rows to ensure</param>
        /// <param name="dtoBuilder">Method to build DTOs for keys that were not found.</param>
        /// <param name="options"><see cref="JsonSerializerOptions"/> used for serialization.</param>
        private async Task EnsureRawRows<T>(
            string dbName,
            string tableName,
            IEnumerable<string> keys,
            Func<IEnumerable<string>, IDictionary<string, T>> dtoBuilder,
            JsonSerializerOptions options,
            CancellationToken token
        )
        {
            var rows = await GetRawRows(dbName, tableName, new[] { "," }, token);
            var existing = rows.Select(row => row.Key);

            var toCreate = keys.Except(existing);
            if (!toCreate.Any())
                return;
            log.LogInformation("Creating {Count} raw rows in CDF", toCreate.Count());

            var createDtos = dtoBuilder(toCreate);

            await destination.InsertRawRowsAsync(dbName, tableName, createDtos, options, token);
        }

        /// <summary>
        /// Insert or update raw rows given by <paramref name="toRetrieve"/> in table
        /// given by <paramref name="dbName"/> and <paramref name="tableName"/>.
        /// The dtoBuilder is called with chunks of 10000 rows, and finally with null to indicate that there are no more rows.
        /// </summary>
        /// <typeparam name="T">Type of DTO to build</typeparam>
        /// <param name="dbName">Name of database in CDF Raw</param>
        /// <param name="tableName">Name of table in CDF Raw</param>
        /// <param name="dtoBuilder">Method to build DTOs, called with existing rows.</param>
        /// <param name="options"><see cref="JsonSerializerOptions"/> used for serialization.</param>
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

        public async Task<IEnumerable<RawRow<Dictionary<string, JsonElement>>>> GetRawRows(
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

        private async Task MarkRawRowsAsDeleted(
            string dbName,
            string tableName,
            IEnumerable<string> keys,
            CancellationToken token
        )
        {
            var keySet = new HashSet<string>(keys);
            var rows = await GetRawRows(dbName, tableName, keys, token);
            var trueElem = JsonDocument.Parse("true").RootElement;
            var toMark = rows.Where(r => keySet.Contains(r.Key)).ToList();
            foreach (var row in toMark)
            {
                row.Columns[fullConfig.Extraction.Deletes.DeleteMarker] = trueElem;
            }
            await destination.InsertRawRowsAsync(
                dbName,
                tableName,
                toMark.ToDictionary(e => e.Key, e => e.Columns),
                token
            );
        }

        #endregion

        #region references
        /// <summary>
        /// Create the given list of relationships in CDF, handles duplicates.
        /// </summary>
        /// <param name="relationships">Relationships to create</param>
        private async Task<int> PushReferencesChunk(
            IEnumerable<RelationshipCreate> relationships,
            CancellationToken token
        )
        {
            if (!relationships.Any())
                return 0;
            try
            {
                await destination.CogniteClient.Relationships.CreateAsync(relationships, token);
                return relationships.Count();
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
                            if (value is MultiValue.String strValue)
                            {
                                existing.Add(strValue.Value);
                            }
                        }
                    }
                    if (!existing.Any())
                        throw;

                    relationships = relationships
                        .Where(rel => !existing.Contains(rel.ExternalId))
                        .ToList();
                    return await PushReferencesChunk(relationships, token);
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Create the given list of relationships in CDF Raw, skips rows that already exist.
        /// </summary>
        /// <param name="relationships">Relationships to create.</param>
        private async Task PushRawReferences(
            IEnumerable<RelationshipCreate> relationships,
            BrowseReport report,
            CancellationToken token
        )
        {
            if (
                config.RawMetadata?.Database == null
                || config.RawMetadata.RelationshipsTable == null
            )
                return;

            await EnsureRawRows(
                config.RawMetadata.Database,
                config.RawMetadata.RelationshipsTable,
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
                new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase },
                token
            );
        }

        private async Task MarkReferencesAsDeleted(
            IEnumerable<string> externalIds,
            CancellationToken token
        )
        {
            bool useRawRelationships =
                config.RawMetadata != null
                && !string.IsNullOrWhiteSpace(config.RawMetadata.Database)
                && !string.IsNullOrWhiteSpace(config.RawMetadata.RelationshipsTable);

            if (useRawRelationships)
            {
                await MarkRawRowsAsDeleted(
                    config.RawMetadata!.Database!,
                    config.RawMetadata!.RelationshipsTable!,
                    externalIds,
                    token
                );
            }
            else if (config.DeleteRelationships)
            {
                var tasks = externalIds
                    .ChunkBy(1000)
                    .Select(
                        chunk =>
                            destination.CogniteClient.Relationships.DeleteAsync(chunk, true, token)
                    );
                await Task.WhenAll(tasks);
            }
        }
        #endregion

        /// <summary>
        /// Make sure any configuration that requires synchronizing with CDF is properly initialized.
        /// </summary>
        private async Task EnsureConfigInit(CancellationToken token)
        {
            if (config.DataSet != null)
            {
                try
                {
                    var id = await destination.CogniteClient.DataSets.GetId(config.DataSet, token);
                    config.DataSet.Id = id;
                }
                catch (ResponseException ex)
                {
                    log.LogError(
                        "Could not fetch data set by external id. It may not exist, or the user may lack"
                            + " sufficient access rights. Project {Project} at {Host}, id {Id}: {Message}",
                        config.Project,
                        config.Host,
                        config.DataSet.ExternalId,
                        ex.Message
                    );
                    throw;
                }
            }
        }

        public void Dispose() { }
    }
}

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
            if (cdfWriter.FDM != null)
            {
                cdfWriter.FDM.Extractor = value;
            }
        } }
        public IPusherConfig BaseConfig { get; }

        private readonly HashSet<string> mismatchedTimeseries = new HashSet<string>();
        private readonly HashSet<string> missingTimeseries = new HashSet<string>();
        private readonly CogniteDestination destination;

        private readonly BrowseCallback? callback;
        private RawMetadataTargetConfig? RawMetadataTargetConfig => fullConfig.Cognite?.MetadataTargets?.Raw;
        private CleanMetadataTargetConfig? CleanMetadataTargetConfig => fullConfig.Cognite?.MetadataTargets?.Clean;


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
        /// <param name="references"> List of references to be synchronized</param>
        /// <param name="update">Configuration of what fields, if any, should be updated.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if no operation failed unexpectedly</returns>
        public async Task<PushResult> PushNodes(IEnumerable<BaseUANode> objects,
                IEnumerable<UAVariable> variables, IEnumerable<UAReference> references, UpdateConfig update, CancellationToken token)
        {
            var result = new PushResult();
            var report = new BrowseReport
            {
                IdPrefix = fullConfig.Extraction.IdPrefix,
                RawDatabase = RawMetadataTargetConfig?.Database,
                AssetsTable = RawMetadataTargetConfig?.AssetsTable,
                TimeSeriesTable = RawMetadataTargetConfig?.TimeseriesTable,
                RelationshipsTable = RawMetadataTargetConfig?.RelationshipsTable
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
                if (cdfWriter.FDM != null)
                {
                    await cdfWriter.FDM.PushNodes(objects, variables, references, Extractor, token);
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
                result.RawObjects = false;
                result.RawReferences = false;
                result.RawVariables = false;
                nodeEnsuringFailures.Inc();
                return result;
            }

            var tasks = new List<Task>();

            tasks.Add(PushAssets(objects, update.Objects, report, result, token));
            
            tasks.Add(PushTimeseries(variables, update.Variables, report, result, token));

            tasks.Add(PushReferences(references, report, result, token));

            tasks.Add(PushFdm(objects, variables, references, result, token));

            await Task.WhenAll(tasks);

            log.LogInformation("Finish pushing nodes to CDF");

            if (
                result.Objects 
                && result.References
                && result.Variables
                && result.RawObjects
                && result.RawVariables
                && result.RawReferences
            )
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

        /// <summary>
        /// Synchronized all objects, variables and references with FDM 
        /// </summary>
        /// <param name="objects">List of objects to be synchronized</param>
        /// <param name="variables">List of variables to synchronize</param>
        /// <param name="references">List of references to synchronize<param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushFdm(IEnumerable<BaseUANode> objects,
                IEnumerable<UAVariable> variables, IEnumerable<UAReference> references, PushResult result, CancellationToken token)
        {
            if (cdfWriter.FDM == null) return;
            bool pushResult = true;
            try
            {
                pushResult = await cdfWriter.FDM.PushNodes(objects, variables, references, Extractor, token);
            }
            catch
            {
                pushResult = false;
            }
            result.Variables = pushResult;
            result.Objects = pushResult;
            result.References = pushResult;
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
        /// Maps objects to their keys while filtering
        /// </summary>
        /// <param name="objects">List of objects to be mapped</param>
        /// <returns>A dictionary of mapping</returns>
        private ConcurrentDictionary<string, BaseUANode> MapAssets(IEnumerable<BaseUANode> objects)
        {
            return new ConcurrentDictionary<string, BaseUANode>(
                objects
                    .Where(node => node.Source != NodeSource.CDF)
                    .ToDictionary(obj => Extractor.GetUniqueId(obj.Id)!)
            );
        }

        /// <summary>
        /// Synchronize all objects to CDF
        /// </summary>
        /// <param name="objects">List of objects to be synchronized</param>
        /// <param name="update">Update configuration</param>
        /// <param name="report">Browse report</param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushAssets(IEnumerable<BaseUANode> objects, TypeUpdateConfig update, BrowseReport report, PushResult result, CancellationToken token)
        {
            if (!objects.Any() && cdfWriter.Assets == null && cdfWriter.Raw == null) return;

            var assetsMap = MapAssets(objects);
            if (cdfWriter.Assets != null)
            {
                await PushCleanAssets(assetsMap, update, report, result, token);
            } 
            if (cdfWriter.Raw != null && RawMetadataTargetConfig?.AssetsTable != null)
            {
                await PushRawAssets(assetsMap, update, report, result, token);
            }
        }

        /// <summary>
        /// Synchronize all objects to CDF assets
        /// </summary>
        /// <param name="assetsMap">Synchronizes all objects maps to CDF assets</param>
        /// <param name="update">Update configuration</param>
        /// <param name="report">Browse report</param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushCleanAssets(IDictionary<string, BaseUANode> assetsMap, TypeUpdateConfig update, BrowseReport report, PushResult result, CancellationToken token)
        {
            try
            {
                var _result = await cdfWriter.Assets!.PushNodes(Extractor, assetsMap, nodeToAssetIds, update, token);
                report.AssetsCreated += _result.Created;
                report.AssetsUpdated += _result.Updated;
            }
            catch
            {
                result.Objects = false;
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
            PushResult result,
            CancellationToken token
        )
        {
            try
            {
                var _result = await cdfWriter.Raw!.PushNodes(
                    Extractor, 
                    RawMetadataTargetConfig!.Database!,
                    RawMetadataTargetConfig!.AssetsTable!,
                    assetsMap,
                    ConverterType.Node,
                    update.AnyUpdate,
                    token
                );
                report.RawAssetsCreated += _result.Created;
                report.RawAssetsUpdated += _result.Updated;
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to ensure assets");
                result.RawObjects = false;
            }
        }

        private async Task MarkAssetsAsDeleted(
            IEnumerable<string> externalIds,
            CancellationToken token
        )
        {
            bool useRawAssets =
                RawMetadataTargetConfig != null
                && !string.IsNullOrWhiteSpace(RawMetadataTargetConfig.Database)
                && !string.IsNullOrWhiteSpace(RawMetadataTargetConfig.AssetsTable);

            if (useRawAssets)
            {
                await MarkRawRowsAsDeleted(
                    RawMetadataTargetConfig!.Database!,
                    RawMetadataTargetConfig!.AssetsTable!,
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
        /// Maps variables to their keys
        /// </summary>
        /// <param name="objects">List of variables to be mapped</param>
        /// <returns>A dictionary of mapping</returns>
        private ConcurrentDictionary<string, UAVariable> MapTimeseries(IEnumerable<UAVariable> variables)
        {
            return new ConcurrentDictionary<string, UAVariable>(
                variables.ToDictionary(ts => ts.GetUniqueId(Extractor)!)
            );
        }

        /// <summary>
        /// Synchronize all variables to CDF
        /// </summary>
        /// <param name="variables">List of variables to be synchronized</param>
        /// <param name="update">Update configuration</param>
        /// <param name="report">Browse report</param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushTimeseries(IEnumerable<UAVariable> variables, TypeUpdateConfig update, BrowseReport report, PushResult result, CancellationToken token)
        {
            if (!variables.Any()) return;

            var timeseriesMap = MapTimeseries(variables);
            await PushCleanTimeseries(timeseriesMap, update, report, result, token);
            if (cdfWriter.Raw != null && (RawMetadataTargetConfig?.TimeseriesTable != null))
            {
                await PushRawTimeseries(timeseriesMap, update, report, result, token);
            }
        }

        /// <summary>
        /// Synchronize all variables to CDF timeseries
        /// </summary>
        /// <param name="timeseriesMap">Synchronizes all variable maps to CDF timeseries</param>
        /// <param name="update">Update configuration</param>
        /// <param name="report">Browse report</param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushCleanTimeseries(IDictionary<string, UAVariable> timeseriesMap, TypeUpdateConfig update, BrowseReport report, PushResult result, CancellationToken token)
        {
            try
            {
                var _result = await cdfWriter.Timeseries!.PushVariables(Extractor, timeseriesMap, nodeToAssetIds, mismatchedTimeseries, update, token);
                var createMinimal = !(CleanMetadataTargetConfig?.Timeseries ?? false); 
                if (createMinimal)
                {
                    report.MinimalTimeSeriesCreated += _result.Created;
                }
                else
                {
                    report.TimeSeriesCreated += _result.Created;
                }
                report.TimeSeriesUpdated += _result.Updated;
            }
            catch
            {
                result.Variables = false;
            }
        }

       /// <summary>
        /// Synchronize all variables to CDF raw
        /// </summary>
        /// <param name="tsIds">Synchronizes all variables maps to CDF raw</param>
        /// <param name="update">Update configuration</param>
        /// <param name="report">Browse report</param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushRawTimeseries(ConcurrentDictionary<string, UAVariable> tsIds, TypeUpdateConfig update, BrowseReport report, PushResult result, CancellationToken token)
        {
            try
            {
                var toPushMeta = tsIds
                    .Where(kvp => kvp.Value.Source != NodeSource.CDF)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                var _result = await cdfWriter.Raw!.PushNodes(
                    Extractor, 
                    RawMetadataTargetConfig!.Database!,
                    RawMetadataTargetConfig!.TimeseriesTable!,
                    toPushMeta,
                    ConverterType.Variable,
                    update.AnyUpdate,
                    token
                );
                report.RawTimeseriesCreated += _result.Created;
                report.RawTimeseriesUpdated += _result.Updated;
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to ensure timeseries");
                result.RawVariables = false;
            }
        }

        private async Task MarkTimeSeriesAsDeleted(
            IEnumerable<string> externalIds,
            CancellationToken token
        )
        {
            bool useRawTss =
                RawMetadataTargetConfig != null
                && !string.IsNullOrWhiteSpace(RawMetadataTargetConfig.Database)
                && !string.IsNullOrWhiteSpace(RawMetadataTargetConfig.TimeseriesTable);

            if (useRawTss)
            {
                await MarkRawRowsAsDeleted(
                    RawMetadataTargetConfig!.Database!,
                    RawMetadataTargetConfig!.TimeseriesTable!,
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
        /// Synchronize all references to CDF
        /// </summary>
        /// <param name="references">List of references to be synchronized</param>
        /// <param name="update">Update configuration</param>
        /// <param name="report">Browse report</param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushReferences(IEnumerable<UAReference> references, BrowseReport report, PushResult result, CancellationToken token)
        {
            if (!references.Any() && cdfWriter.Relationships == null && cdfWriter.Raw == null) return;

            var relationships = references
                .Select(reference => reference.ToRelationship(config.DataSet?.Id, Extractor))
                .DistinctBy(rel => rel.ExternalId);

            if (cdfWriter.Relationships != null)
            {
                await PushCleanReferences(relationships, report, result, token);
            }

            if (cdfWriter.Raw != null && RawMetadataTargetConfig?.RelationshipsTable != null)
            {
                await PushRawReferences(relationships, report, result, token);
            }
        }

        /// <summary>
        /// Synchronize all references to CDF relationship
        /// </summary>
        /// <param name="relationships">Synchronizes all references maps to CDF relationships</param>
        /// <param name="update">Update configuration</param>
        /// <param name="report">Browse report</param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushCleanReferences(IEnumerable<RelationshipCreate> relationships, BrowseReport report, PushResult result, CancellationToken token)
        {
            try
            {
                var _result =  await cdfWriter.Relationships!.PushReferences(relationships, token);
                report.RelationshipsCreated += _result.Created;
            }
            catch (Exception e)
            {
                log.LogError(e, "Failed to ensure relationships");
                result.References = false;
            }
        }

        /// <summary>
        /// Synchronize all references to CDF
        /// </summary>
        /// <param name="relationships">Synchronizes all references maps to CDF assets</param>
        /// <param name="update">Update configuration</param>
        /// <param name="report">Browse report</param>
        /// <param name="result">Push result</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private async Task PushRawReferences(IEnumerable<RelationshipCreate> relationships, BrowseReport report, PushResult result, CancellationToken token)
        {
            try
            {
                var _result = await cdfWriter.Raw!.PushReferences(RawMetadataTargetConfig!.Database!, RawMetadataTargetConfig!.RelationshipsTable!, relationships, token);
                report.RawRelationshipsCreated += _result.Created;
            } catch (Exception e)
            {
                log.LogError(e, "Failed to ensure raw relationships");
                result.RawReferences = false;
            }
        }

        private async Task MarkReferencesAsDeleted(
            IEnumerable<string> externalIds,
            CancellationToken token
        )
        {
            bool useRawRelationships =
                RawMetadataTargetConfig != null
                && !string.IsNullOrWhiteSpace(RawMetadataTargetConfig.Database)
                && !string.IsNullOrWhiteSpace(RawMetadataTargetConfig.RelationshipsTable);

            if (useRawRelationships)
            {
                await MarkRawRowsAsDeleted(
                    RawMetadataTargetConfig!.Database!,
                    RawMetadataTargetConfig!.RelationshipsTable!,
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


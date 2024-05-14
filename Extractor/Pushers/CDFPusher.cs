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
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers.Writers;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Prometheus;
using System;
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
        private readonly CDFWriter cdfWriter;

        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }

        public PusherInput? PendingNodes { get; set; }

        public UAExtractor Extractor { get; set; }
        public IPusherConfig BaseConfig { get; }

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
            Extractor = null!;
            this.log = log;
            this.config = config;
            BaseConfig = config;
            this.destination = destination;
            this.fullConfig = fullConfig;
            cdfWriter = provider.GetRequiredService<CDFWriter>();
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
                .GroupBy(dp => dp.Id)
                .Where(group => !cdfWriter.MismatchedTimeseries.Contains(group.Key)
                    && !missingTimeseries.Contains(group.Key))
                .ToDictionary(group => group.Key, group => group.ToList());

            int count = dataPointList.Aggregate(0, (seed, points) => seed + points.Value.Count);

            if (count == 0) return null;

            var inserts = dataPointList.ToDictionary(kvp =>
                Identity.Create(kvp.Key),
                kvp => kvp.Value.SelectNonNull(
                    dp => dp.ToCDFDataPoint(fullConfig.Extraction.StatusCodes.IngestStatusCodes, log))
                );

            if (fullConfig.DryRun)
            {
                log.LogInformation("Dry run enabled. Would insert {Count} datapoints over {C2} timeseries to CDF", count, inserts.Count);
                return null;
            }

            try
            {
                CogniteResult<DataPointInsertError> result;
                result = await destination.InsertDataPointsAsync(inserts, SanitationMode.Clean, RetryMode.OnError, token);

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
                            cdfWriter.MismatchedTimeseries.Add(skipped.Id.ExternalId);
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
                log.LogError(e, "Failed to push {Count} points to CDF: {Message}", count, e.Message);
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
                    .Select(evt => evt.ToCDFEvent(Extractor, config.DataSet?.Id, cdfWriter.NodeToAssetIds))
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

            if (!fullConfig.DryRun)
            {
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
            }

            await cdfWriter.PushNodesAndReferences(
                objects,
                variables,
                references,
                report,
                update,
                result,
                Extractor,
                token
            );

            if (fullConfig.DryRun) return result;

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
        /// Reset the pusher, preparing it to be restarted
        /// </summary>
        public void Reset()
        {
            missingTimeseries.Clear();
            cdfWriter.MismatchedTimeseries.Clear();
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
            if (fullConfig.DryRun) return true;
            try
            {
                await cdfWriter.ExecuteDeletes(deletes, Extractor, token);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to mark resources as deleted: {Message}", ex.Message);
                return false;
            }

            return true;
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
            return await WriterUtils.GetRawRows(dbName, tableName, destination, columns, log, token);
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


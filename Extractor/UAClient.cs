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

using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Subscriptions;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using Prometheus;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Browser = Cognite.OpcUa.Browse.Browser;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Client managing the connection to the opcua server, and providing wrapper methods to simplify interaction with the server.
    /// </summary>
    public class UAClient : IDisposable, IUAClientAccess
    {
        protected FullConfig Config { get; set; }
        protected ISession? Session => SessionManager.Session;
        protected ApplicationConfiguration? AppConfig { get; set; }
        public TypeManager TypeManager { get; }
        public SubscriptionManager? SubscriptionManager { get; private set; }

        public IClientCallbacks Callbacks { get; set; } = null!;

        private readonly SemaphoreSlim subscriptionSem = new SemaphoreSlim(1);
        public bool Started { get; private set; }
        private CancellationToken liveToken;

        private static readonly Counter attributeRequests = Metrics
            .CreateCounter("opcua_attribute_requests", "Number of attributes fetched from the server");
        private static readonly Counter numHistoryReads = Metrics
            .CreateCounter("opcua_history_reads", "Number of historyread operations performed");
        private static readonly Counter numBrowse = Metrics
            .CreateCounter("opcua_browse_operations", "Number of browse operations performed");
        private static readonly Counter attributeRequestFailures = Metrics
            .CreateCounter("opcua_attribute_request_failures", "Number of failed requests for attributes to OPC-UA");
        private static readonly Counter historyReadFailures = Metrics
            .CreateCounter("opcua_history_read_failures", "Number of failed history read operations");
        private static readonly Counter browseFailures = Metrics
            .CreateCounter("opcua_browse_failures", "Number of failures on browse operations");

        private readonly NodeMetricsManager? metricsManager;

        public SessionManager SessionManager { get; }

        private readonly ILogger<UAClient> log;
        private readonly ILogger<Tracing> traceLog;
        private LogLevel? traceLevel;

        public StringConverter StringConverter { get; }
        public Browser Browser { get; }

        public SessionContext Context => SessionManager.Context;

        /// <summary>
        /// Constructor, does not start the client.
        /// </summary>
        /// <param name="config">Full configuartion object</param>
        public UAClient(IServiceProvider provider, FullConfig config)
        {
            this.Config = config;
            log = provider.GetRequiredService<ILogger<UAClient>>();
            traceLog = provider.GetRequiredService<ILogger<Tracing>>();
            if (config.Metrics.Nodes != null)
            {
                metricsManager = new NodeMetricsManager(this, config.Metrics.Nodes);
            }
            StringConverter = new StringConverter(provider.GetRequiredService<ILogger<StringConverter>>(), this, config);
            Browser = new Browser(provider.GetRequiredService<ILogger<Browser>>(), this, config);
            TypeManager = new TypeManager(config, this, provider.GetRequiredService<ILogger<TypeManager>>());
            SessionManager = new SessionManager(Config, this, provider.GetRequiredService<ILogger<SessionManager>>());
        }
        #region Session management
        /// <summary>
        /// Entrypoint for starting the opcua Session. Must be called before any further requests can be made.
        /// </summary>
        public async Task Run(CancellationToken token, int timeout = -1)
        {
            liveToken = token;
            await StartSession(timeout);
            await StartNodeMetrics();
        }

        /// <summary>
        /// Close the Session, cleaning up any client data on the server
        /// </summary>
        public async Task Close(CancellationToken token)
        {
            try
            {
                await SessionManager.Close(token);
            }
            finally
            {
                Started = false;
            }
        }

        private void ConfigureUtilsTrace()
        {
            if (Config.Logger?.UaTraceLevel == null) return;
            Utils.SetTraceMask(Utils.TraceMasks.All);
            if (traceLevel != null) return;
            traceLevel = Config.Logger.UaTraceLevel switch
            {
                "verbose" => LogLevel.Trace,
                "debug" => LogLevel.Debug,
                "information" => LogLevel.Information,
                "warning" => LogLevel.Warning,
                "error" => LogLevel.Error,
                "fatal" => LogLevel.Critical,
                _ => LogLevel.Trace
            };
            Utils.SetLogger(traceLog);
            Utils.SetLogLevel(traceLevel.Value);
        }

        public void LogDump<T>(string message, T item)
        {
            if (Config.Logger.UaSessionTracing) log.LogDump(message, item);
        }

        /// <summary>
        /// Load XML configuration file, override certain fields with environment variables if set.
        /// </summary>
        protected async Task<ApplicationConfiguration> LoadAppConfig()
        {
            var application = new ApplicationInstance
            {
                ApplicationName = ".NET OPC-UA Extractor",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "opc.ua.net.extractor"
            };
            log.LogInformation("Load OPC-UA Configuration from {Root}/opc.ua.net.extractor.Config.xml", Config.Source.ConfigRoot);
            try
            {
                AppConfig = await application.LoadApplicationConfiguration($"{Config.Source.ConfigRoot}/opc.ua.net.extractor.Config.xml", false);
            }
            catch (ServiceResultException exc)
            {
                throw new ExtractorFailureException("Failed to load OPC-UA xml configuration file", exc);
            }
            catch (DirectoryNotFoundException dex)
            {
                throw new ExtractorFailureException(
                    $"Failed to load OPC-UA xml configuration, the {Config.Source.ConfigRoot} directory does not exist", dex);
            }
            catch (IOException exc)
            {
                throw new ExtractorFailureException("Failed to load OPC-UA xml configuration file", exc);
            }

            string ownCertificateDir = Environment.GetEnvironmentVariable("OPCUA_OWN_CERTIFICATE_DIR");
            string certificateDir = Environment.GetEnvironmentVariable("OPCUA_CERTIFICATE_DIR");
            string certificateSubject = Environment.GetEnvironmentVariable("OPCUA_CERTIFICATE_SUBJECT");
            if (!string.IsNullOrEmpty(certificateDir))
            {
                AppConfig.SecurityConfiguration.TrustedIssuerCertificates.StorePath = $"{certificateDir}/pki/issuer";
                AppConfig.SecurityConfiguration.TrustedPeerCertificates.StorePath = $"{certificateDir}/pki/trusted";
                AppConfig.SecurityConfiguration.RejectedCertificateStore.StorePath = $"{certificateDir}/pki/rejected";
            }

            if (!string.IsNullOrEmpty(ownCertificateDir))
            {
                AppConfig.SecurityConfiguration.ApplicationCertificate.StoreType = "Directory";
                AppConfig.SecurityConfiguration.ApplicationCertificate.StorePath = $"{ownCertificateDir}";
            }

            if (!string.IsNullOrEmpty(certificateSubject))
            {
                AppConfig.SecurityConfiguration.ApplicationCertificate.SubjectName = certificateSubject;
            }

            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0, Config.Source.CertificateExpiry);
            if (!validAppCert)
            {
                log.LogWarning("Missing application certificate, using insecure connection.");
            }
            else
            {
                AppConfig.ApplicationUri = X509Utils.GetApplicationUriFromCertificate(
                    AppConfig.SecurityConfiguration.ApplicationCertificate.Certificate);
                Config.Source.AutoAccept |= AppConfig.SecurityConfiguration.AutoAcceptUntrustedCertificates;
                AppConfig.CertificateValidator.CertificateValidation += CertificateValidationHandler;
            }

            LogDump("Configuration", AppConfig);

            ConfigureUtilsTrace();

            return AppConfig;
        }

        /// <summary>
        /// Load security configuration for the Session, then start the server.
        /// </summary>
        private async Task StartSession(int timeout = -1)
        {
            if (Callbacks == null) throw new InvalidOperationException("Attempted to start UAClient without setting callbacks");

            var appConfig = await LoadAppConfig();

            SessionManager.Initialize(appConfig, liveToken, timeout);

            if (SubscriptionManager == null)
            {
                SubscriptionManager = new SubscriptionManager(this, Config, log);
                Callbacks.TaskScheduler.ScheduleTask("SubscriptionManager", SubscriptionManager.RunTaskLoop);
            }

            await SessionManager.Connect();

            liveToken.ThrowIfCancellationRequested();

            Started = true;
            log.LogInformation("Successfully connected to server at {EndpointURL}", SessionManager.EndpointUrl);
        }

        /// <summary>
        /// Called after succesful validation of a server certificate. Handles the case where the certificate is untrusted.
        /// </summary>
        private void CertificateValidationHandler(CertificateValidator validator,
            CertificateValidationEventArgs eventArgs)
        {
            LogDump("Certificate Validation", eventArgs);
            if (eventArgs.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                eventArgs.Accept |= Config.Source.AutoAccept;
            }
            else if (!StatusCode.IsGood(eventArgs.Error.StatusCode) && Config.Source.IgnoreCertificateIssues)
            {
                log.LogWarning("Ignoring bad certificate: {Err}", eventArgs.Error.StatusCode);
                eventArgs.Accept = true;
            }

            if (eventArgs.Accept)
            {
                log.LogWarning("Accepted Certificate {CertificateSubject}", eventArgs.Certificate.Subject);
            }
            else
            {
                log.LogError("Rejected Bad Certificate {CertificateSubject}, {Err}",
                    eventArgs.Certificate.Subject, eventArgs.Error.StatusCode);
            }
        }

        private readonly OperationWaiter waiter = new OperationWaiter();

        /// <summary>
        /// Wait for all opcua operations to finish
        /// </summary>
        public async Task WaitForOperations(CancellationToken token)
        {
            await waiter.Wait(100_000, token);
        }

        /// <summary>
        /// Start collecting metrics from configured nodes, if enabled.
        /// </summary>
        private async Task StartNodeMetrics()
        {
            if (metricsManager == null) return;
            await metricsManager.StartNodeMetrics(TypeManager, liveToken);
        }
        #endregion

        #region Browse
        /// <summary>
        /// Retrieve a representation of the server node
        /// </summary>
        /// <returns></returns>
        public async Task<BaseUANode> GetServerNode(CancellationToken token)
        {
            var desc = (await Browser.GetRootNodes(new[] { ObjectIds.Server }, token)).FirstOrDefault();
            if (desc == null) throw new ExtractorFailureException("Server node is null. Invalid server configuration");

            var node = BaseUANode.Create(desc, NodeId.Null, null, this, TypeManager);
            if (node == null) throw new ExtractorFailureException($"Root node {desc.NodeId} is unexpected node class: {desc.NodeClass}");
            await ReadNodeData(new[] { node }, token, "the server node");
            return node;
        }

        private List<BrowseNode> HandleBrowseResult(List<BrowseNode> nodes, BrowseResultCollection results,
            ExtractorUtils.SourceOp op)
        {
            var ret = new List<BrowseNode>();
            if (nodes.Count != results.Count)
            {
                StatusCode? code = null;
                foreach (var res in results)
                {
                    if (StatusCode.IsBad(res.StatusCode))
                    {
                        code = res.StatusCode;
                        break;
                    }
                }
                throw new ServiceCallFailureException($"Incorrect number of results from {op} service. Got {results.Count}, expected {nodes.Count}. " +
                    $"This is illegal behavior, and caused by a bug in the server. Status: {code}");
            }

            for (int i = 0; i < nodes.Count; i++)
            {
                var result = results[i];
                var node = nodes[i];
                LogDump("Browse node", node);
                LogDump("Browse result", result);

                node.AddReferences(result.References);
                node.ContinuationPoint = result.ContinuationPoint;
                if (node.ContinuationPoint != null) ret.Add(node);
            }
            return ret;
        }

        public async Task GetReferences(BrowseParams browseParams, bool readToCompletion, CancellationToken token)
        {
            await RetryUtil.RetryAsync("browse", async () => await GetReferencesInternal(browseParams, readToCompletion, token), Config.Source.Retries, Config.Source.Retries.ShouldRetryException, log, token);
        }

        private async Task GetReferencesInternal(BrowseParams browseParams, bool readToCompletion, CancellationToken token)
        {
            if (browseParams == null || browseParams.Nodes == null) throw new ArgumentNullException(nameof(browseParams));

            var toBrowse = new List<BrowseNode>();
            var toBrowseNext = new List<BrowseNode>();
            foreach (var node in browseParams.Nodes.Values)
            {
                if (node.ContinuationPoint == null) toBrowse.Add(node);
                else toBrowseNext.Add(node);
            }

            using var operation = waiter.GetInstance();

            if (toBrowse.Count != 0)
            {
                var results = await RetryUtil.RetryResultAsync("browse", async () => await GetReferencesChunk(browseParams, toBrowse, token), Config.Source.Retries, Config.Source.Retries.ShouldRetryException, log, token);

                var next = HandleBrowseResult(toBrowse, results, ExtractorUtils.SourceOp.Browse);
                if (readToCompletion) toBrowseNext.AddRange(next);
            }

            while (toBrowseNext.Count != 0)
            {
                var results = await RetryUtil.RetryResultAsync("browse next", async () => await GetNextReferencesChunk(toBrowseNext, token), Config.Source.Retries, Config.Source.Retries.ShouldRetryException, log, token);

                var next = HandleBrowseResult(toBrowseNext, results, ExtractorUtils.SourceOp.BrowseNext);
                if (readToCompletion) toBrowseNext = next;
                else break;
            }
        }

        private async Task<BrowseResultCollection> GetReferencesChunk(BrowseParams browseParams, List<BrowseNode> toBrowse, CancellationToken token)
        {
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            var descriptions = new BrowseDescriptionCollection(toBrowse.Select(node => browseParams.ToDescription(node)));
            try
            {
                token.ThrowIfCancellationRequested();
                var result = await Session.BrowseAsync(
                    null,
                    null,
                    browseParams.MaxPerNode,
                    descriptions,
                    token);

                numBrowse.Inc();
                LogDump("Browse diagnostics", result.DiagnosticInfos);
                LogDump("Browse header", result.ResponseHeader);

                foreach (var res in result.Results)
                {
                    if (StatusCode.IsBad(res.StatusCode)
                    && res.StatusCode != StatusCodes.BadNodeIdUnknown)
                    {
                        throw ExtractorUtils.HandleServiceResult(log, new ServiceResultException(res.StatusCode), ExtractorUtils.SourceOp.Browse);
                    }
                }

                return result.Results;
            }
            catch (Exception ex)
            {
                token.ThrowIfCancellationRequested();
                browseFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.Browse);
            }
        }

        private async Task<BrowseResultCollection> GetNextReferencesChunk(List<BrowseNode> toBrowse, CancellationToken token)
        {
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            var cps = new ByteStringCollection(toBrowse.Select(node => node.ContinuationPoint));
            try
            {
                token.ThrowIfCancellationRequested();
                var result = await Session.BrowseNextAsync(
                    null,
                    false,
                    cps,
                    token);

                numBrowse.Inc();
                LogDump("BrowseNext diagnostics", result.DiagnosticInfos);
                LogDump("BrowseNext header", result.ResponseHeader);

                foreach (var res in result.Results)
                {
                    if (StatusCode.IsBad(res.StatusCode)
                    && res.StatusCode != StatusCodes.BadNodeIdUnknown)
                    {
                        throw ExtractorUtils.HandleServiceResult(log, new ServiceResultException(res.StatusCode), ExtractorUtils.SourceOp.BrowseNext);
                    }
                }

                return result.Results;
            }
            catch (Exception ex)
            {
                token.ThrowIfCancellationRequested();
                browseFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.BrowseNext);
            }
        }

        public async Task AbortBrowse(IEnumerable<BrowseNode> nodes)
        {
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            var toAbort = nodes.Where(node => node.ContinuationPoint != null).ToList();
            if (toAbort.Count == 0) return;
            var cps = new ByteStringCollection(nodes.Select(node => node.ContinuationPoint));
            try
            {
                await Session.BrowseNextAsync(
                    null,
                    true,
                    cps,
                    CancellationToken.None);
            }
            catch (Exception ex)
            {
                browseFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.BrowseNext);
            }
            foreach (var node in toAbort) node.ContinuationPoint = null;
        }

        #endregion

        #region Get node data
        /// <summary>
        /// Call the Read service with the given <paramref name="readValueIds"/>.
        /// </summary>
        /// <param name="readValueIds">Attributes to read.</param>
        /// <param name="distinctNodeCount">Number of distinct nodes</param>
        /// <param name="purpose">Purpose, for logging</param>
        /// <returns>List of retrieved datavalues,
        /// if the server is compliant this will have length equal to <paramref name="readValueIds"/></returns>
        public async Task<IList<DataValue>> ReadAttributes(ReadValueIdCollection readValueIds,
            int distinctNodeCount,
            CancellationToken token,
            string purpose = "")
        {
            var values = new List<DataValue>();
            if (readValueIds == null || !readValueIds.Any()) return values;
            if (!string.IsNullOrEmpty(purpose)) purpose = $" for {purpose}";

            using var operation = waiter.GetInstance();
            int total = readValueIds.Count;
            int attrCount = 0;

            int count = 0;
            foreach (var nextValues in readValueIds.ChunkBy(Config.Source.AttributesChunk))
            {
                if (token.IsCancellationRequested) break;

                count++;
                var chunk = await RetryUtil.RetryResultAsync($"read for {purpose}", async () => await ReadAttributesChunk(nextValues, token), Config.Source.Retries, Config.Source.Retries.ShouldRetryException, log, token);
                values.AddRange(chunk);
                attrCount += chunk.Count();
                log.LogDebug("Read {NumAttributesRead} / {Total} attributes{Purpose}", attrCount, total, purpose);
            }
            log.LogInformation(
                "Read {TotalAttributesRead} attributes with {NumAttributeReadOperations} operations for {NodeCount} nodes{Purpose}",
                values.Count, count, distinctNodeCount, purpose);

            return values;
        }

        private async Task<IEnumerable<DataValue>> ReadAttributesChunk(IEnumerable<ReadValueId> readValueIds, CancellationToken token)
        {
            if (token.IsCancellationRequested) return Enumerable.Empty<DataValue>();
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            try
            {
                var response = await Session.ReadAsync(
                    null,
                    0,
                    TimestampsToReturn.Source,
                    new ReadValueIdCollection(readValueIds),
                    token);

                attributeRequests.Inc();
                return response.Results;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { return Enumerable.Empty<DataValue>(); }
            catch (Exception ex)
            {
                attributeRequestFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadAttributes);
            }
        }

        /// <summary>
        /// Gets attributes for the given list of nodes. The attributes retrieved for each node depends on its NodeClass.
        /// </summary>
        /// <param name="nodes">Nodes to be updated with data from the opcua server</param>
        /// <param name="purpose">Purpose, for logging</param>
        public async Task ReadNodeData(IEnumerable<BaseUANode> nodes, CancellationToken token, string purpose = "")
        {
            nodes = nodes.DistinctBy(node => node.Attributes).Where(node => !node.Attributes.IsDataRead).ToList();
            if (!nodes.Any()) return;

            int expected = 0;
            var readValueIds = new ReadValueIdCollection();
            foreach (var node in nodes)
            {
                var attributes = node.Attributes.GetAttributeSet(Config);
                readValueIds.AddRange(attributes.Select(attr => new ReadValueId { AttributeId = attr, NodeId = node.Id }));
                expected += attributes.Count();
            }

            IList<DataValue> values;
            values = await ReadAttributes(readValueIds, nodes.Count(), token, purpose);

            int total = values.Count;

            log.LogInformation("Retrieved {Total}/{Expected} core attributes for {Count} nodes",
                total, expected, nodes.Count());
            if (total < expected && !token.IsCancellationRequested)
            {
                throw new ServiceCallFailureException(
                    $"Too few results in ReadNodeData, this is a bug in the OPC-UA server implementation, total : {total}, expected: {expected}");
            }

            int idx = 0;
            foreach (var node in nodes)
            {
                var attributes = node.Attributes.GetAttributeSet(Config);
                bool unknown = false;
                foreach (var attr in attributes)
                {
                    var val = values[idx];

                    if (val.StatusCode == StatusCodes.BadNodeIdUnknown && !unknown)
                    {
                        unknown = true;
                        log.LogWarning("Node {Id} {Name} does not exist on the server", node.Id, node.Name);
                        node.Ignore = true;
                    }

                    node.Attributes.LoadAttribute(values[idx], attr, TypeManager);
                    idx++;
                }
                node.Attributes.IsDataRead = true;
            }
        }

        /// <summary>
        /// Get the raw values for each given node id.
        /// Nodes must be variables
        /// </summary>
        /// <param name="ids">Nodes to get values for</param>
        /// <returns>A map from given nodeId to DataValue</returns>
        public async Task<Dictionary<NodeId, DataValue>> ReadRawValues(IEnumerable<NodeId> ids, CancellationToken token,
            string purpose = "")
        {
            var readValueIds = ids.Distinct().Select(id => new ReadValueId { AttributeId = Attributes.Value, NodeId = id }).ToList();
            var values = await ReadAttributes(new ReadValueIdCollection(readValueIds), ids.Count(), token, purpose);
            return values.Select((dv, index) => (ids.ElementAt(index), dv)).ToDictionary(pair => pair.Item1, pair => pair.dv);
        }

        /// <summary>
        /// Gets the values of the given list of variables, then updates each variable with a BufferedDataPoint
        /// </summary>
        /// <remarks>
        /// Note that there is a fixed maximum message size, and we here fetch a large number of values at the same time.
        /// To avoid complications, avoid fetching data of unknown large size here.
        /// </remarks>
        /// <param name="nodes">List of variables to be updated</param>
        public async Task ReadNodeValues(IEnumerable<BaseUANode> nodes, CancellationToken token)
        {
            nodes = nodes.DistinctBy(node => node.Attributes).ToList();
            if (!nodes.Any()) return;
            log.LogInformation("Get the current values of {Count} variables", nodes.Count());
            var readValueIds = new ReadValueIdCollection(
                nodes.Select(node => new ReadValueId { AttributeId = Attributes.Value, NodeId = node.Id }));

            var attributes = new List<uint> { Attributes.Value };
            var values = await ReadAttributes(readValueIds, nodes.Count(), token, "node values");

            var idx = 0;
            foreach (var node in nodes)
            {
                node.Attributes.LoadAttribute(values[idx], Attributes.Value, TypeManager);
                idx++;
            }
        }
        #endregion

        #region Synchronization

        /// <summary>
        /// Abort a history read chunk, releasing continuation points
        /// </summary>
        /// <param name="readParams"></param>
        /// <exception cref="InvalidOperationException"></exception>
        public async Task AbortHistoryRead(HistoryReadParams readParams, CancellationToken token)
        {
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            using var operation = waiter.GetInstance();

            var ids = new HistoryReadValueIdCollection();
            foreach (var node in readParams.Nodes)
            {
                if (node.ContinuationPoint == null) continue;
                ids.Add(new HistoryReadValueId
                {
                    NodeId = node.Id,
                    ContinuationPoint = node.ContinuationPoint
                });
                node.ContinuationPoint = null;
            }
            if (!ids.Any()) return;

            if (readParams.Details is ReadRawModifiedDetails dpDetails)
            {
                dpDetails.NumValuesPerNode = 1;
            }
            else if (readParams.Details is ReadEventDetails evtDetails)
            {
                evtDetails.NumValuesPerNode = 1;
            }

            try
            {
                await Session.HistoryReadAsync(
                    null,
                    new ExtensionObject(readParams.Details),
                    TimestampsToReturn.Source,
                    true,
                    ids,
                    token);
            }
            catch (ServiceResultException ex)
            {
                historyReadFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, readParams.Details is ReadEventDetails
                    ? ExtractorUtils.SourceOp.HistoryReadEvents
                    : ExtractorUtils.SourceOp.HistoryRead);
            }
            catch
            {
                historyReadFailures.Inc();
                throw;
            }
        }


        /// <summary>
        /// Modifies passed HistoryReadParams while doing a single config-limited iteration of history read.
        /// </summary>
        /// <param name="readParams"></param>
        /// <returns>Pairs of NodeId and history read results as IEncodable</returns>
        public async Task DoHistoryRead(HistoryReadParams readParams, CancellationToken token)
        {
            using var operation = waiter.GetInstance();

            var ids = new HistoryReadValueIdCollection();
            foreach (var node in readParams.Nodes)
            {
                ids.Add(new HistoryReadValueId
                {
                    NodeId = node.Id,
                    ContinuationPoint = node.ContinuationPoint
                });
            }

            var results = await RetryUtil.RetryResultAsync(
                readParams.Details is ReadEventDetails ? "read history events" : "read history datapoints",
                () => ReadHistoryChunk(readParams, ids, token),
                Config.Source.Retries,
                Config.Source.Retries.ShouldRetryException,
                log,
                token);

            for (int i = 0; i < readParams.Nodes.Count; i++)
            {
                var data = results[i];
                var node = readParams.Nodes[i];
                node.LastStatus = data.StatusCode;

                if (StatusCode.IsBad(data.StatusCode)) continue;

                LogDump("HistoryRead node", node);
                LogDump("HistoryRead data", data);

                node.LastResult = ExtensionObject.ToEncodeable(data.HistoryData);
                node.ContinuationPoint = data.ContinuationPoint;
            }

            log.LogDebug("Fetched historical {Type} for {NodeCount} nodes",
                    readParams.Details is ReadEventDetails ? "events" : "datapoints",
                    readParams.Nodes.Count);

        }

        private async Task<HistoryReadResultCollection> ReadHistoryChunk(HistoryReadParams readParams, HistoryReadValueIdCollection ids, CancellationToken token)
        {
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            var response = await Session.HistoryReadAsync(
                    null,
                    new ExtensionObject(readParams.Details),
                    TimestampsToReturn.Source,
                    false,
                    ids,
                    token);
            var results = response.Results;
            LogDump("HistoryRead diagnostics", response.DiagnosticInfos);
            LogDump("HistoryRead header", response.ResponseHeader);

            numHistoryReads.Inc();

            return results;
        }
        #endregion

        #region Events

        /// <summary>
        /// Return systemContext. Can be used by SDK-tools for converting events.
        /// </summary>
        public ISystemContext SystemContext => Context.SystemContext;

        public NamespaceTable NamespaceTable => Context.NamespaceTable;

        /// <summary>
        /// Return MessageContext, used for serialization
        /// </summary>
        public IServiceMessageContext MessageContext => Context.MessageContext;
        /// <summary>
        /// Constructs a filter from the given list of permitted eventids, the already constructed field map and an optional receivedAfter property.
        /// </summary>
        /// <returns>The final event filter</returns>
        public EventFilter BuildEventFilter(Dictionary<NodeId, UAObjectType> eventFields)
        {
            /*
             * Essentially equivalent to SELECT Message, EventId, SourceNode, Time FROM [source] WHERE EventId IN eventIds;
             * using the internal query language in OPC-UA
             */
            var whereClause = new ContentFilter();

            if (eventFields.Keys.Count != 0 && ((Config.Events.EventIds?.Any() ?? false) || !Config.Events.AllEvents))
            {
                log.LogDebug("Limit event results to the following ids: {Ids}", string.Join(", ", eventFields.Keys));
                var eventListOperand = new SimpleAttributeOperand
                {
                    TypeDefinitionId = ObjectTypeIds.BaseEventType,
                    AttributeId = Attributes.Value
                };
                eventListOperand.BrowsePath.Add(BrowseNames.EventType);
                IEnumerable<FilterOperand> eventOperands = eventFields.Keys.Select(id =>
                    new LiteralOperand
                    {
                        Value = id
                    });

                whereClause.Push(FilterOperator.InList, eventOperands.Prepend(eventListOperand).ToArray<object>());
            }


            var fieldList = eventFields
                .Aggregate((IEnumerable<TypeField>)new List<TypeField>(), (agg, kvp) => agg.Concat(kvp.Value.CollectedFields))
                .Distinct();

            if (!fieldList.Any())
            {
                log.LogWarning("Missing valid event fields, no results will be returned");
            }
            var selectClauses = new SimpleAttributeOperandCollection();
            foreach (var field in fieldList)
            {
                if (Config.Events.ExcludeProperties.Contains(field.Name)
                    || Config.Events.BaseExcludeProperties.Contains(field.Name)) continue;
                var operand = new SimpleAttributeOperand
                {
                    AttributeId = Attributes.Value,
                    TypeDefinitionId = ObjectTypeIds.BaseEventType
                };
                operand.BrowsePath = field.BrowsePath;
                selectClauses.Add(operand);
            }

            return new EventFilter
            {
                WhereClause = whereClause,
                SelectClauses = selectClauses
            };
        }

        #endregion

        #region Utils

        public void AddExternalNamespaces(string[] table)
        {
            SessionManager.RegisterExternalNamespaces(table);
        }

        /// <summary>
        /// Convert a datavalue into a double representation, testing for edge cases.
        /// </summary>
        /// <param name="datavalue">Datavalue to be converted</param>
        /// <returns>Double value, will return 0 if the datavalue is invalid</returns>
        public static double ConvertToDouble(object datavalue)
        {
            if (datavalue == null) return 0;
            if (datavalue is Variant variant) return ConvertToDouble(variant.Value);
            // Check if the value is somehow an array
            if (datavalue is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                if (enumerator.MoveNext())
                {
                    return ConvertToDouble(enumerator.Current);
                }
                return 0;
            }
            // Give up if there is no clear way to convert it
            if (!typeof(IConvertible).IsAssignableFrom(datavalue.GetType())) return 0;
            try
            {
                return Convert.ToDouble(datavalue, CultureInfo.InvariantCulture);
            }
            catch
            {
                return 0;
            }
        }

        public string? GetUniqueId(ExpandedNodeId id, int index = -1)
        {
            return Context.GetUniqueId(id, index);
        }

        public string GetRelationshipId(UAReference reference)
        {
            return Context.GetRelationshipId(reference);
        }

        public NodeId ToNodeId(ExpandedNodeId id)
        {
            return Context.ToNodeId(id);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            try
            {
                Close(CancellationToken.None).Wait();
            }
            catch (Exception ex)
            {
                log.LogWarning("Failed to close UAClient: {Message}", ex.Message);
            }
            waiter.Dispose();
            if (AppConfig != null)
            {
                AppConfig.CertificateValidator.CertificateValidation -= CertificateValidationHandler;
            }
            subscriptionSem.Dispose();
            SessionManager.Dispose();
        }
        #endregion
    }
}
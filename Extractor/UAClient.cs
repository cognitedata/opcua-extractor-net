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
using Cognite.OpcUa.NodeSources;
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
using System.Text;
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
        protected ISession? Session => SessionManager?.Session;
        protected ApplicationConfiguration? AppConfig { get; set; }
        public TypeManager TypeManager { get; }

        public IClientCallbacks Callbacks { get; set; } = null!;

        private readonly SemaphoreSlim subscriptionSem = new SemaphoreSlim(1);
        private readonly Dictionary<NodeId, string> nodeOverrides = new Dictionary<NodeId, string>();
        public bool Started { get; private set; }
        private CancellationToken liveToken;

        private readonly Dictionary<ushort, string> nsPrefixMap = new Dictionary<ushort, string>();

        private static readonly Counter attributeRequests = Metrics
            .CreateCounter("opcua_attribute_requests", "Number of attributes fetched from the server");
        private static readonly Gauge numSubscriptions = Metrics
            .CreateGauge("opcua_subscriptions", "Number of variables with an active subscription");
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

        public SessionManager? SessionManager { get; private set; }

        private readonly ILogger<UAClient> log;
        private readonly ILogger<Tracing> traceLog;
        private LogLevel? traceLevel;

        public StringConverter StringConverter { get; }
        public Browser Browser { get; }

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
                metricsManager = new NodeMetricsManager(this, config.Subscriptions, config.Metrics.Nodes);
            }
            StringConverter = new StringConverter(provider.GetRequiredService<ILogger<StringConverter>>(), this, config);
            Browser = new Browser(provider.GetRequiredService<ILogger<Browser>>(), this, config);
            TypeManager = new TypeManager(config, this, provider.GetRequiredService<ILogger<TypeManager>>());
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
                if (SessionManager != null)
                {
                    await SessionManager.Close(token);
                }
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
        protected async Task LoadAppConfig()
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
        }

        /// <summary>
        /// Load security configuration for the Session, then start the server.
        /// </summary>
        private async Task StartSession(int timeout = -1)
        {
            if (Callbacks == null) throw new InvalidOperationException("Attempted to start UAClient without setting callbacks");

            // A restarted Session might mean a restarted server, so all server-relevant data must be cleared.
            // This includes any stored NodeId, which may refer to an outdated namespaceIndex
            nodeOverrides?.Clear();

            await LoadAppConfig();

            if (SessionManager == null)
            {
                SessionManager = new SessionManager(Config.Source, this, AppConfig!, log, liveToken, timeout);
            }
            else
            {
                SessionManager.Timeout = timeout;
            }

            await SessionManager.Connect();

            liveToken.ThrowIfCancellationRequested();

            Started = true;
            log.LogInformation("Successfully connected to server at {EndpointURL}", SessionManager.EndpointUrl);
        }

        private bool ShouldRetryException(Exception ex)
        {
            if (ex is ServiceResultException serviceExc)
            {
                var code = serviceExc.StatusCode;
                return Config.Source.Retries.FinalRetryStatusCodes.Contains(code);
            }
            else if (ex is ServiceCallFailureException failureExc)
            {
                return failureExc.Cause == ServiceCallFailure.SessionMissing;
            }
            else if (ex is SilentServiceException silentExc)
            {
                if (silentExc.InnerServiceException != null)
                {
                    return Config.Source.Retries.FinalRetryStatusCodes.Contains(silentExc.InnerServiceException.StatusCode);
                }
            }
            else if (ex is AggregateException aex)
            {
                // Only retry aggregate exceptions if one of the inner exceptions should be retried...
                var flat = aex.Flatten();
                return aex.InnerExceptions.Any(e => ShouldRetryException(e));
            }
            return false;
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
        /// <summary>
        /// Add externalId override for a single node
        /// </summary>
        /// <param name="nodeId">Id of node to be overridden</param>
        /// <param name="externalId">ExternalId to be used</param>
        public void AddNodeOverride(NodeId nodeId, string externalId)
        {
            if (nodeId == null || nodeId == NodeId.Null) return;
            nodeOverrides[nodeId] = externalId;
        }
        /// <summary>
        /// Remove all externalId overrides
        /// </summary>
        public void ClearNodeOverrides()
        {
            nodeOverrides.Clear();
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
            await RetryUtil.RetryAsync("browse", async () => await GetReferencesInternal(browseParams, readToCompletion, token), Config.Source.Retries, ShouldRetryException, log, token);
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

            if (toBrowse.Any())
            {
                var results = await RetryUtil.RetryResultAsync("browse", async () => await GetReferencesChunk(browseParams, toBrowse, token), Config.Source.Retries, ShouldRetryException, log, token);

                var next = HandleBrowseResult(toBrowse, results, ExtractorUtils.SourceOp.Browse);
                if (readToCompletion) toBrowseNext.AddRange(next);
            }

            while (toBrowseNext.Any())
            {
                var results = await RetryUtil.RetryResultAsync("browse next", async () => await GetNextReferencesChunk(toBrowseNext, token), Config.Source.Retries, ShouldRetryException, log, token);

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
            if (!toAbort.Any()) return;
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
                var chunk = await RetryUtil.RetryResultAsync($"read for {purpose}", async () => await ReadAttributesChunk(nextValues, token), Config.Source.Retries, ShouldRetryException, log, token);
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

                foreach (var node in readParams.Nodes)
                {
                    node.ContinuationPoint = null;
                    node.Completed = true;
                }
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
                ShouldRetryException,
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
                if (data.ContinuationPoint == null)
                {
                    node.Completed = true;
                    node.ContinuationPoint = null;
                }
                else
                {
                    node.ContinuationPoint = data.ContinuationPoint;
                }
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

        /// <summary>
        /// Add MonitoredItems to the given list of states.
        /// </summary>
        /// <param name="nodeList">States to subscribe to</param>
        /// <param name="subName">Name of subscription</param>
        /// <param name="handler">Callback for the items</param>
        /// <param name="builder">Method to build monitoredItems from states</param>
        /// <returns>Constructed subscription</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Bad analysis")]
        public async Task<Subscription> AddSubscriptions(
            IEnumerable<UAHistoryExtractionState> nodeList,
            string subName,
            MonitoredItemNotificationEventHandler handler,
            Func<UAHistoryExtractionState, MonitoredItem> builder,
            CancellationToken token,
            string purpose = "")
        {
            if (!string.IsNullOrEmpty(purpose)) purpose = $"{purpose} ";
            await subscriptionSem.WaitAsync(token);

            using var operation = waiter.GetInstance();
            try
            {
                var subscription = await RetryUtil.RetryResultAsync("create subscription", () => CreateSubscription(subName, token), Config.Source.Retries, ShouldRetryException, log, token);

                int count = 0;
                var hasSubscription = subscription.MonitoredItems.Select(sub => sub.ResolvedNodeId).ToHashSet();
                int total = nodeList.Count();
                foreach (var chunk in nodeList.ChunkBy(Config.Source.SubscriptionChunk))
                {
                    if (token.IsCancellationRequested) break;
                    int lcount = 0;
                    subscription.AddItems(chunk
                        .Where(node => !hasSubscription.Contains(node.SourceId))
                        .Select(node =>
                        {
                            var monitor = builder(node);
                            monitor.Notification += handler;
                            lcount++;
                            return monitor;
                        })
                    );
                    log.LogDebug("Add {Purpose}subscriptions for {NumNodes} nodes, {Subscribed} / {Total} done.",
                        purpose, lcount, count, total);
                    count += lcount;

                    if (lcount > 0)
                    {
                        await RetryUtil.RetryAsync("create monitored items", () => CreateMonitoredItemsChunk(subscription, token), Config.Source.Retries, ShouldRetryException, log, token);
                    }
                }
                return subscription;
            }
            finally
            {
                subscriptionSem.Release();
            }
        }

        private async Task CreateMonitoredItemsChunk(Subscription subscription, CancellationToken token)
        {
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            try
            {
                await subscription.CreateItemsAsync(token);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateMonitoredItems);
            }
        }

        private async Task<Subscription> CreateSubscription(string name, CancellationToken token)
        {
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            var subscription = Session.Subscriptions
                .FirstOrDefault(sub => sub.DisplayName.StartsWith(name, StringComparison.InvariantCulture));

            if (subscription == null)
            {
                subscription = new Subscription(Session.DefaultSubscription)
                {
                    PublishingInterval = Config.Source.PublishingInterval,
                    DisplayName = name,
                    KeepAliveCount = Config.Subscriptions.KeepAliveCount,
                    LifetimeCount = Config.Subscriptions.LifetimeCount
                };
                subscription.PublishStatusChanged += OnSubscriptionPublishStatusChange;
            }
            if (!subscription.Created)
            {
                try
                {
                    Session.AddSubscription(subscription);
                    await subscription.CreateAsync(token);
                }
                catch (Exception ex)
                {
                    try
                    {
                        await Session.RemoveSubscriptionAsync(subscription);
                    }
                    finally
                    {
                        subscription.PublishStatusChanged -= OnSubscriptionPublishStatusChange;
                        subscription.Dispose();
                    }
                    throw ExtractorUtils.HandleServiceResult(log, ex,
                        ExtractorUtils.SourceOp.CreateSubscription);
                }
            }

            return subscription;
        }

        private readonly HashSet<uint> pendingRecreates = new HashSet<uint>();

        private async Task RecreateSubscription(Subscription sub, CancellationToken token)
        {
            if (Session == null || !Session.Connected || token.IsCancellationRequested) return;

            if (!sub.PublishingStopped) return;

            log.LogWarning("Subscription no longer responding: {Name}. Trying to re-enable.", sub.DisplayName);
            try
            {
                using var operation = waiter.GetInstance();
                // To avoid creating tons of blocking threads if this is very slow.
                lock (pendingRecreates)
                {
                    if (!pendingRecreates.Add(sub.Id)) return;
                }

                await subscriptionSem.WaitAsync(token);
                // The subscription might already have been deleted, best to check after receiving the lock.
                if (!Session.Subscriptions.Any(s => s.Id == sub.Id)) return;

                // Send keep-alive message to make sure the session isn't just down.
                try
                {
                    var result = await Session.ReadAsync(null, 0, TimestampsToReturn.Neither, new ReadValueIdCollection {
                        new ReadValueId {
                            NodeId = Variables.Server_ServerStatus_State,
                            AttributeId = Attributes.Value
                        }
                    }, token);
                    var dv = result.Results.First();
                    var state = (ServerState)(int)dv.Value;
                    // If the server is in a bad state that is why the subscription is failing
                    if (state != ServerState.Running) return;
                }
                catch (Exception ex)
                {
                    log.LogError("Failed to obtain server state. Server is likely down: ", ex.Message);
                    return;
                }

                // Try to modify the subscription
                try
                {
                    log.LogWarning("Server is available, but subscription is not responding to notifications. Attempting to recreate.");
                    await Session.RemoveSubscriptionAsync(sub);
                }
                catch (ServiceResultException serviceEx)
                {
                    var symId = StatusCode.LookupSymbolicId(serviceEx.StatusCode);
                    log.LogWarning("Error attempting to remove subscription from the server: {Err}. It has most likely been dropped. " +
                        "This is not legal behavior and likely to be a bug in the server. Attempting to recreate...", symId);
                }
                finally
                {
                    sub.Dispose();
                }

                // Create a new subscription with the same name and monitored items
                var name = sub.DisplayName.Split(' ').First();
                var newSub = await RetryUtil.RetryResultAsync("create subscription", () => CreateSubscription(name, token), Config.Source.Retries, ShouldRetryException, log, token);

                int count = 0;
                uint total = sub.MonitoredItemCount;

                foreach (var chunk in sub.MonitoredItems.ChunkBy(Config.Source.SubscriptionChunk))
                {
                    if (token.IsCancellationRequested) return;
                    var lcount = chunk.Count();
                    log.LogDebug("Recreate subscriptions for {Name} for {NumNodes} nodes, {Subscribed} / {Total} done.",
                        name, lcount, count, total);
                    count += lcount;
                    newSub.AddItems(chunk);

                    await RetryUtil.RetryAsync("create monitored items", () => CreateMonitoredItemsChunk(newSub, token), Config.Source.Retries, ShouldRetryException, log, token);
                }
            }
            catch (Exception ex)
            {
                ExtractorUtils.LogException(log, ex, "Failed to recreate subscription");
            }
            finally
            {
                subscriptionSem.Release();
                lock (pendingRecreates)
                {
                    pendingRecreates.Remove(sub.Id);
                }
            }
        }


        private void OnSubscriptionPublishStatusChange(object sender, EventArgs e)
        {
            if (sender is not Subscription sub || !sub.PublishingStopped) return;

            if (Callbacks?.TaskScheduler == null)
            {
                log.LogWarning("Failed to recreate subscription, client callbacks are not set");
                return;
            }

            Callbacks.TaskScheduler.ScheduleTask(null, t => RecreateSubscription(sub, t));
        }

        /// <summary>
        /// Create datapoint subscriptions for given list of nodes
        /// </summary>
        /// <param name="nodeList">List of buffered variables to synchronize</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Bad analysis")]
        public async Task SubscribeToNodes(IEnumerable<VariableExtractionState> nodeList,
            MonitoredItemNotificationEventHandler subscriptionHandler,
            CancellationToken token)
        {
            if (!nodeList.Any()) return;

            var sub = await AddSubscriptions(
                nodeList,
                "DataChangeListener",
                subscriptionHandler,
                node =>
                {
                    var config = Config.Subscriptions.GetMatchingConfig(node);
                    return new MonitoredItem
                    {
                        StartNodeId = node.SourceId,
                        DisplayName = "Value: " + (node as VariableExtractionState)?.DisplayName,
                        SamplingInterval = config.SamplingInterval,
                        QueueSize = (uint)Math.Max(0, config.QueueLength),
                        AttributeId = Attributes.Value,
                        NodeClass = NodeClass.Variable,
                        CacheQueueSize = Math.Max(0, config.QueueLength),
                        Filter = config.DataChangeFilter?.Filter
                    };
                }, token, "datapoint");

            numSubscriptions.Set(sub.MonitoredItemCount);
        }
        /// <summary>
        /// Subscribe to events from the given list of emitters.
        /// </summary>
        /// <param name="emitters">List of emitters. These are the actual targets of the subscription.</param>
        /// <param name="subscriptionHandler">Subscription handler, should be a function returning void that takes a
        /// <see cref="MonitoredItem"/> and <see cref="MonitoredItemNotificationEventArgs"/></param>
        /// <returns>Map of fields, EventTypeId->(SourceTypeId, BrowseName)</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Bad analysis")]
        public async Task SubscribeToEvents(IEnumerable<EventExtractionState> emitters,
            MonitoredItemNotificationEventHandler subscriptionHandler,
            Dictionary<NodeId, UAObjectType> eventFields,
            CancellationToken token)
        {
            var filter = BuildEventFilter(eventFields);
            LogDump("Event filter", filter);

            await AddSubscriptions(
                emitters,
                "EventListener",
                subscriptionHandler,
                node =>
                {
                    var config = Config.Subscriptions.GetMatchingConfig(node);
                    return new MonitoredItem
                    {
                        StartNodeId = node.SourceId,
                        AttributeId = Attributes.EventNotifier,
                        DisplayName = "Events: " + node.Id,
                        SamplingInterval = config.SamplingInterval,
                        QueueSize = (uint)Math.Max(0, config.QueueLength),
                        Filter = filter,
                        NodeClass = NodeClass.Object
                    };
                },
                token, "event");
        }

        /// <summary>
        /// Deletes a subscription starting with the given name.
        /// The client manages three subscriptions: EventListener, DataChangeListener and AuditListener,
        /// if the subscription does not exist, nothing happens.
        /// </summary>
        /// <param name="name"></param>
        public async Task RemoveSubscription(string name)
        {
            if (TryGetSubscription(name, out var subscription) && subscription!.Created)
            {
                try
                {
                    await Session!.RemoveSubscriptionAsync(subscription);
                }
                catch
                {
                    // A failure to delete the subscription generally means it just doesn't exist.
                }
                finally
                {
                    subscription!.Dispose();
                }
            }
        }

        /// <summary>
        /// Tries to get an existing subscription from a list of session subscriptions.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="subscription"></param>
        public bool TryGetSubscription(string name, out Subscription? subscription)
        {
            subscription = Session?.Subscriptions?.FirstOrDefault(sub =>
                sub.DisplayName.StartsWith(name, StringComparison.InvariantCulture));
            return subscription != null;
        }
        #endregion

        #region Events

        private ISystemContext? dummyContext;
        /// <summary>
        /// Return systemContext. Can be used by SDK-tools for converting events.
        /// </summary>
        public ISystemContext? SystemContext => Session?.SystemContext ?? dummyContext;

        private IServiceMessageContext? dummyMessageContext;
        /// <summary>
        /// Return MessageContext, used for serialization
        /// </summary>
        public IServiceMessageContext? MessageContext => Session?.MessageContext ?? dummyMessageContext;
        /// <summary>
        /// Constructs a filter from the given list of permitted eventids, the already constructed field map and an optional receivedAfter property.
        /// </summary>
        /// <param name="nodeIds">Permitted SourceNode ids</param>
        /// <param name="receivedAfter">Optional, if defined, attempt to filter out events with [ReceiveTimeProperty] > receivedAfter</param>
        /// <returns>The final event filter</returns>
        public EventFilter BuildEventFilter(Dictionary<NodeId, UAObjectType> eventFields)
        {
            /*
             * Essentially equivalent to SELECT Message, EventId, SourceNode, Time FROM [source] WHERE EventId IN eventIds;
             * using the internal query language in OPC-UA
             */
            var whereClause = new ContentFilter();

            if (eventFields.Keys.Any() && ((Config.Events.EventIds?.Any() ?? false) || !Config.Events.AllEvents))
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
        /// <summary>
        /// Build ContentFilter to be used when subscribing to audit events.
        /// </summary>
        /// <returns>Final EventFilter</returns>
        private static EventFilter BuildAuditFilter()
        {
            var whereClause = new ContentFilter();
            var eventTypeOperand = new SimpleAttributeOperand
            {
                TypeDefinitionId = ObjectTypeIds.BaseEventType,
                AttributeId = Attributes.Value
            };
            eventTypeOperand.BrowsePath.Add(BrowseNames.EventType);
            var op1 = new LiteralOperand
            {
                Value = ObjectTypeIds.AuditAddNodesEventType
            };
            var op2 = new LiteralOperand
            {
                Value = ObjectTypeIds.AuditAddReferencesEventType
            };
            var elem1 = whereClause.Push(FilterOperator.Equals, eventTypeOperand, op1);
            var elem2 = whereClause.Push(FilterOperator.Equals, eventTypeOperand, op2);
            whereClause.Push(FilterOperator.Or, elem1, elem2);
            var selectClauses = new SimpleAttributeOperandCollection();
            foreach (string path in new[]
            {
                BrowseNames.EventType,
                BrowseNames.NodesToAdd,
                BrowseNames.ReferencesToAdd,
                BrowseNames.EventId
            })
            {
                var op = new SimpleAttributeOperand
                {
                    AttributeId = Attributes.Value,
                    TypeDefinitionId = ObjectTypeIds.BaseEventType
                };
                op.BrowsePath.Add(path);
                selectClauses.Add(op);
            }
            return new EventFilter
            {
                WhereClause = whereClause,
                SelectClauses = selectClauses
            };
        }
        /// <summary>
        /// Subscribe to audit events on the server node
        /// </summary>
        /// <param name="callback">Callback to use for subscriptions</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Bad analysis")]
        public async Task SubscribeToAuditEvents(MonitoredItemNotificationEventHandler callback, CancellationToken token)
        {
            if (Session == null) throw new ServiceCallFailureException("Session is not connected", ServiceCallFailure.SessionMissing);
            var filter = BuildAuditFilter();
            LogDump("Audit filter", filter);
            await subscriptionSem.WaitAsync(token);

            using var operation = waiter.GetInstance();

            Subscription? subscription = null;
            try
            {
                subscription = await RetryUtil.RetryResultAsync("create subscription", () => CreateSubscription("AuditListener", token), Config.Source.Retries, ShouldRetryException, log, token);

                if (subscription.MonitoredItemCount != 0) return;

                var item = new MonitoredItem
                {
                    StartNodeId = ObjectIds.Server,
                    Filter = filter,
                    AttributeId = Attributes.EventNotifier,
                    SamplingInterval = Config.Subscriptions.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, Config.Subscriptions.QueueLength),
                    NodeClass = NodeClass.Object,
                    DisplayName = "Audit: Server"
                };
                item.Notification += callback;
                subscription.AddItem(item);
                log.LogInformation("Subscribe to auditing events on the server node");

                await RetryUtil.RetryAsync("create monitored items", () => CreateMonitoredItemsChunk(subscription, token), Config.Source.Retries, ShouldRetryException, log, token);
            }
            catch (Exception)
            {
                log.LogError("Failed to create audit subscription");
                throw;
            }
            finally
            {
                if (subscription != null && !subscription.Created)
                {
                    subscription.Dispose();
                }
                subscriptionSem.Release();
            }
        }

        #endregion

        #region Utils

        public NamespaceTable? ExternalNamespaceUris { get; set; }

        public void AddExternalNamespaces(string[] table)
        {
            if (ExternalNamespaceUris == null)
            {
                ExternalNamespaceUris = new NamespaceTable(new[]
                {
                    "http://opcfoundation.org/UA/"
                });
                dummyContext = new DummySystemContext(ExternalNamespaceUris);
                dummyMessageContext = new DummyMessageContext(ExternalNamespaceUris);
            }
            if (table == null) return;
            foreach (var ns in table)
            {
                ExternalNamespaceUris.GetIndexOrAppend(ns);
            }
        }


        public NamespaceTable? NamespaceTable => Session?.NamespaceUris ?? ExternalNamespaceUris;
        /// <summary>
        /// Converts an ExpandedNodeId into a NodeId using the Session
        /// </summary>
        /// <param name="nodeid"></param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(ExpandedNodeId nodeid)
        {
            if (nodeid == null || nodeid.IsNull || NamespaceTable == null) return NodeId.Null;
            return ExpandedNodeId.ToNodeId(nodeid, NamespaceTable);
        }
        /// <summary>
        /// Converts identifier string and namespaceUri into NodeId. Identifier will be on form i=123 or s=abc etc.
        /// </summary>
        /// <param name="identifier">Full identifier on form i=123 or s=abc etc.</param>
        /// <param name="namespaceUri">Full namespaceUri</param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(string? identifier, string? namespaceUri)
        {
            if (identifier == null || namespaceUri == null || NamespaceTable == null) return NodeId.Null;
            int idx = NamespaceTable.GetIndex(namespaceUri);
            if (idx < 0)
            {
                log.LogInformation("Namespace {NS} not found in {Table}", namespaceUri, NamespaceTable.ToArray());
                if (Config.Extraction.NamespaceMap.ContainsValue(namespaceUri))
                {
                    string readNs = Config.Extraction.NamespaceMap.First(kvp => kvp.Value == namespaceUri).Key;
                    idx = NamespaceTable.GetIndex(readNs);
                    if (idx < 0) return NodeId.Null;
                }
                else
                {
                    return NodeId.Null;
                }
            }

            string nsString = "ns=" + idx;
            return new NodeId(nsString + ";" + identifier);
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

        /// <summary>
        /// Returns consistent unique string representation of a <see cref="NodeId"/> given its namespaceUri
        /// </summary>
        /// <remarks>
        /// NodeId is, according to spec, unique in combination with its namespaceUri. We use this to generate a consistent, unique string
        /// to be used for mapping assets and timeseries in CDF to opcua nodes.
        /// To avoid having to send the entire namespaceUri to CDF, we allow mapping Uris to prefixes in the config file.
        /// </remarks>
        /// <param name="id">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        public string? GetUniqueId(ExpandedNodeId id, int index = -1)
        {
            var nodeId = ToNodeId(id);
            if (nodeId.IsNullNodeId) return null;
            if (nodeOverrides.TryGetValue(nodeId, out var nodeOverride))
            {
                if (index <= -1) return nodeOverride;
                return $"{nodeOverride}[{index}]";
            }

            // ExternalIds shorter than 32 chars are unlikely, this will generally avoid at least 1 re-allocation of the buffer,
            // and usually boost performance.
            var buffer = new StringBuilder(Config.Extraction.IdPrefix, 32);

            if (!nsPrefixMap.TryGetValue(nodeId.NamespaceIndex, out var prefix))
            {
                var namespaceUri = id.NamespaceUri ?? NamespaceTable!.GetString(nodeId.NamespaceIndex);
                string newPrefix;
                if (namespaceUri is not null)
                {
                    if (Config.Extraction.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode))
                    {
                        newPrefix = prefixNode;
                    }
                    else
                    {
                        newPrefix = namespaceUri + ":";
                    }
                }
                else
                {
                    log.LogWarning("NodeID received with null NamespaceUri, and index not in NamespaceTable. This is likely a bug in the server. {Id}, {Index}",
                        nodeId, nodeId.NamespaceIndex);
                    newPrefix = $"UNKNOWN_NS_{nodeId.NamespaceIndex}";
                }
                nsPrefixMap[nodeId.NamespaceIndex] = prefix = newPrefix;
            }

            buffer.Append(prefix);
            // Use 0 as namespace-index. This means that the namespace is not appended, as the string representation
            // of a base namespace nodeId does not include the namespace-index, which fits our use-case.
            NodeId.Format(buffer, nodeId.Identifier, nodeId.IdType, 0);

            TrimEnd(buffer);

            if (index > -1)
            {
                // Modifying buffer.Length effectively removes the last few elements, but more efficiently than modifying strings,
                // StringBuilder is just a char array.
                // 255 is max length, Log10(Max(1, index)) + 3 is the length of the index suffix ("[123]").
                buffer.Length = Math.Min(buffer.Length, 255 - ((int)Math.Log10(Math.Max(1, index)) + 3));
                buffer.AppendFormat(CultureInfo.InvariantCulture, "[{0}]", index);
            }
            else
            {
                buffer.Length = Math.Min(buffer.Length, 255);
            }
            return buffer.ToString();
        }
        /// <summary>
        /// Used to trim the whitespace off the end of a StringBuilder
        /// </summary> 
        private static void TrimEnd(StringBuilder sb)
        {
            if (sb == null || sb.Length == 0) return;

            int i = sb.Length - 1;
            for (; i >= 0; i--)
                if (!char.IsWhiteSpace(sb[i]))
                    break;

            if (i < sb.Length - 1)
                sb.Length = i + 1;

            return;
        }

        /// <summary>
        /// Append NodeId and namespace to the given StringBuilder.
        /// </summary>
        /// <param name="buffer">Builder to append to</param>
        /// <param name="nodeId">NodeId to append</param>
        private void AppendNodeId(StringBuilder buffer, NodeId nodeId)
        {
            if (nodeOverrides.TryGetValue(nodeId, out var nodeOverride))
            {
                buffer.Append(nodeOverride);
                return;
            }

            if (!nsPrefixMap.TryGetValue(nodeId.NamespaceIndex, out var prefix))
            {
                var namespaceUri = NamespaceTable!.GetString(nodeId.NamespaceIndex);
                string newPrefix = Config.Extraction.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");
                nsPrefixMap[nodeId.NamespaceIndex] = prefix = newPrefix;
            }

            buffer.Append(prefix);

            NodeId.Format(buffer, nodeId.Identifier, nodeId.IdType, 0);

            TrimEnd(buffer);
        }

        /// <summary>
        /// Get string representation of NodeId on the form i=123 or s=string, etc.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private string GetNodeIdString(NodeId id)
        {
            var buffer = new StringBuilder();
            AppendNodeId(buffer, id);
            return buffer.ToString();
        }

        /// <summary>
        /// Get the unique reference id, on the form [prefix][reference-name];[sourceId];[targetId]
        /// </summary>
        /// <param name="reference">Reference to get id for</param>
        /// <returns>String reference id</returns>
        public string GetRelationshipId(UAReference reference)
        {
            var buffer = new StringBuilder(Config.Extraction.IdPrefix, 64);
            buffer.Append(reference.Type.GetName(!reference.IsForward));
            buffer.Append(';');
            AppendNodeId(buffer, reference.Source.Id);
            buffer.Append(';');
            AppendNodeId(buffer, reference.Target.Id);

            if (buffer.Length > 255)
            {
                // This is an edge-case. If the id overflows, it is most sensible to cut from the
                // start of the id, as long ids are likely (from experience) to be similar to
                // system.subsystem.sensor.measurement...
                // so cutting from the start is less likely to cause conflicts
                var overflow = (int)Math.Ceiling((buffer.Length - 255) / 2.0);
                buffer = new StringBuilder(Config.Extraction.IdPrefix, 255);
                buffer.Append(reference.Type.GetName(!reference.IsForward));
                buffer.Append(';');
                buffer.Append(GetNodeIdString(reference.Source.Id).AsSpan(overflow));
                buffer.Append(';');
                buffer.Append(GetNodeIdString(reference.Target.Id).AsSpan(overflow));
            }
            return buffer.ToString();
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
            SessionManager?.Dispose();
        }
        #endregion
    }
}
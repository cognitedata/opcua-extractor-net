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
using Cognite.OpcUa.History;
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

namespace Cognite.OpcUa
{
    /// <summary>
    /// Client managing the connection to the opcua server, and providing wrapper methods to simplify interaction with the server.
    /// </summary>
    public class UAClient : IDisposable, IUAClientAccess
    {
        protected FullConfig Config { get; set; }
        protected Session? Session { get; set; }
        protected ApplicationConfiguration? AppConfig { get; set; }
        private ReverseConnectManager? reverseConnectManager;
        private SessionReconnectHandler? reconnectHandler;
        public DataTypeManager DataTypeManager { get; }
        public NodeTypeManager ObjectTypeManager { get; }

        private readonly SemaphoreSlim subscriptionSem = new SemaphoreSlim(1);
        private readonly Dictionary<NodeId, string> nodeOverrides = new Dictionary<NodeId, string>();
        public bool Started { get; private set; }
        private CancellationToken liveToken;
        private Dictionary<NodeId, UAEventType>? eventFields;

        private readonly Dictionary<ushort, string> nsPrefixMap = new Dictionary<ushort, string>();

        public event EventHandler? OnServerDisconnect;
        public event EventHandler? OnServerReconnect;

        private static readonly Counter connects = Metrics
            .CreateCounter("opcua_connects", "Number of times the client has connected to and mapped the opcua server");
        private static readonly Gauge connected = Metrics
            .CreateGauge("opcua_connected", "Whether or not the client is currently connected to the opcua server");
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
            DataTypeManager = new DataTypeManager(provider.GetRequiredService<ILogger<DataTypeManager>>(),
                this, config.Extraction.DataTypes);
            ObjectTypeManager = new NodeTypeManager(provider.GetRequiredService<ILogger<NodeTypeManager>>(), this);
            if (config.Metrics.Nodes != null)
            {
                metricsManager = new NodeMetricsManager(this, config.Source, config.Metrics.Nodes);
            }
            StringConverter = new StringConverter(provider.GetRequiredService<ILogger<StringConverter>>(), this, config);
            Browser = new Browser(provider.GetRequiredService<ILogger<Browser>>(), this, config);
        }
        #region Session management
        /// <summary>
        /// Entrypoint for starting the opcua Session. Must be called before any further requests can be made.
        /// </summary>
        public async Task Run(CancellationToken token)
        {
            liveToken = token;
            await StartSession();
            await StartNodeMetrics();
        }
        /// <summary>
        /// Close the Session, cleaning up any client data on the server
        /// </summary>
        public async Task Close(CancellationToken token)
        {
            reconnectHandler?.Dispose();
            reconnectHandler = null;
            try
            {
                if (Session != null && !Session.Disposed)
                {
                    var closeTask = Session.CloseSessionAsync(null, true, token);
                    var resultTask = await Task.WhenAny(Task.Delay(5000, token), closeTask);
                    if (closeTask != resultTask)
                    {
                        log.LogWarning("Failed to close session, timed out");
                    }
                    Session.Dispose();
                    Session = null;
                }
            }
            finally
            {
                connected.Set(0);
                Started = false;
            }

        }

        private void ConfigureUtilsTrace()
        {
            if (Config.Logger?.UaTraceLevel == null) return;
            Utils.SetTraceMask(Utils.TraceMasks.All);
            if (traceLevel != null) return;
            Utils.Tracing.TraceEventHandler += TraceEventHandler;
            switch (Config.Logger.UaTraceLevel)
            {
                case "verbose": traceLevel = LogLevel.Trace; break;
                case "debug": traceLevel = LogLevel.Debug; break;
                case "information": traceLevel = LogLevel.Information; break;
                case "warning": traceLevel = LogLevel.Warning; break;
                case "error": traceLevel = LogLevel.Error; break;
                case "fatal": traceLevel = LogLevel.Critical; break;
            }
        }

        private void TraceEventHandler(object sender, TraceEventArgs e)
        {
            if (e.Exception != null)
            {
#pragma warning disable CA2254 // Template should be a static expression - we are injecting format from a different logger
                traceLog.Log(traceLevel!.Value, e.Exception, e.Format, e.Arguments);
            }
            else
            {
                traceLog.Log(traceLevel!.Value, e.Format, e.Arguments);
#pragma warning restore CA2254 // Template should be a static expression
            }
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

            Utils.SetTraceOutput(Utils.TraceOutput.DebugAndFile);
            Utils.SetTraceMask(Utils.TraceMasks.All);
            Utils.SetTraceLog("./logs/opcua-client.log", true);

            string certificateDir = Environment.GetEnvironmentVariable("OPCUA_CERTIFICATE_DIR");
            if (!string.IsNullOrEmpty(certificateDir))
            {
                AppConfig.SecurityConfiguration.TrustedIssuerCertificates.StorePath = $"{certificateDir}/pki/issuer";
                AppConfig.SecurityConfiguration.TrustedPeerCertificates.StorePath = $"{certificateDir}/pki/trusted";
                AppConfig.SecurityConfiguration.RejectedCertificateStore.StorePath = $"{certificateDir}/pki/rejected";
            }

            bool validAppCert = await application.CheckApplicationInstanceCertificate(false, 0);
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

            ConfigureUtilsTrace();
        }

        private async Task CreateSessionDirect()
        {
            log.LogInformation("Attempt to select endpoint from: {EndpointURL}", Config.Source.EndpointUrl);
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(Config.Source.EndpointUrl, Config.Source.Secure);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.SelectEndpoint);
            }
            var endpointConfiguration = EndpointConfiguration.Create(AppConfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            var identity = AuthenticationUtils.GetUserIdentity(Config.Source);
            log.LogInformation("Attempt to connect to endpoint with security: {SecurityPolicyUri} using user identity {Identity}",
                endpoint.Description.SecurityPolicyUri,
                identity.DisplayName);
            try
            {
                if (Session?.Connected ?? false)
                {
                    Session.Close();
                }
                Session?.Dispose();


                Session = await Session.Create(
                    AppConfig,
                    endpoint,
                    false,
                    ".NET OPC-UA Extractor Client",
                    0,
                    identity,
                    null
                );
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }
        private async Task WaitForReverseConnect()
        {
            if (AppConfig == null) throw new InvalidOperationException("AppConfig must be initialized");
            reverseConnectManager?.Dispose();

            AppConfig.ClientConfiguration.ReverseConnect = new ReverseConnectClientConfiguration
            {
                WaitTimeout = 300000,
                HoldTime = 30000
            };

            reverseConnectManager = new ReverseConnectManager();
            var endpointUrl = new Uri(Config.Source.EndpointUrl);
            var reverseUrl = new Uri(Config.Source.ReverseConnectUrl);
            reverseConnectManager.AddEndpoint(reverseUrl);
            reverseConnectManager.StartService(AppConfig);

            log.LogInformation("Waiting for reverse connection from: {EndpointURL}", Config.Source.EndpointUrl);
            var connection = await reverseConnectManager.WaitForConnection(endpointUrl, null);
            if (connection == null)
            {
                log.LogError("Reverse connect failed, no connection established");
                throw new ExtractorFailureException("Failed to obtain reverse connection from server");
            }
            EndpointDescription selectedEndpoint;
            try
            {
                selectedEndpoint = CoreClientUtils.SelectEndpoint(AppConfig, connection, Config.Source.Secure, 30000);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.SelectEndpoint);
            }
            var endpointConfiguration = EndpointConfiguration.Create(AppConfig);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            var identity = AuthenticationUtils.GetUserIdentity(Config.Source);
            log.LogInformation("Attempt to connect to endpoint with security: {SecurityPolicyUri} using user identity {Identity}",
                endpoint.Description.SecurityPolicyUri,
                identity.DisplayName);

            try
            {
                if (Session?.Connected ?? false)
                {
                    Session.Close();
                }
                Session?.Dispose();

                connection = await reverseConnectManager.WaitForConnection(endpointUrl, null);
                if (connection == null)
                {
                    log.LogError("Reverse connect failed, no connection established");
                    throw new ExtractorFailureException("Failed to obtain reverse connection from server");
                }


                Session = await Session.Create(
                    AppConfig,
                    connection,
                    endpoint,
                    false,
                    false,
                    ".NET OPC-UA Extractor Client",
                    0,
                    identity,
                    null);
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateSession);
            }
        }

        /// <summary>
        /// Load security configuration for the Session, then start the server.
        /// </summary>
        private async Task StartSession()
        {
            Browser.ResetVisitedNodes();
            // A restarted Session might mean a restarted server, so all server-relevant data must be cleared.
            // This includes any stored NodeId, which may refer to an outdated namespaceIndex
            eventFields?.Clear();
            nodeOverrides?.Clear();

            await LoadAppConfig();

            if (!string.IsNullOrEmpty(Config.Source.ReverseConnectUrl))
            {
                await WaitForReverseConnect();
            }
            else
            {
                await CreateSessionDirect();
            }
            if (Session == null) return;
            Session.KeepAliveInterval = Config.Source.KeepAliveInterval;
            Session.KeepAlive += ClientKeepAlive;
            Started = true;
            connects.Inc();
            connected.Set(1);
            log.LogInformation("Successfully connected to server at {EndpointURL}", Config.Source.EndpointUrl);
        }

        /// <summary>
        /// Event triggered after a succesfull reconnect.
        /// </summary>
        private void ClientReconnectComplete(object sender, EventArgs eventArgs)
        {
            if (!ReferenceEquals(sender, reconnectHandler)) return;
            if (reconnectHandler == null) return;
            Session = reconnectHandler.Session;
            reconnectHandler.Dispose();
            log.LogWarning("--- RECONNECTED ---");

            OnServerReconnect?.Invoke(this, EventArgs.Empty);

            connects.Inc();
            connected.Set(1);
            reconnectHandler = null;
        }

        /// <summary>
        /// Called on client keep alive, handles the case where the server has stopped responding and the connection timed out.
        /// </summary>
        private void ClientKeepAlive(Session sender, KeepAliveEventArgs eventArgs)
        {
            if (eventArgs.Status == null || !ServiceResult.IsNotGood(eventArgs.Status)) return;
            log.LogWarning("Keep alive failed: {Status}", eventArgs.Status);
            if (reconnectHandler != null) return;
            connected.Set(0);
            log.LogWarning("--- RECONNECTING ---");
            if (!Config.Source.ForceRestart && !liveToken.IsCancellationRequested)
            {
                reconnectHandler = new SessionReconnectHandler();
                if (reverseConnectManager != null)
                {
                    reconnectHandler.BeginReconnect(sender, reverseConnectManager, 5000, ClientReconnectComplete);
                }
                else
                {
                    reconnectHandler.BeginReconnect(sender, 5000, ClientReconnectComplete);
                }
            }
            else
            {
                try
                {
                    Session?.Close();
                }
                catch
                {
                    log.LogWarning("Client failed to close, quitting");
                }
            }
            OnServerDisconnect?.Invoke(this, EventArgs.Empty);
        }
        /// <summary>
        /// Called after succesful validation of a server certificate. Handles the case where the certificate is untrusted.
        /// </summary>
        private void CertificateValidationHandler(CertificateValidator validator,
            CertificateValidationEventArgs eventArgs)
        {

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
            await metricsManager.StartNodeMetrics(liveToken);
        }
        #endregion

        #region Browse
        /// <summary>
        /// Retrieve a representation of the server node
        /// </summary>
        /// <returns></returns>
        public async Task<UANode> GetServerNode(CancellationToken token)
        {
            var desc = (await Browser.GetRootNodes(new[] { ObjectIds.Server }, token)).FirstOrDefault();
            if (desc == null) throw new ExtractorFailureException("Server node is null. Invalid server configuration");

            var node = new UANode(ObjectIds.Server, desc.DisplayName.Text, NodeId.Null, NodeClass.Object);
            await ReadNodeData(new[] { node }, token);
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
                throw new FatalException($"Incorrect number of results from {op} service. Got {results.Count}, expected {nodes.Count}. " +
                    $"This is illegal behavior, and caused by a bug in the server. Status: {code}");
            }

            for (int i = 0; i < nodes.Count; i++)
            {
                var result = results[i];
                var node = nodes[i];
                if (StatusCode.IsBad(result.StatusCode)
                    && result.StatusCode != StatusCodes.BadNodeIdUnknown)
                {
                    throw ExtractorUtils.HandleServiceResult(log, new ServiceResultException(result.StatusCode), op);
                }

                node.AddReferences(result.References);
                node.ContinuationPoint = result.ContinuationPoint;
                if (node.ContinuationPoint != null) ret.Add(node);
            }
            return ret;
        }

        public async Task GetReferences(BrowseParams browseParams, bool readToCompletion, CancellationToken token)
        {
            if (browseParams == null || browseParams.Nodes == null) throw new ArgumentNullException(nameof(browseParams));
            if (Session == null) throw new InvalidOperationException("Requires open session");

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
                var descriptions = new BrowseDescriptionCollection(toBrowse.Select(node => browseParams.ToDescription(node)));
                BrowseResultCollection results;
                try
                {
                    var result = await Session.BrowseAsync(
                        null,
                        null,
                        browseParams.MaxPerNode,
                        descriptions,
                        token);

                    results = result.Results;
                    numBrowse.Inc();
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
                catch (Exception ex)
                {
                    browseFailures.Inc();
                    throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.Browse);
                }

                var next = HandleBrowseResult(toBrowse, results, ExtractorUtils.SourceOp.Browse);
                if (readToCompletion) toBrowseNext.AddRange(next);
            }

            while (toBrowseNext.Any())
            {
                var cps = new ByteStringCollection(toBrowseNext.Select(node => node.ContinuationPoint));
                BrowseResultCollection results;
                try
                {
                    var result = await Session.BrowseNextAsync(
                        null,
                        false,
                        cps,
                        token);

                    results = result.Results;
                    numBrowse.Inc();
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
                catch (Exception ex)
                {
                    browseFailures.Inc();
                    throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.BrowseNext);
                }
                var next = HandleBrowseResult(toBrowseNext, results, ExtractorUtils.SourceOp.BrowseNext);
                if (readToCompletion) toBrowseNext = next;
                else break;
            }
        }

        public async Task AbortBrowse(IEnumerable<BrowseNode> nodes)
        {
            if (Session == null) throw new InvalidOperationException("Requires open session");
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
            if (Session == null) throw new InvalidOperationException("Requires open session");
            var values = new List<DataValue>();
            if (readValueIds == null || !readValueIds.Any()) return values;
            if (!string.IsNullOrEmpty(purpose)) purpose = $" for {purpose}";

            using var operation = waiter.GetInstance();
            int total = readValueIds.Count;
            int attrCount = 0;
            try
            {
                int count = 0;
                foreach (var nextValues in readValueIds.ChunkBy(Config.Source.AttributesChunk))
                {
                    if (token.IsCancellationRequested) break;
                    count++;
                    await Session.ReadAsync(
                        null,
                        0,
                        TimestampsToReturn.Source,
                        new ReadValueIdCollection(nextValues),
                        token);

                    Session.Read(
                        null,
                        0,
                        TimestampsToReturn.Source,
                        new ReadValueIdCollection(nextValues),
                        out DataValueCollection lvalues,
                        out _
                    );
                    attributeRequests.Inc();
                    values.AddRange(lvalues);
                    attrCount += lvalues.Count;
                    log.LogDebug("Read {NumAttributesRead} / {Total} attributes{Purpose}", attrCount, total, purpose);
                }
                log.LogInformation(
                    "Read {TotalAttributesRead} attributes with {NumAttributeReadOperations} operations for {NodeCount} nodes{Purpose}",
                    values.Count, count, distinctNodeCount, purpose);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
            catch (Exception ex)
            {
                attributeRequestFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadAttributes);
            }

            return values;
        }

        /// <summary>
        /// Gets attributes for the given list of nodes. The attributes retrieved for each node depends on its NodeClass.
        /// </summary>
        /// <param name="nodes">Nodes to be updated with data from the opcua server</param>
        /// <param name="purpose">Purpose, for logging</param>
        public async Task ReadNodeData(IEnumerable<UANode> nodes, CancellationToken token)
        {
            nodes = nodes.Where(node => (node is not UAVariable variable || variable.Index == -1) && !node.DataRead).ToList();

            int expected = 0;
            var readValueIds = new ReadValueIdCollection();
            foreach (var node in nodes)
            {
                var attributes = node.Attributes.GetAttributeIds(Config);
                readValueIds.AddRange(attributes.Select(attr => new ReadValueId { AttributeId = attr, NodeId = node.Id }));
                expected += attributes.Count();
            }

            IList<DataValue> values;
            try
            {
                values = await ReadAttributes(readValueIds, nodes.Count(), token, "node hierarchy information");
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadAttributes);
            }
            int total = values.Count;

            log.LogInformation("Retrieved {Total}/{Expected} core attributes for {Count} nodes",
                total, expected, nodes.Count());
            if (total < expected && !token.IsCancellationRequested)
            {
                throw new ExtractorFailureException(
                    $"Too few results in ReadNodeData, this is a bug in the OPC-UA server implementation, total : {total}, expected: {expected}");
            }

            int idx = 0;
            foreach (var node in nodes)
            {
                idx = node.Attributes.HandleAttributeRead(Config, values, idx, this);
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
        public async Task ReadNodeValues(IEnumerable<UAVariable> nodes, CancellationToken token)
        {
            nodes = nodes.Where(node => !node.ValueRead && node.Index == -1).ToList();
            if (!nodes.Any()) return;
            log.LogInformation("Get the current values of {Count} variables", nodes.Count());
            var readValueIds = new ReadValueIdCollection(
                nodes.Select(node => new ReadValueId { AttributeId = Attributes.Value, NodeId = node.Id }));
            IEnumerable<DataValue> values;
            try
            {
                var attributes = new List<uint> { Attributes.Value };
                values = await ReadAttributes(readValueIds, nodes.Count(), token, "node values");
            }
            catch (Exception ex)
            {
                throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.ReadAttributes);
            }

            var enumerator = values.GetEnumerator();
            foreach (var node in nodes)
            {
                node.ValueRead = true;
                enumerator.MoveNext();
                node.SetDataPoint(enumerator.Current?.WrappedValue ?? Variant.Null);
            }
            enumerator.Dispose();
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
            if (Session == null) throw new InvalidOperationException("Requires open session");
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
            if (Session == null) throw new InvalidOperationException("Requires open session");
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

            try
            {
                var response = await Session.HistoryReadAsync(
                    null,
                    new ExtensionObject(readParams.Details),
                    TimestampsToReturn.Source,
                    false,
                    ids,
                    token);
                var results = response.Results;

                numHistoryReads.Inc();
                for (int i = 0; i < readParams.Nodes.Count; i++)
                {
                    var data = results[i];
                    var node = readParams.Nodes[i];
                    if (StatusCode.IsBad(data.StatusCode))
                    {
                        throw new ServiceResultException(data.StatusCode);
                    }
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
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
            catch (Exception ex)
            {
                historyReadFailures.Inc();
                throw ExtractorUtils.HandleServiceResult(log, ex, readParams.Details is ReadEventDetails
                    ? ExtractorUtils.SourceOp.HistoryReadEvents
                    : ExtractorUtils.SourceOp.HistoryRead);
            }
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
            if (Session == null) throw new InvalidOperationException("Requires open session");

            if (!string.IsNullOrEmpty(purpose)) purpose = $"{purpose} ";
            await subscriptionSem.WaitAsync(token);

            Subscription? subscription = null;

            using var operation = waiter.GetInstance();
            try
            {
                subscription = Session.Subscriptions
                    .FirstOrDefault(sub => sub.DisplayName.StartsWith(subName, StringComparison.InvariantCulture));

                if (subscription == null)
                {
#pragma warning disable CA2000 // Dispose objects before losing scope
                    subscription = new Subscription(Session.DefaultSubscription)
                    {
                        PublishingInterval = Config.Source.PublishingInterval,
                        DisplayName = subName
                    };
#pragma warning restore CA2000 // Dispose objects before losing scope
                }

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

                    if (lcount > 0 && subscription.Created)
                    {
                        try
                        {
                            await subscription.CreateItemsAsync(token);
                        }
                        catch (Exception ex)
                        {
                            throw ExtractorUtils.HandleServiceResult(log, ex, ExtractorUtils.SourceOp.CreateMonitoredItems);
                        }
                    }
                    else if (lcount > 0)
                    {
                        try
                        {
                            Session.AddSubscription(subscription);
                            await subscription.CreateAsync(token);
                        }
                        catch (Exception ex)
                        {
                            throw ExtractorUtils.HandleServiceResult(log, ex,
                                ExtractorUtils.SourceOp.CreateSubscription);
                        }
                    }
                }
            }
            finally
            {
#pragma warning disable CA1508 // Avoid dead conditional code - the warning is due to a dotnet bug
                if (subscription != null && !subscription.Created)
#pragma warning restore CA1508 // Avoid dead conditional code
                {
                    subscription.Dispose();
                }
                subscriptionSem.Release();
            }
            return subscription;
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
                node => new MonitoredItem
                {
                    StartNodeId = node.SourceId,
                    DisplayName = "Value: " + (node as VariableExtractionState)?.DisplayName,
                    SamplingInterval = Config.Source.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, Config.Source.QueueLength),
                    AttributeId = Attributes.Value,
                    NodeClass = NodeClass.Variable,
                    CacheQueueSize = Math.Max(0, Config.Source.QueueLength),
                    Filter = Config.Subscriptions.DataChangeFilter?.Filter
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
            CancellationToken token)
        {
            var filter = BuildEventFilter();

            await AddSubscriptions(
                emitters,
                "EventListener",
                subscriptionHandler,
                node => new MonitoredItem
                {
                    StartNodeId = node.SourceId,
                    AttributeId = Attributes.EventNotifier,
                    DisplayName = "Events: " + node.Id,
                    SamplingInterval = Config.Source.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, Config.Source.QueueLength),
                    Filter = filter,
                    NodeClass = NodeClass.Object
                },
                token, "event");
        }

        /// <summary>
        /// Deletes a subscription starting with the given name.
        /// The client manages three subscriptions: EventListener, DataChangeListener and AuditListener,
        /// if the subscription does not exist, nothing happens.
        /// </summary>
        /// <param name="name"></param>
        public void RemoveSubscription(string name)
        {
            if (Session == null || Session.Subscriptions == null) return;
            var subscription = Session.Subscriptions.FirstOrDefault(sub =>
                                       sub.DisplayName.StartsWith(name, StringComparison.InvariantCulture));
            if (subscription == null || !subscription.Created) return;
            Session.RemoveSubscription(subscription);
        }
        #endregion

        #region Events
        /// <summary>
        /// Return systemContext. Can be used by SDK-tools for converting events.
        /// </summary>
        public ISystemContext? SystemContext => Session?.SystemContext;
        /// <summary>
        /// Return MessageContext, used for serialization
        /// </summary>
        public IServiceMessageContext? MessageContext => Session?.MessageContext;
        /// <summary>
        /// Fetch event fields from the server and store them on the client
        /// </summary>
        /// <param name="token"></param>
        /// <returns>The collected event fields</returns>
        public async Task<Dictionary<NodeId, UAEventType>> GetEventFields(IEventFieldSource? source, CancellationToken token)
        {
            if (eventFields != null) return eventFields;
            if (source == null)
            {
                source = new EventFieldCollector(log, this, Config.Events);
            }
            eventFields = await source.GetEventIdFields(token);
            foreach (var pair in eventFields)
            {
                log.LogTrace("Collected event field: {Id}", pair.Key);
                foreach (var fields in pair.Value.CollectedFields)
                {
                    log.LogTrace("    {Name}", fields.Name);
                }
            }
            return eventFields;
        }
        /// <summary>
        /// Remove collected event fields
        /// </summary>
        public void ClearEventFields()
        {
            eventFields = null;
        }
        /// <summary>
        /// Constructs a filter from the given list of permitted eventids, the already constructed field map and an optional receivedAfter property.
        /// </summary>
        /// <param name="nodeIds">Permitted SourceNode ids</param>
        /// <param name="receivedAfter">Optional, if defined, attempt to filter out events with [ReceiveTimeProperty] > receivedAfter</param>
        /// <returns>The final event filter</returns>
        public EventFilter BuildEventFilter()
        {
            /*
             * Essentially equivalent to SELECT Message, EventId, SourceNode, Time FROM [source] WHERE EventId IN eventIds;
             * using the internal query language in OPC-UA
             */
            var whereClause = new ContentFilter();

            if (eventFields == null) eventFields = new Dictionary<NodeId, UAEventType>();

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
                .Aggregate((IEnumerable<EventField>)new List<EventField>(), (agg, kvp) => agg.Concat(kvp.Value.CollectedFields))
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
            foreach ((var source, string path) in new[]
            {
                (ObjectTypeIds.BaseEventType, BrowseNames.EventType),
                (ObjectTypeIds.AuditAddNodesEventType, BrowseNames.NodesToAdd),
                (ObjectTypeIds.AuditAddReferencesEventType, BrowseNames.ReferencesToAdd)
            })
            {
                var op = new SimpleAttributeOperand
                {
                    AttributeId = Attributes.Value,
                    TypeDefinitionId = source
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
            if (Session == null) throw new InvalidOperationException("Requires open session");
            var filter = BuildAuditFilter();
            await subscriptionSem.WaitAsync(token);

            Subscription? subscription = null;
            try
            {
                subscription = Session.Subscriptions.FirstOrDefault(sub => sub.DisplayName.StartsWith("AuditListener", StringComparison.InvariantCulture))
                ?? new Subscription(Session.DefaultSubscription)
                {
                    PublishingInterval = Config.Source.PublishingInterval,
                    DisplayName = "AuditListener"
                };

                if (subscription.MonitoredItemCount != 0) return;
                var item = new MonitoredItem
                {
                    StartNodeId = ObjectIds.Server,
                    Filter = filter,
                    AttributeId = Attributes.EventNotifier,
                    SamplingInterval = Config.Source.SamplingInterval,
                    QueueSize = (uint)Math.Max(0, Config.Source.QueueLength),
                    NodeClass = NodeClass.Object
                };
                item.Notification += callback;
                subscription.AddItem(item);
                log.LogInformation("Subscribe to auditing events on the server node");

                using var operation = waiter.GetInstance();

                if (subscription.Created && subscription.MonitoredItemCount == 0)
                {
                    await subscription.CreateItemsAsync(token);
                }
                else if (!subscription.Created)
                {
                    log.LogInformation("Add audit events subscription to the Session");
                    Session.AddSubscription(subscription);
                    await subscription.CreateAsync(token);
                }
            }
            catch (Exception)
            {
                log.LogError("Failed to create audit subscription");
                throw;
            }
            finally
            {
#pragma warning disable CA1508 // Avoid dead conditional code - the warning is due to a dotnet bug
                if (subscription != null && !subscription.Created)
#pragma warning restore CA1508
                {
                    subscription.Dispose();
                }
                subscriptionSem.Release();
            }
        }

        #endregion

        #region Utils

        public NamespaceTable? NamespaceTable => Session?.NamespaceUris;
        /// <summary>
        /// Converts an ExpandedNodeId into a NodeId using the Session
        /// </summary>
        /// <param name="nodeid"></param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(ExpandedNodeId nodeid)
        {
            if (nodeid == null || nodeid.IsNull || Session == null) return NodeId.Null;
            return ExpandedNodeId.ToNodeId(nodeid, Session.NamespaceUris);
        }
        /// <summary>
        /// Converts identifier string and namespaceUri into NodeId. Identifier will be on form i=123 or s=abc etc.
        /// </summary>
        /// <param name="identifier">Full identifier on form i=123 or s=abc etc.</param>
        /// <param name="namespaceUri">Full namespaceUri</param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(string? identifier, string? namespaceUri)
        {
            if (identifier == null || namespaceUri == null || Session == null) return NodeId.Null;
            int idx = Session.NamespaceUris.GetIndex(namespaceUri);
            if (idx < 0)
            {
                if (Config.Extraction.NamespaceMap.ContainsValue(namespaceUri))
                {
                    string readNs = Config.Extraction.NamespaceMap.First(kvp => kvp.Value == namespaceUri).Key;
                    idx = Session.NamespaceUris.GetIndex(readNs);
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
                enumerator.MoveNext();
                return ConvertToDouble(enumerator.Current);
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
                var namespaceUri = id.NamespaceUri ?? Session!.NamespaceUris.GetString(nodeId.NamespaceIndex);
                string newPrefix = Config.Extraction.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");
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
                var namespaceUri = Session!.NamespaceUris.GetString(nodeId.NamespaceIndex);
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
            buffer.Append(reference.GetName());
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
                buffer.Append(reference.GetName());
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
            reconnectHandler?.Dispose();
            reverseConnectManager?.Dispose();
            waiter.Dispose();
            Utils.Tracing.TraceEventHandler -= TraceEventHandler;
            if (AppConfig != null)
            {
                AppConfig.CertificateValidator.CertificateValidation -= CertificateValidationHandler;
            }
            if (Session != null)
            {
                Session.KeepAlive -= ClientKeepAlive;
            }
            subscriptionSem.Dispose();
        }
        #endregion
    }
}
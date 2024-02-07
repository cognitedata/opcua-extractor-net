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

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Server;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    internal sealed class DummyValidator : ICertificateValidator
    {
        public void Validate(X509Certificate2 certificate)
        {
            throw ServiceResultException.Create(StatusCodes.BadCertificateInvalid, "Bad certificate");
        }

        public void Validate(X509Certificate2Collection certificates)
        {
            throw ServiceResultException.Create(StatusCodes.BadCertificateInvalid, "Bad certificate");
        }

        public Task ValidateAsync(X509Certificate2 certificate, CancellationToken ct)
        {
            throw ServiceResultException.Create(StatusCodes.BadCertificateInvalid, "Bad certificate");
        }

        public Task ValidateAsync(X509Certificate2Collection certificateChain, CancellationToken ct)
        {
            throw ServiceResultException.Create(StatusCodes.BadCertificateInvalid, "Bad certificate");
        }
    }

    internal sealed class NodeSetBundle
    {
        public NodeStateCollection Nodes { get; }
        public IEnumerable<string> NamespaceUris { get; }
        public NodeSetBundle(NodeStateCollection nodes, IEnumerable<string> namespaceUris)
        {
            Nodes = nodes;
            NamespaceUris = namespaceUris;
        }
    }

    /// <summary>
    /// The actual server itself, sets up the node managers and 
    /// contains a few methods defering calls to the custom node manager.
    /// </summary>
    public sealed class TestServer : ReverseConnectServer
    {
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in superclass
        private TestNodeManager custom;
#pragma warning restore CA2213 // Disposable fields should be disposed
        public NodeIdReference Ids => custom.Ids;
        public ServerIssueConfig Issues { get; } = new ServerIssueConfig();

        private readonly IEnumerable<PredefinedSetup> setups;

        private ICertificateValidator certificateValidator;
        private ApplicationConfiguration fullConfig;

        public bool AllowAnonymous { get; set; } = true;
        private readonly string mqttUrl;
        private readonly bool logTrace;
        private readonly ILogger traceLog;
        private readonly ILogger logger;
        private readonly IServiceProvider provider;
        private readonly IEnumerable<string> nodeSetFiles;

        public IServerRequestCallbacks Callbacks { get; set; }

        public TestServer(
            IEnumerable<PredefinedSetup> setups,
            string mqttUrl,
            IServiceProvider provider,
            bool logTrace = false,
            IEnumerable<string> nodeSetFiles = null)
        {
            this.setups = setups;
            this.mqttUrl = mqttUrl;
            this.logTrace = logTrace;
            logger = provider.GetRequiredService<ILogger<TestServer>>();
            this.provider = provider;
            traceLog = provider.GetRequiredService<ILogger<Tracing>>();
            this.nodeSetFiles = nodeSetFiles;

            Callbacks = new AggregateCallbacks(
                new MaxPerRequestCallbacks(Issues),
                new RandomFailureCallbacks(Issues),
                new FailureCountdownCallbacks(Issues)
            );
        }

        protected override void OnServerStarting(ApplicationConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            configuration.ServerConfiguration.ReverseConnect = new ReverseConnectServerConfiguration
            {
                ConnectInterval = 1000,
                Clients = new ReverseConnectClientCollection()
            };

            ConfigureUtilsTrace();

            base.OnServerStarting(configuration);
            fullConfig = configuration;
        }

        private LogLevel? traceLevel;
        private void ConfigureUtilsTrace()
        {
            if (!logTrace) return;
            Utils.SetTraceMask(Utils.TraceMasks.All);
            if (traceLevel != null) return;
            traceLevel = LogLevel.Trace;
            Utils.SetLogLevel(traceLevel.Value);
            Utils.SetLogger(traceLog);
        }

        public void SetValidator(bool failAlways)
        {
            if (failAlways)
            {
                certificateValidator = new DummyValidator();
            }
            else
            {
                var validator = new CertificateValidator();
                validator.Update(fullConfig.SecurityConfiguration);
                validator.Update(
                    fullConfig.SecurityConfiguration.UserIssuerCertificates,
                    fullConfig.SecurityConfiguration.TrustedUserCertificates,
                    fullConfig.SecurityConfiguration.RejectedCertificateStore);

                certificateValidator = validator;
            }
        }

        protected override void OnServerStarted(IServerInternal server)
        {
            ArgumentNullException.ThrowIfNull(server);

            base.OnServerStarted(server);

            // request notifications when the user identity is changed. all valid users are accepted by default.
            server.SessionManager.ImpersonateUser += new ImpersonateEventHandler(ImpersonateUser);
            // Auto accept untrusted, for testing
            CertificateValidator.AutoAcceptUntrustedCertificates = true;
        }

        protected override MasterNodeManager CreateMasterNodeManager(IServerInternal server, ApplicationConfiguration configuration)
        {
            custom = new TestNodeManager(server, configuration, setups, mqttUrl, provider, Issues, BuildNodeSetFiles(server.DefaultSystemContext));
            var nodeManagers = new List<INodeManager> { custom };
            // create the custom node managers.

            // create master node manager.
            return new DebugMasterNodeManager(server, configuration, null, Issues, this, nodeManagers.ToArray());
        }
        protected override ServerProperties LoadServerProperties()
        {
            ServerProperties properties = new ServerProperties
            {
                ManufacturerName = "Cognite",
                ProductName = "Test Server",
                SoftwareVersion = Utils.GetAssemblySoftwareVersion(),
                BuildNumber = Utils.GetAssemblyBuildNumber(),
                BuildDate = Utils.GetAssemblyTimestamp()
            };

            return properties;
        }


        private void ImpersonateUser(Session _, ImpersonateEventArgs args)
        {
            if (args.NewIdentity is UserNameIdentityToken userNameToken)
            {
                if (userNameToken.UserName != "testuser" || userNameToken.DecryptedPassword != "testpassword")
                    throw ServiceResultException.Create(StatusCodes.BadIdentityTokenRejected,
                        "Incorrect username or password");
            }
            if (args.NewIdentity is X509IdentityToken x509Token)
            {
                try
                {
                    certificateValidator.Validate(x509Token.Certificate);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message + " " + e.StackTrace);
                    throw ServiceResultException.Create(StatusCodes.BadIdentityTokenRejected,
                        "Bad or untrusted certificate");
                }
            }
            if (args.NewIdentity is AnonymousIdentityToken)
            {
                if (!AllowAnonymous) throw ServiceResultException.Create(StatusCodes.BadIdentityTokenRejected,
                        "Anonymous token not permitted");
            }
            args.Identity = new UserIdentity();
            args.Identity.GrantedRoleIds.Add(ObjectIds.WellKnownRole_ConfigureAdmin);
            args.Identity.GrantedRoleIds.Add(ObjectIds.WellKnownRole_SecurityAdmin);
        }

        private IEnumerable<NodeSetBundle> BuildNodeSetFiles(ISystemContext context)
        {
            if (nodeSetFiles == null || !nodeSetFiles.Any()) return Enumerable.Empty<NodeSetBundle>();
            var results = new List<NodeSetBundle>();

            foreach (var file in nodeSetFiles)
            {
                var predefinedNodes = new NodeStateCollection();
                using var stream = new FileStream(file, FileMode.Open);
                var nodeSet = Opc.Ua.Export.UANodeSet.Read(stream);
                foreach (var ns in nodeSet.NamespaceUris)
                {
                    context.NamespaceUris.GetIndexOrAppend(ns);
                }
                nodeSet.Import(context, predefinedNodes);
                results.Add(new NodeSetBundle(predefinedNodes, nodeSet.NamespaceUris));
            }

            return results;
        }

        public void UpdateNode(NodeId id, object value, StatusCode? code = null)
        {
            custom.UpdateNode(id, value, code: code);
        }

        public string GetNamespace(uint index)
        {
            return ServerInternal.NamespaceUris.GetString(index);
        }

        public IEnumerable<DataValue> GetHistory(NodeId id)
        {
            return custom.FetchHistory(id);
        }

        public IEnumerable<BaseEventState> GetEventHistory(NodeId id)
        {
            return custom.FetchEventHistory(id);
        }

        public void TriggerEvent<T>(NodeId eventId, NodeId emitter, NodeId source, string message, Action<T> builder = null)
            where T : ManagedEvent
        {
            custom.TriggerEvent(eventId, emitter, source, message, builder);
        }

        public void PopulateHistory(
            NodeId id,
            int count,
            DateTime start,
            string type = "int",
            int msdiff = 10,
            Func<int, object> valueBuilder = null,
            Func<int, StatusCode> statusBuilder = null,
            bool notifyLast = true)
        {
            custom.PopulateHistory(id, count, start, type, msdiff, valueBuilder, statusBuilder, notifyLast);
        }

        public void SetEventConfig(bool auditing, bool server, bool serverAuditing)
        {
            custom.SetEventConfig(auditing, server, serverAuditing);
        }

        public void SetServerRedundancyStatus(byte serviceLevel, RedundancySupport support)
        {
            custom.SetServerRedundancyStatus(serviceLevel, support);
        }
        public void PopulateEventHistory<T>(NodeId eventId,
            NodeId emitter,
            NodeId source,
            string message,
            int count,
            DateTime start,
            int msdiff = 10,
            Action<ManagedEvent, int> builder = null)
            where T : ManagedEvent
        {
            custom.PopulateEventHistory<T>(eventId, emitter, source, message, count, start, msdiff, builder);
        }

        public NodeId AddObject(NodeId parentId, string name, bool audit = false)
        {
            return custom.AddObject(parentId, name, audit);
        }

        public NodeId AddVariable(NodeId parentId, string name, NodeId dataType, bool audit = false)
        {
            return custom.AddVariable(parentId, name, dataType, audit);
        }
        public void AddReference(NodeId sourceId, NodeId parentId, NodeId type, bool audit = false)
        {
            custom.AddReference(sourceId, parentId, type, audit);
        }
        public NodeId AddProperty<T>(NodeId parentId, string name, NodeId dataType, object value, int rank = -1)
        {
            return custom.AddProperty<T>(parentId, name, dataType, value, rank);
        }
        public void RemoveProperty(NodeId parentId, string name)
        {
            custom.RemoveProperty(parentId, name);
        }
        public void RemoveNode(NodeId id)
        {
            custom.RemoveNode(id);
        }
        public void MutateNode(NodeId id, Action<NodeState> mutation)
        {
            ArgumentNullException.ThrowIfNull(mutation);
            custom.MutateNode(id, mutation);
        }
        public void ReContextualize(NodeId id, NodeId oldParentId, NodeId newParentId, NodeId referenceType)
        {
            custom.ReContextualize(id, oldParentId, newParentId, referenceType);
        }
        public void WipeHistory(NodeId id, object value)
        {
            custom.WipeHistory(id, value);
        }
        public void WipeEventHistory(NodeId id = null)
        {
            custom.WipeEventHistory(id);
        }
        public void SetDiagnosticsEnabled(bool value)
        {
            ServerInternal.NodeManager.DiagnosticsNodeManager.SetDiagnosticsEnabled(ServerInternal.DefaultSystemContext, value);
        }

        public void SetNamespacePublicationDate(DateTime time)
        {
            custom.SetNamespacePublicationDate(time);
        }

        public NodeId GetNamespacePublicationDateId()
        {
            var nsm = custom.GetNamespacePublicationDate();
            return nsm.NodeId;
        }

        public void DropSubscriptions()
        {
            var subs = ServerInternal.SubscriptionManager.GetSubscriptions();

            int cnt = 0;

            foreach (var session in ServerInternal.SessionManager.GetSessions())
            {
                var context = new OperationContext(session, DiagnosticsMasks.All);

                foreach (var sub in subs)
                {
                    if (sub.SessionId == session.Id)
                    {
                        ServerInternal.SubscriptionManager.DeleteSubscription(context, sub.Id);
                        cnt++;
                    }
                }
            }
            if (cnt > 0)
            {
                logger.LogDebug("Deleted {Cnt} subscriptions manually", cnt);
            }
        }

        #region overrides
        public override ResponseHeader CreateSubscription(RequestHeader requestHeader, double requestedPublishingInterval, uint requestedLifetimeCount, uint requestedMaxKeepAliveCount, uint maxNotificationsPerPublish, bool publishingEnabled, byte priority, out uint subscriptionId, out double revisedPublishingInterval, out uint revisedLifetimeCount, out uint revisedMaxKeepAliveCount)
        {
            var context = ValidateRequest(requestHeader, RequestType.CreateSubscription);
            Callbacks.OnCreateSubscription(context, requestedPublishingInterval, requestedLifetimeCount, requestedMaxKeepAliveCount, maxNotificationsPerPublish, publishingEnabled, priority);

            return base.CreateSubscription(requestHeader, requestedPublishingInterval, requestedLifetimeCount, requestedMaxKeepAliveCount, maxNotificationsPerPublish, publishingEnabled, priority, out subscriptionId, out revisedPublishingInterval, out revisedLifetimeCount, out revisedMaxKeepAliveCount);
        }

        public override ResponseHeader ModifyMonitoredItems(RequestHeader requestHeader, uint subscriptionId, TimestampsToReturn timestampsToReturn, MonitoredItemModifyRequestCollection itemsToModify, out MonitoredItemModifyResultCollection results, out DiagnosticInfoCollection diagnosticInfos)
        {
            return base.ModifyMonitoredItems(requestHeader, subscriptionId, timestampsToReturn, itemsToModify, out results, out diagnosticInfos);
        }

        public override ResponseHeader ModifySubscription(RequestHeader requestHeader, uint subscriptionId, double requestedPublishingInterval, uint requestedLifetimeCount, uint requestedMaxKeepAliveCount, uint maxNotificationsPerPublish, byte priority, out double revisedPublishingInterval, out uint revisedLifetimeCount, out uint revisedMaxKeepAliveCount)
        {
            return base.ModifySubscription(requestHeader, subscriptionId, requestedPublishingInterval, requestedLifetimeCount, requestedMaxKeepAliveCount, maxNotificationsPerPublish, priority, out revisedPublishingInterval, out revisedLifetimeCount, out revisedMaxKeepAliveCount);
        }

        public override ResponseHeader DeleteMonitoredItems(RequestHeader requestHeader, uint subscriptionId, UInt32Collection monitoredItemIds, out StatusCodeCollection results, out DiagnosticInfoCollection diagnosticInfos)
        {
            return base.DeleteMonitoredItems(requestHeader, subscriptionId, monitoredItemIds, out results, out diagnosticInfos);
        }

        public override ResponseHeader DeleteSubscriptions(RequestHeader requestHeader, UInt32Collection subscriptionIds, out StatusCodeCollection results, out DiagnosticInfoCollection diagnosticInfos)
        {
            return base.DeleteSubscriptions(requestHeader, subscriptionIds, out results, out diagnosticInfos);
        }
        #endregion
    }
}

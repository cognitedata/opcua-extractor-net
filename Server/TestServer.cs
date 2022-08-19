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

using Opc.Ua;
using Opc.Ua.Server;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace Server
{
    internal class DummyValidator : ICertificateValidator
    {
        public void Validate(X509Certificate2 certificate)
        {
            throw ServiceResultException.Create(StatusCodes.BadCertificateInvalid, "Bad certificate");
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
        private readonly IServiceProvider provider;

        public TestServer(IEnumerable<PredefinedSetup> setups, string mqttUrl, IServiceProvider provider, bool logTrace = false)
        {
            this.setups = setups;
            this.mqttUrl = mqttUrl;
            this.logTrace = logTrace;
            this.provider = provider;
            this.traceLog = provider.GetRequiredService<ILogger<Tracing>>();
        }

        protected override void OnServerStarting(ApplicationConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
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
            Utils.Tracing.TraceEventHandler += TraceEventHandler;
            traceLevel = LogLevel.Debug;
        }

        private Regex traceGroups = new Regex("{([0-9]+)}");
        private object[] ReOrderArguments(string format, object[] args)
        {
            // OPC-UA Trace uses the stringbuilder style of arguments, which allows them to be out of order
            // If we want nice coloring in logs (we do), then we have to re-order arguments like this.
            // There's a cost, but this is only enabled when debugging anyway.
            if (!args.Any()) return args;

            var matches = traceGroups.Matches(format);
            var indices = matches.Select(m => Convert.ToInt32(m.Groups[1].Value)).ToArray();

            return indices.Select(i => args[i]).ToArray();
        }

        private void TraceEventHandler(object sender, TraceEventArgs e)
        {
            object[] args = e.Arguments;
            try
            {
                args = ReOrderArguments(e.Format, e.Arguments);
            }
            catch
            {
            }

            if (e.Exception != null)
            {
#pragma warning disable CA2254 // Template should be a static expression - we are injecting format from a different logger
                traceLog.Log(traceLevel!.Value, e.Exception, e.Format, args);
            }
            else
            {
                traceLog.Log(traceLevel!.Value, e.Format, args);
#pragma warning restore CA2254 // Template should be a static expression
            }
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
            if (server == null) throw new ArgumentNullException(nameof(server));

            base.OnServerStarted(server);

            // request notifications when the user identity is changed. all valid users are accepted by default.
            server.SessionManager.ImpersonateUser += new ImpersonateEventHandler(ImpersonateUser);
            // Auto accept untrusted, for testing
            CertificateValidator.AutoAcceptUntrustedCertificates = true;
        }

        protected override MasterNodeManager CreateMasterNodeManager(IServerInternal server, ApplicationConfiguration configuration)
        {
            custom = new TestNodeManager(server, configuration, setups, mqttUrl, provider);
            var nodeManagers = new List<INodeManager> { custom };
            // create the custom node managers.

            // create master node manager.
            return new DebugMasterNodeManager(server, configuration, null, Issues, nodeManagers.ToArray());
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
        }

        public void UpdateNode(NodeId id, object value)
        {
            custom.UpdateNode(id, value);
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

        public void PopulateHistory(NodeId id, int count, DateTime start, string type = "int", int msdiff = 10, Func<int, object> valueBuilder = null)
        {
            custom.PopulateHistory(id, count, start, type, msdiff, valueBuilder);
        }

        public void SetEventConfig(bool auditing, bool server, bool serverAuditing)
        {
            custom.SetEventConfig(auditing, server, serverAuditing);
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
        public void MutateNode(NodeId id, Action<NodeState> mutation)
        {
            if (mutation == null) throw new ArgumentNullException(nameof(mutation));
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
                Console.WriteLine($"Deleted {cnt} subscriptions manually");
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                Utils.Tracing.TraceEventHandler -= TraceEventHandler;
            }
        }
    }
}

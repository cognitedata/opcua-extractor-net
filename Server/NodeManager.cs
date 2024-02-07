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
using System.Linq;

namespace Server
{
    /// <summary>
    /// Node manager containing necessary code for services in the extractor.
    /// CustomNodeManager2 is a built in class that contains a lot of default code for an in-memory
    /// OPC-UA server. It handles most of the use cases for the test server, the exceptions are:
    /// 
    /// Generating new nodes, History, and setting up the node hierarchy.
    /// </summary>
    internal sealed class TestNodeManager : CustomNodeManager2
    {
        private readonly HistoryMemoryStore store;
        private uint nextId;
        private readonly IEnumerable<PredefinedSetup> predefinedNodes;
        public NodeIdReference Ids { get; }
        private readonly ILogger log;

        private readonly PubSubManager pubSub;
        private IEnumerable<NodeSetBundle> nodeSetFiles;
        private readonly ServerIssueConfig issues;

        public TestNodeManager(IServerInternal server, ApplicationConfiguration configuration, IServiceProvider provider, ServerIssueConfig issues, IEnumerable<NodeSetBundle> nodeSetFiles = null)
            : base(server, configuration, GetNamespaces(nodeSetFiles))
        {
            SystemContext.NodeIdFactory = this;
            store = new HistoryMemoryStore(provider.GetRequiredService<ILogger<HistoryMemoryStore>>());
            log = provider.GetRequiredService<ILogger<TestNodeManager>>();
            Ids = new NodeIdReference();
            this.nodeSetFiles = nodeSetFiles;
            this.issues = issues;
        }

        public TestNodeManager(IServerInternal server,
            ApplicationConfiguration configuration,
            IEnumerable<PredefinedSetup> predefinedNodes,
            string mqttUrl,
            IServiceProvider provider,
            ServerIssueConfig issues,
            IEnumerable<NodeSetBundle> nodeSetFiles = null) :
            this(server, configuration, provider, issues, nodeSetFiles)
        {
            this.predefinedNodes = predefinedNodes;
            pubSub = new PubSubManager(mqttUrl, provider.GetRequiredService<ILogger<PubSubManager>>());
        }

        private static string[] GetNamespaces(IEnumerable<NodeSetBundle> nodeSetFiles)
        {
            if (nodeSetFiles == null)
            {
                return new[] { "opc.tcp://test.localhost" };
            }
            return nodeSetFiles.SelectMany(n => n.NamespaceUris).Prepend("opc.tcp://test.localhost").Distinct().ToArray();
        }

        #region access
        public void UpdateNode(NodeId id, object value, DateTime? timestamp = null, StatusCode? code = null)
        {
            PredefinedNodes.TryGetValue(id, out var pstate);
            if (pstate is not BaseDataVariableState state) return;
            var ts = timestamp ?? DateTime.UtcNow;
            state.Value = value;
            state.Timestamp = ts;
            state.StatusCode = code ?? StatusCodes.Good;
            if (state.Historizing)
            {
                store.UpdateNode(state);
            }
            state.ClearChangeMasks(SystemContext, false);
            pubSub.ReportDataChange(new DataValue(new Variant(value), StatusCodes.Good, ts, DateTime.UtcNow), id);
        }

        public IEnumerable<DataValue> FetchHistory(NodeId id)
        {
            return store.GetFullHistory(id);
        }

        public void WipeHistory(NodeId id, object value)
        {
            store.WipeHistory(id);
            var state = PredefinedNodes[id] as BaseDataVariableState;
            state.Value = value;
        }
        public void WipeEventHistory(NodeId id = null)
        {
            if (id == null)
            {
                store.WipeEventHistory();
            }
            else
            {
                store.WipeEventHistory(id);
            }
        }

        public IEnumerable<BaseEventState> FetchEventHistory(NodeId id)
        {
            return store.GetFullEventHistory(id);
        }

        public void TriggerEvent<T>(NodeId eventId, NodeId emitter, NodeId source, string message, Action<T> builder = null)
            where T : ManagedEvent
        {
            var eventState = (BaseObjectTypeState)PredefinedNodes[eventId];
            var emitterState = emitter == null || emitter.NamespaceIndex == 0 ? null : PredefinedNodes[emitter];
            var sourceState = source == null ? null : PredefinedNodes[source];

            var manager = new TestEventManager<T>(SystemContext, eventState, NamespaceUris.First());

            var evt = manager.CreateEvent(emitterState, sourceState, message);
            builder?.Invoke(evt);
            if (emitter == Ids.Event.Obj1 || emitter == ObjectIds.Server)
            {
                store.HistorizeEvent(emitter, evt);
            }
            if (emitterState == null)
            {
                Server.ReportEvent(SystemContext, evt);
            }
            else
            {
                emitterState.ReportEvent(SystemContext, evt);
            }
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
            for (int i = 0; i < count; i++)
            {
                var dv = new DataValue();
                switch (type)
                {
                    case "int":
                        dv.Value = i;
                        break;
                    case "double":
                        dv.Value = ((double)i) / 10;
                        break;
                    case "string":
                        dv.Value = $"str: {i}";
                        break;
                    case "custom":
                        dv.Value = valueBuilder(i);
                        break;
                }

                StatusCode code = statusBuilder == null ? StatusCodes.Good : statusBuilder(i);

                if (i == count - 1 && start > DateTime.UtcNow.AddSeconds(-1) && notifyLast)
                {
                    UpdateNode(id, dv.Value, start, code);
                }
                else
                {
                    dv.SourceTimestamp = start;
                    dv.ServerTimestamp = start;
                    dv.StatusCode = code;
                    store.HistorizeDataValue(id, dv);
                }
                start = start.AddMilliseconds(msdiff);
            }
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
            var eventState = (BaseObjectTypeState)PredefinedNodes[eventId];
            var emitterState = emitter == null || emitter.NamespaceIndex == 0 ? null : PredefinedNodes[emitter];
            var sourceState = source == null ? null : PredefinedNodes[source];

            var manager = new TestEventManager<T>(SystemContext, eventState, NamespaceUris.First());

            for (int i = 0; i < count; i++)
            {
                var evt = manager.CreateEvent(emitterState, sourceState, message + " " + i);
                builder?.Invoke(evt, i);
                evt.Time.Value = start;
                store.HistorizeEvent(emitter, evt);
                start = start.AddMilliseconds(msdiff);
            }
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public NodeId AddObject(NodeId parentId, string name, bool audit = false)
        {
            log.LogDebug("Add object: Parent: {Parent}, Name: {Name}", parentId, name);

            var parent = PredefinedNodes[parentId];
            var obj = CreateObject(name);
            AddNodeRelation(obj, parent, ReferenceTypeIds.Organizes);

            if (audit)
            {
                var evtAdd = new AddNodesItem
                {
                    ParentNodeId = parentId,
                    NodeClass = NodeClass.Object,
                    TypeDefinition = ObjectTypeIds.BaseObjectType,
                    ReferenceTypeId = ReferenceTypeIds.Organizes,
                };
                var evt = new AuditAddNodesEventState(null);

                evt.Initialize(SystemContext, null, EventSeverity.Medium, new LocalizedText($"Audit add: {name}"), true, DateTime.UtcNow);
                evt.SetChildValue(SystemContext, BrowseNames.SourceNode, ObjectIds.Server, false);
                evt.SetChildValue(SystemContext, BrowseNames.SourceName, "NodeManagement/AddNodes", false);
                evt.SetChildValue(SystemContext, BrowseNames.LocalTime, Utils.GetTimeZoneInfo(), false);
                evt.SetChildValue(SystemContext, BrowseNames.NodesToAdd, new[] { evtAdd }, false);

                AddPredefinedNode(SystemContext, obj);

                Server.ReportEvent(SystemContext, evt);
            }
            else
            {
                AddPredefinedNode(SystemContext, obj);
            }
            return obj.NodeId;
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public NodeId AddVariable(NodeId parentId, string name, NodeId dataType, bool audit = false)
        {
            log.LogDebug("Add variable: Parent: {Parent}, Name {Name}, DataType: {DataType}", parentId, name, dataType);

            var parent = PredefinedNodes[parentId];
            var obj = CreateVariable(name, dataType);
            AddNodeRelation(obj, parent, ReferenceTypeIds.HasComponent);

            if (audit)
            {
                var evtAdd = new AddNodesItem
                {
                    ParentNodeId = parentId,
                    NodeClass = NodeClass.Variable,
                    TypeDefinition = VariableTypeIds.BaseDataVariableType,
                    ReferenceTypeId = ReferenceTypeIds.HasComponent
                };
                var evt = new AuditAddNodesEventState(null);

                evt.Initialize(SystemContext, null, EventSeverity.Medium, new LocalizedText($"Audit add: {name}"), true, DateTime.UtcNow);
                evt.SetChildValue(SystemContext, BrowseNames.SourceNode, ObjectIds.Server, false);
                evt.SetChildValue(SystemContext, BrowseNames.SourceName, "NodeManagement/AddNodes", false);
                evt.SetChildValue(SystemContext, BrowseNames.LocalTime, Utils.GetTimeZoneInfo(), false);
                evt.SetChildValue(SystemContext, BrowseNames.NodesToAdd, new[] { evtAdd }, false);
                evt.SetChildValue(SystemContext, BrowseNames.EventType, ObjectTypeIds.AuditAddNodesEventType, false);

                AddPredefinedNode(SystemContext, obj);

                Server.ReportEvent(SystemContext, evt);
            }
            else
            {
                AddPredefinedNode(SystemContext, obj);
            }
            return obj.NodeId;
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public void AddReference(NodeId targetId, NodeId parentId, NodeId type, bool audit = false)
        {
            log.LogDebug("Add reference from {Parent} to {Target} of type {Type}", parentId, targetId, type);

            var target = PredefinedNodes[targetId];
            var parent = PredefinedNodes[parentId];
            if (audit)
            {
                var evtRef = new AddReferencesItem
                {
                    IsForward = true,
                    SourceNodeId = parentId,
                    TargetNodeId = targetId,
                    ReferenceTypeId = type
                };
                var evt = new AuditAddReferencesEventState(null);

                evt.Initialize(SystemContext, null, EventSeverity.Medium, new LocalizedText($"Audit add reference"), true, DateTime.UtcNow);
                evt.SetChildValue(SystemContext, BrowseNames.SourceNode, ObjectIds.Server, false);
                evt.SetChildValue(SystemContext, BrowseNames.SourceName, "NodeManagement/AddReferences", false);
                evt.SetChildValue(SystemContext, BrowseNames.LocalTime, Utils.GetTimeZoneInfo(), false);
                evt.SetChildValue(SystemContext, BrowseNames.ReferencesToAdd, new[] { evtRef }, false);
                AddNodeRelation(target, parent, type);

                Server.ReportEvent(SystemContext, evt);
            }
            else
            {
                AddNodeRelation(target, parent, type);
            }
        }
        public void MutateNode(NodeId id, Action<NodeState> mutation)
        {
            PredefinedNodes.TryGetValue(id, out var state);
            if (state == null) return;
            mutation(state);
        }
        public NodeId AddProperty<T>(NodeId parentId, string name, NodeId dataType, object value, int rank = -1)
        {
            log.LogDebug("Add property of type {Type} name {Name} and DataType {DataType} to {Parent}",
                typeof(T), name, dataType, parentId);

            var parent = PredefinedNodes[parentId];
            var prop = parent.AddProperty<T>(name, dataType, rank);
            prop.NodeId = GenerateNodeId();
            prop.Value = value;
            AddPredefinedNode(SystemContext, prop);
            return prop.NodeId;
        }
        public void RemoveProperty(NodeId parentId, string name)
        {
            log.LogDebug("Remove property of {Parent} with name {Name}", parentId, name);

            var parent = PredefinedNodes[parentId];
            var children = new List<BaseInstanceState>();
            parent.GetChildren(SystemContext, children);
            var prop = children.First(child => child.DisplayName.Text == name);
            parent.RemoveChild(prop);
            prop.Delete(SystemContext);
        }

        public void RemoveNode(NodeId id)
        {
            log.LogDebug("Remove node with ID {Id}", id);

            var node = PredefinedNodes[id];
            var refsToRemove = new List<LocalReference>();
            RemovePredefinedNode(SystemContext, node, refsToRemove);
            if (refsToRemove.Count != 0)
            {
                Server.NodeManager.RemoveReferences(refsToRemove);
            }
        }

        // There is a very stupid bug in the SDK, this is hacky workaround until they fix it.
        private static bool RemoveReferenceHack(NodeState state, NodeId referenceType, bool isInverse, NodeId target)
        {
            var references = (IReferenceDictionary<object>)typeof(NodeState)
                .GetField("m_references", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .GetValue(state);
            return references.Remove(new NodeStateReference(referenceType, isInverse, target));
        }

        public void ReContextualize(NodeId id, NodeId oldParentId, NodeId newParentId, NodeId referenceType)
        {
            log.LogDebug("Move node {Id} from {Old} to {New} with reference type {Ref}",
                id, oldParentId, newParentId, referenceType);

            var state = PredefinedNodes[id];
            var oldParent = PredefinedNodes[oldParentId];
            var newParent = PredefinedNodes[newParentId];
            if (!RemoveReferenceHack(oldParent, referenceType, false, id))
            {
                log.LogWarning("Failed to remove reference of type {Type} from {OldP} to {Id}",
                    referenceType, oldParent.NodeId, id);
            }
            if (!RemoveReferenceHack(state, referenceType, true, oldParentId))
            {
                log.LogWarning("Failed to remove reference of type {Type} from {OldP} to {Id}",
                    referenceType, state.NodeId, oldParentId);
            }

            log.LogDebug("Add forward ref");
            newParent.AddReference(referenceType, false, id);
            log.LogDebug("Add inverse ref");
            state.AddReference(referenceType, true, newParentId);
        }

        public void SetServerRedundancyStatus(byte serviceLevel, RedundancySupport support)
        {
            log.LogInformation("Set server redundancy statues: Level: {Level}, Support: {Support}", serviceLevel, support);
            lock (Server.DiagnosticsNodeManager.Lock)
            {
                ServerObjectState serverObject = (ServerObjectState)Server.DiagnosticsNodeManager.FindPredefinedNode(
                    ObjectIds.Server,
                    typeof(ServerObjectState));

                // update server capabilities.
                serverObject.ServiceLevel.Value = serviceLevel;
                serverObject.ServiceLevel.ClearChangeMasks(SystemContext, true);
                serverObject.ServerRedundancy.RedundancySupport.Value = support;
            }
        }

        public void SetEventConfig(bool auditing, bool server, bool serverAuditing)
        {
            log.LogInformation("Set server event options. Auditing: {Auditing}, Server emitting events: {Server}", auditing, server);
            var cfnm = (ConfigurationNodeManager)Server.NodeManager.NodeManagers.First(nm => nm.GetType() == typeof(ConfigurationNodeManager));
            lock (cfnm.Lock)
            {
                var serverAud = (PropertyState)cfnm.Find(VariableIds.Server_Auditing);
                serverAud.Value = auditing;
            }

            Server.GetType().GetField("m_auditing", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .SetValue(Server, auditing);

            var dfnm = Server.DiagnosticsNodeManager;
            lock (dfnm.Lock)
            {
                var serverAud = (PropertyState)cfnm.Find(VariableIds.Server_Auditing);
                log.LogInformation("Auditing node: {Val}, {Id}, {Parent}", serverAud.Value, serverAud.NodeId, serverAud.Parent.NodeId);
                serverAud.Value = auditing;
            }

            var serverNode = (BaseObjectState)cfnm.Find(ObjectIds.Server);
            if (server)
            {
                serverNode.EventNotifier |= EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;

            }
            else
            {
                serverNode.EventNotifier = EventNotifiers.None;
            }
            if (serverAuditing)
            {
                serverNode.AddReferences(new[]
                {
                    new NodeStateReference(ReferenceTypeIds.GeneratesEvent, false, ObjectTypeIds.AuditAddNodesEventType),
                    new NodeStateReference(ReferenceTypeIds.GeneratesEvent, false, ObjectTypeIds.AuditAddReferencesEventType),
                });
            }
            else
            {
                serverNode.RemoveReferences(ReferenceTypeIds.GeneratesEvent, true);
                serverNode.RemoveReferences(ReferenceTypeIds.GeneratesEvent, false);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        private void CreateNamespaceMetadataNode(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            var serverNamespacesNode = Server.NodeManager.ConfigurationNodeManager
                    .FindPredefinedNode(ObjectIds.Server_Namespaces, typeof(NamespacesState)) as NamespacesState;
            var namespaceUri = "opc.tcp://test.localhost";
            var namespaceMetadataState = new NamespaceMetadataState(serverNamespacesNode);
            namespaceMetadataState.BrowseName = new QualifiedName(namespaceUri, NamespaceIndex);
            namespaceMetadataState.Create(SystemContext, null, namespaceMetadataState.BrowseName, null, true);
            namespaceMetadataState.DisplayName = namespaceUri;
            namespaceMetadataState.SymbolicName = namespaceUri;
            namespaceMetadataState.NamespaceUri.Value = namespaceUri;

            // add node as child of ServerNamespaces and in predefined nodes
            serverNamespacesNode.AddChild(namespaceMetadataState);
            serverNamespacesNode.ClearChangeMasks(Server.DefaultSystemContext, true);
            AddPredefinedNode(SystemContext, namespaceMetadataState);

            Ids.NamespaceMetadata = namespaceMetadataState.NodeId;
        }

        private NamespaceMetadataState GetNamespaceMetadata()
        {
            return FindPredefinedNode(Ids.NamespaceMetadata, typeof(NamespaceMetadataState)) as NamespaceMetadataState;
        }

        public void SetNamespacePublicationDate(DateTime time)
        {
            var ns = GetNamespaceMetadata();
            ns.NamespacePublicationDate.Value = time;
            ns.ClearChangeMasks(SystemContext, true);
        }

        public PropertyState<DateTime> GetNamespacePublicationDate()
        {
            var ns = GetNamespaceMetadata();
            return ns.NamespacePublicationDate;
        }
        #endregion


        private NodeId GenerateNodeId()
        {
            return new NodeId(++nextId, NamespaceIndex);
        }

        #region address_space
        /// <summary>
        /// This method calls the various methods to create the test address spaces.
        /// </summary>
        /// <param name="externalReferences"></param>
        public override void CreateAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create address space");
            try
            {
                LoadPredefinedNodes(SystemContext, externalReferences);

                // This is completely undocumented, but it is also the only way I found to change properties on the server object
                // Supposedly the "DiagnosticNodeManager" should handle this sort of stuff. But it doesn't, there exists a weird
                // GetDefaultHistoryCapability, but that seems to create a /new/ duplicate node on the server. This changes the existing one
                // (Creating a new one makes no sense whatsoever).
                var cfnm = Server.NodeManager.ConfigurationNodeManager;
                lock (cfnm.Lock)
                {
                    var accessDataCap = (PropertyState)cfnm.Find(VariableIds.HistoryServerCapabilities_AccessHistoryDataCapability);
                    var accessEventsCap = (PropertyState)cfnm.Find(VariableIds.HistoryServerCapabilities_AccessHistoryEventsCapability);

                    accessDataCap.Value = true;
                    accessEventsCap.Value = true;

                    // Seems like this node manager manages the entire server node, and so is very relevant when it comes to presenting
                    // information about the server to the client. I suspect that this may be configurable in xml (hence "configuration"),
                    // but I can't find any documentation.
                    var server = (BaseObjectState)cfnm.Find(ObjectIds.Server);
                    if (predefinedNodes.Contains(PredefinedSetup.Events))
                    {
                        server.EventNotifier |= EventNotifiers.HistoryRead | EventNotifiers.SubscribeToEvents;
                    }

                    if (predefinedNodes.Contains(PredefinedSetup.Auditing))
                    {
                        var auditing = (PropertyState)cfnm.Find(VariableIds.Server_Auditing);
                        auditing.Value = true;
                    }


                    var dataSets = (BaseObjectState)cfnm.Find(ObjectIds.PublishSubscribe_PublishedDataSets);
                }

                if (predefinedNodes != null)
                {
                    foreach (var set in predefinedNodes)
                    {
                        switch (set)
                        {
                            case PredefinedSetup.Base:
                                CreateBaseSpace(externalReferences);
                                break;
                            case PredefinedSetup.Full:
                                CreateFullAddressSpace(externalReferences);
                                break;
                            case PredefinedSetup.Custom:
                                CreateCustomAddressSpace(externalReferences);
                                break;
                            case PredefinedSetup.Events:
                                CreateEventAddressSpace(externalReferences);
                                break;
                            case PredefinedSetup.Auditing:
                                CreateAuditAddressSpace(externalReferences);
                                break;
                            case PredefinedSetup.Wrong:
                                CreateWrongAddressSpace(externalReferences);
                                break;
                            case PredefinedSetup.VeryLarge:
                                CreateVeryLargeAddressSpace(externalReferences);
                                break;
                            case PredefinedSetup.PubSub:
                                CreatePubSubNodes(externalReferences);
                                break;
                            case PredefinedSetup.Types:
                                CreateTypeAddressSpace(externalReferences);
                                break;
                        }
                    }
                }

                LoadAddressSpaceFiles(externalReferences);

                CreateNamespaceMetadataNode(externalReferences);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to create address space: {Message}", ex.Message);
            }
        }

        private void CreateBaseSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create base address space");

            lock (Lock)
            {
                var myobj = CreateObject("BaseRoot");
                AddNodeToExt(myobj, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                var myvar = CreateVariable("Variable 1", DataTypes.Double);
                myvar.Value = 0.0;
                AddNodeRelation(myvar, myobj, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(myvar, BuiltInType.Double, 0);

                var myvar2 = CreateVariable("Variable 2", DataTypes.Double);
                AddNodeRelation(myvar2, myobj, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(myvar2, BuiltInType.Double, 0);

                var mystring = CreateVariable("Variable string", DataTypes.String);
                AddNodeRelation(mystring, myobj, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(mystring, BuiltInType.String, 0);

                var tsprop1 = myvar.AddProperty<string>("TS Property 1", DataTypes.String, -1);
                tsprop1.Value = "test";
                tsprop1.NodeId = GenerateNodeId();

                var tsprop2 = myvar.AddProperty<double>("TS Property 2", DataTypes.Double, -1);
                tsprop2.Value = 123.20;
                tsprop2.NodeId = GenerateNodeId();

                var asprop1 = myobj.AddProperty<string>("Asset Property 1", DataTypes.String, -1);
                asprop1.Value = "test";
                asprop1.NodeId = GenerateNodeId();

                var asprop2 = myobj.AddProperty<double>("Asset Property 2", DataTypes.Double, -1);
                asprop2.Value = 123.21;
                asprop2.NodeId = GenerateNodeId();

                var mybool = CreateVariable("Variable bool", DataTypes.Boolean);
                AddNodeRelation(mybool, myobj, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(mybool, BuiltInType.Boolean, 0);

                var myint = CreateVariable("Variable int", DataTypes.Int64);
                AddNodeRelation(myint, myobj, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(myint, BuiltInType.Int64, 0);

                store.AddHistorizingNode(myvar);
                store.AddHistorizingNode(mystring);
                store.AddHistorizingNode(myint);

                AddPredefinedNodes(SystemContext, myobj, myvar, myvar2, mystring, tsprop1, tsprop2, asprop1, asprop2, mybool, myint);
                Ids.Base.Root = myobj.NodeId;
                Ids.Base.DoubleVar1 = myvar.NodeId;
                Ids.Base.DoubleVar2 = myvar2.NodeId;
                Ids.Base.StringVar = mystring.NodeId;
                Ids.Base.BoolVar = mybool.NodeId;
                Ids.Base.IntVar = myint.NodeId;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        private void CreateFullAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create large address space");

            lock (Lock)
            {
                var root = CreateObject("FullRoot");
                root.Description = "FullRoot Description";
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);
                AddPredefinedNode(SystemContext, root);

                var wideroot = CreateObject("WideRoot");
                AddNodeRelation(wideroot, root, ReferenceTypeIds.Organizes);
                AddPredefinedNode(SystemContext, wideroot);

                for (int i = 0; i < 2000; i++)
                {
                    var varch = CreateVariable("SubVariable " + i, DataTypes.Double);
                    AddNodeRelation(varch, wideroot, ReferenceTypeIds.HasComponent);
                    AddPredefinedNode(SystemContext, varch);
                }

                var deeproot = CreateObject("DeepRoot");
                AddNodeRelation(deeproot, root, ReferenceTypeIds.Organizes);
                AddPredefinedNode(SystemContext, deeproot);

                for (int i = 0; i < 5; i++)
                {
                    var lastdeepobj = deeproot;
                    for (int j = 0; j < 30; j++)
                    {
                        var deepobj = CreateObject($"DeepObject {i}, {j}");
                        AddNodeRelation(deepobj, lastdeepobj, ReferenceTypeIds.Organizes);
                        AddPredefinedNode(SystemContext, deepobj);
                        lastdeepobj = deepobj;
                    }
                }
                Ids.Full.Root = root.NodeId;
                Ids.Full.WideRoot = wideroot.NodeId;
                Ids.Full.DeepRoot = deeproot.NodeId;
            }
        }

        private void CreateCustomAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create custom address space");

            lock (Lock)
            {
                var root = CreateObject("CustomRoot");
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                var myarray = CreateVariable("Variable Array", DataTypes.Double, 4);
                myarray.Value = new double[] { 0, 0, 0, 0 };
                AddNodeRelation(myarray, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(myarray, BuiltInType.Double, 1);

                var mystrarray = CreateVariable("Variable StringArray", DataTypes.String, 2);
                mystrarray.Value = new[] { "test1", "test2" };
                AddNodeRelation(mystrarray, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(mystrarray, BuiltInType.String, 1);

                // Custom types
                // String parseable type
                var stringyType = CreateDataType("StringyType", DataTypes.BaseDataType, externalReferences);
                // Type to be ignored
                var ignoreType = CreateDataType("IgnoreType", DataTypes.BaseDataType, externalReferences);
                // Numeric type situated at number node
                var numberType = CreateDataType("MysteryType", DataTypes.Number, externalReferences);
                // Numeric type outside number node
                var numberType2 = CreateDataType("NumberType", DataTypes.BaseDataType, externalReferences);

                // Create instances
                var stringyVar = CreateVariable("StringyVar", stringyType.NodeId);
                AddNodeRelation(stringyVar, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(stringyVar, BuiltInType.String, 1);

                var ignoreVar = CreateVariable("IgnoreVar", ignoreType.NodeId);
                AddNodeRelation(ignoreVar, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(ignoreVar, BuiltInType.String, 1);

                var mysteryVar = CreateVariable("MysteryVar", numberType.NodeId);
                AddNodeRelation(mysteryVar, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(mysteryVar, BuiltInType.Double, 1);

                var numberVar2 = CreateVariable("NumberVar", numberType2.NodeId);
                AddNodeRelation(numberVar2, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(numberVar2, BuiltInType.Double, 1);

                var euinf = new EUInformation("°C", "degree Celsius", "http://www.opcfoundation.org/UA/units/un/cefact")
                {
                    UnitId = 4408652
                };
                var euprop = mysteryVar.AddProperty<EUInformation>("EngineeringUnits", DataTypes.EUInformation, -1);
                euprop.NodeId = GenerateNodeId();
                euprop.Value = euinf;

                var eurange = new Opc.Ua.Range(100, 0);

                var rangeprop = mysteryVar.AddProperty<Opc.Ua.Range>("EURange", DataTypes.Range, -1);
                rangeprop.NodeId = GenerateNodeId();
                rangeprop.Value = eurange;

                var obj = CreateObject("ChildObject");
                AddNodeRelation(obj, root, ReferenceTypeIds.Organizes);

                var obj2 = CreateObject("ChildObject2");
                var objProp = obj2.AddProperty<long>("NumericProp", DataTypeIds.Int64, -1);
                objProp.NodeId = GenerateNodeId();
                objProp.Value = 1234L;

                var objProp2 = obj2.AddProperty<string>("StringProp", DataTypeIds.String, -1);
                objProp2.NodeId = GenerateNodeId();
                objProp2.Value = "String prop value";

                AddNodeRelation(obj2, root, ReferenceTypeIds.Organizes);

                var arrprop = myarray.AddProperty<EUInformation>("EngineeringUnits", DataTypes.EUInformation, -1);
                arrprop.NodeId = GenerateNodeId();
                arrprop.Value = euinf;

                var arrprop2 = myarray.AddProperty<Opc.Ua.Range>("EURange", DataTypes.Range, -1);
                arrprop2.NodeId = GenerateNodeId();
                arrprop2.Value = eurange;

                var deepProp = CreateObject("DeepProp");
                AddNodeRelation(deepProp, numberVar2, ReferenceTypeIds.Organizes);

                var deepProp2 = CreateObject("DeepProp2");
                AddNodeRelation(deepProp2, deepProp, ReferenceTypeIds.Organizes);

                var deepPropVal1 = deepProp2.AddProperty<string>("val1", DataTypeIds.String, -1);
                deepPropVal1.NodeId = GenerateNodeId();
                deepPropVal1.Value = "value 1";

                var deepPropVal2 = deepProp2.AddProperty<string>("val2", DataTypeIds.String, -1);
                deepPropVal2.NodeId = GenerateNodeId();
                deepPropVal2.Value = "value 2";

                var enumType1 = CreateDataType("CustomEnumType1", DataTypes.Enumeration, externalReferences);
                var enumProp1 = enumType1.AddProperty<LocalizedText[]>("EnumStrings", DataTypes.LocalizedText, ValueRanks.OneDimension);
                enumProp1.NodeId = GenerateNodeId();
                enumProp1.Value = new LocalizedText[] { "Enum1", "Enum2", "Enum3" };

                var enumType2 = CreateDataType("CustomEnumType2", DataTypes.Enumeration, externalReferences);
                var enumProp2 = enumType2.AddProperty<EnumValueType[]>("EnumValues", DataTypes.EnumValueType, ValueRanks.OneDimension);
                enumProp2.NodeId = GenerateNodeId();
                enumProp2.Value = new EnumValueType[] {
                    new EnumValueType { Value = 321, DisplayName = "VEnum1", Description = "VEnumDesc1" },
                    new EnumValueType { Value = 123, DisplayName = "VEnum2", Description = "VEnumDesc2" }
                };

                var enumVar1 = CreateVariable("EnumVar1", enumType1.NodeId);
                enumVar1.Value = 1;
                AddNodeRelation(enumVar1, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(enumVar1, BuiltInType.Int32, 1);

                var enumVar2 = CreateVariable("EnumVar2", enumType2.NodeId);
                enumVar2.NodeId = new NodeId("enumvar", NamespaceIndex);
                enumVar2.Value = 123;
                AddNodeRelation(enumVar2, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(enumVar2, BuiltInType.Int32, 1);

                var enumVar3 = CreateVariable("EnumVar3", enumType2.NodeId, 4);
                enumVar3.Value = new[] { 123, 123, 321, 123 };
                AddNodeRelation(enumVar3, root, ReferenceTypeIds.HasComponent);
                pubSub.AddPubSubVariable(enumVar3, BuiltInType.Int32, 1);

                // Custom references
                var refType1 = CreateReferenceType("HasCustomRelation", "IsCustomRelationOf",
                    ReferenceTypeIds.NonHierarchicalReferences, externalReferences);
                var refType2 = CreateReferenceType("HasSymmetricRelation", null,
                    ReferenceTypeIds.NonHierarchicalReferences, externalReferences);

                // object, array (asset-asset)
                AddNodeRelation(root, myarray, refType1.NodeId);
                // array, array (asset-asset)
                AddNodeRelation(myarray, mystrarray, refType2.NodeId);
                // object, variable (asset-timeseries)
                AddNodeRelation(root, stringyVar, refType1.NodeId);
                // variable, variable (timeseries-timeseries)
                AddNodeRelation(mysteryVar, stringyVar, refType2.NodeId);
                // variable, ignored (asset-none, ignored)
                AddNodeRelation(stringyVar, ignoreVar, refType2.NodeId);

                AddTypesToTypeTree(refType1);
                AddTypesToTypeTree(refType2);

                // Custom object and variable type
                var objType = CreateObjectType("CustomObjectType", ObjectTypeIds.BaseObjectType, externalReferences);
                var variableType = CreateVariableType("CustomVariableType", DataTypeIds.Double, VariableTypeIds.BaseDataVariableType,
                    externalReferences);
                variableType.Value = 123.123;

                AddTypesToTypeTree(objType);
                AddTypesToTypeTree(variableType);

                store.AddHistorizingNode(myarray);
                store.AddHistorizingNode(mysteryVar);
                store.AddHistorizingNode(stringyVar);

                AddPredefinedNodes(SystemContext, root, myarray, mystrarray, stringyType, ignoreType, numberType, numberType2, stringyVar,
                    ignoreVar, mysteryVar, numberVar2, euprop, rangeprop, obj, obj2, objProp, objProp2, arrprop, arrprop2,
                    enumType1, enumType2, enumProp1, enumProp2, enumVar1, enumVar2, enumVar3, refType1, refType2, objType, variableType,
                    deepProp, deepProp2);

                Ids.Custom.Root = root.NodeId;
                Ids.Custom.Array = myarray.NodeId;
                Ids.Custom.StringArray = mystrarray.NodeId;
                Ids.Custom.StringyType = stringyType.NodeId;
                Ids.Custom.IgnoreType = ignoreType.NodeId;
                Ids.Custom.MysteryType = numberType.NodeId;
                Ids.Custom.NumberType = numberType2.NodeId;
                Ids.Custom.StringyVar = stringyVar.NodeId;
                Ids.Custom.IgnoreVar = ignoreVar.NodeId;
                Ids.Custom.MysteryVar = mysteryVar.NodeId;
                Ids.Custom.NumberVar = numberVar2.NodeId;
                Ids.Custom.RangeProp = rangeprop.NodeId;
                Ids.Custom.Obj1 = obj.NodeId;
                Ids.Custom.Obj2 = obj2.NodeId;
                Ids.Custom.ObjProp = objProp.NodeId;
                Ids.Custom.ObjProp2 = objProp2.NodeId;
                Ids.Custom.EUProp = euprop.NodeId;
                Ids.Custom.EnumType1 = enumType1.NodeId;
                Ids.Custom.EnumType2 = enumType2.NodeId;
                Ids.Custom.EnumVar1 = enumVar1.NodeId;
                Ids.Custom.EnumVar2 = enumVar2.NodeId;
                Ids.Custom.EnumVar3 = enumVar3.NodeId;
                Ids.Custom.RefType1 = refType1.NodeId;
                Ids.Custom.RefType2 = refType2.NodeId;
                Ids.Custom.ObjectType = objType.NodeId;
                Ids.Custom.VariableType = variableType.NodeId;
            }
        }

        private void CreateEventAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create event address space");

            lock (Lock)
            {
                var root = CreateObject("EventRoot");
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                var obj1 = CreateObject("Object 1");
                AddNodeRelation(obj1, root, ReferenceTypeIds.Organizes);

                var obj2 = CreateObject("Object 2");
                AddNodeRelation(obj2, root, ReferenceTypeIds.Organizes);

                var objexclude = CreateObject("EXCLUDE Object");
                AddNodeRelation(objexclude, root, ReferenceTypeIds.Organizes);

                var var1 = CreateVariable("Variable 1", DataTypes.Double);
                AddNodeRelation(var1, obj1, ReferenceTypeIds.HasComponent);

                var var2 = CreateVariable("Variable 2", DataTypes.Double);
                AddNodeRelation(var2, obj1, ReferenceTypeIds.HasComponent);


                var propType = CreateObjectType("EventExtraProperties", ObjectTypeIds.BaseEventType, externalReferences);
                var prop1 = propType.AddProperty<float>("PropertyNum", DataTypes.Float, -1);
                prop1.NodeId = GenerateNodeId();
                var prop2 = propType.AddProperty<string>("PropertyString", DataTypes.String, -1);
                prop2.NodeId = GenerateNodeId();
                var prop3 = propType.AddProperty<string>("SubType", DataTypes.String, -1);
                prop3.NodeId = GenerateNodeId();

                var basicType1 = CreateObjectType("EventBasic 1", ObjectTypeIds.BaseEventType, externalReferences);

                var basicType2 = CreateObjectType("EventBasic 2", ObjectTypeIds.BaseEventType, externalReferences);

                var customType = CreateObjectType("EventCustomType", ObjectTypeIds.BaseEventType, externalReferences);
                var prop4 = customType.AddProperty<string>("TypeProp", DataTypes.String, -1);
                prop4.NodeId = GenerateNodeId();

                var deepType = CreateObjectType("EventDeepProperties", null, null);
                var deepObj = CreateObject("DeepObj");
                var deepProp = deepObj.AddProperty<string>("DeepProp", DataTypeIds.String, -1);
                deepProp.NodeId = GenerateNodeId();

                AddNodeRelation(deepType, propType, ReferenceTypeIds.HasSubtype);
                AddNodeRelation(deepObj, deepType, ReferenceTypeIds.HasComponent);

                AddNodeRelation(propType, obj1, ReferenceTypeIds.GeneratesEvent);
                AddNodeRelation(basicType1, obj1, ReferenceTypeIds.GeneratesEvent);
                AddNodeRelation(propType, obj2, ReferenceTypeIds.GeneratesEvent);
                AddNodeToExt(propType, ObjectIds.Server, ReferenceTypeIds.GeneratesEvent, externalReferences);
                AddNodeToExt(basicType1, ObjectIds.Server, ReferenceTypeIds.GeneratesEvent, externalReferences);
                AddNodeToExt(basicType2, ObjectIds.Server, ReferenceTypeIds.GeneratesEvent, externalReferences);
                AddNodeToExt(customType, ObjectIds.Server, ReferenceTypeIds.GeneratesEvent, externalReferences);

                AddPredefinedNodes(SystemContext, root, obj1, obj2, objexclude, var1, var2, propType, basicType1, basicType2, customType,
                    deepType, deepObj);

                var testEmitter = new TestEventManager<PropertyEvent>(SystemContext, propType, NamespaceUris.First());

                store.AddEventHistorizingEmitter(obj1.NodeId);
                store.AddEventHistorizingEmitter(ObjectIds.Server);
                obj1.EventNotifier = EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead;
                obj2.EventNotifier = EventNotifiers.SubscribeToEvents;

                Ids.Event.Root = root.NodeId;
                Ids.Event.Obj1 = obj1.NodeId;
                Ids.Event.Obj2 = obj2.NodeId;
                Ids.Event.ObjExclude = objexclude.NodeId;
                Ids.Event.Var1 = var1.NodeId;
                Ids.Event.Var2 = var2.NodeId;
                Ids.Event.PropType = propType.NodeId;
                Ids.Event.BasicType1 = basicType1.NodeId;
                Ids.Event.BasicType2 = basicType2.NodeId;
                Ids.Event.CustomType = customType.NodeId;
                Ids.Event.DeepType = deepType.NodeId;
                Ids.Event.DeepObj = deepObj.NodeId;
            }
        }

        private void CreateAuditAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create audit address space");

            lock (Lock)
            {
                var root = CreateObject("GrowingRoot");
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                var addDirect = CreateObject("AddDirect");
                AddNodeRelation(addDirect, root, ReferenceTypeIds.Organizes);

                var addRef = CreateObject("AddRef");
                AddNodeRelation(addRef, root, ReferenceTypeIds.Organizes);

                var exclude = CreateObject("EXCLUDEObj");
                AddNodeRelation(exclude, root, ReferenceTypeIds.Organizes);

                AddPredefinedNodes(SystemContext, root, addDirect, addRef, exclude);

                if (!externalReferences.TryGetValue(ObjectIds.Server, out var references))
                {
                    externalReferences[ObjectIds.Server] = references = new List<IReference>();
                }

                references.Add(new NodeStateReference(ReferenceTypeIds.GeneratesEvent, false, ObjectTypeIds.AuditAddNodesEventType));
                references.Add(new NodeStateReference(ReferenceTypeIds.GeneratesEvent, false, ObjectTypeIds.AuditAddReferencesEventType));

                Ids.Audit.Root = root.NodeId;
                Ids.Audit.DirectAdd = addDirect.NodeId;
                Ids.Audit.RefAdd = addRef.NodeId;
                Ids.Audit.ExcludeObj = exclude.NodeId;
            }
        }

        private void CreateWrongAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create wrong address space");

            lock (Lock)
            {
                var root = CreateObject("WrongRoot");
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                var rankImp = CreateVariable("RankImprecise", DataTypes.Double, 4);
                rankImp.ValueRank = ValueRanks.Any;
                AddNodeRelation(rankImp, root, ReferenceTypeIds.HasComponent);

                var rankImpNoDim = CreateVariable("RankImpreciseNoDim", DataTypes.Double);
                rankImpNoDim.ValueRank = ValueRanks.ScalarOrOneDimension;
                AddNodeRelation(rankImpNoDim, root, ReferenceTypeIds.HasComponent);

                var wrongDim = CreateVariable("WrongDim", DataTypes.Double, 4);
                AddNodeRelation(wrongDim, root, ReferenceTypeIds.HasComponent);

                var tooLargeDimProp = rankImp.AddProperty<int[]>("TooLargeDim", DataTypes.Int32, ValueRanks.OneDimension);
                tooLargeDimProp.Value = Enumerable.Range(0, 20).ToArray();
                tooLargeDimProp.NodeId = GenerateNodeId();
                tooLargeDimProp.ArrayDimensions = new ReadOnlyList<uint>(new List<uint> { 20 });

                var nullType = CreateVariable("NullType", NodeId.Null);
                AddNodeRelation(nullType, root, ReferenceTypeIds.HasComponent);

                var noDim = CreateVariable("NoDim", DataTypes.Double);
                noDim.Value = new double[] { 1.0, 2.0, 3.0 };
                noDim.ValueRank = ValueRanks.OneDimension;
                AddNodeRelation(noDim, root, ReferenceTypeIds.HasComponent);

                var dimInProp = CreateVariable("DimInProp", DataTypes.Double);
                dimInProp.ValueRank = ValueRanks.OneDimension;
                var maxLengthProp = dimInProp.AddProperty<int>("MaxArrayLength", DataTypes.Int32, ValueRanks.Scalar);
                maxLengthProp.NodeId = GenerateNodeId();
                maxLengthProp.Value = 4;
                AddNodeRelation(dimInProp, root, ReferenceTypeIds.HasComponent);

                AddPredefinedNodes(SystemContext, root, rankImp, rankImpNoDim, wrongDim, tooLargeDimProp, nullType, noDim,
                    dimInProp, maxLengthProp);

                Ids.Wrong.Root = root.NodeId;
                Ids.Wrong.RankImprecise = rankImp.NodeId;
                Ids.Wrong.RankImpreciseNoDim = rankImpNoDim.NodeId;
                Ids.Wrong.WrongDim = wrongDim.NodeId;
                Ids.Wrong.TooLargeProp = tooLargeDimProp.NodeId;
                Ids.Wrong.NullType = nullType.NodeId;
                Ids.Wrong.NoDim = noDim.NodeId;
                Ids.Wrong.DimInProp = dimInProp.NodeId;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        private void CreateVeryLargeAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create very large address space");

            lock (Lock)
            {
                var root = CreateObject("VeryLargeRoot");
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);
                AddPredefinedNode(SystemContext, root);
                for (int i = 0; i < 100; i++)
                {
                    var obj1 = CreateObject($"Object {i}");
                    AddNodeRelation(obj1, root, ReferenceTypeIds.Organizes);
                    AddPredefinedNode(SystemContext, obj1);
                    for (int j = 0; j < 2000; j++)
                    {
                        var obj2 = CreateObject($"Object {i} {j}");
                        var prop1 = obj2.AddProperty<string>($"Property {i} {j}", DataTypeIds.String, ValueRanks.Scalar);
                        prop1.NodeId = GenerateNodeId();
                        prop1.Value = $"string-value-{i}-{j}";
                        var prop2 = obj2.AddProperty<long>($"Property {i} {j} long", DataTypeIds.Int64, ValueRanks.Scalar);
                        prop2.NodeId = GenerateNodeId();
                        prop2.Value = i * j;
                        AddNodeRelation(obj2, obj1, ReferenceTypeIds.HasComponent);
                        AddPredefinedNodes(SystemContext, obj2, prop1, prop2);

                    }
                }

            }
        }

        private void CreatePubSubNodes(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            var cfnm = (ConfigurationNodeManager)Server.NodeManager.NodeManagers.First(nm => nm.GetType() == typeof(ConfigurationNodeManager));
            lock (cfnm.Lock)
            {
                // Update state
                var status = (BaseDataVariableState)cfnm.Find(VariableIds.PublishSubscribeType_Status_State);
                status.Value = PubSubState.Operational;
            }
            var dataSetMap = new Dictionary<string, IList<NodeState>>();


            var config = pubSub.Build();

            foreach (var conn in config.Connections)
            {
                var c = CreateObject<PubSubConnectionState>(conn.Name);
                AddNodeToExt(c, ObjectIds.PublishSubscribe, ReferenceTypeIds.HasPubSubConnection, externalReferences);
                AddProperty(c, "PublisherId", DataTypeIds.BaseDataType, -1, conn.PublisherId);

                var cProp = AddProperty(c, "ConnectionProperties", DataTypeIds.KeyValuePair,
                    conn.ConnectionProperties.Count, conn.ConnectionProperties.ToArray());

                var cAddr = CreateObject<NetworkAddressUrlState>("Address");
                AddNodeRelation(cAddr, c, ReferenceTypeIds.HasComponent);

                var addr = conn.Address.Body as NetworkAddressUrlDataType;
                var cAddrInt = AddVariable<string, SelectionListState>("NetworkInterface", DataTypeIds.String, -1, addr.NetworkInterface);
                AddNodeRelation(cAddrInt, cAddr, ReferenceTypeIds.HasComponent);
                var cAddrUrl = AddVariable<string, BaseDataVariableState>("Url", DataTypeIds.String, -1, addr.Url);
                AddNodeRelation(cAddrUrl, cAddr, ReferenceTypeIds.HasComponent);

                var cStatus = CreateObject<PubSubStatusState>("Status");
                AddNodeRelation(cStatus, c, ReferenceTypeIds.HasComponent);
                var cStatusState = AddVariable<PubSubState, BaseDataVariableState>("State", DataTypeIds.PubSubState, -1, PubSubState.Operational);
                AddNodeRelation(cStatusState, cStatus, ReferenceTypeIds.HasComponent);

                var cTProfile = AddVariable<string, SelectionListState>("TransportProfileUri", DataTypeIds.String, -1, conn.TransportProfileUri);
                AddNodeRelation(cTProfile, c, ReferenceTypeIds.HasComponent);

                if (conn.TransportSettings?.Body is BrokerConnectionTransportDataType tSettings)
                {
                    var cTSettings = CreateObject<BrokerConnectionTransportState>("TransportSettings");
                    AddNodeRelation(cTSettings, c, ReferenceTypeIds.HasComponent);
                    AddProperty(cTSettings, "AuthenticationProfileUri", DataTypeIds.String, -1, tSettings.AuthenticationProfileUri);
                    AddProperty(cTSettings, "ResourceUri", DataTypeIds.String, -1, tSettings.ResourceUri);
                    AddPredefinedNode(SystemContext, cTSettings);
                }

                foreach (var group in conn.WriterGroups)
                {
                    var g = CreateObject<WriterGroupState>(group.Name);
                    AddNodeRelation(g, c, ReferenceTypeIds.HasWriterGroup);

                    AddProperty(g, "GroupProperties", DataTypeIds.KeyValuePair, group.GroupProperties.Count, group.GroupProperties.ToArray());
                    AddProperty(g, "HeaderLayoutUri", DataTypeIds.String, -1, group.HeaderLayoutUri);
                    AddProperty(g, "KeepAliveTime", DataTypeIds.Duration, -1, group.KeepAliveTime);
                    AddProperty(g, "LocalId", DataTypeIds.LocaleId, group.LocaleIds.Count, group.LocaleIds.ToArray());
                    AddProperty(g, "MaxNetworkMessageSize", DataTypeIds.UInt32, -1, group.MaxNetworkMessageSize);
                    AddProperty(g, "Priority", DataTypeIds.Byte, -1, group.Priority);
                    AddProperty(g, "PublishingInterval", DataTypeIds.Duration, -1, group.PublishingInterval);
                    AddProperty(g, "SecurityMode", DataTypeIds.MessageSecurityMode, -1, group.SecurityMode);
                    AddProperty(g, "WriterGroupId", DataTypeIds.UInt16, -1, group.WriterGroupId);

                    var gStatus = CreateObject<PubSubStatusState>("Status");
                    AddNodeRelation(gStatus, g, ReferenceTypeIds.HasComponent);
                    var gStatusState = AddVariable<PubSubState, BaseDataVariableState>("State", DataTypeIds.PubSubState, -1, PubSubState.Operational);
                    AddNodeRelation(gStatusState, gStatus, ReferenceTypeIds.HasComponent);

                    if (group.MessageSettings?.Body is UadpWriterGroupMessageDataType mSettings)
                    {
                        var gMSettings = CreateObject<UadpWriterGroupMessageState>("MessageSettings");
                        AddNodeRelation(gMSettings, g, ReferenceTypeIds.HasComponent);
                        AddProperty(gMSettings, "DataSetOrdering", DataTypeIds.DataSetOrderingType, -1, mSettings.DataSetOrdering);
                        AddProperty(gMSettings, "GroupVersion", DataTypeIds.VersionTime, -1, mSettings.GroupVersion);
                        AddProperty(gMSettings, "NetworkMessageContentMask",
                            DataTypeIds.UadpNetworkMessageContentMask, -1, mSettings.NetworkMessageContentMask);
                        AddProperty(gMSettings, "PublishingOffset", DataTypeIds.Duration,
                            mSettings.PublishingOffset.Count, mSettings.PublishingOffset.ToArray());
                        AddProperty(gMSettings, "SamplingOffset", DataTypeIds.Duration, -1, mSettings.SamplingOffset);
                        AddPredefinedNode(SystemContext, gMSettings);
                    }
                    else if (group.MessageSettings?.Body is JsonWriterGroupMessageDataType jmSettings)
                    {
                        var gMSettings = CreateObject<JsonWriterGroupMessageState>("MessageSettings");
                        AddNodeRelation(gMSettings, g, ReferenceTypeIds.HasComponent);
                        AddProperty(gMSettings, "NetworkMessageContentMask", DataTypeIds.JsonNetworkMessageContentMask,
                            -1, jmSettings.NetworkMessageContentMask);
                        AddPredefinedNode(SystemContext, gMSettings);
                    }

                    if (group.TransportSettings?.Body is BrokerWriterGroupTransportDataType tSettingsG)
                    {
                        var gTSettings = CreateObject<BrokerWriterGroupTransportState>("TransportSettings");
                        AddNodeRelation(gTSettings, g, ReferenceTypeIds.HasComponent);
                        AddProperty(gTSettings, "AuthenticationProfileUri", DataTypeIds.String, -1, tSettingsG.AuthenticationProfileUri);
                        AddProperty(gTSettings, "QueueName", DataTypeIds.String, -1, tSettingsG.QueueName);
                        AddProperty(gTSettings, "RequestedDeliveryGuarantee", DataTypeIds.BrokerTransportQualityOfService,
                            -1, tSettingsG.RequestedDeliveryGuarantee);
                        AddProperty(gTSettings, "ResourceUri", DataTypeIds.String, -1, tSettingsG.ResourceUri);
                        AddPredefinedNode(SystemContext, gTSettings);
                    }

                    foreach (var writer in group.DataSetWriters)
                    {
                        var w = CreateObject<DataSetWriterState>(writer.Name);
                        AddNodeRelation(w, g, ReferenceTypeIds.HasDataSetWriter);

                        if (!dataSetMap.TryGetValue(writer.DataSetName, out var items))
                        {
                            dataSetMap[writer.DataSetName] = items = new List<NodeState>();
                        }
                        items.Add(w);

                        AddProperty(w, "DataSetFieldContentMask", DataTypeIds.DataSetFieldContentMask, -1, writer.DataSetFieldContentMask);
                        AddProperty(w, "DataSetWriterId", DataTypeIds.UInt16, -1, writer.DataSetWriterId);
                        AddProperty(w, "DataSetWriterProperties", DataTypeIds.KeyValuePair,
                            writer.DataSetWriterProperties.Count, writer.DataSetWriterProperties.ToArray());
                        AddProperty(w, "KeyFrameCount", DataTypeIds.UInt32, -1, writer.KeyFrameCount);

                        var wStatus = CreateObject<PubSubStatusState>("Status");
                        AddNodeRelation(wStatus, w, ReferenceTypeIds.HasComponent);
                        var wStatusState = AddVariable<PubSubState, BaseDataVariableState>("State", DataTypeIds.PubSubState, -1, PubSubState.Operational);
                        AddNodeRelation(wStatusState, wStatus, ReferenceTypeIds.HasComponent);

                        if (writer.MessageSettings?.Body is UadpDataSetWriterMessageDataType mSettingsW)
                        {
                            var wMSettings = CreateObject<UadpDataSetWriterMessageState>("MessageSettings");
                            AddNodeRelation(wMSettings, w, ReferenceTypeIds.HasComponent);
                            AddProperty(wMSettings, "ConfiguredSize", DataTypeIds.UInt16, -1, mSettingsW.ConfiguredSize);
                            AddProperty(wMSettings, "DataSetMessageContentMask",
                                DataTypeIds.UadpDataSetMessageContentMask, -1, mSettingsW.DataSetMessageContentMask);
                            AddProperty(wMSettings, "DataSetOffset", DataTypeIds.UInt16, -1, mSettingsW.DataSetOffset);
                            AddProperty(wMSettings, "NetworkMessageNumber", DataTypeIds.UInt16, -1, mSettingsW.NetworkMessageNumber);
                            AddPredefinedNode(SystemContext, wMSettings);
                        }
                        else if (writer.MessageSettings?.Body is JsonDataSetWriterMessageDataType jmSettingsW)
                        {
                            var gMSettings = CreateObject<JsonDataSetWriterMessageState>("MessageSettings");
                            AddNodeRelation(gMSettings, w, ReferenceTypeIds.HasComponent);
                            AddProperty(gMSettings, "DataSetMessageContentMask", DataTypeIds.JsonDataSetMessageContentMask,
                                -1, jmSettingsW.DataSetMessageContentMask);
                            AddPredefinedNode(SystemContext, gMSettings);
                        }

                        if (writer.TransportSettings?.Body is BrokerDataSetWriterTransportDataType tSettingsW)
                        {
                            var wTSettings = CreateObject<BrokerDataSetWriterTransportState>("TransportSettings");
                            AddNodeRelation(wTSettings, w, ReferenceTypeIds.HasComponent);
                            AddProperty(wTSettings, "AuthenticationProfileUri", DataTypeIds.String, -1, tSettingsW.AuthenticationProfileUri);
                            AddProperty(wTSettings, "MetaDataQueueName", DataTypeIds.String, -1, tSettingsW.MetaDataQueueName);
                            AddProperty(wTSettings, "MetaDataUpdateTime", DataTypeIds.Duration, -1, tSettingsW.MetaDataUpdateTime);
                            AddProperty(wTSettings, "QueueName", DataTypeIds.String, -1, tSettingsW.QueueName);
                            AddProperty(wTSettings, "RequestedDeliveryGuarantee", DataTypeIds.BrokerTransportQualityOfService,
                                -1, tSettingsW.RequestedDeliveryGuarantee);
                            AddProperty(wTSettings, "ResourceUri", DataTypeIds.String, -1, tSettingsW.ResourceUri);
                            AddPredefinedNode(SystemContext, wTSettings);
                        }

                        AddPredefinedNodes(SystemContext, w, wStatus, wStatusState);
                    }

                    AddPredefinedNodes(SystemContext, g, gStatus, gStatusState);
                }

                AddPredefinedNodes(SystemContext, c, cAddr, cAddrInt, cAddrUrl, cStatus, cStatusState, cTProfile);
            }

            foreach (var set in config.PublishedDataSets)
            {
                var s = CreateObject<PublishedDataSetState>(set.Name);
                AddNodeToExt(s, ObjectIds.PublishSubscribe_PublishedDataSets, ReferenceTypeIds.Organizes, externalReferences);

                if (dataSetMap.TryGetValue(set.Name, out var writers))
                {
                    foreach (var state in writers)
                    {
                        AddNodeRelation(state, s, ReferenceTypeIds.DataSetToWriter);
                    }
                }

                AddProperty(s, "ConfigurationVersion", DataTypeIds.ConfigurationVersionDataType, -1, set.DataSetMetaData.ConfigurationVersion);
                AddProperty(s, "DataSetClassId", DataTypeIds.Guid, -1, set.DataSetMetaData.DataSetClassId);
                AddProperty(s, "DataSetMetaData", DataTypeIds.DataSetMetaDataType, -1, set.DataSetMetaData);
                if (set.DataSetSource?.Body is PublishedDataItemsDataType items)
                {
                    AddProperty(s, "PublishedData", DataTypeIds.PublishedVariableDataType,
                        items.PublishedData.Count, items.PublishedData.ToArray());
                }

                AddPredefinedNode(SystemContext, s);
            }
            pubSub.Start();
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public void CreateTypeAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogInformation("Create types address space");
            lock (Lock)
            {
                var root = CreateObject("TypesRoot");
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                // Create a handful of object and variable types
                // Trivial type
                var trivialType = CreateObjectType("TrivialType");
                AddNodeToExt(trivialType, ObjectTypeIds.BaseObjectType, ReferenceTypeIds.HasSubtype, externalReferences);

                // Simple type with 3 properties and 2 simple variables
                var simpleType = CreateObjectType("SimpleType");
                AddNodeToExt(simpleType, ObjectTypeIds.BaseObjectType, ReferenceTypeIds.HasSubtype, externalReferences);
                CreateSimpleInstance(simpleType, 123L, "Default", 123.123);

                // Simple variable type with more complex properties
                var simpleVarType = CreateVariableType("SimpleVarType", DataTypeIds.Double);
                AddNodeToExt(simpleVarType, VariableTypeIds.BaseDataVariableType, ReferenceTypeIds.HasSubtype, externalReferences);
                CreateSimpleVarInstance(simpleVarType, new long[] { 1, 2, 3 }, NodeId.Null);

                // Subtype with nested subfields
                var nestedType = CreateObjectType("NestedType");
                AddNodeRelation(nestedType, simpleType, ReferenceTypeIds.HasSubtype);
                CreateNestedInstance(nestedType, "Default", 123.123);

                // Complex supertype. Consists of multiple varTypes, one trivial type, and one nested type
                var complexType = CreateObjectType("ComplexType");
                AddNodeToExt(complexType, ObjectTypeIds.BaseObjectType, ReferenceTypeIds.HasSubtype, externalReferences);

                var placeholder = CreateVariable("<VarPlaceholder>", DataTypeIds.Double);
                placeholder.TypeDefinitionId = simpleVarType.NodeId;
                CreateSimpleVarInstance(placeholder, new long[] { 1, 2, 3 }, complexType.NodeId);
                placeholder.AddReference(ReferenceTypeIds.HasModellingRule, false, ObjectIds.ModellingRule_OptionalPlaceholder);
                AddNodeRelation(placeholder, complexType, ReferenceTypeIds.HasComponent);

                var trivial = CreateObject("Trivial");
                trivial.TypeDefinitionId = trivialType.NodeId;
                AddNodeRelation(trivial, complexType, ReferenceTypeIds.HasComponent);

                var nested = CreateObject("Data");
                nested.TypeDefinitionId = nestedType.NodeId;
                AddNodeRelation(nested, complexType, ReferenceTypeIds.HasComponent);
                CreateSimpleInstance(nested, 123L, "Default", 123.123);
                CreateNestedInstance(nested, "Default", 123.123);

                Ids.Types.Root = root.NodeId;
                Ids.Types.TrivialType = trivialType.NodeId;
                Ids.Types.SimpleType = simpleType.NodeId;
                Ids.Types.SimpleVarType = simpleVarType.NodeId;
                Ids.Types.ComplexType = complexType.NodeId;
                Ids.Types.NestedType = nestedType.NodeId;


                // Add three instances with different sets of variables
                var complex1 = CreateComplexInstance("DeviceOne", new[] { "Reading", "Store", "Measurement" }, 42, "Device One", 3.14);
                AddNodeRelation(complex1, root, ReferenceTypeIds.Organizes);
                var complex2 = CreateComplexInstance("DeviceTwo", Array.Empty<string>(), 15, "Device Two", 7.3);
                AddNodeRelation(complex2, root, ReferenceTypeIds.Organizes);
                var complex3 = CreateComplexInstance("DeviceThree", new[] { "Reading" }, 19, "Device Three", 0);
                AddNodeRelation(complex3, root, ReferenceTypeIds.Organizes);

                AddPredefinedNodes(SystemContext, root, trivialType, simpleType, simpleVarType, nestedType, complexType, placeholder,
                    trivial, nested, complex1, complex2, complex3);
            }
        }

        private BaseObjectState CreateComplexInstance(string name, string[] variables, long lValue, string sValue, double dValue)
        {
            var node = CreateObject(name);
            node.TypeDefinitionId = Ids.Types.ComplexType;

            foreach (var nm in variables)
            {
                var placeholder = CreateVariable(nm, DataTypeIds.Double);
                placeholder.TypeDefinitionId = Ids.Types.SimpleVarType;
                CreateSimpleVarInstance(placeholder, new long[] { 1, 2, 3 }, node.NodeId);
                AddNodeRelation(placeholder, node, ReferenceTypeIds.HasComponent);
                AddPredefinedNodes(SystemContext, placeholder);
            }

            var trivial = CreateObject("Trivial");
            trivial.TypeDefinitionId = Ids.Types.TrivialType;
            AddNodeRelation(trivial, node, ReferenceTypeIds.HasComponent);

            var nested = CreateObject("Data");
            nested.TypeDefinitionId = Ids.Types.NestedType;
            AddNodeRelation(nested, node, ReferenceTypeIds.HasComponent);
            CreateSimpleInstance(nested, lValue, sValue, dValue);
            CreateNestedInstance(nested, sValue, dValue);

            AddPredefinedNodes(SystemContext, trivial, nested);

            return node;
        }

        private void CreateSimpleInstance(NodeState node, long lValue, string sValue, double dValue)
        {
            var simpleProp1 = node.AddProperty<long>("LongProp", DataTypeIds.Int64, ValueRanks.Scalar);
            simpleProp1.NodeId = GenerateNodeId();
            simpleProp1.Value = lValue;
            var simpleProp2 = node.AddProperty<string>("StringProp", DataTypeIds.String, ValueRanks.Scalar);
            simpleProp2.NodeId = GenerateNodeId();
            simpleProp2.Value = sValue;
            var simpleProp3 = node.AddProperty<double>("DoubleProp", DataTypeIds.Double, ValueRanks.Scalar);
            simpleProp3.NodeId = GenerateNodeId();
            simpleProp3.Value = dValue;

            var simpleVar1 = CreateVariable("VarChild1", DataTypeIds.Double);
            AddNodeRelation(simpleVar1, node, ReferenceTypeIds.HasComponent);
            var simpleVar2 = CreateVariable("VarChild2", DataTypeIds.String);
            AddNodeRelation(simpleVar2, node, ReferenceTypeIds.HasComponent);
            AddPredefinedNodes(SystemContext, simpleProp1, simpleProp2, simpleProp3, simpleVar1, simpleVar2);
        }

        private void CreateSimpleVarInstance(NodeState node, long[] lValue, NodeId refValue)
        {
            var simpleVarProp1 = node.AddProperty<long[]>("LongProp", DataTypeIds.Int64, ValueRanks.OneDimension);
            simpleVarProp1.NodeId = GenerateNodeId();
            simpleVarProp1.ArrayDimensions = new uint[] { 3 };
            simpleVarProp1.Value = lValue;
            var simpleVarProp2 = node.AddProperty<EUInformation>("EUInformation", DataTypeIds.String, ValueRanks.Scalar);
            simpleVarProp2.NodeId = GenerateNodeId();
            simpleVarProp2.Value = new EUInformation("Degrees Celsius", "°C", "opc.tcp://test.localhost");
            var simpleVarProp3 = node.AddProperty<NodeId>("RefProp", DataTypeIds.NodeId, ValueRanks.Scalar);
            simpleVarProp3.NodeId = GenerateNodeId();
            simpleVarProp3.Value = refValue;

            AddPredefinedNodes(SystemContext, simpleVarProp1, simpleVarProp2, simpleVarProp3);
        }

        private void CreateNestedInstance(NodeState node, string sValue, double dValue)
        {
            var childObj = CreateObject("Sub");
            AddNodeRelation(childObj, node, ReferenceTypeIds.HasComponent);

            var subProp1 = childObj.AddProperty<string>("StringProp", DataTypeIds.String, ValueRanks.Scalar);
            subProp1.NodeId = GenerateNodeId();
            subProp1.Value = sValue;
            var subProp2 = childObj.AddProperty<double>("DoubleProp", DataTypeIds.Double, ValueRanks.Scalar);
            subProp2.NodeId = GenerateNodeId();
            subProp2.Value = dValue;

            AddPredefinedNodes(SystemContext, childObj, subProp1, subProp2);
        }
        private void LoadAddressSpaceFiles(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            if (nodeSetFiles == null) return;
            foreach (var file in nodeSetFiles)
            {
                log.LogInformation("Loading namespaces: {Ns}", string.Join(", ", file.NamespaceUris));

                foreach (var node in file.Nodes)
                {
                    AddPredefinedNode(SystemContext, node);
                }
            }
            nodeSetFiles = null;

            AddReverseReferences(externalReferences);
        }

        // Utility methods to create nodes
        private void AddNodeToExt(NodeState state, NodeId id, NodeId typeId,
            IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.LogTrace("Add node {Id} {Name} as child of external node {Parent} with reference type {Type}",
                state.NodeId, state.DisplayName, id, typeId);

            if (!externalReferences.TryGetValue(id, out var references))
            {
                externalReferences[id] = references = new List<IReference>();
            }

            state.AddReference(typeId, true, id);
            references.Add(new NodeStateReference(typeId, false, state.NodeId));
        }
        private void AddNodeRelation(NodeState state, NodeState parent, NodeId typeId)
        {
            log.LogTrace("Add reference of type {Type} from {Parent} {PName} to {Id} {Name}",
                typeId, parent.NodeId, parent.DisplayName, state.NodeId, state.DisplayName);

            state.AddReference(typeId, true, parent.NodeId);
            parent.AddReference(typeId, false, state.NodeId);
        }

        private TVar AddVariable<TValue, TVar>(string name, NodeId dataType, int dim = -1, TValue value = default)
            where TVar : BaseVariableState
        {
            var state = (TVar)Activator.CreateInstance(typeof(TVar), new object[] { null });
            state.NodeId = GenerateNodeId();
            state.BrowseName = new QualifiedName(name, NamespaceIndex);
            state.DisplayName = state.BrowseName.Name;
            state.TypeDefinitionId = state.GetDefaultTypeDefinitionId(SystemContext);

            if (dim > -1)
            {
                state.ArrayDimensions = new[] { (uint)dim };
                state.ValueRank = ValueRanks.OneDimension;
            }
            else
            {
                state.ValueRank = ValueRanks.Scalar;
            }
            state.DataType = dataType;
            state.Value = value;

            log.LogTrace("Create variable: Id: {Id}, Name: {Name}, VariableType: {Type}, DataType: {DataType} {ActualType}",
                state.NodeId, state.DisplayName, typeof(TVar), dataType, typeof(TValue));

            return state;
        }

        private PropertyState AddProperty<T>(NodeState parent, string name, NodeId dataType, int dim = -1, T value = default)
        {
            var prop = parent.AddProperty<T>(name, dataType, dim > -1 ? ValueRanks.OneDimension : ValueRanks.Scalar);
            if (dim > -1)
            {
                prop.ArrayDimensions = new[] { (uint)dim };
            }
            prop.Value = value;
            prop.NodeId = GenerateNodeId();

            log.LogTrace("Create property {Name} of type {Type} for {Parent} {ParentName}",
                name, typeof(T), parent.NodeId, parent.DisplayName);

            return prop;
        }

        private T CreateObject<T>(string name) where T : BaseObjectState
        {
            var state = (T)Activator.CreateInstance(typeof(T), new object[] { null });
            state.NodeId = GenerateNodeId();
            state.BrowseName = new QualifiedName(name, NamespaceIndex);
            state.DisplayName = state.BrowseName.Name;
            state.TypeDefinitionId = state.GetDefaultTypeDefinitionId(SystemContext);

            log.LogTrace("Create object: Id: {Id}, Name: {Name}, ObjectType: {Type}",
                state.NodeId, state.DisplayName, typeof(T));

            return state;
        }

        private BaseObjectState CreateObject(string name)
        {
            var state = new BaseObjectState(null)
            {
                NodeId = GenerateNodeId(),
                BrowseName = new QualifiedName(name, NamespaceIndex)
            };
            state.DisplayName = state.BrowseName.Name;
            state.TypeDefinitionId = ObjectTypeIds.BaseObjectType;

            log.LogTrace("Create object: Id: {Id}, Name: {Name}",
                state.NodeId, state.DisplayName);

            return state;
        }

        private BaseDataVariableState CreateVariable(string name, NodeId dataType, int dim = -1)
        {
            var state = new BaseDataVariableState(null)
            {
                NodeId = GenerateNodeId(),
                BrowseName = new QualifiedName(name, NamespaceIndex)
            };
            state.DisplayName = state.BrowseName.Name;
            state.TypeDefinitionId = VariableTypeIds.BaseDataVariableType;
            state.DataType = dataType;
            state.ValueRank = ValueRanks.Scalar;
            if (dim > -1)
            {
                state.ValueRank = ValueRanks.OneDimension;
                state.ArrayDimensions = new[] { (uint)dim };
            }

            log.LogTrace("Create variable: Id: {Id}, Name: {Name}, DataType: {DataType}",
                state.NodeId, state.DisplayName, dataType);

            return state;
        }

        private DataTypeState CreateDataType(string name, NodeId parent, IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            var type = new DataTypeState
            {
                NodeId = GenerateNodeId(),
                BrowseName = new QualifiedName(name, NamespaceIndex)
            };
            type.DisplayName = type.BrowseName.Name;
            if (!externalReferences.TryGetValue(parent, out var references))
            {
                externalReferences[parent] = references = new List<IReference>();
            }

            type.AddReference(ReferenceTypeIds.HasSubtype, true, parent);
            references.Add(new NodeStateReference(ReferenceTypeIds.HasSubtype, false, type.NodeId));

            log.LogTrace("Create data type: Id: {Id}, Name: {Name}", type.NodeId, type.DisplayName);

            return type;
        }

        private BaseObjectTypeState CreateObjectType(string name, NodeId parent = null, IDictionary<NodeId, IList<IReference>> externalReferences = null)
        {
            var type = new BaseObjectTypeState
            {
                NodeId = GenerateNodeId(),
                BrowseName = new QualifiedName(name, NamespaceIndex)
            };
            type.DisplayName = type.BrowseName.Name;
            if (parent == null) return type;
            if (!externalReferences.TryGetValue(parent, out var references))
            {
                externalReferences[parent] = references = new List<IReference>();
            }

            type.AddReference(ReferenceTypeIds.HasSubtype, true, parent);
            references.Add(new NodeStateReference(ReferenceTypeIds.HasSubtype, false, type.NodeId));

            log.LogTrace("Create object type: Id: {Id}, Name: {Name}", type.NodeId, type.DisplayName);

            return type;
        }

        private BaseDataVariableTypeState CreateVariableType(string name, NodeId dataType, NodeId parent = null,
            IDictionary<NodeId, IList<IReference>> externalReferences = null)
        {
            var type = new BaseDataVariableTypeState
            {
                NodeId = GenerateNodeId(),
                BrowseName = new QualifiedName(name, NamespaceIndex),
                DataType = dataType
            };
            type.DisplayName = type.BrowseName.Name;

            if (parent == null) return type;

            if (!externalReferences.TryGetValue(parent, out var references))
            {
                externalReferences[parent] = references = new List<IReference>();
            }
            type.AddReference(ReferenceTypeIds.HasSubtype, true, parent);
            references.Add(new NodeStateReference(ReferenceTypeIds.HasSubtype, false, type.NodeId));

            log.LogTrace("Create variable type: Id: {Id}, Name: {Name}", type.NodeId, type.DisplayName);

            return type;
        }

        private ReferenceTypeState CreateReferenceType(string name, string inverseName,
            NodeId parent, IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            var type = new ReferenceTypeState
            {
                NodeId = GenerateNodeId(),
                BrowseName = new QualifiedName(name, NamespaceIndex),
                DisplayName = name,
                InverseName = inverseName,
                Symmetric = inverseName == null,
                SuperTypeId = parent
            };
            if (!externalReferences.TryGetValue(parent, out var references))
            {
                externalReferences[parent] = references = new List<IReference>();
            }

            type.AddReference(ReferenceTypeIds.HasSubtype, true, parent);
            references.Add(new NodeStateReference(ReferenceTypeIds.HasSubtype, false, type.NodeId));

            log.LogTrace("Create reference type: Id: {Id}, Name: {Name}", type.NodeId, type.DisplayName);

            return type;
        }

        private void AddPredefinedNodes(ServerSystemContext context, params NodeState[] nodes)
        {
            foreach (var node in nodes)
            {
                AddPredefinedNode(context, node);
            }
        }

        #endregion

        #region overrides
        protected override NodeHandle GetManagerHandle(ServerSystemContext context, NodeId nodeId,
            IDictionary<NodeId, NodeState> cache)
        {
            lock (Lock)
            {
                // quickly exclude nodes that are not in the namespace. 
                if (!IsNodeIdInNamespace(nodeId))
                {
                    return null;
                }

                NodeState node;

                // check cache (the cache is used because the same node id can appear many times in a single request).
                if (cache != null)
                {
                    if (cache.TryGetValue(nodeId, out node))
                    {
                        return new NodeHandle(nodeId, node);
                    }
                }

                // look up predefined node.
                if (PredefinedNodes.TryGetValue(nodeId, out node))
                {
                    NodeHandle handle = new NodeHandle(nodeId, node);

                    cache?.Add(nodeId, node);

                    return handle;
                }

                // node not found.
                return null;
            }
        }

        /// <summary>
        /// Verifies that the specified node exists.
        /// </summary>
        protected override NodeState ValidateNode(
            ServerSystemContext context,
            NodeHandle handle,
            IDictionary<NodeId, NodeState> cache)
        {
            // not valid if no root.
            if (handle == null)
            {
                return null;
            }

            // check if previously validated.
            if (handle.Validated)
            {
                return handle.Node;
            }

            // lookup in operation cache.
            NodeState target = FindNodeInCache(context, handle, cache);

            if (target != null)
            {
                handle.Node = target;
                handle.Validated = true;
                return handle.Node;
            }

            // put root into operation cache.
            if (cache != null)
            {
                cache[handle.NodeId] = null;
            }

            handle.Node = null;
            handle.Validated = true;
            return handle.Node;
        }

        public override NodeId New(ISystemContext context, NodeState node)
        {
            if (node is BaseInstanceState instance && instance.Parent != null)
            {
                return GenerateNodeId();
            }

            return node.NodeId;
        }
        #endregion

        #region history
        protected override void HistoryReadRawModified(
            ServerSystemContext context,
            ReadRawModifiedDetails details,
            TimestampsToReturn timestampsToReturn,
            IList<HistoryReadValueId> nodesToRead,
            IList<HistoryReadResult> results,
            IList<ServiceResult> errors,
            List<NodeHandle> nodesToProcess,
            IDictionary<NodeId, NodeState> cache)
        {
            foreach (var handle in nodesToProcess)
            {
                var nodeToRead = nodesToRead[handle.Index];
                var result = results[handle.Index];

                try
                {
                    NodeState source = ValidateNode(context, handle, cache);
                    if (source == null) continue;

                    InternalHistoryRequest request;
                    if (nodeToRead.ContinuationPoint != null)
                    {
                        request = LoadContinuationPoint(context, nodeToRead.ContinuationPoint);

                        if (request == null)
                        {
                            errors[handle.Index] = StatusCodes.BadContinuationPointInvalid;
                            continue;
                        }
                    }
                    else
                    {
                        request = CreateHistoryReadRequest(details, nodeToRead);
                    }

                    var (rawData, final) = store.ReadHistory(request);
                    var data = new HistoryData();

                    if (issues.HistoryReadStatusOverride.TryGetValue(nodeToRead.NodeId, out var code))
                    {
                        errors[handle.Index] = code;
                    }
                    else
                    {
                        errors[handle.Index] = ServiceResult.Good;
                    }

                    data.DataValues.AddRange(rawData);

                    log.LogInformation("Read raw modified: {Cnt}", rawData.Count());

                    if (!final)
                    {
                        result.ContinuationPoint = SaveContinuationPoint(context, request);
                    }

                    result.HistoryData = new ExtensionObject(data);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to read history");
                    errors[handle.Index] = ServiceResult.Create(ex, StatusCodes.BadUnexpectedError,
                        "Unexpected error processing request.");
                }
            }
        }

        public override void HistoryRead(
            OperationContext context,
            HistoryReadDetails details,
            TimestampsToReturn timestampsToReturn,
            bool releaseContinuationPoints,
            IList<HistoryReadValueId> nodesToRead,
            IList<HistoryReadResult> results,
            IList<ServiceResult> errors)
        {
            if (details is ReadEventDetails edetails)
            {
                for (int i = 0; i < nodesToRead.Count; i++)
                {
                    var nodeToRead = nodesToRead[i];
                    if (nodeToRead.NodeId == ObjectIds.Server)
                    {
                        NodeHandle serverHandle;
                        var cfnm = (ConfigurationNodeManager)Server.NodeManager.NodeManagers.First(nm => nm.GetType() == typeof(ConfigurationNodeManager));
                        lock (cfnm.Lock)
                        {
                            var server = (BaseObjectState)cfnm.Find(ObjectIds.Server);
                            serverHandle = new NodeHandle(ObjectIds.Server, server);
                        }
                        nodeToRead.Processed = true;

                        serverHandle.Index = i;

                        results[i] = new HistoryReadResult
                        {
                            HistoryData = null,
                            ContinuationPoint = null,
                            StatusCode = StatusCodes.Good
                        };
                        if (edetails.NumValuesPerNode == 0)
                        {
                            if (edetails.StartTime == DateTime.MinValue || edetails.EndTime == DateTime.MinValue)
                            {
                                throw new ServiceResultException(StatusCodes.BadInvalidTimestampArgument);
                            }
                        }
                        else
                        {
                            if (edetails.StartTime == DateTime.MinValue && edetails.EndTime == DateTime.MinValue)
                            {
                                throw new ServiceResultException(StatusCodes.BadInvalidTimestampArgument);
                            }
                        }

                        HistoryReadEvents(
                            SystemContext.Copy(context),
                            edetails,
                            timestampsToReturn,
                            nodesToRead,
                            results,
                            errors,
                            new List<NodeHandle> { serverHandle },
                            new NodeIdDictionary<NodeState>()
                            );
                    }
                }
            }

            base.HistoryRead(context, details, timestampsToReturn, releaseContinuationPoints, nodesToRead, results, errors);
        }

        protected override void HistoryReadEvents(
            ServerSystemContext context,
            ReadEventDetails details,
            TimestampsToReturn timestampsToReturn,
            IList<HistoryReadValueId> nodesToRead,
            IList<HistoryReadResult> results,
            IList<ServiceResult> errors,
            List<NodeHandle> nodesToProcess,
            IDictionary<NodeId, NodeState> cache)
        {
            foreach (var handle in nodesToProcess)
            {
                var nodeToRead = nodesToRead[handle.Index];
                var result = results[handle.Index];

                try
                {
                    NodeState source = ValidateNode(context, handle, cache);
                    if (source == null) continue;

                    InternalEventHistoryRequest request;
                    if (nodeToRead.ContinuationPoint != null)
                    {
                        request = LoadContinuationPoint(context, nodeToRead.ContinuationPoint) as InternalEventHistoryRequest;

                        if (request == null)
                        {
                            errors[handle.Index] = StatusCodes.BadContinuationPointInvalid;
                            continue;
                        }
                    }
                    else
                    {
                        request = CreateEventHistoryRequest(SystemContext, details, nodeToRead);
                    }

                    var (rawData, final) = store.ReadEventHistory(request);
                    var events = new HistoryEvent();

                    events.Events.AddRange(rawData.Select(evt => GetEventFields(request, evt)));

                    log.LogInformation("Read events: {Cnt}", rawData.Count());

                    if (issues.HistoryReadStatusOverride.TryGetValue(nodeToRead.NodeId, out var code))
                    {
                        errors[handle.Index] = code;
                    }
                    else
                    {
                        errors[handle.Index] = ServiceResult.Good;
                    }

                    if (!final)
                    {
                        result.ContinuationPoint = SaveContinuationPoint(context, request);
                    }

                    result.HistoryData = new ExtensionObject(events);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to read history");
                    errors[handle.Index] = ServiceResult.Create(ex, StatusCodes.BadUnexpectedError,
                        "Unexpected error processing request.");
                }
            }
        }

        private static InternalHistoryRequest LoadContinuationPoint(
            ServerSystemContext context,
            byte[] continuationPoint)
        {
            Session session = context.OperationContext.Session;

            InternalHistoryRequest request =
                session?.RestoreHistoryContinuationPoint(continuationPoint) as InternalHistoryRequest;

            return request;
        }

        /// <summary>
        /// Saves a history continuation point.
        /// </summary>
        private static byte[] SaveContinuationPoint(
            ServerSystemContext context,
            InternalHistoryRequest request)
        {
            Session session = context.OperationContext.Session;

            if (session == null)
            {
                return null;
            }

            Guid id = Guid.NewGuid();
            session.SaveHistoryContinuationPoint(id, request);
            request.ContinuationPoint = id.ToByteArray();
            return request.ContinuationPoint;
        }

        private static InternalHistoryRequest CreateHistoryReadRequest(
            ReadRawModifiedDetails details,
            HistoryReadValueId nodeToRead)
        {
            bool timeFlowsBackward = (details.StartTime == DateTime.MinValue) ||
                                     (details.EndTime != DateTime.MinValue && details.EndTime < details.StartTime);

            return new InternalHistoryRequest
            {
                ContinuationPoint = null,
                EndTime = details.EndTime,
                Id = nodeToRead.NodeId,
                IsReverse = timeFlowsBackward,
                MemoryIndex = -1,
                NumValuesPerNode = details.NumValuesPerNode,
                StartTime = details.StartTime
            };
        }
        private static InternalEventHistoryRequest CreateEventHistoryRequest(
            ServerSystemContext context,
            ReadEventDetails details,
            HistoryReadValueId nodeToRead)
        {
            FilterContext filterContext = new FilterContext(context.NamespaceUris, context.TypeTable, context.PreferredLocales);
            bool timeFlowsBackward = (details.StartTime == DateTime.MinValue) ||
                                     (details.EndTime != DateTime.MinValue && details.EndTime < details.StartTime);

            return new InternalEventHistoryRequest
            {
                ContinuationPoint = null,
                EndTime = details.EndTime,
                Id = nodeToRead.NodeId,
                IsReverse = timeFlowsBackward,
                MemoryIndex = -1,
                NumValuesPerNode = details.NumValuesPerNode,
                StartTime = details.StartTime,
                Filter = details.Filter,
                FilterContext = filterContext
            };
        }
#pragma warning disable CA1859 // Use concrete types when possible for improved performance
        private HistoryEventFieldList GetEventFields(InternalEventHistoryRequest request, IFilterTarget instance)
#pragma warning restore CA1859 // Use concrete types when possible for improved performance
        {
            HistoryEventFieldList fields = new HistoryEventFieldList();

            foreach (SimpleAttributeOperand clause in request.Filter.SelectClauses)
            {
                object value = instance.GetAttributeValue(
                    request.FilterContext,
                    clause.TypeDefinitionId,
                    clause.BrowsePath,
                    clause.AttributeId,
                    clause.ParsedIndexRange);

                if (value != null)
                {
                    LocalizedText text = value as LocalizedText;

                    if (text != null)
                    {
                        value = Server.ResourceManager.Translate(request.FilterContext.PreferredLocales, text);
                    }

                    fields.EventFields.Add(new Variant(value));
                }
                else
                {
                    fields.EventFields.Add(Variant.Null);
                }
            }

            return fields;
        }
        #endregion

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                pubSub?.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    internal class InternalHistoryRequest
    {
        public NodeId Id;
        public byte[] ContinuationPoint;
        public uint NumValuesPerNode;
        public int MemoryIndex;
        public DateTime StartTime;
        public DateTime EndTime;
        public bool IsReverse;
    }

    internal sealed class InternalEventHistoryRequest : InternalHistoryRequest
    {
        public FilterContext FilterContext;
        public EventFilter Filter;
    }
    public enum PredefinedSetup
    {
        Base,
        Full,
        Custom,
        Events,
        Auditing,
        Wrong,
        VeryLarge,
        PubSub,
        Types
    }

    #region nodeid_reference
    public class NodeIdReference
    {
        public NodeIdReference()
        {
            Base = new BaseNodeReference();
            Full = new FullNodeReference();
            Custom = new CustomNodeReference();
            Event = new EventNodeReference();
            Audit = new AuditNodeReference();
            Wrong = new WrongNodeReference();
            Types = new TypesNodeReference();
        }
        public NodeId NamespaceMetadata { get; set; }
        public BaseNodeReference Base { get; set; }
        public FullNodeReference Full { get; set; }
        public CustomNodeReference Custom { get; set; }
        public EventNodeReference Event { get; set; }
        public AuditNodeReference Audit { get; set; }
        public WrongNodeReference Wrong { get; set; }
        public TypesNodeReference Types { get; set; }
    }

    public class BaseNodeReference
    {
        public NodeId Root { get; set; }
        public NodeId DoubleVar1 { get; set; }
        public NodeId DoubleVar2 { get; set; }
        public NodeId IntVar { get; set; }
        public NodeId BoolVar { get; set; }
        public NodeId StringVar { get; set; }
    }

    public class FullNodeReference
    {
        public NodeId Root { get; set; }
        public NodeId WideRoot { get; set; }
        public NodeId DeepRoot { get; set; }
    }

    public class CustomNodeReference
    {
        public NodeId Root { get; set; }
        public NodeId Array { get; set; }
        public NodeId StringArray { get; set; }
        public NodeId StringyType { get; set; }
        public NodeId IgnoreType { get; set; }
        public NodeId MysteryType { get; set; }
        public NodeId NumberType { get; set; }
        public NodeId StringyVar { get; set; }
        public NodeId IgnoreVar { get; set; }
        public NodeId MysteryVar { get; set; }
        public NodeId NumberVar { get; set; }
        public NodeId RangeProp { get; set; }
        public NodeId Obj1 { get; set; }
        public NodeId Obj2 { get; set; }
        public NodeId ObjProp { get; set; }
        public NodeId ObjProp2 { get; set; }
        public NodeId EUProp { get; set; }
        public NodeId EnumType1 { get; set; }
        public NodeId EnumType2 { get; set; }
        public NodeId EnumVar1 { get; set; }
        public NodeId EnumVar2 { get; set; }
        public NodeId EnumVar3 { get; set; }
        public NodeId RefType1 { get; set; }
        public NodeId RefType2 { get; set; }
        public NodeId ObjectType { get; set; }
        public NodeId VariableType { get; set; }
    }

    public class EventNodeReference
    {
        public NodeId Root { get; set; }
        public NodeId Obj1 { get; set; }
        public NodeId Obj2 { get; set; }
        public NodeId ObjExclude { get; set; }
        public NodeId Var1 { get; set; }
        public NodeId Var2 { get; set; }
        public NodeId PropType { get; set; }
        public NodeId BasicType1 { get; set; }
        public NodeId BasicType2 { get; set; }
        public NodeId CustomType { get; set; }
        public NodeId DeepType { get; set; }
        public NodeId DeepObj { get; set; }
    }

    public class AuditNodeReference
    {
        public NodeId Root { get; set; }
        public NodeId DirectAdd { get; set; }
        public NodeId RefAdd { get; set; }
        public NodeId ExcludeObj { get; set; }
    }

    public class WrongNodeReference
    {
        public NodeId Root { get; set; }
        public NodeId RankImprecise { get; set; }
        public NodeId RankImpreciseNoDim { get; set; }
        public NodeId WrongDim { get; set; }
        public NodeId TooLargeProp { get; set; }
        public NodeId NullType { get; set; }
        public NodeId NoDim { get; set; }
        public NodeId DimInProp { get; set; }
    }

    public class TypesNodeReference
    {
        public NodeId Root { get; set; }
        public NodeId TrivialType { get; set; }
        public NodeId ComplexType { get; set; }
        public NodeId SimpleType { get; set; }
        public NodeId SimpleVarType { get; set; }
        public NodeId NestedType { get; set; }
    }
    #endregion
}

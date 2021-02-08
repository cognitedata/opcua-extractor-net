using System;
using System.Collections.Generic;
using System.Linq;
using Opc.Ua;
using Opc.Ua.Server;
using Serilog;

namespace Server
{
    class TestNodeManager : CustomNodeManager2
    {
        private ApplicationConfiguration config;
        private readonly HistoryMemoryStore store;
        private uint nextId;
        private IEnumerable<PredefinedSetup> predefinedNodes;
        public NodeIdReference Ids { get; }
        private readonly ILogger log = Log.Logger.ForContext(typeof(TestNodeManager));

        public TestNodeManager(IServerInternal server, ApplicationConfiguration configuration)
            : base(server, configuration, "opc.tcp://test.localhost")
        {
            SystemContext.NodeIdFactory = this;
            config = configuration;
            store = new HistoryMemoryStore();
            Ids = new NodeIdReference();
        }

        public TestNodeManager(IServerInternal server, ApplicationConfiguration configuration, IEnumerable<PredefinedSetup> predefinedNodes) :
            this(server, configuration)
        {
            this.predefinedNodes = predefinedNodes;
        }
        #region access
        public void UpdateNode(NodeId id, object value, DateTime? timestamp = null)
        {
            PredefinedNodes.TryGetValue(id, out var pstate);
            var state = pstate as BaseDataVariableState;
            if (state == null) return;
            state.Value = value;
            state.Timestamp = timestamp ?? DateTime.UtcNow;
            if (state.Historizing)
            {
                store.UpdateNode(state);
            }
            state.ClearChangeMasks(SystemContext, false);
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

        public void TriggerEvent<T>(NodeId eventId, NodeId emitter, NodeId source, string message, Action<ManagedEvent> builder = null)
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

        public void PopulateHistory(NodeId id, int count, DateTime start, string type = "int", int msdiff = 10, Func<int, object> valueBuilder = null)
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
                if (i == count - 1 && start > DateTime.UtcNow.AddSeconds(-1))
                {
                    UpdateNode(id, dv.Value, start);
                }
                else
                {
                    dv.SourceTimestamp = start;
                    dv.ServerTimestamp = start;
                    dv.StatusCode = StatusCodes.Good;
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
            var parent = PredefinedNodes[parentId];
            var obj = CreateObject(name);
            AddNodeRelation(obj, parent, ReferenceTypeIds.Organizes);
            
            if (audit)
            {
                var evtAdd = new AddNodesItem
                {
                    ParentNodeId = parentId,
                    NodeClass = NodeClass.Object,
                    TypeDefinition = ObjectTypeIds.BaseObjectType
                };
                var evt = new AuditAddNodesEventState(null);
                evt.NodesToAdd = new PropertyState<AddNodesItem[]>(evt);
                evt.NodesToAdd.Value = new[] { evtAdd };
                evt.Initialize(SystemContext, null, EventSeverity.Medium, new LocalizedText($"Audit add: {name}"));
                AddPredefinedNode(SystemContext, obj);
                Server.ReportEvent(evt);
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
            var parent = PredefinedNodes[parentId];
            var obj = CreateVariable(name, dataType);
            AddNodeRelation(obj, parent, ReferenceTypeIds.HasComponent);

            if (audit)
            {
                var evtAdd = new AddNodesItem
                {
                    ParentNodeId = parentId,
                    NodeClass = NodeClass.Variable,
                    TypeDefinition = VariableTypeIds.BaseDataVariableType
                };
                var evt = new AuditAddNodesEventState(null);
                evt.NodesToAdd = new PropertyState<AddNodesItem[]>(evt)
                {
                    Value = new[] { evtAdd }
                };
                evt.Initialize(SystemContext, null, EventSeverity.Medium, new LocalizedText($"Audit add: {name}"));
                AddPredefinedNode(SystemContext, obj);
                Server.ReportEvent(evt);
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
                evt.ReferencesToAdd = new PropertyState<AddReferencesItem[]>(evt)
                {
                    Value = new[] { evtRef }
                };
                evt.Initialize(SystemContext, null, EventSeverity.Medium, new LocalizedText($"Audit add reference"));
                AddNodeRelation(target, parent, type);
                Server.ReportEvent(evt);
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public NodeId AddProperty<T>(NodeId parentId, string name, NodeId dataType, object value, int rank = -1)
        {
            var parent = PredefinedNodes[parentId];
            var prop = parent.AddProperty<T>(name, dataType, rank);
            prop.NodeId = GenerateNodeId();
            prop.Value = value;
            AddPredefinedNode(SystemContext, prop);
            return prop.NodeId;
        }
        public void RemoveProperty(NodeId parentId, string name)
        {
            var parent = PredefinedNodes[parentId];
            var children = new List<BaseInstanceState>();
            parent.GetChildren(SystemContext, children);
            var prop = children.First(child => child.DisplayName.Text == name);
            parent.RemoveChild(prop);
            prop.Delete(SystemContext);
        }

        public void ReContextualize(NodeId id, NodeId oldParentId, NodeId newParentId, NodeId referenceType)
        {
            var state = PredefinedNodes[id];
            var oldParent = PredefinedNodes[oldParentId];
            var newParent = PredefinedNodes[newParentId];
            if (state == null || oldParent == null || newParent == null) return;
            oldParent.RemoveReference(referenceType, false, id);
            state.RemoveReference(referenceType, true, oldParentId);
            newParent.AddReference(referenceType, false, id);
            state.AddReference(referenceType, true, newParentId);
        }

        #endregion


        private NodeId GenerateNodeId()
        {
            return new NodeId(++nextId, NamespaceIndex);
        }

        #region address_space

        public override void CreateAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            log.Information("Create address space");
            try
            {
                LoadPredefinedNodes(SystemContext, externalReferences);

                // This is completely undocumented, but it is also the only way I found to change properties on the server object
                // Supposedly the "DiagnosticNodeManager" should handle this sort of stuff. But it doesn't, there exists a weird
                // GetDefaultHistoryCapability, but that seems to create a /new/ duplicate node on the server. This changes the existing one
                // (Creating a new one makes no sense whatsoever).
                var cfnm = (ConfigurationNodeManager)Server.NodeManager.NodeManagers.First(nm => nm.GetType() == typeof(ConfigurationNodeManager));
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
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to create address space");
            }
        }
        
        private void CreateBaseSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            lock (Lock)
            {
                var myobj = CreateObject("BaseRoot");
                AddNodeToExt(myobj, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                var myvar = CreateVariable("Variable 1", DataTypes.Double);
                myvar.Value = 0.0;
                AddNodeRelation(myvar, myobj, ReferenceTypeIds.HasComponent);

                var myvar2 = CreateVariable("Variable 2", DataTypes.Double);
                AddNodeRelation(myvar2, myobj, ReferenceTypeIds.HasComponent);

                var mystring = CreateVariable("Variable string", DataTypes.String);
                AddNodeRelation(mystring, myobj, ReferenceTypeIds.HasComponent);

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

                var myint = CreateVariable("Variable int", DataTypes.Int64);
                AddNodeRelation(myint, myobj, ReferenceTypeIds.HasComponent);

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
            lock (Lock)
            {
                var root = CreateObject("CustomRoot");
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                var myarray = CreateVariable("Variable Array", DataTypes.Double, 4);
                myarray.Value = new double[] { 0, 0, 0, 0 };
                AddNodeRelation(myarray, root, ReferenceTypeIds.HasComponent);

                var mystrarray = CreateVariable("Variable StringArray", DataTypes.String, 2);
                mystrarray.Value = new[] { "test1", "test2" };
                AddNodeRelation(mystrarray, root, ReferenceTypeIds.HasComponent);

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

                var ignoreVar = CreateVariable("IgnoreVar", ignoreType.NodeId);
                AddNodeRelation(ignoreVar, root, ReferenceTypeIds.HasComponent);

                var mysteryVar = CreateVariable("MysteryVar", numberType.NodeId);
                AddNodeRelation(mysteryVar, root, ReferenceTypeIds.HasComponent);

                var numberVar2 = CreateVariable("NumberVar", numberType2.NodeId);
                AddNodeRelation(numberVar2, root, ReferenceTypeIds.HasComponent);

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

                var enumVar2 = CreateVariable("EnumVar2", enumType2.NodeId);
                enumVar2.NodeId = new NodeId("enumvar", NamespaceIndex);
                enumVar2.Value = 123;
                AddNodeRelation(enumVar2, root, ReferenceTypeIds.HasComponent);

                var enumVar3 = CreateVariable("EnumVar3", enumType2.NodeId, 4);
                enumVar3.Value = new[] { 123, 123, 321, 123 };
                AddNodeRelation(enumVar3, root, ReferenceTypeIds.HasComponent);

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
                var variableType = CreateVariableType("CustomVariableType", VariableTypeIds.BaseDataVariableType,
                    externalReferences, DataTypeIds.Double);

                AddTypesToTypeTree(objType);
                AddTypesToTypeTree(variableType);

                store.AddHistorizingNode(myarray);
                store.AddHistorizingNode(mysteryVar);
                store.AddHistorizingNode(stringyVar);

                AddPredefinedNodes(SystemContext, root, myarray, mystrarray, stringyType, ignoreType, numberType, numberType2, stringyVar,
                    ignoreVar, mysteryVar, numberVar2, euprop, rangeprop, obj, obj2, objProp, objProp2, arrprop, arrprop2,
                    enumType1, enumType2, enumProp1, enumProp2, enumVar1, enumVar2, enumVar3, refType1, refType2, objType, variableType);

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


                AddNodeRelation(propType, obj1, ReferenceTypeIds.GeneratesEvent);
                AddNodeRelation(basicType1, obj1, ReferenceTypeIds.GeneratesEvent);
                AddNodeRelation(propType, obj2, ReferenceTypeIds.GeneratesEvent);
                AddNodeToExt(propType, ObjectIds.Server, ReferenceTypeIds.GeneratesEvent, externalReferences);
                AddNodeToExt(basicType1, ObjectIds.Server, ReferenceTypeIds.GeneratesEvent, externalReferences);
                AddNodeToExt(basicType2, ObjectIds.Server, ReferenceTypeIds.GeneratesEvent, externalReferences);
                AddNodeToExt(customType, ObjectIds.Server, ReferenceTypeIds.GeneratesEvent, externalReferences);

                AddPredefinedNodes(SystemContext, root, obj1, obj2, objexclude, var1, var2, propType, basicType1, basicType2, customType);

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
            }
        }
        
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        private void CreateAuditAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
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
                if (!externalReferences.TryGetValue(ObjectTypeIds.AuditAddNodesEventType, out var addreferences))
                {
                    externalReferences[ObjectTypeIds.AuditAddNodesEventType] = addreferences = new List<IReference>();
                }
                if (!externalReferences.TryGetValue(ObjectTypeIds.AuditAddReferencesEventType, out var refreferences))
                {
                    externalReferences[ObjectTypeIds.AuditAddReferencesEventType] = refreferences = new List<IReference>();
                }

                references.Add(new NodeStateReference(ReferenceTypeIds.GeneratesEvent, false, ObjectTypeIds.AuditAddNodesEventType));
                addreferences.Add(new NodeStateReference(ReferenceTypeIds.GeneratesEvent, true, ObjectTypeIds.AuditAddNodesEventType));
                references.Add(new NodeStateReference(ReferenceTypeIds.GeneratesEvent, false, ObjectIds.Server));
                refreferences.Add(new NodeStateReference(ReferenceTypeIds.GeneratesEvent, true, ObjectIds.Server));

                Ids.Audit.Root = root.NodeId;
                Ids.Audit.DirectAdd = addDirect.NodeId;
                Ids.Audit.RefAdd = addRef.NodeId;
                Ids.Audit.ExcludeObj = exclude.NodeId;
            }
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public void CreateWrongAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
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

                AddPredefinedNodes(SystemContext, root, rankImp, rankImpNoDim, wrongDim, tooLargeDimProp, nullType);

                Ids.Wrong.Root = root.NodeId;
                Ids.Wrong.RankImprecise = rankImp.NodeId;
                Ids.Wrong.RankImpreciseNoDim = rankImpNoDim.NodeId;
                Ids.Wrong.WrongDim = wrongDim.NodeId;
                Ids.Wrong.TooLargeProp = tooLargeDimProp.NodeId;
                Ids.Wrong.NullType = nullType.NodeId;
            }
        }

        private static void AddNodeToExt(NodeState state, NodeId id, NodeId typeId,
            IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            if (!externalReferences.TryGetValue(id, out var references))
            {
                externalReferences[id] = references = new List<IReference>();
            }

            state.AddReference(typeId, true, id);
            references.Add(new NodeStateReference(typeId, false, state.NodeId));
        }
        private static void AddNodeRelation(NodeState state, NodeState parent, NodeId typeId)
        {
            state.AddReference(typeId, true, parent.NodeId);
            parent.AddReference(typeId, false, state.NodeId);
        }

        private BaseObjectState CreateObject(string name)
        {
            var state = new BaseObjectState(null)
            {
                NodeId = GenerateNodeId(), BrowseName = new QualifiedName(name, NamespaceIndex)
            };
            state.DisplayName = state.BrowseName.Name;
            state.TypeDefinitionId = ObjectTypeIds.BaseObjectType;
            return state;
        }

        private BaseDataVariableState CreateVariable(string name, NodeId dataType, int dim = -1)
        {
            var state = new BaseDataVariableState(null)
            {
                NodeId = GenerateNodeId(), BrowseName = new QualifiedName(name, NamespaceIndex)
            };
            state.DisplayName = state.BrowseName.Name;
            state.TypeDefinitionId = VariableTypeIds.BaseDataVariableType;
            state.DataType = dataType;
            state.ValueRank = ValueRanks.Scalar;
            if (dim > -1)
            {
                state.ValueRank = ValueRanks.OneDimension;
                state.ArrayDimensions = new[] {(uint) dim};
            }

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

            return type;
        }

        private BaseObjectTypeState CreateObjectType(string name, NodeId parent, IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            var type = new BaseObjectTypeState
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

            return type;
        }

        private BaseDataVariableTypeState CreateVariableType(string name, NodeId parent,
            IDictionary<NodeId, IList<IReference>> externalReferences, NodeId dataType)
        {
            var type = new BaseDataVariableTypeState
            {
                NodeId = GenerateNodeId(),
                BrowseName = new QualifiedName(name, NamespaceIndex),
                DataType = dataType
            };
            type.DisplayName = type.BrowseName.Name;
            if (!externalReferences.TryGetValue(parent, out var references))
            {
                externalReferences[parent] = references = new List<IReference>();
            }
            type.AddReference(ReferenceTypeIds.HasSubtype, true, parent);
            references.Add(new NodeStateReference(ReferenceTypeIds.HasSubtype, false, type.NodeId));

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

            return type;
        }

        private void AddPredefinedNodes(ServerSystemContext context, params NodeState[] nodes)
        {
            foreach (var node in nodes)
            {
                log.Debug("Adding node: {name}, {id}", node.DisplayName, node.NodeId);
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

                    data.DataValues.AddRange(rawData);

                    log.Information("Read raw modified: {cnt}", rawData.Count());

                    errors[handle.Index] = ServiceResult.Good;

                    if (!final)
                    {
                        result.ContinuationPoint = SaveContinuationPoint(context, request);
                    }

                    result.HistoryData = new ExtensionObject(data);
                }
                catch (Exception ex)
                {
                    log.Error(ex, "Failed to read history");
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

                        results[i] = new HistoryReadResult();
                        results[i].HistoryData = null;
                        results[i].ContinuationPoint = null;
                        results[i].StatusCode = StatusCodes.Good;
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

                    log.Information("Read events: {cnt}", rawData.Count());

                    errors[handle.Index] = ServiceResult.Good;

                    if (!final)
                    {
                        result.ContinuationPoint = SaveContinuationPoint(context, request);
                    }

                    result.HistoryData = new ExtensionObject(events);
                }
                catch (Exception ex)
                {
                    log.Error(ex, "Failed to read history");
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
        private HistoryEventFieldList GetEventFields(InternalEventHistoryRequest request, IFilterTarget instance)
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
    }

    class InternalHistoryRequest
    {
        public NodeId Id;
        public byte[] ContinuationPoint;
        public uint NumValuesPerNode;
        public int MemoryIndex;
        public DateTime StartTime;
        public DateTime EndTime;
        public bool IsReverse;
    }

    class InternalEventHistoryRequest : InternalHistoryRequest
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
        Wrong
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
        }
        public BaseNodeReference Base { get; set; }
        public FullNodeReference Full { get; set; }
        public CustomNodeReference Custom { get; set; }
        public EventNodeReference Event { get; set; }
        public AuditNodeReference Audit { get; set; }
        public WrongNodeReference Wrong { get; set; }
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
    }
    #endregion
}

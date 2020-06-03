using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
        public void UpdateNode(NodeId id, object value)
        {
            PredefinedNodes.TryGetValue(id, out var pstate);
            var state = pstate as BaseDataVariableState;
            if (state == null) return;
            state.Value = value;
            state.Timestamp = DateTime.Now;
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

        public IEnumerable<BaseEventState> FetchEventHistory(NodeId id)
        {
            return store.GetFullEventHistory(id);
        }

        public void TriggerEvent<T>(NodeId eventId, NodeId emitter, NodeId source, string message, Action<ManagedEvent> builder = null)
            where T : ManagedEvent
        {
            var eventState = (BaseObjectTypeState)PredefinedNodes[eventId];
            var emitterState = PredefinedNodes[emitter];
            var sourceState = PredefinedNodes[source];

            var manager = new TestEventManager<T>(SystemContext, eventState, NamespaceUris.First());

            var evt = manager.CreateEvent(emitterState, sourceState, message);
            builder(evt);
            if (emitter == Ids.Event.Obj1 || emitter == ObjectIds.Server)
            {
                store.HistorizeEvent(emitter, evt);
            }
            emitterState.ReportEvent(SystemContext, evt);
        }

        public void PopulateHistory(NodeId id, int count, string type = "int", int msdiff = 10, Func<int, object> valueBuilder = null)
        {
            var diff = TimeSpan.FromMilliseconds(msdiff);
            var start = DateTime.Now.Subtract(diff * count);
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
                dv.SourceTimestamp = start;
                dv.ServerTimestamp = start;
                dv.StatusCode = StatusCodes.Good;
                store.HistorizeDataValue(id, dv);
                start = start.AddMilliseconds(msdiff);
            }
        }

        public void PopulateEventHistory<T>(NodeId eventId,
            NodeId emitter,
            NodeId source,
            string message,
            int count,
            int msdiff = 10,
            Action<ManagedEvent> builder = null)
            where T : ManagedEvent
        {
            var diff = TimeSpan.FromMilliseconds(msdiff);
            var start = DateTime.Now.Subtract(diff * count);

            var eventState = (BaseObjectTypeState)PredefinedNodes[eventId];
            var emitterState = PredefinedNodes[emitter];
            var sourceState = PredefinedNodes[source];

            var manager = new TestEventManager<T>(SystemContext, eventState, NamespaceUris.First());

            for (int i = 0; i < count; i++)
            {
                var evt = manager.CreateEvent(emitterState, sourceState, message + " " + i);
                builder(evt);
                store.HistorizeEvent(emitter, evt);
                start = start.AddMilliseconds(msdiff);
            }
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public void AddObject(NodeId parentId, string name, bool audit = false)
        {
            var parent = PredefinedNodes[parentId];
            var obj = CreateObject(name);
            AddNodeRelation(obj, parent, ReferenceTypeIds.Organizes);
            
            if (audit)
            {
                var evtAdd = new AddNodesItem
                {
                    ParentNodeId = obj.NodeId,
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
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public void AddVariable(NodeId parentId, string name, NodeId dataType, bool audit = false)
        {
            var parent = PredefinedNodes[parentId];
            var obj = CreateVariable(name, dataType);
            AddNodeRelation(obj, parent, ReferenceTypeIds.HasComponent);

            if (audit)
            {
                var evtAdd = new AddNodesItem
                {
                    ParentNodeId = obj.NodeId,
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
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public void AddReference(NodeId sourceId, NodeId targetId, NodeId type, bool audit = false)
        {
            var source = PredefinedNodes[sourceId];
            var target = PredefinedNodes[targetId];
            if (audit)
            {
                var evtRef = new AddReferencesItem
                {
                    IsForward = true,
                    SourceNodeId = sourceId,
                    TargetNodeId = targetId,
                    ReferenceTypeId = type
                };
                var evt = new AuditAddReferencesEventState(null);
                evt.ReferencesToAdd = new PropertyState<AddReferencesItem[]>(evt)
                {
                    Value = new[] { evtRef }
                };
                evt.Initialize(SystemContext, null, EventSeverity.Medium, new LocalizedText($"Audit add reference"));
                AddNodeRelation(source, target, type);
                Server.ReportEvent(evt);
            }
            else
            {
                AddNodeRelation(source, target, type);
            }
        }
        #endregion


        private NodeId GenerateNodeId()
        {
            return new NodeId(++nextId, NamespaceIndex);
        }

        #region address_space

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
            Justification =
                "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        public override void CreateAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            Log.Information("Create address space");
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
                    var node = cfnm.Find(ObjectIds.HistoryServerCapabilities);
                    if (node.FindChildBySymbolicName(SystemContext, "AccessHistoryDataCapability") is PropertyState variable)
                    {
                        variable.Value = true;
                    }

                    if (node.FindChildBySymbolicName(SystemContext, "AccessHistoryEventsCapability") is PropertyState variable2)
                    {
                        variable2.Value = true;
                    }

                    // Seems like this node manager manages the entire server node, and so is very relevant when it comes to presenting
                    // information about the server to the client. I suspect that this may be configurable in xml (hence "configuration"),
                    // but I can't find any documentation.
                    var server = (BaseObjectState)cfnm.Find(ObjectIds.Server);
                    server.EventNotifier |= EventNotifiers.HistoryRead;
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
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to create address space");
            }
        }
        
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
        private void CreateBaseSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            lock (Lock)
            {
                var myobj = CreateObject("BaseRoot");
                AddNodeToExt(myobj, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);

                var myvar = CreateVariable("Variable 1", DataTypes.Double);
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
                AddNodeToExt(root, ObjectIds.ObjectsFolder, ReferenceTypeIds.Organizes, externalReferences);
                AddPredefinedNode(SystemContext, root);

                var myobj2 = CreateObject("Object 1");
                AddNodeRelation(myobj2, root, ReferenceTypeIds.Organizes);
                AddPredefinedNode(SystemContext, myobj2);

                for (int i = 0; i < 2000; i++)
                {
                    var varch = CreateVariable("SubVariable " + i, DataTypes.Double);
                    AddNodeRelation(varch, myobj2, ReferenceTypeIds.HasComponent);
                    AddPredefinedNode(SystemContext, varch);
                }

                var myobj3 = CreateObject("Object 2");
                AddNodeRelation(myobj3, root, ReferenceTypeIds.Organizes);
                AddPredefinedNode(SystemContext, myobj3);

                for (int i = 0; i < 5; i++)
                {
                    var lastdeepobj = myobj3;
                    for (int j = 0; j < 30; j++)
                    {
                        var deepobj = CreateObject($"DeepObject {i}, {j}");
                        AddNodeRelation(deepobj, lastdeepobj, ReferenceTypeIds.Organizes);
                        AddPredefinedNode(SystemContext, deepobj);
                        lastdeepobj = deepobj;
                    }
                }
                Ids.Full.Root = root.NodeId;
                Ids.Full.WideRoot = myobj2.NodeId;
                Ids.Full.DeepRoot = myobj3.NodeId;
            }
        }
        
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
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

                var numberVar = CreateVariable("MysteryVar", numberType.NodeId);
                AddNodeRelation(numberVar, root, ReferenceTypeIds.HasComponent);

                var numberVar2 = CreateVariable("NumberVar", numberType2.NodeId);
                AddNodeRelation(numberVar2, root, ReferenceTypeIds.HasComponent);

                var euinf = new EUInformation("°C", "degree Celsius", "http://www.opcfoundation.org/UA/units/un/cefact")
                {
                    UnitId = 4408652
                };
                var euprop = numberVar.AddProperty<EUInformation>("EngineeringUnits", DataTypes.EUInformation, -1);
                euprop.NodeId = GenerateNodeId();
                euprop.Value = euinf;

                var eurange = new Opc.Ua.Range(100, 0);

                var rangeprop = numberVar.AddProperty<Opc.Ua.Range>("EURange", DataTypes.Range, -1);
                rangeprop.NodeId = GenerateNodeId();
                rangeprop.Value = eurange;

                store.AddHistorizingNode(myarray);
                store.AddHistorizingNode(numberVar);

                AddPredefinedNodes(SystemContext, root, myarray, mystrarray, stringyType, ignoreType, numberType, numberType2, stringyVar,
                    ignoreVar, numberVar, numberVar2, euprop, rangeprop);

                Ids.Custom.Root = root.NodeId;
                Ids.Custom.Array = myarray.NodeId;
                Ids.Custom.StringArray = mystrarray.NodeId;
                Ids.Custom.StringyType = stringyType.NodeId;
                Ids.Custom.IgnoreType = ignoreType.NodeId;
                Ids.Custom.MysteryType = numberType.NodeId;
                Ids.Custom.NumberType = numberType2.NodeId;
                Ids.Custom.StringyVar = stringyVar.NodeId;
                Ids.Custom.IgnoreVar = ignoreVar.NodeId;
                Ids.Custom.MysteryVar = numberVar.NodeId;
                Ids.Custom.NumberVar = numberVar2.NodeId;
            }
        }
        
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification =
            "NodeStates are disposed in CustomNodeManager2, so long as they are added to the list of predefined nodes")]
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
                obj2.EventNotifier = EventNotifiers.SubscribeToEvents | EventNotifiers.HistoryRead;

                Task.Run(async () =>
                {
                    while (true)
                    {
                        try
                        {
                            var evt = testEmitter.CreateEvent(obj1, obj1, "Test Event");
                            evt.PropertyNum.Value = 123;
                            evt.PropertyString.Value = "TestTest";
                            evt.SubType.Value = "TestSubType";
                            store.HistorizeEvent(obj1.NodeId, evt);
                            obj1.ReportEvent(SystemContext, evt);
                            //Server.ReportEvent(SystemContext, evt);
                            await Task.Delay(1000);
                        }
                        catch (Exception ex)
                        {
                            Log.Error(ex, "Failed to emmit");
                        }
                    }
                });
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

                Task.Run(async () => {
                    int cnt = 0;
                    while (true)
                    {
                        try
                        {
                            var addObj = CreateObject($"AddObject {cnt}");
                            AddNodeRelation(addObj, addDirect, ReferenceTypeIds.HasComponent);

                            var evtAdd = new AddNodesItem
                            {
                                ParentNodeId = addObj.NodeId,
                                NodeClass = NodeClass.Object,
                                TypeDefinition = ObjectTypeIds.BaseObjectType
                            };
                            var evt = new AuditAddNodesEventState(null);
                            evt.NodesToAdd = new PropertyState<AddNodesItem[]>(evt);
                            evt.NodesToAdd.Value = new[] { evtAdd };
                            evt.Initialize(SystemContext, null, EventSeverity.Medium, new LocalizedText($"Audit add: {cnt}"));
                            AddPredefinedNode(SystemContext, addObj);

                            Server.ReportEvent(evt);

                            await Task.Delay(1000);
                        }
                        catch (Exception ex)
                        {
                            Log.Error(ex, "Failure during growing");
                        }
                        cnt++;
                    }
                });
                Ids.Audit.Root = root.NodeId;
                Ids.Audit.DirectAdd = addDirect.NodeId;
                Ids.Audit.RefAdd = addRef.NodeId;
                Ids.Audit.ExcludeObj = exclude.NodeId;
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

        private void AddPredefinedNodes(ServerSystemContext context, params NodeState[] nodes)
        {
            foreach (var node in nodes)
            {
                Log.Debug("Adding node: {name}, {id}", node.DisplayName, node.NodeId);
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

                    Log.Information("Read raw modified: {cnt}", rawData.Count());

                    errors[handle.Index] = ServiceResult.Good;

                    if (!final)
                    {
                        result.ContinuationPoint = SaveContinuationPoint(context, request);
                    }

                    result.HistoryData = new ExtensionObject(data);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to read history");
                    errors[handle.Index] = ServiceResult.Create(ex, StatusCodes.BadUnexpectedError,
                        "Unexpected error processing request.");
                }
            }
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

                    Log.Information("Read events: {cnt}", rawData.Count());

                    errors[handle.Index] = ServiceResult.Good;

                    if (!final)
                    {
                        result.ContinuationPoint = SaveContinuationPoint(context, request);
                    }

                    result.HistoryData = new ExtensionObject(events);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to read history");
                    errors[handle.Index] = ServiceResult.Create(ex, StatusCodes.BadUnexpectedError,
                        "Unexpected error processing request.");
                }
            }
        }

        private InternalHistoryRequest LoadContinuationPoint(
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
        private byte[] SaveContinuationPoint(
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

        private InternalHistoryRequest CreateHistoryReadRequest(
            ReadRawModifiedDetails details,
            HistoryReadValueId nodeToRead)
        {
            bool timeFlowsBackward = (details.StartTime == DateTime.MinValue) ||
                                     (details.EndTime != DateTime.MinValue && details.EndTime < details.StartTime);

            return new InternalHistoryRequest
            {
                ContinuationPoint = null,
                EndTime = details.EndTime.ToLocalTime(),
                Id = nodeToRead.NodeId,
                IsReverse = timeFlowsBackward,
                MemoryIndex = -1,
                NumValuesPerNode = details.NumValuesPerNode,
                StartTime = details.StartTime.ToLocalTime()
            };
        }
        private InternalEventHistoryRequest CreateEventHistoryRequest(
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
                EndTime = details.EndTime.ToLocalTime(),
                Id = nodeToRead.NodeId,
                IsReverse = timeFlowsBackward,
                MemoryIndex = -1,
                NumValuesPerNode = details.NumValuesPerNode,
                StartTime = details.StartTime.ToLocalTime(),
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
        Auditing
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
        }
        public BaseNodeReference Base { get; set; }
        public FullNodeReference Full { get; set; }
        public CustomNodeReference Custom { get; set; }
        public EventNodeReference Event { get; set; }
        public AuditNodeReference Audit { get; set; }
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
    #endregion
}

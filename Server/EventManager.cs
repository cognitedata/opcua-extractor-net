using Opc.Ua.Server;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using Serilog;

namespace Server
{
    class TestEventManager
    {
        protected readonly ServerSystemContext context;
        public readonly BaseObjectTypeState EventType;
        public readonly string NamespaceUri;
        public TestEventManager(ServerSystemContext systemContext, BaseObjectTypeState eventType, string namespaceUri)
        {
            context = systemContext;
            NamespaceUri = namespaceUri;
            EventType = eventType;
        }
    }
    class TestEventManager<T> : TestEventManager where T : ManagedEvent 
    {
        public TestEventManager(ServerSystemContext systemContext, BaseObjectTypeState eventType, string namespaceUri) :
            base(systemContext, eventType, namespaceUri)
        { }

        public T CreateEvent(NodeState emitter, NodeState source, string message = "", EventSeverity severity = EventSeverity.Low)
        {
            var evt = (T)Activator.CreateInstance(typeof(T), emitter, this);
            evt.Initialize(context, source, severity, new LocalizedText(message));
            return evt;
        }
    }

    abstract class ManagedEvent : BaseEventState
    {
        protected TestEventManager manager;

        public ManagedEvent(NodeState parent, TestEventManager manager) : base(parent)
        {
            this.manager = manager;
        }
        protected override NodeId GetDefaultTypeDefinitionId(NamespaceTable namespaceUris)
        {
            return manager.EventType.NodeId;
        }
        protected override void Initialize(ISystemContext context)
        {
            Initialize(context);
            InitializeOptionalChildren(context);
        }

        protected override void Initialize(ISystemContext context, NodeState source)
        {
            InitializeOptionalChildren(context);
            base.Initialize(context, source);
        }

        public override void GetChildren(
            ISystemContext context,
            IList<BaseInstanceState> children)
        {
            foreach (var prop in GetType().GetProperties().Where(prop =>
                typeof(BaseInstanceState).IsAssignableFrom(prop.PropertyType)))
            {
                var value = prop.GetValue(this, null) as BaseInstanceState;
                if (value != null)
                {
                    children.Add(value);
                }
            }

            base.GetChildren(context, children);
        }

        protected override BaseInstanceState FindChild(
            ISystemContext context,
            QualifiedName browseName,
            bool createOrReplace,
            BaseInstanceState replacement)
        {
            if (QualifiedName.IsNull(browseName)) return null;

            var instanceProp = GetType().GetProperties()
                .FirstOrDefault(prop => prop.Name == browseName.Name && typeof(BaseInstanceState).IsAssignableFrom(prop.PropertyType));
            if (instanceProp != null)
            {
                var value = instanceProp.GetValue(this, null) as BaseInstanceState;
                if (createOrReplace)
                {
                    if (value == null)
                    {
                        if (replacement == null)
                        {
                            instanceProp.SetValue(this, Activator.CreateInstance(instanceProp.GetType()));
                        }
                        else
                        {
                            instanceProp.SetValue(this, replacement);
                        }
                    }
                }
                if (value != null)
                {
                    return value;
                }
            }

            return base.FindChild(context, browseName, createOrReplace, replacement);
        }
    }
    class PropertyEvent : ManagedEvent
    {
        public PropertyEvent(NodeState parent, TestEventManager manager) : base(parent, manager)
        {
            PropertyNum = new PropertyState<float>(this);
            PropertyString = new PropertyState<string>(this);
            SubType = new PropertyState<string>(this);
        }

        public PropertyEvent(NodeState parent, TestEventManager manager, float propNum, string propStr, string subtype) : this(parent, manager)
        {
            PropertyNum.Value = propNum;
            PropertyString.Value = propStr;
            SubType.Value = subtype;
        }

        public PropertyState<float> PropertyNum
        {
            get { return propertyNum; }
            set
            {
                if (!ReferenceEquals(propertyNum, value))
                {
                    ChangeMasks |= NodeStateChangeMasks.Children;
                }

                propertyNum = value;
            }
        }
        private PropertyState<float> propertyNum;

        public PropertyState<string> PropertyString
        {
            get { return propertyString; }
            set
            {
                if (!ReferenceEquals(propertyString, value))
                {
                    ChangeMasks |= NodeStateChangeMasks.Children;
                }

                propertyString = value;
            }
        }
        private PropertyState<string> propertyString;

        public PropertyState<string> SubType
        {
            get { return subType; }
            set
            {
                if (!ReferenceEquals(subType, value))
                {
                    ChangeMasks |= NodeStateChangeMasks.Children;
                }

                subType = value;
            }
        }
        private PropertyState<string> subType;


    }
    class BasicEvent1 : ManagedEvent
    {
        public BasicEvent1(NodeState parent, TestEventManager manager) : base(parent, manager) { }
    }
    class BasicEvent2 : ManagedEvent
    {
        public BasicEvent2(NodeState parent, TestEventManager manager) : base(parent, manager) { }
    }
    class CustomEvent : ManagedEvent
    {
        public CustomEvent(NodeState parent, TestEventManager manager) : base(parent, manager)
        {
            TypeProp = new PropertyState<string>(this);
        }

        public CustomEvent(NodeState parent, TestEventManager manager, string typeProp) : this(parent, manager)
        {
            TypeProp.Value = typeProp;
        }

        public PropertyState<string> TypeProp
        {
            get { return typeProp; }
            set
            {
                if (!ReferenceEquals(typeProp, value))
                {
                    ChangeMasks |= NodeStateChangeMasks.Children;
                }

                typeProp = value;
            }
        }
        private PropertyState<string> typeProp;
    }
}

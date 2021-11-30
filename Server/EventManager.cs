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
using System.Collections.Generic;
using System.Linq;

namespace Server
{
    /// <summary>
    /// Creating events in the SDK can be a bit of a pain, the purpose of these classes is to
    /// create wrappers for building events easily, so that we don't have to repeat ourselves that much.
    /// The EventManager is responsible for creating events, and managing the event type, so that the
    /// events have access to information they need.
    /// </summary>
    public class TestEventManager
    {
        protected ServerSystemContext Context { get; }
        public BaseObjectTypeState EventType { get; }

        public string NamespaceUri { get; }
        public TestEventManager(ServerSystemContext systemContext, BaseObjectTypeState eventType, string namespaceUri)
        {
            Context = systemContext;
            NamespaceUri = namespaceUri;
            EventType = eventType;
        }
    }
    public class TestEventManager<T> : TestEventManager where T : ManagedEvent
    {
        public TestEventManager(ServerSystemContext systemContext, BaseObjectTypeState eventType, string namespaceUri) :
            base(systemContext, eventType, namespaceUri)
        { }

        public T CreateEvent(NodeState emitter, NodeState source, string message = "", EventSeverity severity = EventSeverity.Low)
        {
            var evt = (T)Activator.CreateInstance(typeof(T), emitter, this);
            evt.EventType = new PropertyState<NodeId>(evt) { Value = EventType.NodeId };
            evt.Initialize(Context, source, severity, new LocalizedText(message));
            return evt;
        }
    }
    /// <summary>
    /// An implementation of events using reflection. Fields are generated from public properties
    /// which extends BaseInstanceState, then the name is just generated directly from those.
    /// This is convenient, as it makes creating new test event types much easier.
    /// </summary>
    public abstract class ManagedEvent : BaseEventState
    {
        private readonly TestEventManager manager;

        protected ManagedEvent(NodeState parent, TestEventManager manager) : base(parent)
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
            if (children == null)
            {
                base.GetChildren(context, children);
                return;
            }
            foreach (var prop in GetType().GetProperties().Where(prop =>
                typeof(BaseInstanceState).IsAssignableFrom(prop.PropertyType)))
            {
                if (prop.GetValue(this, null) is BaseInstanceState value)
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

    /// <summary>
    /// Event with a few basic properties
    /// </summary>
    public class PropertyEvent : ManagedEvent
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

    /// <summary>
    /// Plain event type
    /// </summary>
    public class BasicEvent1 : ManagedEvent
    {
        public BasicEvent1(NodeState parent, TestEventManager manager) : base(parent, manager) { }
    }

    /// <summary>
    /// Alternative plain event type.
    /// </summary>
    public class BasicEvent2 : ManagedEvent
    {
        public BasicEvent2(NodeState parent, TestEventManager manager) : base(parent, manager) { }
    }

    /// <summary>
    /// Event with a custom "TypeProp" field, used for testing mapping in the extractor.
    /// </summary>
    public class CustomEvent : ManagedEvent
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

    /// <summary>
    /// "Deep" Event in two ways, it is a subtype of the PropertyEvent, and it contains an object property with its own
    /// children, so that we get deep properties.
    /// </summary>
    public class DeepEvent : PropertyEvent
    {
        public BaseObjectState DeepObj
        {
            get { return deepObj; }
            set
            {
                if (!ReferenceEquals(deepObj, value))
                {
                    ChangeMasks |= NodeStateChangeMasks.Children;
                }
                deepObj = value;
            }
        }
        private BaseObjectState deepObj;

        public PropertyState DeepProp { get; }

        public DeepEvent(NodeState parent, TestEventManager manager) : base(parent, manager)
        {
            PropertyNum = new PropertyState<float>(this);
            PropertyString = new PropertyState<string>(this);
            SubType = new PropertyState<string>(this);
            DeepObj = new BaseObjectState(this);
            DeepProp = DeepObj.AddProperty<string>(nameof(DeepProp), DataTypeIds.String, ValueRanks.Scalar);
        }

        public DeepEvent(NodeState parent, TestEventManager manager, float propNum, string propStr, string subtype, string deepVal)
            : base(parent, manager, propNum, propStr, subtype)
        {
            PropertyNum.Value = propNum;
            PropertyString.Value = propStr;
            SubType.Value = subtype;
            DeepProp.Value = deepVal;
        }
    }
}

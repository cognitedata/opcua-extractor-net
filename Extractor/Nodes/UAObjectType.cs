using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Nodes
{
    public class ObjectTypeAttributes : BaseNodeAttributes
    {
        public bool IsAbstract { get; private set; }
        public ObjectTypeAttributes() : base(NodeClass.ObjectType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.IsAbstract;
            foreach (var attr in base.GetAttributeSet(config)) yield return attr;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.IsAbstract:
                    IsAbstract = value.GetValue(false);
                    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }

        public void LoadFromNodeState(BaseObjectTypeState state)
        {
            IsAbstract = state.IsAbstract;
            LoadFromBaseNodeState(state);
        }
    }

    public class UAObjectType : BaseUAType
    {
        public UAObjectType(NodeId id, string? displayName, QualifiedName? browseName, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
        {
            FullAttributes = new ObjectTypeAttributes();
            Attributes.DisplayName = displayName;
            Attributes.BrowseName = browseName;
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UAObjectType(NodeId id) : this(id, null, null, null, null)
        {
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ObjectTypeAttributes FullAttributes { get; }

        public bool IsEventType()
        {
            if (Id == ObjectTypeIds.BaseEventType) return true;
            if (Parent != null) return Parent is UAObjectType tp && tp.IsEventType();
            return false;
        }
    }
}

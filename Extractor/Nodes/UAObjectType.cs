using Cognite.OpcUa.Config;
using Opc.Ua;
using System.Collections.Generic;

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
            yield return Attributes.Description;
            yield return Attributes.IsAbstract;
        }

        public override void LoadAttribute(DataValue value, uint attributeId)
        {
            switch (attributeId)
            {
                case Attributes.IsAbstract:
                    IsAbstract = value.GetValue(false);
                    break;
                default:
                    base.LoadAttribute(value, attributeId);
                    break;
            }
        }
    }

    public class UAObjectType : BaseUANode
    {
        public UAObjectType(NodeId id, string displayName, NodeId parentId) : base(id, displayName, parentId)
        {
            FullAttributes = new ObjectTypeAttributes();
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ObjectTypeAttributes FullAttributes { get; }
    }
}

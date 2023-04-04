using Cognite.OpcUa.Config;
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Nodes
{
    public class ReferenceTypeAttributes : BaseNodeAttributes
    {
        public string? InverseName { get; private set; }
        public ReferenceTypeAttributes() : base(NodeClass.ReferenceType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.Description;
            yield return Attributes.InverseName;
        }

        public override void LoadAttribute(DataValue value, uint attributeId)
        {
            switch (attributeId)
            {
                case Attributes.InverseName:
                    InverseName = value.GetValue<string?>(null);
                    break;
                default:
                    base.LoadAttribute(value, attributeId);
                    break;
            }
        }
    }

    public class UAReferenceType : BaseUANode
    {
        public UAReferenceType(NodeId id, string displayName, NodeId parentId) : base(id, displayName, parentId)
        {
            FullAttributes = new ReferenceTypeAttributes();
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ReferenceTypeAttributes FullAttributes { get; }
    }
}

using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
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

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.InverseName:
                    InverseName = value.GetValue<string?>(null);
                    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }
    }

    public class UAReferenceType : BaseUANode
    {
        public UAReferenceType(NodeId id, string? displayName, BaseUANode? parent, NodeId? parentId) : base(id, displayName, parent, parentId)
        {
            FullAttributes = new ReferenceTypeAttributes();
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UAReferenceType(NodeId id) : this(id, null, null, null)
        {
        }

        public void Initialize(ReferenceDescription referenceDesc, BaseUANode? parent, NodeId? parentId)
        {
            DisplayName = referenceDesc.DisplayName?.Text;
            Parent = parent;
            FallbackParentId = parentId;
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ReferenceTypeAttributes FullAttributes { get; }
    }
}

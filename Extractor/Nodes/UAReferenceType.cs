using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;
using System.Xml.Linq;

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
            yield return Attributes.InverseName;
            foreach (var attr in base.GetAttributeSet(config)) yield return attr;
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

        public void LoadFromNodeState(ReferenceTypeState state)
        {
            InverseName = state.InverseName?.Text;
            LoadFromBaseNodeState(state);
        }
    }

    public class UAReferenceType : BaseUAType
    {
        public UAReferenceType(NodeId id, string? displayName, string? browseName, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
        {
            FullAttributes = new ReferenceTypeAttributes();
            Attributes.DisplayName = displayName;
            Attributes.BrowseName = browseName;
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UAReferenceType(NodeId id) : this(id, null, null, null, null)
        {
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ReferenceTypeAttributes FullAttributes { get; }

        /// <summary>
        /// Retrieve a descriptor for this reference type.
        /// </summary>
        /// <param name="isInverse">True to get the inverse name of this reference type.</param>
        /// <returns>Descriptor for this reference, if set.</returns>
        public string? GetName(bool isInverse)
        {
            if (isInverse && !string.IsNullOrEmpty(FullAttributes.InverseName)) return FullAttributes.InverseName;
            return Attributes.DisplayName;
        }
    }
}

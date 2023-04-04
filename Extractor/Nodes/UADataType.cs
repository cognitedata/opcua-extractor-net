using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Nodes
{
    public class DataTypeAttributes : BaseNodeAttributes
    {
        public bool IsAbstract { get; private set; }
        public Variant? DataTypeDefinition { get; private set; }
        public DataTypeAttributes() : base(NodeClass.DataType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.Description;
            yield return Attributes.DataTypeDefinition;
            yield return Attributes.IsAbstract;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.IsAbstract:
                    IsAbstract = value.GetValue(false);
                    break;
                case Attributes.DataTypeDefinition:
                    DataTypeDefinition = value.WrappedValue;
                    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }
    }

    public class UADataType : BaseUANode
    {
        public UADataType(NodeId id, string? displayName, BaseUANode? parent, NodeId? parentId) : base(id, displayName, parent, parentId)
        {
            FullAttributes = new DataTypeAttributes();
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UADataType(NodeId id) : this(id, null, null, null)
        {
        }

        public void Initialize(ReferenceDescription referenceDesc, BaseUANode? parent, NodeId? parentId)
        {
            DisplayName = referenceDesc.DisplayName?.Text;
            Parent = parent;
            FallbackParentId = parentId;
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public DataTypeAttributes FullAttributes { get; }
    }
}

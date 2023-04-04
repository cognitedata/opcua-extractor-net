using System.Collections.Generic;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;

namespace Cognite.OpcUa.Nodes
{
    public class VariableTypeAttributes : BaseNodeAttributes
    {
        public bool IsAbstract { get; private set; }
        public int ValueRank { get; private set; }
        public UADataType DataType { get; private set; } = null!;
        public int[]? ArrayDimensions { get; private set; }
        public Variant? Value { get; private set; }
        public VariableTypeAttributes() : base(NodeClass.VariableType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.Description;
            yield return Attributes.IsAbstract;
            yield return Attributes.DataType;
            yield return Attributes.ArrayDimensions;
            yield return Attributes.ValueRank;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.IsAbstract:
                    IsAbstract = value.GetValue(false);
                    break;
                case Attributes.DataType:
                    var dataTypeId = value.GetValue(NodeId.Null);
                    DataType = typeManager.GetDataType(dataTypeId);
                    break;
                case Attributes.ValueRank:
                    ValueRank = value.GetValue(ValueRanks.Any);
                    break;
                case Attributes.ArrayDimensions:
                    if (value.Value is int[] dimVal)
                    {
                        ArrayDimensions = dimVal;
                    }
                    break;
                case Attributes.Value:
                    Value = value.WrappedValue;
                    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }
    }

    public class UAVariableType : BaseUANode
    {
        public UAVariableType(NodeId id, string? displayName, BaseUANode? parent, NodeId? parentId) : base(id, displayName, parent, parentId)
        {
            FullAttributes = new VariableTypeAttributes();
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UAVariableType(NodeId id) : this(id, null, null, null)
        {
        }

        public void Initialize(ReferenceDescription referenceDesc, BaseUANode? parent, NodeId? parentId)
        {
            DisplayName = referenceDesc.DisplayName?.Text;
            Parent = parent;
            FallbackParentId = parentId;
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public VariableTypeAttributes FullAttributes { get; }
    }
}

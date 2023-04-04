using System.Collections.Generic;
using Cognite.OpcUa.Config;
using Opc.Ua;

namespace Cognite.OpcUa.Nodes
{
    public class VariableTypeAttributes : BaseNodeAttributes
    {
        public bool IsAbstract { get; private set; }
        public int ValueRank { get; private set; }
        public NodeId DataTypeId { get; private set; } = null!;
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

        public override void LoadAttribute(DataValue value, uint attributeId)
        {
            switch (attributeId)
            {
                case Attributes.IsAbstract:
                    IsAbstract = value.GetValue(false);
                    break;
                case Attributes.DataType:
                    DataTypeId = value.GetValue(NodeId.Null);
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
                    base.LoadAttribute(value, attributeId);
                    break;
            }
        }
    }

    public class UAVariableType : BaseUANode
    {
        public UAVariableType(NodeId id, string displayName, NodeId parentId) : base(id, displayName, parentId)
        {
            FullAttributes = new VariableTypeAttributes();
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public VariableTypeAttributes FullAttributes { get; }
    }
}

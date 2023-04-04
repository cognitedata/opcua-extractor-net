using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Nodes
{
    public class VariableAttributes : BaseNodeAttributes
    {
        public bool Historizing { get; private set; }
        public int ValueRank { get; private set; }
        public UADataType DataType { get; private set; } = null!;
        public int[]? ArrayDimensions { get; set; }
        public byte AccessLevel { get; private set; }
        public Variant? Value { get; private set; }

        public UAVariableType TypeDefinition { get; private set; }

        public VariableAttributes(UAVariableType type) : base(NodeClass.Variable)
        {
            TypeDefinition = type;
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.Description;
            if (config.History.Enabled)
            {
                yield return Attributes.Historizing;
            }
            if (!config.Subscriptions.IgnoreAccessLevel)
            {
                yield return Attributes.UserAccessLevel;
            }
            yield return Attributes.DataType;
            yield return Attributes.ValueRank;
            yield return Attributes.ArrayDimensions;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.Historizing:
                    Historizing = value.GetValue(false);
                    break;
                case Attributes.UserAccessLevel:
                    AccessLevel = value.GetValue<byte>(0);
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

        public bool ShouldReadHistory(FullConfig config)
        {
            if (!config.History.Enabled || !config.History.Data) return false;

            if (config.Subscriptions.IgnoreAccessLevel)
            {
                return Historizing;
            }
            bool shouldRead = (AccessLevel & AccessLevels.HistoryRead) != 0;
            if (config.History.RequireHistorizing)
            {
                shouldRead &= Historizing;
            }
            return shouldRead;
        }

        public bool ShouldSubscribe(FullConfig config)
        {
            if (config.Subscriptions.IgnoreAccessLevel)
            {
                return true;
            }
            return (AccessLevel & AccessLevels.CurrentRead) != 0;
        }
    }

    public class UAVariable : BaseUANode
    {
        public UAVariable(NodeId id, string displayName, BaseUANode? parent, UAVariableType typeDefinition) : base(id, displayName, parent)
        {
            FullAttributes = new VariableAttributes(typeDefinition);
        }

        protected UAVariable(UAVariable other)
            : base(other.Id, other.DisplayName, other)
        {
            FullAttributes = other.FullAttributes;
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public VariableAttributes FullAttributes { get; }

        public int ValueRank => FullAttributes.ValueRank;
        public int[]? ArrayDimensions => FullAttributes.ArrayDimensions;
        public Variant? Value => FullAttributes.Value;
    }

    public class UAVariableMember : UAVariable
    {
        public UAVariable TSParent { get; }
        public int Index { get; } = -1;
        public UAVariableMember(UAVariable parent, int index)
            : base(parent)
        {
            Index = index;
            TSParent = parent;
        }
    }
}

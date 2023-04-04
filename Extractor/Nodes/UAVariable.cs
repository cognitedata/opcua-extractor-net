using Cognite.OpcUa.Config;
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Nodes
{
    public class VariableAttributes : BaseNodeAttributes
    {
        public bool Historizing { get; private set; }
        public int ValueRank { get; private set; }
        public NodeId DataTypeId { get; private set; } = null!;
        public int[]? ArrayDimensions { get; private set; }
        public byte AccessLevel { get; private set; }
        public Variant? Value { get; private set; }

        public VariableAttributes() : base(NodeClass.Variable)
        {
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

        public override void LoadAttribute(DataValue value, uint attributeId)
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
        public UAVariable(NodeId id, string displayName, NodeId parentId) : base(id, displayName, parentId)
        {
            FullAttributes = new VariableAttributes();
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public VariableAttributes FullAttributes { get; }
    }
}

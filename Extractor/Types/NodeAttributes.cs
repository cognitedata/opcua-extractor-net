using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace Cognite.OpcUa.Types
{
    public class NodeAttributes
    {
        public string Description { get; set; }
        public byte EventNotifier { get; set; }
        public UANodeType NodeType { get; set; }
        public bool IsProperty { get; set; }
        public bool Ignore { get; set; }
        public IList<UANode> Properties { get; set; }
        public NodeClass NodeClass { get; }
        public bool PropertiesRead { get; set; }
        public bool DataRead { get; protected set; }
        public NodeAttributes(NodeClass nc)
        {
            NodeClass = nc;
        }
        public IEnumerable<uint> GetAttributeIds(FullConfig config)
        {
            var result = new List<uint> { Attributes.Description };
            switch (NodeClass)
            {
                case NodeClass.Object:
                    if (config.Events.Enabled)
                    {
                        result.Add(Attributes.EventNotifier);
                    }
                    break;
                case NodeClass.Variable:
                    if (config.History.Enabled)
                    {
                        result.Add(Attributes.Historizing);
                    }
                    if (config.Events.Enabled)
                    {
                        result.Add(Attributes.EventNotifier);
                    }
                    goto case NodeClass.VariableType;
                case NodeClass.VariableType:
                    result.Add(Attributes.DataType);
                    result.Add(Attributes.ValueRank);
                    if (IsProperty || config.Extraction.DataTypes.MaxArraySize != 0)
                    {
                        result.Add(Attributes.ArrayDimensions);
                    }
                    break;
            }
            return result;
        }
        public virtual int HandleAttributeRead(FullConfig config, IList<DataValue> values, int idx, UAClient client)
        {
            Description = values[idx++].GetValue<LocalizedText>(null)?.Text;
            if (NodeClass == NodeClass.Object && config.Events.Enabled)
            {
                EventNotifier = values[idx++].GetValue(EventNotifiers.None);
            }
            DataRead = true;
            return idx;
        }
    }

    public class VariableAttributes : NodeAttributes
    {
        public bool Historizing { get; set; }
        public UADataType DataType { get; set; }
        public int ValueRank { get; set; }
        public Collection<int> ArrayDimensions { get; set; }

        public VariableAttributes(NodeClass nc) : base(nc) { }

        public override int HandleAttributeRead(FullConfig config, IList<DataValue> values, int idx, UAClient client)
        {
            Description = values[idx++].GetValue<LocalizedText>(null)?.Text;
            if (NodeClass == NodeClass.Variable)
            {
                if (config.History.Enabled)
                {
                    Historizing = values[idx++].GetValue(false);
                }
                if (config.Events.Enabled)
                {
                    EventNotifier = values[idx++].GetValue(EventNotifiers.None);
                }
            }
            var dt = values[idx++].GetValue(NodeId.Null);
            DataType = client.DataTypeManager.GetDataType(dt) ?? new UADataType(dt);
            ValueRank = values[idx++].GetValue(ValueRanks.Any);
            if (IsProperty || config.Extraction.DataTypes.MaxArraySize != 0)
            {
                if (values[idx++].GetValue(typeof(int[])) is int[] dimVal)
                {
                    ArrayDimensions = new Collection<int>(dimVal);
                }
            }

            DataRead = true;
            return idx;
        }
    }
}

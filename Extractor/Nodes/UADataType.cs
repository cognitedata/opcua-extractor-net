using Cognite.OpcUa.Config;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;

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

        public override void LoadAttribute(DataValue value, uint attributeId)
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
                    base.LoadAttribute(value, attributeId);
                    break;
            }
        }
    }

    internal class UADataType : BaseUANode
    {
        public UADataType(NodeId id, string displayName, NodeId parentId) : base(id, displayName, parentId)
        {
            FullAttributes = new DataTypeAttributes();
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public DataTypeAttributes FullAttributes { get; }
    }
}

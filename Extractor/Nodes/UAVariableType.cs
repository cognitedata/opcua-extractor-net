using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Oryx.Cognite;

namespace Cognite.OpcUa.Nodes
{
    public class VariableTypeAttributes : BaseNodeAttributes
    {
        public bool IsAbstract { get; set; }
        public int ValueRank { get; set; }
        public UADataType DataType { get; set; } = null!;
        public int[]? ArrayDimensions { get; set; }
        public Variant? Value { get; set; }
        public VariableTypeAttributes() : base(NodeClass.VariableType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.IsAbstract;
            yield return Attributes.DataType;
            yield return Attributes.ArrayDimensions;
            yield return Attributes.ValueRank;
            foreach (var attr in base.GetAttributeSet(config)) yield return attr;
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

        public override void LoadFromSavedNode(SavedNode node, TypeManager typeManager)
        {
            ValueRank = node.InternalInfo!.ValueRank;
            DataType = typeManager.GetDataType(node.DataTypeId!);
            ArrayDimensions = node.InternalInfo.ArrayDimensions;

            base.LoadFromSavedNode(node, typeManager);
        }

        public void LoadFromNodeState(BaseVariableTypeState state, TypeManager typeManager)
        {
            IsAbstract = state.IsAbstract;
            ValueRank = state.ValueRank;
            DataType = typeManager.GetDataType(state.DataType);
            ArrayDimensions = state.ArrayDimensions.Cast<int>().ToArray();
            Value = state.WrappedValue;
            LoadFromBaseNodeState(state);
        }
    }

    public class UAVariableType : BaseUAType
    {
        public UAVariableType(NodeId id, string? displayName, QualifiedName? browseName, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
        {
            FullAttributes = new VariableTypeAttributes();
            Attributes.DisplayName = displayName;
            Attributes.BrowseName = browseName;
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UAVariableType(NodeId id) : this(id, null, null, null, null)
        {
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public VariableTypeAttributes FullAttributes { get; }

        public override Dictionary<string, string>? GetExtraMetadata(FullConfig config, IUAClientAccess client)
        {
            Dictionary<string, string>? fields = new Dictionary<string, string>();
            var dt = FullAttributes.DataType;
            if (dt.EnumValues != null)
            {
                foreach (var kvp in dt.EnumValues)
                {
                    fields[kvp.Key.ToString(CultureInfo.InvariantCulture)] = kvp.Value;
                }
            }
            if (config.Extraction.DataTypes.DataTypeMetadata)
            {
                if (dt.Id.NamespaceIndex == 0)
                {
                    fields["dataType"] = DataTypes.GetBuiltInType(dt.Id).ToString();
                }
                else
                {
                    fields["dataType"] = dt.Attributes.DisplayName ?? dt.GetUniqueId(client) ?? "null";
                }
            }
            fields["Value"] = client.StringConverter.ConvertToString(FullAttributes.Value, dt.EnumValues);

            return fields;
        }

        public override int GetUpdateChecksum(TypeUpdateConfig update, bool dataTypeMetadata, bool nodeTypeMetadata)
        {
            int checksum = base.GetUpdateChecksum(update, dataTypeMetadata, nodeTypeMetadata);
            unchecked
            {
                if (dataTypeMetadata)
                {
                    checksum = checksum * 31 + FullAttributes.DataType.Id.GetHashCode();
                }
                checksum = checksum * 31 + FullAttributes.Value.GetHashCode();
            }
            return checksum;
        }

        public override void Format(StringBuilder builder, int indent, bool writeParent = true, bool writeProperties = true)
        {
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}VariableType: {1}", new string(' ', indent), Attributes.DisplayName);
            builder.AppendLine();
            base.Format(builder, indent + 4, writeParent);

            var indt = new string(' ', indent + 4);
            if (FullAttributes.DataType != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}DataType: ", indt);
                builder.AppendLine();
                FullAttributes.DataType.Format(builder, indent + 8, false);
            }
            if (FullAttributes.ValueRank != ValueRanks.Scalar && FullAttributes.ValueRank > 0)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}ValueRank: {1}", indt, FullAttributes.ValueRank);
                builder.AppendLine();
            }
            if (FullAttributes.ArrayDimensions != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}ArrayDimensions: {1}", indt, FullAttributes.ArrayDimensions);
                builder.AppendLine();
            }
            if (FullAttributes.Value != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Value: {1}", indt, FullAttributes.Value.Value);
            }
        }
    }
}

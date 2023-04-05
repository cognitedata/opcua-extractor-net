using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Oryx.Cognite;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Cognite.OpcUa.Nodes
{
    public class DataTypeAttributes : BaseNodeAttributes
    {
        public bool IsAbstract { get; private set; }
        // public Variant? DataTypeDefinition { get; private set; }
        public DataTypeAttributes() : base(NodeClass.DataType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            //yield return Attributes.DataTypeDefinition;
            yield return Attributes.IsAbstract;
            foreach (var attr in base.GetAttributeSet(config)) yield return attr;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.IsAbstract:
                    IsAbstract = value.GetValue(false);
                    break;
                //case Attributes.DataTypeDefinition:
                //    DataTypeDefinition = value.WrappedValue;
                //    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }

        public void LoadFromNodeState(DataTypeState state)
        {
            IsAbstract = state.IsAbstract;
            // DataTypeDefinition = state.DataTypeDefinition;
            LoadFromBaseNodeState(state);
        }
    }

    public class UADataType : BaseUAType
    {
        public bool IsString { get; set; }
        public bool IsStep { get; set; }
        public IDictionary<long, string>? EnumValues { get; set; }

        public UADataType(NodeId id, string? displayName, string? browseName, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
        {
            FullAttributes = new DataTypeAttributes();
            Attributes.DisplayName = displayName;
            Attributes.BrowseName = browseName;
            if (id.IdType == IdType.Numeric && id.NamespaceIndex == 0)
            {
                var identifier = (uint)id.Identifier;
                IsString = (identifier < DataTypes.Boolean || identifier > DataTypes.Double)
                                           && identifier != DataTypes.Integer && identifier != DataTypes.UInteger;
                IsStep = identifier == DataTypes.Boolean;
            }
            else
            {
                IsString = true;
            }
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UADataType(NodeId id) : this(id, null, null, null, null)
        {
        }

        /// <summary>
        /// Construct datatype from config object ProtoDateType and NodeId of datatype. Used when datatypes are being overriden.
        /// </summary>
        /// <param name="protoDataType">Overriding propoDataType</param>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public UADataType(ProtoDataType protoDataType, NodeId id, DataTypeConfig config) : this(id, null, null, null, null)
        {
            IsStep = protoDataType.IsStep;
            IsString = config.EnumsAsStrings && protoDataType.Enum;
            if (protoDataType.Enum)
            {
                EnumValues = new Dictionary<long, string>();
                IsStep = !config.EnumsAsStrings;
            }
        }

        /// <summary>
        /// Construct datatype from a parent datatype and a NodeId.
        /// </summary>
        /// <param name="id">NodeId of new datatype</param>
        /// <param name="other">Parent datatype</param>
        public UADataType(NodeId id, UADataType other) : this(id, null, null, other, other.Id)
        {
            IsStep = other.IsStep;
            IsString = other.IsString;
            if (other.EnumValues != null) EnumValues = new Dictionary<long, string>();
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public DataTypeAttributes FullAttributes { get; }

        /// <summary>
        /// Create the given value and timestamp to a new <see cref="UADataPoint"/>.
        /// </summary>
        /// <param name="client">Client to be used for converting to string</param>
        /// <param name="value">Value to convert</param>
        /// <param name="timestamp">Timestamp of created datapoint</param>
        /// <param name="id">Id of created datapoint</param>
        /// <param name="stringOverride">True to override the IsString parameter of this datatype, converting 
        /// numerical datavalues to string as well.</param>
        /// <returns>Created UADataPoint</returns>
        public UADataPoint ToDataPoint(IUAClientAccess client, object value, DateTime timestamp, string id, bool stringOverride = false)
        {
            if (timestamp == DateTime.MinValue) timestamp = DateTime.UtcNow;
            if (IsString || stringOverride)
            {
                return new UADataPoint(timestamp, id, client.StringConverter.ConvertToString(value, EnumValues));
            }
            return new UADataPoint(timestamp, id, UAClient.ConvertToDouble(value));
        }

        public override string ToString()
        {
            var builder = new StringBuilder("DataType: {");
            builder.AppendLine();
            builder.AppendFormat(CultureInfo.InvariantCulture, "    NodeId: {0}", Id);
            builder.AppendLine();
            if (IsStep)
            {
                builder.AppendLine("    Step: True");
            }
            builder.AppendFormat(CultureInfo.InvariantCulture, "    String: {0}", IsString);
            builder.AppendLine();
            if (EnumValues != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "    EnumValues: [{0}]", string.Join(", ", EnumValues));
                builder.AppendLine();
            }
            builder.Append('}');
            return builder.ToString();
        }
    }
}

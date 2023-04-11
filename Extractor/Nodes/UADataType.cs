﻿using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
        public bool ShouldIgnore { get; set; }

        public UADataType(NodeId id, string? displayName, QualifiedName? browseName, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
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
        public UADataType(ProtoDataType protoDataType, NodeId id, DataTypeConfig config) : this(id)
        {
            Initialize(protoDataType, config);
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

        public void Initialize(ProtoDataType protoDataType, DataTypeConfig config)
        {
            IsStep = protoDataType.IsStep;
            IsString = config.EnumsAsStrings && protoDataType.Enum;
            if (protoDataType.Enum)
            {
                EnumValues = new Dictionary<long, string>();
                IsStep = !config.EnumsAsStrings;
            }
        }

        public void UpdateFromParent(DataTypeConfig config)
        {
            if (Parent is not UADataType parentType) return;
            if (!parentType.IsString)
            {
                if (EnumValues != null && !config.EnumsAsStrings) return;
                IsString = false;
            }
        }

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

        public bool AllowTSMap(
            UAVariable node,
            ILogger log,
            DataTypeConfig config,
            int? arraySizeOverride = null,
            bool overrideString = false)
        {
            // We don't care about the data type of variable types except for as metadata.
            if (node.NodeClass == NodeClass.VariableType) return true;
            if (node.FullAttributes.DataType == null)
            {
                log.LogWarning("Skipping variable {Name} {Id} due to missing datatype", node.Attributes.DisplayName, node.Id);
                return false;
            }

            if (IsString && !config.AllowStringVariables && !overrideString)
            {
                log.LogDebug("Skipping variable {Name} {Id} due to string datatype and allow-string-variables being set to false",
                    node.Attributes.DisplayName, node.Id);
                return false;
            }
            if (ShouldIgnore)
            {
                log.LogDebug("Skipping variable {Name} {Id} due to raw datatype {Raw} being in list of ignored data types",
                    node.Attributes.DisplayName, node.Id, Id);
                return false;
            }
            if (node.ValueRank == ValueRanks.Scalar) return true;

            if (node.ArrayDimensions != null && node.ArrayDimensions.Length == 1)
            {
                int length = node.ArrayDimensions.First();
                int maxArraySize = arraySizeOverride.HasValue ? Math.Max(arraySizeOverride.Value, config.MaxArraySize) : config.MaxArraySize;
                if (config.MaxArraySize < 0 || length > 0 && length <= maxArraySize)
                {
                    return true;
                }
                else
                {
                    log.LogDebug("Skipping variable {Name} {Id} due to non-scalar ValueRank {Rank} and too large dimension {Dim}",
                        node.Attributes.DisplayName, node.Id, ExtractorUtils.GetValueRankString(node.ValueRank), length);
                    return false;
                }
            }
            else if (node.ArrayDimensions == null)
            {
                if (config.UnknownAsScalar && (node.ValueRank == ValueRanks.ScalarOrOneDimension
                    || node.ValueRank == ValueRanks.Any)) return true;
                log.LogDebug("Skipping variable {Name} {Id} due to non-scalar ValueRank {Rank} and null ArrayDimensions",
                    node.Attributes.DisplayName, node.Id, ExtractorUtils.GetValueRankString(node.ValueRank));
                return false;
            }
            else
            {
                log.LogDebug("Skipping variable {Name} {Id} due to non-scalar ValueRank {Rank} and too high dimensionality {Dim}",
                    node.Attributes.DisplayName, node.Id, ExtractorUtils.GetValueRankString(node.ValueRank), node.ArrayDimensions.Length);
                return false;
            }
        }


        public void SetEnumStrings(ILogger log, Variant? variant)
        {
            if (variant == null) return;
            var value = variant.Value.Value;

            if (value is LocalizedText[] strings)
            {
                for (int i = 0; i < strings.Length; i++)
                {
                    EnumValues![i] = strings[i].Text;
                }
            }
            else if (value is ExtensionObject[] exts)
            {
                foreach (var ext in exts)
                {
                    if (ext.Body is EnumValueType val)
                    {
                        EnumValues![val.Value] = val.DisplayName.Text;
                    }
                }
            }
            else
            {
                log.LogWarning("Unknown enum strings type: {Type}", value.GetType());
            }
        }
    }
}
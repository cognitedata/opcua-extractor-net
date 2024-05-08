/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Cognite.OpcUa.Config;
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
        public Variant? DataTypeDefinition { get; private set; }
        public DataTypeAttributes() : base(NodeClass.DataType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            if (config.Cognite?.MetadataTargets?.DataModels?.Enabled ?? false)
            {
                yield return Attributes.DataTypeDefinition;
            }
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
                case Attributes.DataTypeDefinition:
                    DataTypeDefinition = value.WrappedValue;
                    break;
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
                                           && identifier != DataTypes.Integer && identifier != DataTypes.UInteger
                                           && identifier != DataTypes.Number;
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

        public void UpdateFromParent(DataTypeConfig config)
        {
            foreach (var node in EnumerateTypedAncestors<UADataType>().Prepend(this))
            {
                if (node.Id == DataTypeIds.Number)
                {
                    IsString = false;
                    return;
                }
                if (node.Id == DataTypeIds.Boolean)
                {
                    IsString = false;
                    IsStep = true;
                    return;
                }
                if (node.Id == DataTypeIds.Enumeration)
                {
                    IsString = config.EnumsAsStrings;
                    IsStep = !config.EnumsAsStrings;
                    EnumValues ??= new Dictionary<long, string>();
                    return;
                }
                if (!node.IsString)
                {
                    IsString = false;
                }
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
        public UADataPoint ToDataPoint(IUAClientAccess client, object value, DateTime timestamp, string id, StatusCode status, bool stringOverride = false)
        {
            if (timestamp == DateTime.MinValue) timestamp = DateTime.UtcNow;
            if (value is null)
            {
                return new UADataPoint(timestamp, id, IsString, status);
            }

            if (IsString || stringOverride)
            {
                return new UADataPoint(timestamp, id, client.StringConverter.ConvertToString(value, EnumValues), status);
            }
            return new UADataPoint(timestamp, id, UAClient.ConvertToDouble(value), status);
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
            DataTypeConfig config)
        {
            if (IsString && !config.AllowStringVariables)
            {
                log.LogDebug("Skipping variable {Name} {Id} due to string datatype and allow-string-variables being set to false",
                    node.Name, node.Id);
                return false;
            }
            if (ShouldIgnore)
            {
                log.LogDebug("Skipping variable {Name} {Id} due to raw datatype {Raw} being in list of ignored data types",
                    node.Name, node.Id, Id);
                return false;
            }
            if (node.ValueRank == ValueRanks.Scalar) return true;

            if (node.ArrayDimensions != null && node.ArrayDimensions.Length == 1)
            {
                int length = node.ArrayDimensions.First();
                int maxArraySize = config.MaxArraySize;
                if (config.MaxArraySize < 0 || length > 0 && length <= maxArraySize)
                {
                    return true;
                }
                else
                {
                    log.LogDebug("Skipping variable {Name} {Id} due to non-scalar ValueRank {Rank} and too large dimension {Dim}",
                        node.Name, node.Id, ExtractorUtils.GetValueRankString(node.ValueRank), length);
                    return false;
                }
            }
            // Check for null ArrayDimensions or empty array
            else if (node.ArrayDimensions == null || node.ArrayDimensions.Length == 0)
            {
                if (config.UnknownAsScalar && (node.ValueRank == ValueRanks.ScalarOrOneDimension
                    || node.ValueRank == ValueRanks.Any))
                {
                    return true;
                }
                log.LogDebug("Skipping variable {Name} {Id} due to non-scalar ValueRank {Rank} and null ArrayDimensions",
                    node.Name, node.Id, ExtractorUtils.GetValueRankString(node.ValueRank));
                return false;
            }
            else
            {
                log.LogDebug("Skipping variable {Name} {Id} due to non-scalar ValueRank {Rank} and too high dimensionality {Dim}",
                    node.Name, node.Id, ExtractorUtils.GetValueRankString(node.ValueRank), node.ArrayDimensions.Length);
                return false;
            }
        }

        public bool AllowValueRead(
            BaseUANode node,
            int[]? arrayDimensions,
            int valueRank,
            ILogger log,
            DataTypeConfig config,
            bool ignoreDimension)
        {
            const int MAX_COMBINED_LENGTH = 100;
            if (ShouldIgnore)
            {
                log.LogDebug("Skipping value read on {Name} {Id} due to datatype {Raw} being in list of ignored data types",
                    node.Name, node.Id, Id);
                return false;
            }

            if (valueRank == ValueRanks.Scalar) return true;
            if (arrayDimensions != null)
            {
                var combinedSize = arrayDimensions.Aggregate(1, (seed, dim) => seed * dim);
                var maxLength = Math.Max(config.MaxArraySize, MAX_COMBINED_LENGTH);

                if (combinedSize > maxLength)
                {
                    log.LogDebug("Skipping value read on {Name} {Id} due to too large total size {Size}",
                        node.Name, node.Id, combinedSize);
                    return false;
                }
                return true;
            }
            else
            {
                if (ignoreDimension) return true;
                if (config.UnknownAsScalar && (valueRank == ValueRanks.ScalarOrOneDimension || valueRank == ValueRanks.Any)) return true;
                log.LogDebug("Skipping value read on variable {Name} {Id} due to non-scalar ValueRank {Rank} and null ArrayDimensions",
                    node.Name, node.Id, ExtractorUtils.GetValueRankString(valueRank));
                return false;
            }
        }


        public void SetEnumStrings(ILogger log, Variant? variant)
        {
            if (variant == null || variant.Value.Value == null) return;
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


        public override void Format(StringBuilder builder, int indent, bool writeParent = true, bool writeProperties = true)
        {
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}DataType: {1}", new string(' ', indent), Name);
            builder.AppendLine();
            base.Format(builder, indent + 4, writeParent);

            var indt = new string(' ', indent + 4);
            if (FullAttributes.IsAbstract)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}IsAbstract: {1}", indt, FullAttributes.IsAbstract);
                builder.AppendLine();
            }
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}IsString: {1}", indt, IsString);
            builder.AppendLine();

            if (IsStep)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}IsStep: {1}", indt, IsStep);
                builder.AppendLine();
            }
        }
    }
}

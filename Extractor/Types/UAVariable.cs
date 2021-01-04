/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using Opc.Ua;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents an opcua variable, which may be either a piece of metadata or a cdf timeseries
    /// </summary>
    public class UAVariable : UANode
    {
        /// <summary>
        /// Data type of this variable
        /// </summary>
        public UADataType DataType { get; set; }
        /// <summary>
        /// True if the opcua node stores its own history
        /// </summary>
        public bool Historizing { get; set; }
        /// <summary>
        /// ValueRank in opcua
        /// </summary>
        public int ValueRank { get; set; }
        /// <summary>
        /// True if variable is a property
        /// </summary>
        public bool IsProperty { get; set; }
        /// <summary>
        /// Value of variable as string or double
        /// </summary>
        public UADataPoint Value { get; private set; }
        /// <summary>
        /// True if attributes have been read from OPC-UA for this variable
        /// </summary>
        public bool DataRead { get; set; }
        public override string ToDebugDescription()
        {
            var builder = new StringBuilder();
            builder.AppendFormat(CultureInfo.InvariantCulture, "Variable: {0}\n", DisplayName);
            builder.AppendFormat(CultureInfo.InvariantCulture, "Id: {0}\n", Id);
            if (ParentId != null && !ParentId.IsNullNodeId)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "ParentId: {0}\n", ParentId);
            }
            if (Description != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "Description: {0}\n", Description);
            }
            if (DataType != null)
            {
                builder.Append(DataType);
                builder.Append('\n');
            }
            if (Historizing)
            {
                builder.Append("Historizing: True\n");
            }
            if (ValueRank != ValueRanks.Scalar)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "ValueRank: {0}\n", ValueRank);
            }
            if (ArrayDimensions != null && ArrayDimensions.Count == 1)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "Dimension: {0}\n", ArrayDimensions[0]);
            }
            if (NodeType != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "NodeType: {0}\n", NodeType.Name ?? NodeType.Id);
            }

            if (Properties != null && Properties.Any())
            {
                builder.Append("Properties: {\n");
                foreach (var prop in Properties)
                {
                    builder.AppendFormat(CultureInfo.InvariantCulture, "    {0}: {1}\n", prop.DisplayName, prop.Value?.StringValue ?? "??");
                    if (prop.Properties != null && prop.Properties.Any())
                    {
                        foreach (var prop2 in prop.Properties)
                        {
                            builder.AppendFormat(CultureInfo.InvariantCulture, "        {0}: {1}\n",
                                prop2.DisplayName, prop2.Value?.StringValue ?? "??");
                        }
                    }
                }
                builder.Append('}');
            }
            return builder.ToString();
        }
        /// <summary>
        /// Parent if this represents an element of an array.
        /// </summary>
        public UAVariable ArrayParent { get; }
        /// <summary>
        /// Children if this represents the parent in an array
        /// </summary>
        public IEnumerable<UAVariable> ArrayChildren { get; private set; }
        /// <summary>
        /// Fixed dimensions of the array-type variable, if any
        /// </summary>
        public Collection<int> ArrayDimensions { get; set; }
        /// <summary>
        /// Index of the variable in array, if relevant. -1 if the variable is scalar.
        /// </summary>
        public int Index { get; } = -1;
        /// <param name="Id">NodeId of buffered node</param>
        /// <param name="DisplayName">DisplayName of buffered node</param>
        /// <param name="ParentId">Id of parent of buffered node</param>
        public UAVariable(NodeId id, string displayName, NodeId parentId) : base(id, displayName, true, parentId) { }
        /// <summary>
        /// True if this node represents an array
        /// </summary>
        public bool IsArray => ArrayDimensions != null && ArrayDimensions.Count == 1 && ArrayDimensions[0] > 0;
        /// <summary>
        /// Sets the datapoint to provided DataValue.
        /// </summary>
        /// <param name="value">Value to set</param>
        /// <param name="sourceTimestamp">Timestamp from source</param>
        /// <param name="client">Current client context</param>
        public void SetDataPoint(object value, DateTime sourceTimestamp, UAClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (value == null) return;
            if (IsProperty || (DataType?.IsString ?? true))
            {
                Value = new UADataPoint(
                    sourceTimestamp <= DateTime.MinValue ? DateTime.UtcNow : sourceTimestamp,
                    client.GetUniqueId(Id),
                    client.ConvertToString(value));
            }
            else
            {
                Value = new UADataPoint(
                    sourceTimestamp <= DateTime.MinValue ? DateTime.UtcNow : sourceTimestamp,
                    client.GetUniqueId(Id),
                    UAClient.ConvertToDouble(value));
            }
        }
        /// <summary>
        /// Create an array-element variable.
        /// </summary>
        /// <param name="other">Parent variable</param>
        /// <param name="index">Index in the array</param>
        private UAVariable(UAVariable other, int index)
            : base(OtherNonNull(other).Id, other.DisplayName + $"[{index}]", true, other.Id)
        {
            ArrayParent = other;
            Index = index;
            Historizing = other.Historizing;
            DataType = other.DataType;
            ValueRank = other.ValueRank;
            ArrayDimensions = other.ArrayDimensions;
            NodeType = other.NodeType;
        }
        /// <summary>
        /// Returns given variable if it is not null, otherwise throws an error.
        /// Used to prevent warnings when calling base constructor.
        /// </summary>
        private static UAVariable OtherNonNull(UAVariable other)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            return other;
        }
        /// <summary>
        /// Create array child nodes
        /// </summary>
        public IEnumerable<UAVariable> CreateArrayChildren()
        {
            if (!IsArray) return Enumerable.Empty<UAVariable>();
            var children = new List<UAVariable>();
            for (int i = 0; i < ArrayDimensions[0]; i++)
            {
                children.Add(new UAVariable(this, i));
            }
            ArrayChildren = children;
            return children;
        }
    }
}

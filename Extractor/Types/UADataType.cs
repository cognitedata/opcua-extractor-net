/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

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
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents a simplified OPC-UA datatype, containing information relevant to us (isString, isStep, etc.)
    /// </summary>
    public class UADataType
    {
        public uint Identifier { get; set; }
        public bool IsStep { get; set; }
        public bool IsString { get; set; }
        public NodeId Raw { get; }
        public IDictionary<long, string>? EnumValues { get; set; }
        /// <summary>
        /// Construct BufferedDataType from NodeId of datatype
        /// </summary>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public UADataType(NodeId rawDataType)
        {
            Raw = rawDataType;
            if (rawDataType.IdType == IdType.Numeric && rawDataType.NamespaceIndex == 0)
            {
                Identifier = (uint)rawDataType.Identifier;
                IsString = (Identifier < DataTypes.Boolean || Identifier > DataTypes.Double)
                           && Identifier != DataTypes.Integer && Identifier != DataTypes.UInteger;
                IsStep = Identifier == DataTypes.Boolean;
            }
            else
            {
                IsString = true;
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

        /// <summary>
        /// Construct datatype from config object ProtoDateType and NodeId of datatype. Used when datatypes are being overriden.
        /// </summary>
        /// <param name="protoDataType">Overriding propoDataType</param>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public UADataType(ProtoDataType protoDataType, NodeId rawDataType, DataTypeConfig config) : this(rawDataType)
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
        /// <param name="rawDataType">NodeId of new datatype</param>
        /// <param name="other">Parent datatype</param>
        public UADataType(NodeId rawDataType, UADataType other) : this(rawDataType)
        {
            IsStep = other.IsStep;
            IsString = other.IsString;
            Raw = rawDataType;
            if (other.EnumValues != null) EnumValues = new Dictionary<long, string>();
        }

        public override string ToString()
        {
            var builder = new StringBuilder("DataType: {");
            builder.AppendLine();
            builder.AppendFormat(CultureInfo.InvariantCulture, "    NodeId: {0}", Raw);
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

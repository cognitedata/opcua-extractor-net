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

using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace Cognite.OpcUa.TypeCollectors
{
    /// <summary>
    /// Represents a simplified OPC-UA datatype, containing information relevant to us (isString, isStep)
    /// </summary>
    public class BufferedDataType
    {
        public uint Identifier { get; set; }
        public bool IsStep { get; set; }
        public bool IsString { get; set; }
        public NodeId Raw { get; }
        public IDictionary<long, string> EnumValues { get; set; }
        /// <summary>
        /// Construct BufferedDataType from NodeId of datatype
        /// </summary>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public BufferedDataType(NodeId rawDataType)
        {
            if (rawDataType == null) throw new ArgumentNullException(nameof(rawDataType));
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
        public BufferedDataPoint ToDataPoint(UAExtractor extractor, object value, DateTime timestamp, string id)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            if (IsString)
            {
                if (EnumValues != null)
                {
                    try
                    {
                        var longVal = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                        if (EnumValues.TryGetValue(longVal, out string enumVal))
                        {
                            return new BufferedDataPoint(timestamp, id, enumVal);
                        }
                    }
                    catch
                    {
                    }
                }
                return new BufferedDataPoint(timestamp, id, extractor.ConvertToString(value));
            }
            return new BufferedDataPoint(timestamp, id, UAClient.ConvertToDouble(value));
        }

        /// <summary>
        /// Construct datatype from config object ProtoDateType and NodeId of datatype. Used when datatypes are being overriden.
        /// </summary>
        /// <param name="protoDataType">Overriding propoDataType</param>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public BufferedDataType(ProtoDataType protoDataType, NodeId rawDataType, DataTypeConfig config) : this(rawDataType)
        {
            if (protoDataType == null) throw new ArgumentNullException(nameof(protoDataType));
            if (config == null) throw new ArgumentNullException(nameof(config));
            IsStep = protoDataType.IsStep;
            IsString = config.EnumsAsStrings && protoDataType.Enum;
            if (protoDataType.Enum)
            {
                EnumValues = new Dictionary<long, string>();
                IsStep = !config.EnumsAsStrings;
            }
        }

        public BufferedDataType(NodeId rawDataType, BufferedDataType other) : this(rawDataType)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            IsStep = other.IsStep;
            IsString = other.IsString;
            Raw = rawDataType;
            if (other.EnumValues != null) EnumValues = new Dictionary<long, string>();
        }

        public override string ToString()
        {
            return "DataType: {\n" +
                $"    NodeId: {Raw}\n" +
                $"    isStep: {IsStep}\n" +
                $"    isString: {IsString}\n" +
                (EnumValues != null ? 
                $"    EnumValues: {string.Concat(EnumValues)}\n"
                : "") +
                "}";
        }
    }
}

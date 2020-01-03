﻿/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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
using System.Globalization;
using System.Linq;
using Opc.Ua;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Represents an opcua node.
    /// </summary>
    public class BufferedNode
    {
        /// <summary>
        /// Raw NodeId in OPC-UA.
        /// </summary>
        public readonly NodeId Id;
        /// <summary>
        /// DisplayName in OPC-UA
        /// </summary>
        public readonly string DisplayName;
        /// <summary>
        /// True if this is also a BufferedVariable.
        /// </summary>
        public readonly bool IsVariable;
        /// <summary>
        /// NodeId of the parent. May be NodeId.Null for rootNodes
        /// </summary>
        public readonly NodeId ParentId;
        /// <summary>
        /// True if the properties of this object has been read.
        /// </summary>
        public bool PropertiesRead { get; set; } = false;
        /// <summary>
        /// Description in OPC-UA
        /// </summary>
        public string Description { get; set; }
        public virtual string ToDebugDescription()
        {
            string propertyString = "properties: {" + (Properties != null && Properties.Any() ? "\n" : "");
            if (Properties != null)
            {
                foreach (var prop in Properties)
                {
                    propertyString += $"    {prop.DisplayName} : {prop.Value?.StringValue ?? "??"},\n";
                }
            }
            propertyString += "}";

            string ret = $"DisplayName: {DisplayName}\n"
                + $"ParentId: {ParentId?.ToString()}\n"
                + $"Id: {Id.ToString()}\n"
                + propertyString + "\n";
            return ret;
        }
        public IList<BufferedVariable> Properties;
        /// <param name="Id">NodeId of buffered node</param>
        /// <param name="DisplayName">DisplayName of buffered node</param>
        /// <param name="ParentId">Id of parent of buffered node</param>
        public BufferedNode(NodeId id, string displayName, NodeId parentId) : this(id, displayName, false, parentId) { }
        /// <param name="id">NodeId of buffered node</param>
        /// <param name="displayName">DisplayName of buffered node</param>
        /// <param name="isVariable">True if this is a variable</param>
        /// <param name="parentId">Id of parent of buffered node</param>
        protected BufferedNode(NodeId id, string displayName, bool isVariable, NodeId parentId)
        {
            Id = id;
            DisplayName = displayName;
            IsVariable = isVariable;
            ParentId = parentId;
        }
    }
    /// <summary>
    /// Represents a simplified OPC-UA datatype, containing information relevant to us (isString, isStep)
    /// </summary>
    public class BufferedDataType
    {
        public readonly uint Identifier;
        public readonly bool IsStep;
        public readonly bool IsString;
        public readonly NodeId raw;
        /// <summary>
        /// Construct BufferedDataType from NodeId of datatype
        /// </summary>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public BufferedDataType(NodeId rawDataType)
        {
            raw = rawDataType;
            if (rawDataType.IdType == IdType.Numeric)
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
        /// Construct datatype from config object ProtoDateType and NodeId of datatype. Used when datatypes are being overriden.
        /// </summary>
        /// <param name="protoDataType">Overriding propoDataType</param>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public BufferedDataType(ProtoDataType protoDataType, NodeId rawDataType) : this(rawDataType)
        {
            IsStep = protoDataType.IsStep;
            IsString = false;
        }
        public override string ToString()
        {
            return "DataType: {\n" +
                $"    numIdentifier: {Identifier}\n" +
                $"    isStep: {IsStep}\n" +
                $"    isString: {IsString}\n" +
                "}";
        }
    }
    /// <summary>
    /// Represents an opcua variable, which may be either a piece of metadata or a cdf timeseries
    /// </summary>
    public class BufferedVariable : BufferedNode
    {
        /// <summary>
        /// DataType in opcua
        /// </summary>
        public BufferedDataType DataType { get; private set; }
        public void SetDataType(NodeId dataType, Dictionary<NodeId, ProtoDataType> numericalTypeMap)
        {
            if (numericalTypeMap.ContainsKey(dataType))
            {
                var proto = numericalTypeMap[dataType];
                DataType = new BufferedDataType(proto, dataType);
            }
            DataType = new BufferedDataType(dataType);
        }
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
        /// Local browse name of variable, sometimes useful for properties
        /// </summary>
        public QualifiedName BrowseName { get; set; }
        /// <summary>
        /// Value of variable as string or double
        /// </summary>
        public BufferedDataPoint Value { get; private set; }
        public bool DataRead { get; set; } = false;
        public override string ToDebugDescription()
        {
            string propertyString = "properties: {" + (Properties != null && Properties.Any() ? "\n" : "");
            if (Properties != null)
            {
                foreach (var prop in Properties)
                {
                    propertyString += $"{prop.DisplayName} : {prop.Value.StringValue},\n";
                }
            }
            propertyString += "}";

            string ret = $"DisplayName: {DisplayName}\n"
                + $"ParentId: {ParentId?.ToString()}\n"
                + $"Id: {Id.ToString()}\n"
                + $"Historizing: {Historizing}\n"
                + propertyString + "\n"
                + DataType + "\n";
            return ret;
        }
        public BufferedVariable ArrayParent { get; }
        /// <summary>
        /// Fixed dimensions of the array-type variable, if any
        /// </summary>
        public int[] ArrayDimensions { get; set; }
        /// <summary>
        /// Index of the variable in array, if relevant. -1 if the variable is scalar.
        /// </summary>
        public int Index { get; } = -1;
        /// <param name="Id">NodeId of buffered node</param>
        /// <param name="DisplayName">DisplayName of buffered node</param>
        /// <param name="ParentId">Id of parent of buffered node</param>
        public BufferedVariable(NodeId id, string displayName, NodeId parentId) : base(id, displayName, true, parentId) { }
        /// <summary>
        /// Sets the datapoint to provided DataValue.
        /// </summary>
        /// <param name="value">Value to set</param>
        /// <param name="SourceTimestamp">Timestamp from source</param>
        /// <param name="client">Current client context</param>
        public void SetDataPoint(object value, DateTime sourceTimestamp, UAClient client)
        {
            if (value == null) return;
            if (DataType.IsString || IsProperty)
            {
                Value = new BufferedDataPoint(
                    sourceTimestamp <= DateTime.MinValue ? DateTime.Now : sourceTimestamp,
                    client.GetUniqueId(Id),
                    client.ConvertToString(value));
            }
            else
            {
                Value = new BufferedDataPoint(
                    sourceTimestamp <= DateTime.MinValue ? DateTime.Now : sourceTimestamp,
                    client.GetUniqueId(Id),
                    UAClient.ConvertToDouble(value));
            }
        }
        public BufferedVariable(BufferedVariable other, int index) : base(other.Id, other.DisplayName + $"[{index}]", true, other.Id)
        {
            ArrayParent = other;
            Index = index;
            Historizing = other.Historizing;
            DataType = other.DataType;
            ValueRank = other.ValueRank;
            ArrayDimensions = other.ArrayDimensions;
        }
    }
    /// <summary>
    /// Represents a single value at specified timestamp
    /// </summary>
    public class BufferedDataPoint
    {
        public readonly DateTime Timestamp;
        public readonly string Id;
        public readonly double DoubleValue;
        public readonly string StringValue;
        public readonly bool IsString;
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="Id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(DateTime timestamp, string id, double value)
        {
            Timestamp = timestamp;
            Id = id;
            DoubleValue = value;
            IsString = false;
        }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="Id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(DateTime timestamp, string id, string value)
        {
            Timestamp = timestamp;
            Id = id;
            StringValue = value;
            IsString = true;
        }

        public BufferedDataPoint(BufferedDataPoint other, string replacement)
        {
            Timestamp = other.Timestamp;
            Id = other.Id;
            StringValue = replacement;
            IsString = other.IsString;
        }
        public BufferedDataPoint(BufferedDataPoint other, double replacement)
        {
            Timestamp = other.Timestamp;
            Id = other.Id;
            DoubleValue = replacement;
            IsString = other.IsString;
        }
        /// <summary>
        /// Converts datapoint into an array of bytes which may be written to file.
        /// The structure is as follows: ushort size | unknown encoding string externalid | double value | long timestamp
        /// </summary>
        /// <remarks>
        /// This will obviously break if the system encoding changes somehow, it is not intended as any kind of cross-system storage.
        /// Convert the datapoint into an array of bytes. The string is converted directly, as the system encoding is unknown, but can
        /// be assumed to be constant.
        /// </remarks>
        /// <returns>Array of bytes</returns>
        public byte[] ToStorableBytes()
        {
            string externalId = Id;
            ushort size = (ushort)(externalId.Length * sizeof(char) + sizeof(double) + sizeof(long));
            byte[] bytes = new byte[size + sizeof(ushort)];
            Buffer.BlockCopy(externalId.ToCharArray(), 0, bytes, sizeof(ushort), externalId.Length * sizeof(char));
            Buffer.BlockCopy(BitConverter.GetBytes(DoubleValue), 0, bytes, sizeof(ushort) + externalId.Length * sizeof(char), sizeof(double));
            Buffer.BlockCopy(BitConverter.GetBytes(new DateTimeOffset(Timestamp).ToUnixTimeMilliseconds()), 0, bytes, sizeof(ushort) + externalId.Length * sizeof(char)
                + sizeof(double), sizeof(long));
            Buffer.BlockCopy(BitConverter.GetBytes(size), 0, bytes, 0, sizeof(ushort));
            return bytes;
        }
        /// <summary>
        /// Initializes BufferedDataPoint from array of bytes, array should not contain the short size, which is just used to know how much
        /// to read at a time.
        /// </summary>
        /// <param name="bytes">Bytes to convert</param>
        public BufferedDataPoint(byte[] bytes)
        {
            if (bytes.Length < sizeof(long) + sizeof(double)) return;
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(BitConverter.ToInt64(bytes, bytes.Length - sizeof(long))).DateTime;
            DoubleValue = BitConverter.ToDouble(bytes, bytes.Length - sizeof(double) - sizeof(long));
            char[] chars = new char[(bytes.Length - sizeof(long) - sizeof(double))/sizeof(char)];
            Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length - sizeof(long) - sizeof(double));
            Id = new string(chars);
            IsString = false;
        }
        public string ToDebugDescription()
        {
            return $"Update timeseries {Id} to {(IsString ? "\"" + StringValue + "\"" : DoubleValue.ToString(CultureInfo.InvariantCulture))}" +
                   $" at {Timestamp.ToString(CultureInfo.InvariantCulture)}";
        }
    }
    /// <summary>
    /// Represents a single OPC-UA event, with especially relevant information stored as properties and the rest in a dictionary.
    /// </summary>
    public class BufferedEvent
    {
        /// <summary>
        /// Message sent with the original event.
        /// </summary>
        public string Message { get; set; }
        /// <summary>
        /// Transformed ID of the event. The raw id is a byte-string. This is the byte-string transformed into Base64 and prepended the globalprefix.
        /// </summary>
        public string EventId { get; set; } // Base64
        /// <summary>
        /// NodeId of the SourceNode
        /// </summary>
        public NodeId SourceNode { get; set; }
        /// <summary>
        /// Time this event triggered.
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// NodeId of the eventType of this event.
        /// </summary>
        public NodeId EventType { get; set; }
        /// <summary>
        /// string->object dictionary of the remaining properties that haven't been filtered out.
        /// </summary>
        public Dictionary<string, object> MetaData { get; set; }
        /// <summary>
        /// Timestamp this event was received locally
        /// </summary>
        public DateTime ReceivedTime { get; set; }
        /// <summary>
        /// Id of the node that emitted the event in opc-ua
        /// </summary>
        public NodeId EmittingNode { get; set; }

        public string ToDebugDescription()
        {
            string metadata = "{";
            if (MetaData != null)
            {
                metadata += "\n";
                foreach (var kvp in MetaData)
                {
                    metadata += $"        {kvp.Key} : {kvp.Value}\n";
                }
            }
            metadata += "    }";
            return $"EventId: {EventId}\n"
                + $"    Message: {Message}\n"
                + $"    SourceNodeId: {SourceNode}\n"
                + $"    Time: {Time}\n"
                + $"    EventTypeId: {EventType}\n"
                + $"    MetaData: {metadata}\n";
        }
    }
}

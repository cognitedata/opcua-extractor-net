/* Cognite Extractor for OPC-UA
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
using Opc.Ua;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Represents an opcua node.
    /// </summary>
    public class BufferedNode
    {
        public readonly NodeId Id;
        public readonly string DisplayName;
        public readonly bool IsVariable;
        public readonly NodeId ParentId;
        public bool PropertiesRead { get; set; } = false;
        /// <summary>
        /// Description in opcua
        /// </summary>
        public string Description { get; set; }
        public virtual string ToDebugDescription()
        {
            string propertyString = "properties: {";
            if (properties != null)
            {
                foreach (var prop in properties)
                {
                    propertyString += $"{prop.DisplayName} : {prop.Value.stringValue},\n";
                }
            }
            propertyString += "}";

            string ret = $"DisplayName: {DisplayName}\n"
                + $"ParentId: {ParentId?.ToString()}\n"
                + propertyString + "\n";
            return ret;
        }
        public IList<BufferedVariable> properties;
        /// <param name="Id">NodeId of buffered node</param>
        /// <param name="DisplayName">DisplayName of buffered node</param>
        /// <param name="ParentId">Id of parent of buffered node</param>
        public BufferedNode(NodeId Id, string DisplayName, NodeId ParentId) : this(Id, DisplayName, false, ParentId) { }
        protected BufferedNode(NodeId Id, string DisplayName, bool IsVariable, NodeId ParentId)
        {
            this.Id = Id;
            this.DisplayName = DisplayName;
            this.IsVariable = IsVariable;
            this.ParentId = ParentId;
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
        public uint DataType { get; set; }
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
        private DateTime _latestTimestamp = new DateTime(1970, 1, 1);
        /// <summary>
        /// Latest timestamp historizing value was read from CDF
        /// </summary>
        public DateTime LatestTimestamp
        {
            get { return _latestTimestamp; }
            set
            {
                var tsRef = Index != -1 ? ArrayParent.LatestTimestamp : _latestTimestamp;
                if (tsRef == new DateTime(1970, 1, 1) || value < tsRef)
                {
                    if (Index != -1)
                    {
                        ArrayParent.LatestTimestamp = value;
                    }
                    else
                    {
                        _latestTimestamp = value;
                    }
                }
            }
        }
        /// <summary>
        /// Value of variable as string or double
        /// </summary>
        public BufferedDataPoint Value { get; private set; }
        public bool DataRead { get; set; } = false;
        public override string ToDebugDescription()
        {
            string propertyString = "properties: {";
            if (properties != null)
            {
                foreach (var prop in properties)
                {
                    propertyString += $"{prop.DisplayName} : {prop.Value.stringValue},\n";
                }
            }
            propertyString += "}";

            string ret = $"DisplayName: {DisplayName}\n"
                + $"ParentId: {ParentId?.ToString()}\n"
                + $"Historizing: {Historizing}\n"
                + propertyString + "\n";
            return ret;
        }
        public BufferedVariable ArrayParent { get; private set; } = null;
        /// <summary>
        /// Fixed dimensions of the array-type variable, if any
        /// </summary>
        public int[] ArrayDimensions { get; set; } = null;
        /// <summary>
        /// Index of the variable in array, if relevant. -1 if the variable is scalar.
        /// </summary>
        public int Index { get; private set; } = -1;
        /// <param name="Id">NodeId of buffered node</param>
        /// <param name="DisplayName">DisplayName of buffered node</param>
        /// <param name="ParentId">Id of parent of buffered node</param>
        public BufferedVariable(NodeId Id, string DisplayName, NodeId ParentId) : base(Id, DisplayName, true, ParentId) { }
        /// <summary>
        /// Sets the datapoint to provided DataValue.
        /// </summary>
        /// <param name="value">Value to set</param>
        /// <param name="SourceTimestamp">Timestamp from source</param>
        /// <param name="client">Current client context</param>
        public void SetDataPoint(object value, DateTime SourceTimestamp, UAClient client)
        {
            if (value == null) return;
            if (!client.IsNumericType(DataType) || IsProperty)
            {
                Value = new BufferedDataPoint(
                    SourceTimestamp <= DateTime.MinValue ? DateTime.Now : SourceTimestamp,
                    client.GetUniqueId(Id),
                    UAClient.ConvertToString(value));
            }
            else
            {
                Value = new BufferedDataPoint(
                    SourceTimestamp <= DateTime.MinValue ? DateTime.Now : SourceTimestamp,
                    client.GetUniqueId(Id),
                    UAClient.ConvertToDouble(value));
            }
        }
        public BufferedVariable(BufferedVariable other, int index) : base(other.Id, other.DisplayName + $"[{index}]", true, other.Id)
        {
            ArrayParent = other;
            Index = index;
            LatestTimestamp = other.LatestTimestamp;
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
        public readonly DateTime timestamp;
        public readonly string Id;
        public readonly double doubleValue;
        public readonly string stringValue;
        public readonly bool isString;
        public readonly bool historizing;
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="Id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(DateTime timestamp, string Id, double value)
        {
            this.timestamp = timestamp;
            this.Id = Id;
            doubleValue = value;
            isString = false;
        }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="Id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(DateTime timestamp, string Id, string value)
        {
            this.timestamp = timestamp;
            this.Id = Id;
            stringValue = value;
            isString = true;
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
            Buffer.BlockCopy(BitConverter.GetBytes(doubleValue), 0, bytes, sizeof(ushort) + externalId.Length * sizeof(char), sizeof(double));
            Buffer.BlockCopy(BitConverter.GetBytes(new DateTimeOffset(timestamp).ToUnixTimeMilliseconds()), 0, bytes, sizeof(ushort) + externalId.Length * sizeof(char)
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
            timestamp = DateTimeOffset.FromUnixTimeMilliseconds(BitConverter.ToInt64(bytes, bytes.Length - sizeof(long))).DateTime;
            doubleValue = BitConverter.ToDouble(bytes, bytes.Length - sizeof(double) - sizeof(long));
            char[] chars = new char[(bytes.Length - sizeof(long) - sizeof(double))/sizeof(char)];
            Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length - sizeof(long) - sizeof(double));
            Id = new string(chars);
            isString = false;
        }
        public string ToDebugDescription()
        {
            return $"Update timeseries {Id} to {(isString ? stringValue : doubleValue.ToString())} at {timestamp.ToString()}";
        }
    }
}

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
        /// <summary>
        /// Latest timestamp historizing value was read from CDF
        /// </summary>
        public DateTime LatestTimestamp { get; set; } = new DateTime(1970, 1, 1);
        /// <summary>
        /// Value of variable as string or double
        /// </summary>
        public BufferedDataPoint Value { get; private set; }
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
            if (client.IsNumericType(DataType) || IsProperty)
            {
                Value = new BufferedDataPoint(
                    new DateTimeOffset(SourceTimestamp <= DateTime.MinValue ? DateTime.Now : SourceTimestamp).ToUnixTimeMilliseconds(),
                    client.GetUniqueId(Id),
                    UAClient.ConvertToString(value));
            }
            else
            {
                Value = new BufferedDataPoint(
                    new DateTimeOffset(SourceTimestamp <= DateTime.MinValue ? DateTime.Now : SourceTimestamp).ToUnixTimeMilliseconds(),
                    client.GetUniqueId(Id),
                    UAClient.ConvertToDouble(value));
            }
        }

    }
    /// <summary>
    /// Represents a single value at specified timestamp
    /// </summary>
    public class BufferedDataPoint
    {
        public readonly long timestamp;
        public readonly string Id;
        public readonly double doubleValue;
        public readonly string stringValue;
        public readonly bool isString;
        public readonly bool historizing;
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="Id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(long timestamp, string Id, double value)
        {
            this.timestamp = timestamp;
            this.Id = Id;
            doubleValue = value;
            isString = false;
        }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="Id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(long timestamp, string Id, string value)
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
            Buffer.BlockCopy(BitConverter.GetBytes(timestamp), 0, bytes, sizeof(ushort) + externalId.Length * sizeof(char)
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
            timestamp = BitConverter.ToInt64(bytes, bytes.Length - sizeof(long));
            doubleValue = BitConverter.ToDouble(bytes, bytes.Length - sizeof(double) - sizeof(long));
            char[] chars = new char[(bytes.Length - sizeof(long) - sizeof(double))/sizeof(char)];
            Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length - sizeof(long) - sizeof(double));
            Id = new string(chars);
            isString = false;
        }
    }
}

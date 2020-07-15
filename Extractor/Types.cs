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
using System.Collections.ObjectModel;
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
        public NodeId Id { get; }
        /// <summary>
        /// DisplayName in OPC-UA
        /// </summary>
        public string DisplayName { get; }
        /// <summary>
        /// True if this is also a BufferedVariable.
        /// </summary>
        public bool IsVariable { get; }
        /// <summary>
        /// NodeId of the parent. May be NodeId.Null for rootNodes
        /// </summary>
        public NodeId ParentId { get; }
        /// <summary>
        /// True if the properties of this object has been read.
        /// </summary>
        public bool PropertiesRead { get; set; } = false;
        /// <summary>
        /// Description in OPC-UA
        /// </summary>
        public string Description { get; set; }
        /// <summary>
        /// True if the node has been modified after pushing.
        /// </summary>
        public bool Changed { get; set; }
        /// <summary>
        /// Return a string description, for logging
        /// </summary>
        /// <returns>Descriptive string</returns>
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
                + $"Description: {Description}\n"
                + propertyString + "\n";
            return ret;
        }
        /// <summary>
        /// Properties in OPC-UA
        /// </summary>
        public IList<BufferedVariable> Properties { get; set; }
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
        public uint Identifier { get; }
        public bool IsStep { get; }
        public bool IsString { get; }
        public NodeId Raw { get; }
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
        /// <summary>
        /// Construct datatype from config object ProtoDateType and NodeId of datatype. Used when datatypes are being overriden.
        /// </summary>
        /// <param name="protoDataType">Overriding propoDataType</param>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public BufferedDataType(ProtoDataType protoDataType, NodeId rawDataType) : this(rawDataType)
        {
            if (protoDataType == null) throw new ArgumentNullException(nameof(protoDataType));
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
        /// <summary>
        /// Sets the datatype based on OPC-UA nodeId and configured map of numerical types.
        /// </summary>
        /// <param name="dataType">Raw nodeid of datatype from OPC-UA</param>
        /// <param name="numericalTypeMap">Mapping of numerical types</param>
        public void SetDataType(NodeId dataType, Dictionary<NodeId, ProtoDataType> numericalTypeMap)
        {
            if (numericalTypeMap == null) throw new ArgumentNullException(nameof(numericalTypeMap));
            if (numericalTypeMap.ContainsKey(dataType))
            {
                var proto = numericalTypeMap[dataType];
                DataType = new BufferedDataType(proto, dataType);
                return;
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
        /// <summary>
        /// True if attributes have been read from OPC-UA for this variable
        /// </summary>
        public bool DataRead { get; set; } = false;
        public override string ToDebugDescription()
        {
            string propertyString = "properties: {" + (Properties != null && Properties.Any() ? "\n" : "");
            if (Properties != null)
            {
                foreach (var prop in Properties)
                {
                    propertyString += $"    {prop.DisplayName}: {prop.Value?.StringValue},\n";
                }
            }
            propertyString += "}";

            string ret = $"DisplayName: {DisplayName}\n"
                + $"ParentId: {ParentId?.ToString()}\n"
                + $"Id: {Id.ToString()}\n"
                + $"Description: {Description}\n"
                + $"Historizing: {Historizing}\n"
                + $"ValueRank: {ValueRank}\n"
                + $"Dimension: {(ArrayDimensions != null && ArrayDimensions.Count == 1 ? ArrayDimensions[0] : -1)}\n"
                + propertyString + "\n"
                + DataType + "\n";
            return ret;
        }
        /// <summary>
        /// Parent if this represents an element of an array.
        /// </summary>
        public BufferedVariable ArrayParent { get; }
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
        public BufferedVariable(NodeId id, string displayName, NodeId parentId) : base(id, displayName, true, parentId) { }
        /// <summary>
        /// Sets the datapoint to provided DataValue.
        /// </summary>
        /// <param name="value">Value to set</param>
        /// <param name="SourceTimestamp">Timestamp from source</param>
        /// <param name="client">Current client context</param>
        public void SetDataPoint(object value, DateTime sourceTimestamp, UAClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (value == null) return;
            if (DataType.IsString || IsProperty)
            {
                Value = new BufferedDataPoint(
                    sourceTimestamp <= DateTime.MinValue ? DateTime.UtcNow : sourceTimestamp,
                    client.GetUniqueId(Id),
                    client.ConvertToString(value));
            }
            else
            {
                Value = new BufferedDataPoint(
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
        public BufferedVariable(BufferedVariable other, int index)
            : base(OtherNonNull(other).Id, other.DisplayName + $"[{index}]", true, other.Id)
        {
            ArrayParent = other;
            Index = index;
            Historizing = other.Historizing;
            DataType = other.DataType;
            ValueRank = other.ValueRank;
            ArrayDimensions = other.ArrayDimensions;
        }
        /// <summary>
        /// Returns given variable if it is not null, otherwise throws an error.
        /// Used to prevent warnings when calling base constructor.
        /// </summary>
        private static BufferedVariable OtherNonNull(BufferedVariable other)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            return other;
        }
    }
    /// <summary>
    /// Represents a single value at specified timestamp
    /// </summary>
    public class BufferedDataPoint
    {
        public DateTime Timestamp { get; }
        public string Id { get; }
        public double DoubleValue { get; }
        public string StringValue { get; }
        public bool IsString { get; }
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
        /// <summary>
        /// Copy given datapoint with given replacement value
        /// </summary>
        /// <param name="other">Datapoint to copy</param>
        /// <param name="replacement">Replacement value</param>
        public BufferedDataPoint(BufferedDataPoint other, string replacement)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            Timestamp = other.Timestamp;
            Id = other.Id;
            StringValue = replacement;
            IsString = other.IsString;
        }
        /// <summary>
        /// Copy given datapoint with given replacement value
        /// </summary>
        /// <param name="other">Datapoint to copy</param>
        /// <param name="replacement">Replacement value</param>
        public BufferedDataPoint(BufferedDataPoint other, double replacement)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
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
            ushort size = (ushort)(Id.Length * sizeof(char) + sizeof(ushort) + sizeof(long) + sizeof(bool));
            if (IsString)
            {
                size += (ushort)(StringValue.Length * sizeof(char) + sizeof(ushort));
            }
            else
            {
                size += sizeof(double);
            }
            var bytes = new byte[size + sizeof(ushort)];
            int pos = 0;
            Buffer.BlockCopy(BitConverter.GetBytes(size), 0, bytes, pos, sizeof(ushort));
            pos += sizeof(ushort);
            var idBytes = ExtractorUtils.StringToStorable(Id);
            Buffer.BlockCopy(idBytes, 0, bytes, pos, idBytes.Length);
            pos += idBytes.Length;
            Buffer.BlockCopy(BitConverter.GetBytes(Timestamp.ToBinary()), 0, bytes, pos, sizeof(long));
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(IsString), 0, bytes, pos, sizeof(bool));
            pos += sizeof(bool);

            if (IsString)
            {
                var valBytes = ExtractorUtils.StringToStorable(StringValue);
                Buffer.BlockCopy(valBytes, 0, bytes, pos, valBytes.Length);
            }
            else
            {
                Buffer.BlockCopy(BitConverter.GetBytes(DoubleValue), 0, bytes, pos, sizeof(double));
            }

            return bytes;
        }
        /// <summary>
        /// Initializes BufferedDataPoint from array of bytes, array should not contain the short size, which is just used to know how much
        /// to read at a time.
        /// </summary>
        /// <param name="bytes">Bytes to convert</param>
        public static (BufferedDataPoint DataPoint, int Position) FromStorableBytes(byte[] bytes, int next)
        {
            if (bytes == null || bytes.Length < sizeof(long) + sizeof(double) + sizeof(bool)) return (null, next);
            string txt;
            (txt, next) = ExtractorUtils.StringFromStorable(bytes, next);
            string id = txt;
            long dt = BitConverter.ToInt64(bytes, next);
            next += sizeof(long);
            DateTime ts = DateTime.FromBinary(dt);
            bool isstr = BitConverter.ToBoolean(bytes, next);
            next += sizeof(bool);

            if (isstr)
            {
                (txt, next) = ExtractorUtils.StringFromStorable(bytes, next);
                return (new BufferedDataPoint(ts, id, txt), next);
            }
            double val = BitConverter.ToDouble(bytes, next);
            next += sizeof(double);
            return (new BufferedDataPoint(ts, id, val), next);
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
        /// Transformed ID of the event. The Raw id is a byte-string. This is the byte-string transformed into Base64 and prepended the globalprefix.
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
        /// <summary>
        /// Converts event into array of bytes which may be written to file.
        /// The format is [ushort length][string message][string eventId][string sourceNode][long timestamp][string type]
        /// [string emitter][ushort metadata count][[string key][string value]...]
        /// Strings are stored on the format [ushort length][string]
        /// </summary>
        /// <param name="extractor">Extractor to use for nodeId conversions</param>
        /// <returns>Array of converted bytes</returns>
        public byte[] ToStorableBytes(UAExtractor extractor)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            var bytes = new List<byte>();
            bytes.AddRange(ExtractorUtils.StringToStorable(Message));
            bytes.AddRange(ExtractorUtils.StringToStorable(EventId));
            bytes.AddRange(ExtractorUtils.StringToStorable(extractor.GetUniqueId(SourceNode)));
            bytes.AddRange(BitConverter.GetBytes(Time.ToBinary()));
            bytes.AddRange(ExtractorUtils.StringToStorable(extractor.GetUniqueId(EventType)));
            bytes.AddRange(ExtractorUtils.StringToStorable(extractor.GetUniqueId(EmittingNode)));
            var metaDataBytes = new List<byte>();
            ushort count = 0;
            foreach (var kvp in MetaData)
            {
                count++;
                metaDataBytes.AddRange(ExtractorUtils.StringToStorable(kvp.Key));
                metaDataBytes.AddRange(ExtractorUtils.StringToStorable(extractor.ConvertToString(kvp.Value)));
            }
            bytes.AddRange(BitConverter.GetBytes(count));
            bytes.AddRange(metaDataBytes);
            bytes.InsertRange(0, BitConverter.GetBytes((ushort)bytes.Count));
            return bytes.ToArray();
        }
        /// <summary>
        /// Read event from given array of bytes. See BufferedEvent.ToStorableBytes for details.
        /// </summary>
        /// <param name="bytes">Bytes to read from</param>
        /// <param name="extractor">Extractor to use for nodeId conversions</param>
        /// <param name="next">Start position</param>
        /// <returns>Converted event and new position in array</returns>
        public static (BufferedEvent Event, int Position) FromStorableBytes(byte[] bytes, UAExtractor extractor, int next)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            if (bytes == null) throw new ArgumentNullException(nameof(bytes));
            var evt = new BufferedEvent();
            string txt;
            (txt, next) = ExtractorUtils.StringFromStorable(bytes, next);
            evt.Message = txt;
            (txt, next) = ExtractorUtils.StringFromStorable(bytes, next);
            evt.EventId = txt;
            (txt, next) = ExtractorUtils.StringFromStorable(bytes, next);
            evt.SourceNode = extractor.State.GetNodeId(txt);
            long dt = BitConverter.ToInt64(bytes, next);
            next += sizeof(long);
            evt.Time = DateTime.FromBinary(dt);
            string eventType;
            (eventType, next) = ExtractorUtils.StringFromStorable(bytes, next);
            (txt, next) = ExtractorUtils.StringFromStorable(bytes, next);
            evt.EmittingNode = extractor.State.GetEmitterState(txt).SourceId;
            
            ushort count = BitConverter.ToUInt16(bytes, next);
            next += sizeof(ushort);

            evt.MetaData = new Dictionary<string, object>
            {
                ["Type"] = eventType
            };

            for (int i = 0; i < count; i++)
            {
                string key, value;
                (key, next) = ExtractorUtils.StringFromStorable(bytes, next);
                (value, next) = ExtractorUtils.StringFromStorable(bytes, next);
                evt.MetaData[key] = value;
            }

            return (evt, next);
        }
    }
}

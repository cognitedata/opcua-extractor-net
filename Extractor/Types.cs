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
using System.IO;
using System.Linq;
using Cognite.Extensions;
using Cognite.Extractor.Utils;
using Opc.Ua;
using Serilog;

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
        public bool PropertiesRead { get; set; }
        /// <summary>
        /// Description in OPC-UA
        /// </summary>
        public string Description { get; set; }
        /// <summary>
        /// True if the node has been modified after pushing.
        /// </summary>
        public bool Changed { get; set; }
        /// <summary>
        /// Raw OPC-UA EventNotifier attribute.
        /// </summary>
        public byte EventNotifier { get; set; }
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
        public int GetUpdateChecksum(TypeUpdateConfig update, bool dataTypeMetadata)
        {
            if (update == null || !update.AnyUpdate) return 0;
            int checksum = 0;
            unchecked
            {
                if (update.Context)
                {
                    checksum += ParentId?.GetHashCode() ?? 0;
                }
                if (update.Description)
                {
                    checksum = checksum * 31 + Description?.GetHashCode() ?? 0;
                }
                if (update.Name)
                {
                    checksum = checksum * 31 + DisplayName?.GetHashCode() ?? 0;
                }
                if (update.Metadata)
                {
                    int metaHash = 0;
                    if (Properties != null)
                    {
                        foreach (var prop in Properties.OrderBy(prop => prop.DisplayName))
                        {
                            metaHash *= 31;
                            if (prop.Value == null || prop.DisplayName == null) continue;
                            metaHash += (prop.DisplayName, prop.Value.StringValue).GetHashCode();
                        }
                        if (dataTypeMetadata && this is BufferedVariable variable)
                        {
                            metaHash = metaHash * 31 + variable.DataType.Raw.GetHashCode();
                        }
                    }
                    checksum = checksum * 31 + metaHash;
                }
            }
            return checksum;
        }
    }
    
    /// <summary>
    /// Represents an opcua variable, which may be either a piece of metadata or a cdf timeseries
    /// </summary>
    public class BufferedVariable : BufferedNode
    {
        /// <summary>
        /// Data type of this variable
        /// </summary>
        public BufferedDataType DataType { get; set; }
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
        public BufferedDataPoint Value { get; private set; }
        /// <summary>
        /// True if attributes have been read from OPC-UA for this variable
        /// </summary>
        public bool DataRead { get; set; }
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
        /// Children if this represents the parent in an array
        /// </summary>
        public IEnumerable<BufferedVariable> ArrayChildren { get; private set; }
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
        private BufferedVariable(BufferedVariable other, int index)
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
        /// <summary>
        /// Create array child nodes
        /// </summary>
        public IEnumerable<BufferedVariable> CreateArrayChildren()
        {
            var children = new List<BufferedVariable>();
            for (int i = 0; i < ArrayDimensions[0]; i++)
            {
                children.Add(new BufferedVariable(this, i));
            }
            ArrayChildren = children;
            return children;
        }
    }
    /// <summary>
    /// Represents a single value at specified timestamp
    /// </summary>
    public class BufferedDataPoint
    {
        public DateTime Timestamp { get; }
        public string Id { get; }
        public double? DoubleValue { get; }
        public string StringValue { get; }
        public bool IsString { get => !DoubleValue.HasValue; }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="Id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(DateTime timestamp, string id, double value)
        {
            Timestamp = timestamp;
            Id = id;
            DoubleValue = value;
        }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="Id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(DateTime timestamp, string id, string value)
        {
            Timestamp = timestamp;
            Id = id;
            StringValue = value;
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
            var bytes = new List<byte>();
            int pos = 0;
            pos += sizeof(ushort);
            bytes.AddRange(CogniteUtils.StringToStorable(Id));
            bytes.AddRange(BitConverter.GetBytes(Timestamp.ToBinary()));
            bytes.AddRange(BitConverter.GetBytes(IsString));

            if (IsString)
            {
                bytes.AddRange(CogniteUtils.StringToStorable(StringValue));
            }
            else
            {
                bytes.AddRange(BitConverter.GetBytes(DoubleValue.Value));
            }

            return bytes.ToArray();
        }
        /// <summary>
        /// Initializes BufferedDataPoint from array of bytes, array should not contain the short size, which is just used to know how much
        /// to read at a time.
        /// </summary>
        /// <param name="bytes">Bytes to convert</param>
        public static BufferedDataPoint FromStream(Stream stream)
        {
            if (stream == null) return null;
            string id = CogniteUtils.StringFromStream(stream);
            if (id == null) return null;
            var buffer = new byte[sizeof(long)];
            if (stream.Read(buffer, 0, sizeof(long)) < sizeof(long)) return null;
            DateTime ts = DateTime.FromBinary(BitConverter.ToInt64(buffer, 0));
            if (stream.Read(buffer, 0, sizeof(bool)) < sizeof(bool)) return null;
            bool isstr = BitConverter.ToBoolean(buffer, 0);
            if (isstr)
            {
                var value = CogniteUtils.StringFromStream(stream);
                return new BufferedDataPoint(ts, id, value);
            }
            else
            {
                if (stream.Read(buffer, 0, sizeof(double)) < sizeof(double)) return null;
                var value = BitConverter.ToDouble(buffer, 0);
                return new BufferedDataPoint(ts, id, value);
            }
        }
        public string ToDebugDescription()
        {
            return $"Update timeseries {Id} to {(IsString ? "\"" + StringValue + "\"" : DoubleValue.Value.ToString(CultureInfo.InvariantCulture))}" +
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
            bytes.AddRange(CogniteUtils.StringToStorable(Message));
            bytes.AddRange(CogniteUtils.StringToStorable(EventId));
            bytes.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(SourceNode)));
            bytes.AddRange(BitConverter.GetBytes(Time.ToBinary()));
            bytes.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(EventType)));
            bytes.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(EmittingNode)));
            var metaDataBytes = new List<byte>();
            ushort count = 0;
            foreach (var kvp in MetaData)
            {
                count++;
                metaDataBytes.AddRange(CogniteUtils.StringToStorable(kvp.Key));
                metaDataBytes.AddRange(CogniteUtils.StringToStorable(extractor.ConvertToString(kvp.Value)));
            }
            bytes.AddRange(BitConverter.GetBytes(count));
            bytes.AddRange(metaDataBytes);
            return bytes.ToArray();
        }
        /// <summary>
        /// Read event from given stream. See BufferedEvent.ToStorableBytes for details.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="extractor">Extractor to use for nodeId conversions</param>
        /// <returns>Converted event</returns>
        public static BufferedEvent FromStream(Stream stream, UAExtractor extractor)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            var evt = new BufferedEvent();
            evt.Message = CogniteUtils.StringFromStream(stream);
            evt.EventId = CogniteUtils.StringFromStream(stream);
            evt.SourceNode = extractor.State.GetNodeId(CogniteUtils.StringFromStream(stream));
            var buffer = new byte[sizeof(long)];

            if (stream.Read(buffer, 0, sizeof(long)) < sizeof(long)) return null;
            long dt = BitConverter.ToInt64(buffer, 0);
            evt.Time = DateTime.FromBinary(dt);
            var eventType = CogniteUtils.StringFromStream(stream);
            evt.EmittingNode = extractor.State.GetEmitterState(CogniteUtils.StringFromStream(stream))?.SourceId;
            if (evt.EmittingNode == null) return null;

            if (stream.Read(buffer, 0, sizeof(ushort)) < sizeof(ushort)) return null;
            ushort count = BitConverter.ToUInt16(buffer, 0);

            evt.MetaData = new Dictionary<string, object>
            {
                ["Type"] = eventType
            };

            for (int i = 0; i < count; i++)
            {
                string key = CogniteUtils.StringFromStream(stream);
                string value = CogniteUtils.StringFromStream(stream);
                evt.MetaData[key] = value;
            }

            return evt;
        }
    }
    /// <summary>
    /// Represents a non-hierarchical reference between two nodes in the hierarchy
    /// </summary>
    public class BufferedReference
    {
        /// <summary>
        /// NodeId of the OPC-UA reference type
        /// </summary>
        public BufferedReferenceType Type { get; }
        /// <summary>
        /// True if this is a forward reference, false otherwise
        /// </summary>
        public bool IsForward { get; }
        /// <summary>
        /// NodeId of the source node
        /// </summary>
        public ReferenceVertex Source { get; }
        /// <summary>
        /// NodeId of the target node
        /// </summary>
        public ReferenceVertex Target { get; }
        // Slight hack here to properly get vertex types without needing the full node objects.
        public BufferedReference(ReferenceDescription desc, BufferedNode source,
            NodeId target, NodeExtractionState targetState, ReferenceTypeManager manager)
        {
            if (desc == null) throw new ArgumentNullException(nameof(desc));
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (target == null) throw new ArgumentNullException(nameof(target));
            if (manager == null) throw new ArgumentNullException(nameof(manager));
            Type = manager.GetReferenceType(desc.ReferenceTypeId);
            IsForward = desc.IsForward;
            Source = new ReferenceVertex(source.Id, (source is BufferedVariable variable) && !variable.IsArray);
            Target = new ReferenceVertex(target, desc.NodeClass == NodeClass.Variable && (targetState == null || !targetState.IsArray));
        }
        // For hierarchical references, here the source should always be an object...
        public BufferedReference(ReferenceDescription desc, NodeId source, BufferedNode target, ReferenceTypeManager manager, bool inverse)
        {
            if (desc == null) throw new ArgumentNullException(nameof(desc));
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (target == null) throw new ArgumentNullException(nameof(target));
            if (manager == null) throw new ArgumentNullException(nameof(manager));
            Type = manager.GetReferenceType(desc.ReferenceTypeId);
            IsForward = !inverse;
            Source = new ReferenceVertex(source, false);
            Target = new ReferenceVertex(target.Id, target is BufferedVariable variable && !variable.IsArray);
            if (inverse)
            {
                var temp = Source;
                Source = Target;
                Target = temp;
            }
        }
        public string GetName()
        {
            return Type.GetName(!IsForward);
        }
        public override bool Equals(object obj)
        {
            if (!(obj is BufferedReference other)) return false;
            return other.Source == Source
                && other.Target == Target
                && other.Type.Id == Type.Id;
        }

        public override int GetHashCode()
        {
            return (Source, Target, Type.Id).GetHashCode();
        }
    }
    public class ReferenceVertex
    {
        public NodeId Id { get; }
        public int Index { get; }
        public bool IsTimeSeries { get; }
        public ReferenceVertex(NodeId id, bool isVariable)
        {
            Id = id;
            IsTimeSeries = isVariable;
        }
    }
}

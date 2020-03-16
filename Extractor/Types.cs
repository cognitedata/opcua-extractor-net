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
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
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

        public BufferedDataPoint(BufferedDataPoint other, string replacement)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            Timestamp = other.Timestamp;
            Id = other.Id;
            StringValue = replacement;
            IsString = other.IsString;
        }
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
        public static (BufferedDataPoint, int) FromStorableBytes(byte[] bytes, int next)
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

        public byte[] ToStorableBytes(Extractor extractor)
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

        public static (BufferedEvent, int) FromStorableBytes(byte[] bytes, Extractor extractor, int next)
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
            evt.EmittingNode = extractor.State.GetEmitterState(txt).Id;
            
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
    public class InternalId
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:Uri properties should not be strings",
            Justification = "NamespaceUris are arbitrary")]
        private string namespaceUri;

        private object identifier;
        private IdType idType;

        private string externalId;
        private NodeId nodeId;

        public int Index { get; private set; } = -1;

        private readonly ExtractionConfig config;

        private static readonly Regex idTypeRegex = new Regex(@"([g|i|b|s])=", RegexOptions.Singleline);
        private static readonly Regex indexRegex = new Regex(@"[^\\]\[([0-9]+)\]$");

        public bool Invalid { get; private set; }

        private bool parsed;

        public InternalId(NodeId id, UAClient uaClient, ExtractionConfig config, int index = -1)
        {
            if (id == null || id.IsNullNodeId)
            {
                Invalid = true;
                return;
            }
            if (uaClient == null) throw new ArgumentNullException(nameof(uaClient));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            namespaceUri = uaClient.GetNamespaceTable().GetString(id.NamespaceIndex);
            identifier = id.Identifier;
            idType = id.IdType;
            Index = index;
            nodeId = id;
            parsed = true;
        }

        public InternalId(string id, ExtractionConfig config)
        {
            if (id == null)
            {
                Invalid = true;
                Log.Error("Invalid due to null id");
                return;
            }
            this.config = config ?? throw new ArgumentNullException(nameof(config));

            externalId = id;
            if (externalId.EndsWith(']'))
            {
                ParseExternalId();
            }
        }

        private void ParseExternalId()
        {
            int skip = config.IdPrefix.Length;
            if (!externalId.StartsWith(config.IdPrefix, StringComparison.InvariantCulture))
            {
                skip += 7;
            }

            string id = externalId.Substring(skip);

            var idMatch = idTypeRegex.Match(id);
            if (!idMatch.Success)
            {
                Log.Error("Invalid due to no match: {id}", id);
                Invalid = true;
                return;
            }

            idType = FromSymbol(idMatch.Groups[1].Captures[0].Value.First());

            var namespaceStr = id.Substring(0, idMatch.Index);
            var map = config.NamespaceMap.FirstOrDefault(kvp => kvp.Value == namespaceStr);
            namespaceUri = map.Key ?? namespaceStr[..^1];

            var idtStr = id.Substring(idMatch.Index + 2);



            if (idtStr.EndsWith(']'))
            {
                var idxMatch = indexRegex.Match(idtStr);
                if (idxMatch.Success)
                {
                    Index = Convert.ToInt32(idxMatch.Groups[1].Captures[0].Value, CultureInfo.InvariantCulture);
                    identifier = idtStr.Substring(0, idtStr.Length - idxMatch.Length + 1);
                }
                else
                {
                    Index = -1;
                    identifier = idtStr;
                }
            }
            else
            {
                Index = -1;
                identifier = idtStr;
            }

            identifier = Regex.Unescape((string)identifier);


            parsed = true;
        }

        private static IdType FromSymbol(char symbol)
        {
            return symbol switch
            {
                'g' => IdType.Guid,
                'i' => IdType.Numeric,
                'b' => IdType.Opaque,
                's' => IdType.String,
                _ => IdType.String
            };
        }

        private static string SanitizeIdentifier(object idtf, IdType idType)
        {
            string id = Regex.Escape(IdentifierToString(idtf, idType));

            return id;
        }

        public string ToExternalId()
        {
            if (externalId != null) return externalId;
            string prefix = config.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");

            string id = $"{config.IdPrefix}{prefix}{SanitizeIdentifier(identifier, idType)}";


            if (Index > -1)
            {
                string idxString = $"[{Index}]";
                if (id.Length > 255 - idxString.Length)
                {
                    id = id.Substring(0, 255 - idxString.Length);
                }

                id += idxString;
            }
            else if (id.Length > 255)
            {
                id = id.Substring(0, 255);
            }

            externalId = id;
            return externalId;
        }

        public NodeId ToNodeId(UAClient uaClient)
        {
            if (uaClient == null) throw new ArgumentNullException(nameof(uaClient));
            if (nodeId != null) return nodeId;
            if (!parsed)
            {
                ParseExternalId();
            }
            if (Invalid) return NodeId.Null;
            object transformedIdentifier = identifier;
            if (identifier is string strIdentifier && idType != IdType.String)
            {
                transformedIdentifier = idType switch
                {
                    IdType.Guid => Guid.Parse(strIdentifier),
                    IdType.Numeric => Convert.ToUInt32(strIdentifier, CultureInfo.InvariantCulture),
                    IdType.Opaque => Convert.FromBase64String(strIdentifier),
                    _ => transformedIdentifier
                };
            }

            nodeId = Invalid ? NodeId.Null : NodeId.Create(transformedIdentifier, namespaceUri, uaClient.GetNamespaceTable());
            return nodeId;
        }

        public override string ToString()
        {
            return ToExternalId();
        }

        private static string IdentifierToString(object identifier, IdType idType)
        {
            switch (idType)
            {
                case IdType.Numeric:
                    if (identifier == null)
                    {
                        return "i=0";
                    }

                    return $"i={identifier}";
                case IdType.String:
                    return $"s={identifier}";
                case IdType.Guid:
                    if (identifier == null)
                    {
                        return $"g={Guid.Empty}";
                    }

                    return $"g={identifier}";
                case IdType.Opaque:
                    if (identifier == null)
                        return "b=";
                    if (identifier is string)
                    {
                        return $"b={identifier}";
                    }
                    return $"b={Convert.ToBase64String((byte[]) identifier)}";
                default:
                    return "s=";
            }
        }

        public override bool Equals(object other)
        {
            if (other == null || !(other is InternalId otherId) || otherId.Invalid) return false;

            return otherId.ToExternalId() == ToExternalId();
        }

        public override int GetHashCode()
        {
            return ToExternalId().GetHashCode(StringComparison.InvariantCulture);
        }
    }
}

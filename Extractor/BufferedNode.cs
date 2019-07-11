using System;
using System.Collections.Generic;
using System.Linq;
using Cognite.Sdk.Assets;
using Cognite.Sdk.Timeseries;
using Opc.Ua;

namespace Cognite.OpcUa
{
    public class BufferedNode
    {
        public readonly NodeId Id;
        public readonly string DisplayName;
        public readonly bool IsVariable;
        public readonly NodeId ParentId;
        public string Description { get; set; }
        public IList<BufferedVariable> properties;
        public BufferedNode(NodeId Id, string DisplayName, NodeId ParentId) : this(Id, DisplayName, false, ParentId) { }
        protected BufferedNode(NodeId Id, string DisplayName, bool IsVariable, NodeId ParentId)
        {
            this.Id = Id;
            this.DisplayName = DisplayName;
            this.IsVariable = IsVariable;
            this.ParentId = ParentId;
        }
        public AssetWritePoco ToAsset(string externalId, NodeId rootNode, long rootAsset, UAClient UAClient)
        {
            var writePoco = new AssetWritePoco
            {
                Description = Description,
                ExternalId = externalId,
                Name = DisplayName
            };
            if (ParentId == rootNode)
            {
                writePoco.ParentId = rootAsset;
            }
            else
            {
                writePoco.ParentExternalId = UAClient.GetUniqueId(ParentId);
            }
            if (properties != null && properties.Any())
            {
                writePoco.MetaData = new Dictionary<string, string>();
                foreach (var property in properties)
                {
                    if (property.Value != null)
                    {
                        writePoco.MetaData.Add(property.DisplayName, property.Value.stringValue);
                    }
                }
            }
            return writePoco;
        }
    }
    public class BufferedVariable : BufferedNode
    {
        public uint DataType { get; set; }
        public bool Historizing { get; set; }
        public int ValueRank { get; set; }
        public bool IsProperty { get; set; }
        public DateTime LatestTimestamp { get; set; } = new DateTime(1970, 1, 1);
        public BufferedDataPoint Value { get; set; }
        public BufferedVariable(NodeId Id, string DisplayName, NodeId ParentId) : base(Id, DisplayName, true, ParentId) { }
        public void SetDataPoint(DataValue value, UAClient client)
        {
            if (value == null || value.Value == null) return;
            if (DataType < DataTypes.Boolean || DataType > DataTypes.Double || IsProperty)
            {
                Value = new BufferedDataPoint(
                    (long)value.SourceTimestamp.Subtract(Extractor.epoch).TotalMilliseconds,
                    client.GetUniqueId(Id),
                    UAClient.ConvertToString(value));
            }
            else
            {
                Value = new BufferedDataPoint(
                    (long)value.SourceTimestamp.Subtract(Extractor.epoch).TotalMilliseconds,
                    client.GetUniqueId(Id),
                    UAClient.ConvertToDouble(value));
            }
        }
        public TimeseriesWritePoco ToTimeseries(string externalId, IDictionary<NodeId, long> nodeToAssetIds)
        {
            var writePoco = new TimeseriesWritePoco
            {
                Description = Description,
                ExternalId = externalId,
                AssetId = nodeToAssetIds[ParentId],
                Name = DisplayName,
                LegacyName = externalId
            };
            if (properties != null && properties.Any())
            {
                writePoco.MetaData = new Dictionary<string, string>();
                foreach (var property in properties)
                {
                    if (property.Value != null)
                    {
                        writePoco.MetaData.Add(property.DisplayName, property.Value.stringValue);
                    }
                }
            }
            writePoco.IsStep |= DataType == DataTypes.Boolean;
            return writePoco;
        }
    }
    public class BufferedDataPoint
    {
        public readonly long timestamp;
        public readonly string nodeId;
        public readonly double doubleValue;
        public readonly string stringValue;
        public readonly bool isString;
        public BufferedDataPoint(long timestamp, string nodeId, double value)
        {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
            doubleValue = value;
            isString = false;
        }
        public BufferedDataPoint(long timestamp, string nodeId, string value)
        {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
            stringValue = value;
            isString = true;
        }
        // This will obviously break if the system encoding changes somehow, it is not intended as any kind of cross-system storage.
        // Convert the datapoint into an array of bytes. The string is converted directly, as the system encoding is unknown, but can
        // be assumed to be constant.
        // The structure is as follows: ushort size | unknown encoding string externalid | double value | long timestamp
        public byte[] ToStorableBytes()
        {
            string externalId = nodeId;
            ushort size = (ushort)(externalId.Length * sizeof(char) + sizeof(double) + sizeof(long));
            byte[] bytes = new byte[size + sizeof(ushort)];
            Buffer.BlockCopy(externalId.ToCharArray(), 0, bytes, sizeof(ushort), externalId.Length * sizeof(char));
            Buffer.BlockCopy(BitConverter.GetBytes(doubleValue), 0, bytes, sizeof(ushort) + externalId.Length * sizeof(char), sizeof(double));
            Buffer.BlockCopy(BitConverter.GetBytes(timestamp), 0, bytes, sizeof(ushort) + externalId.Length * sizeof(char)
                + sizeof(double), sizeof(long));
            Buffer.BlockCopy(BitConverter.GetBytes(size), 0, bytes, 0, sizeof(ushort));
            return bytes;
        }
        public BufferedDataPoint(byte[] bytes)
        {
            timestamp = BitConverter.ToInt64(bytes, bytes.Length - sizeof(long));
            doubleValue = BitConverter.ToDouble(bytes, bytes.Length - sizeof(double) - sizeof(long));
            char[] chars = new char[(bytes.Length - sizeof(long) - sizeof(double))/sizeof(char)];
            Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length - sizeof(long) - sizeof(double));
            nodeId = new string(chars);
            isString = false;
        }
    }
}

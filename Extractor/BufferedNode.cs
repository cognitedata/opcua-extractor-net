using System;
using System.Collections.Generic;
using System.Linq;
using Cognite.Sdk.Assets;
using Cognite.Sdk.Timeseries;
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
        /// <summary>
        /// Converts BufferedNode into asset write poco.
        /// </summary>
        /// <param name="externalId">External id, this is known when being called, so we pass it for efficiency</param>
        /// <param name="rootNode">Root node for the extractor</param>
        /// <param name="rootAsset">Root asset for the extractor</param>
        /// <param name="UAClient">Active UAClient</param>
        /// <returns>Full asset write poco</returns>
        public AssetWritePoco ToAsset(string externalId, NodeId rootNode, long rootAsset, UAClient UAClient)
        {
            if (IsVariable)
            {
                throw new Exception("ToAsset called on variable");
            }
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
        /// <param name="Id">NodeId of buffered node</param>
        /// <param name="DisplayName">DisplayName of buffered node</param>
        /// <param name="ParentId">Id of parent of buffered node</param>
        public BufferedVariable(NodeId Id, string DisplayName, NodeId ParentId) : base(Id, DisplayName, true, ParentId) { }
        /// <summary>
        /// Sets the datapoint to provided DataValue.
        /// </summary>
        /// <param name="value">Value to set</param>
        /// <param name="client">Current client context</param>
        public void SetDataPoint(DataValue value, UAClient client)
        {
            if (value == null || value.Value == null) return;
            if (DataType < DataTypes.Boolean || DataType > DataTypes.Double || IsProperty)
            {
                Value = new BufferedDataPoint(
                    (long)value.SourceTimestamp.Subtract(Extractor.Epoch).TotalMilliseconds,
                    client.GetUniqueId(Id),
                    UAClient.ConvertToString(value));
            }
            else
            {
                Value = new BufferedDataPoint(
                    (long)value.SourceTimestamp.Subtract(Extractor.Epoch).TotalMilliseconds,
                    client.GetUniqueId(Id),
                    UAClient.ConvertToDouble(value));
            }
        }
        /// <summary>
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="externalId">ExternalId is usually known in context, so pass it here</param>
        /// <param name="nodeToAssetIds">Map containing parent id and asset id</param>
        /// <returns>Complete timeseries write poco</returns>
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
    /// <summary>
    /// Represents a single value at specified timestamp
    /// </summary>
    public class BufferedDataPoint
    {
        public readonly long timestamp;
        public readonly string nodeId;
        public readonly double doubleValue;
        public readonly string stringValue;
        public readonly bool isString;
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="nodeId">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(long timestamp, string nodeId, double value)
        {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
            doubleValue = value;
            isString = false;
        }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="nodeId">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public BufferedDataPoint(long timestamp, string nodeId, string value)
        {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
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
            nodeId = new string(chars);
            isString = false;
        }
    }
}

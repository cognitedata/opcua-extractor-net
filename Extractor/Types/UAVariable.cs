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

using Cognite.OpcUa.Pushers;
using CogniteSdk;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents an opcua variable, which may be either a piece of metadata or a cdf timeseries
    /// </summary>
    public class UAVariable : UANode
    {
        /// <summary>
        /// Data type of this variable
        /// </summary>
        public UADataType DataType { get; set; }
        /// <summary>
        /// True if the opcua node stores its own history
        /// </summary>
        public bool Historizing { get; set; }
        /// <summary>
        /// ValueRank in opcua
        /// </summary>
        public int ValueRank { get; set; }

        /// <summary>
        /// Value of variable as string or double
        /// </summary>
        public UADataPoint Value { get; private set; }
        /// <summary>
        /// Whether the value of this variable has been read from the server.
        /// </summary>
        public bool ValueRead { get; set; }
        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.AppendFormat(CultureInfo.InvariantCulture, "Variable: {0}\n", DisplayName);
            builder.AppendFormat(CultureInfo.InvariantCulture, "Id: {0}\n", Id);
            if (ParentId != null && !ParentId.IsNullNodeId)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "ParentId: {0}\n", ParentId);
            }
            if (Description != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "Description: {0}\n", Description);
            }
            if (DataType != null)
            {
                builder.Append(DataType);
                builder.Append('\n');
            }
            if (Historizing)
            {
                builder.Append("Historizing: True\n");
            }
            if (ValueRank != ValueRanks.Scalar)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "ValueRank: {0}\n", ValueRank);
            }
            if (ArrayDimensions != null && ArrayDimensions.Count == 1)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "Dimension: {0}\n", ArrayDimensions[0]);
            }
            if (NodeType != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "NodeType: {0}\n", NodeType.Name ?? NodeType.Id);
            }

            if (Properties != null && Properties.Any())
            {
                var meta = BuildMetadata(null);
                builder.Append("Properties: {\n");
                foreach (var prop in meta)
                {
                    builder.AppendFormat(CultureInfo.InvariantCulture, "    {0}: {1}\n", prop.Key, prop.Value ?? "??");
                }
                builder.Append('}');
            }
            return builder.ToString();
        }
        /// <summary>
        /// Parent if this represents an element of an array.
        /// </summary>
        public UAVariable ArrayParent { get; }
        /// <summary>
        /// Children if this represents the parent in an array
        /// </summary>
        public IEnumerable<UAVariable> ArrayChildren { get; private set; }
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
        public UAVariable(NodeId id, string displayName, NodeId parentId) : base(id, displayName, true, parentId) { }
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
            Value = DataType.ToDataPoint(client, value, sourceTimestamp, client.GetUniqueId(Id), IsProperty);
        }
        /// <summary>
        /// Create an array-element variable.
        /// </summary>
        /// <param name="other">Parent variable</param>
        /// <param name="index">Index in the array</param>
        private UAVariable(UAVariable other, int index)
            : base(OtherNonNull(other).Id, other.DisplayName + $"[{index}]", true, other.Id)
        {
            ArrayParent = other;
            Index = index;
            Historizing = other.Historizing;
            DataType = other.DataType;
            ValueRank = other.ValueRank;
            ArrayDimensions = other.ArrayDimensions;
            NodeType = other.NodeType;
        }
        /// <summary>
        /// Returns given variable if it is not null, otherwise throws an error.
        /// Used to prevent warnings when calling base constructor.
        /// </summary>
        private static UAVariable OtherNonNull(UAVariable other)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            return other;
        }
        /// <summary>
        /// Create array child nodes
        /// </summary>
        public IEnumerable<UAVariable> CreateArrayChildren()
        {
            if (!IsArray) return Enumerable.Empty<UAVariable>();
            var children = new List<UAVariable>();
            for (int i = 0; i < ArrayDimensions[0]; i++)
            {
                children.Add(new UAVariable(this, i));
            }
            ArrayChildren = children;
            return children;
        }
        private void HandleMetaMap(Dictionary<string, string> metaMap, TimeSeriesCreate writePoco, Action<string> parentIdHandler)
        {
            if (Properties == null || !Properties.Any() || metaMap == null || !metaMap.Any()) return;
            foreach (var prop in Properties)
            {
                if (!prop.IsVariable || !(prop is UAVariable propVar)) continue;
                if (!string.IsNullOrWhiteSpace(propVar.Value?.StringValue) && metaMap.TryGetValue(prop.DisplayName, out var mapped))
                {
                    var value = propVar.Value.StringValue;
                    switch (mapped)
                    {
                        case "description": writePoco.Description = value; break;
                        case "name": writePoco.Name = value; break;
                        case "unit": writePoco.Unit = value; break;
                        case "parentId":
                            parentIdHandler(value);
                            break;
                    }
                }
            }
        }

        public StatelessTimeSeriesCreate ToStatelessTimeSeries(
            UAExtractor extractor,
            long? dataSetId,
            Dictionary<string, string> metaMap)
        {
            if (extractor == null) return null;
            string externalId = extractor.GetUniqueId(Id, Index);
            var writePoco = new StatelessTimeSeriesCreate
            {
                Description = Description,
                ExternalId = externalId,
                AssetExternalId = extractor.GetUniqueId(ParentId),
                Name = DisplayName,
                LegacyName = externalId,
                IsString = DataType.IsString,
                IsStep = DataType.IsStep,
                DataSetId = dataSetId
            };

            writePoco.Metadata = BuildMetadata(extractor);

            HandleMetaMap(metaMap, writePoco, value => writePoco.AssetExternalId = value);

            return writePoco;
        }
        public TimeSeriesCreate ToTimeseries(
            UAExtractor extractor,
            long? dataSetId,
            IDictionary<NodeId, long> nodeToAssetIds,
            Dictionary<string, string> metaMap,
            bool minimal = false)
        {
            if (extractor == null) return null;

            string externalId = extractor.GetUniqueId(Id, Index);

            if (minimal)
            {
                return new TimeSeriesCreate
                {
                    ExternalId = externalId,
                    IsString = DataType.IsString,
                    IsStep = DataType.IsStep,
                    DataSetId = dataSetId
                };
            }

            var writePoco = new TimeSeriesCreate
            {
                Description = Description,
                ExternalId = externalId,
                Name = DisplayName,
                LegacyName = externalId,
                IsString = DataType.IsString,
                IsStep = DataType.IsStep,
                DataSetId = dataSetId
            };

            if (nodeToAssetIds != null && nodeToAssetIds.TryGetValue(ParentId, out long parent))
            {
                writePoco.AssetId = parent;
            }

            writePoco.Metadata = BuildMetadata(extractor);

            HandleMetaMap(metaMap, writePoco, value =>
            {
                var id = extractor.State.GetNodeId(value);
                if (id != null && nodeToAssetIds != null && nodeToAssetIds.TryGetValue(id, out long assetId))
                {
                    writePoco.AssetId = assetId;
                }
            });

            return writePoco;
        }
    }
}

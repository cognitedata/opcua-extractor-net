/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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

using Cognite.OpcUa.Config;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Nodes
{
    public class VariableAttributes : BaseNodeAttributes
    {
        public bool Historizing { get; set; }
        public int ValueRank { get; set; }
        public UADataType DataType { get; set; } = null!;
        public int[]? ArrayDimensions { get; set; }
        public byte AccessLevel { get; set; }
        public Variant? Value { get; set; }

        public UAVariableType? TypeDefinition { get; set; }

        public VariableAttributes(UAVariableType? type) : base(NodeClass.Variable)
        {
            TypeDefinition = type;
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            if (config.History.Enabled)
            {
                yield return Attributes.Historizing;
            }
            if (!config.Subscriptions.IgnoreAccessLevel)
            {
                yield return Attributes.UserAccessLevel;
            }
            yield return Attributes.DataType;
            yield return Attributes.ValueRank;
            yield return Attributes.ArrayDimensions;
            foreach (var attr in base.GetAttributeSet(config)) yield return attr;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.Historizing:
                    Historizing = value.GetValue(false);
                    break;
                case Attributes.UserAccessLevel:
                    AccessLevel = value.GetValue<byte>(0);
                    break;
                case Attributes.DataType:
                    var dataTypeId = value.GetValue(NodeId.Null);
                    DataType = typeManager.GetDataType(dataTypeId);
                    break;
                case Attributes.ValueRank:
                    ValueRank = value.GetValue(ValueRanks.Any);
                    break;
                case Attributes.ArrayDimensions:
                    if (value.Value is int[] dimVal)
                    {
                        ArrayDimensions = dimVal;
                    }
                    break;
                case Attributes.Value:
                    Value = value.WrappedValue;
                    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }

        public bool? ShouldSubscribeOverride { get; set; }
        public bool? ShouldReadHistoryOverride { get; set; }

        public bool ShouldReadHistory(FullConfig config)
        {
            if (!config.History.Enabled || !config.History.Data) return false;

            if (ShouldReadHistoryOverride != null) return ShouldReadHistoryOverride.Value;

            if (config.Subscriptions.IgnoreAccessLevel)
            {
                return Historizing;
            }
            bool shouldRead = (AccessLevel & AccessLevels.HistoryRead) != 0;
            if (config.History.RequireHistorizing)
            {
                shouldRead &= Historizing;
            }
            return shouldRead;
        }

        public bool ShouldSubscribe(FullConfig config)
        {
            if (!config.Subscriptions.DataPoints && !config.PubSub.Enabled) return false;

            if (ShouldSubscribeOverride != null) return ShouldSubscribeOverride.Value;

            if (config.Subscriptions.IgnoreAccessLevel)
            {
                return true;
            }
            return (AccessLevel & AccessLevels.CurrentRead) != 0;
        }

        public override void LoadFromSavedNode(SavedNode node, TypeManager typeManager)
        {
            Historizing = node.InternalInfo!.Historizing;
            ValueRank = node.InternalInfo.ValueRank;
            DataType = typeManager.GetDataType(node.DataTypeId!);
            ArrayDimensions = node.InternalInfo.ArrayDimensions;
            AccessLevel = node.InternalInfo.AccessLevel;
            ShouldSubscribeOverride = node.InternalInfo.ShouldSubscribeData;
            ShouldReadHistoryOverride = node.InternalInfo.ShouldReadHistoryData;

            base.LoadFromSavedNode(node, typeManager);
        }

        public void LoadFromNodeState(BaseVariableState state, TypeManager typeManager)
        {
            Historizing = state.Historizing;
            ValueRank = state.ValueRank;
            DataType = typeManager.GetDataType(state.DataType);
            ArrayDimensions = state.ArrayDimensions.Select(Convert.ToInt32).ToArray();
            if (!ArrayDimensions.Any())
            {
                ArrayDimensions = null;
            }
            AccessLevel = state.AccessLevel;
            // Only assign non-null values here. There is no real way at this stage to
            // distinguish between no value being set, and the value being null.
            // Leaving it out means that we can load it later.
            if (state.WrappedValue != Variant.Null && state.WrappedValue.Value != null)
            {
                Value = state.WrappedValue;
            }
            LoadFromBaseNodeState(state);
        }
    }

    public class UAVariable : BaseUANode
    {
        public UAVariable(NodeId id, string? displayName, QualifiedName? browseName, BaseUANode? parent, NodeId? parentId, UAVariableType? typeDefinition)
            : base(id, parent, parentId)
        {
            FullAttributes = new VariableAttributes(typeDefinition);
            Attributes.DisplayName = displayName;
            Attributes.BrowseName = browseName;
        }

        protected UAVariable(UAVariable other)
            : base(other.Id, other, other.Id)
        {
            FullAttributes = other.FullAttributes;
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public VariableAttributes FullAttributes { get; }

        public int ValueRank => FullAttributes.ValueRank;
        public int[]? ArrayDimensions => FullAttributes.ArrayDimensions;
        public Variant? Value => FullAttributes.Value;

        [MemberNotNullWhen(true, nameof(ArrayDimensions))]
        public bool IsArray => ArrayDimensions != null && ArrayDimensions.Length == 1 && ArrayDimensions[0] > 0;

        private bool isObject;

        public bool IsObject { get => isObject || IsArray && (this is not UAVariableMember); set => isObject = value; }
        public bool AsEvents { get; set; }


        /// <summary>
        /// If this is an object, this is the matching timeseries
        /// </summary>
        public UAVariableMember? TimeSeries { get; private set; }

        public IEnumerable<UAVariable> CreateTimeseries()
        {
            if (IsArray)
            {
                return CreateArrayChildren();
            }
            else if (IsObject)
            {
                if (TimeSeries == null)
                {
                    TimeSeries = new UAVariableMember(this, -1);
                }
                return new[] { TimeSeries };
            }
            else
            {
                return new[] { this };
            }
        }

        /// <summary>
        /// Children if this represents the parent in an array
        /// </summary>
        public IEnumerable<UAVariable>? ArrayChildren { get; private set; }

        private IEnumerable<UAVariable> CreateArrayChildren()
        {
            if (!IsArray) return Enumerable.Empty<UAVariableMember>();
            if (ArrayChildren != null) return ArrayChildren;
            var children = new List<UAVariable>();
            for (int i = 0; i < ArrayDimensions[0]; i++)
            {
                children.Add(new UAVariableMember(this, i));
            }
            ArrayChildren = children;
            return children;
        }

        public override NodeId? TypeDefinition => FullAttributes.TypeDefinition?.Id;

        public struct VariableGroups
        {
            public bool IsSourceObject;
            public bool IsSourceVariable;
            public bool IsDestinationObject;
            public bool IsDestinationVariable;

            public bool Any()
            {
                return IsSourceObject || IsSourceVariable || IsDestinationObject || IsDestinationVariable;
            }
        }

        public bool AllowTSMap(
            ILogger log,
            DataTypeConfig config)
        {
            return FullAttributes.DataType.AllowTSMap(this, log, config);
        }

        public override bool AllowValueRead(ILogger logger, DataTypeConfig config, bool ignoreDimension)
        {
            return FullAttributes.Value == null
                   && FullAttributes.DataType.AllowValueRead(this, ArrayDimensions, ValueRank, logger, config, ignoreDimension);
        }

        public VariableGroups GetVariableGroups(ILogger log, DataTypeConfig config)
        {
            var allowTsMap = AllowTSMap(log, config);
            return new VariableGroups
            {
                // Source object if it's not a variable
                IsSourceObject = NodeClass != NodeClass.Variable,
                // Source variable if we wish to subscribe to it
                IsSourceVariable = allowTsMap && NodeClass == NodeClass.Variable,
                // Destination object if it's an object directly (through isObject)
                // it's a mapped array, or it's not a variable.
                IsDestinationObject = IsArray && allowTsMap || isObject,
                // Destination variable if allowTsMap is true and it's a variable.
                IsDestinationVariable = allowTsMap && NodeClass == NodeClass.Variable,
            };
        }

        public virtual (NodeId, int) DestinationId()
        {
            return (Id, -1);
        }

        public override Dictionary<string, string>? GetExtraMetadata(FullConfig config, SessionContext context, StringConverter converter)
        {
            Dictionary<string, string>? fields = null;
            if (config.Extraction.NodeTypes.Metadata && FullAttributes.TypeDefinition?.Name != null)
            {
                fields ??= new Dictionary<string, string>();
                fields["TypeDefinition"] = FullAttributes.TypeDefinition.Name;
            }

            var dt = FullAttributes.DataType;
            if (dt.EnumValues != null)
            {
                fields ??= new Dictionary<string, string>();
                foreach (var kvp in dt.EnumValues)
                {
                    fields[kvp.Key.ToString(CultureInfo.InvariantCulture)] = kvp.Value;
                }
            }
            if (config.Extraction.DataTypes.DataTypeMetadata)
            {
                fields ??= new Dictionary<string, string>();
                if (dt.Id.NamespaceIndex == 0)
                {
                    fields["dataType"] = DataTypes.GetBuiltInType(dt.Id).ToString();
                }
                else
                {
                    fields["dataType"] = dt.Name ?? dt.GetUniqueId(context) ?? "null";
                }
            }
            return fields;
        }

        public override int GetUpdateChecksum(TypeUpdateConfig update, bool dataTypeMetadata, bool nodeTypeMetadata)
        {
            int checksum = base.GetUpdateChecksum(update, dataTypeMetadata, nodeTypeMetadata);
            if (update.Metadata)
            {
                unchecked
                {
                    if (dataTypeMetadata)
                    {
                        checksum = checksum * 31 + FullAttributes.DataType.Id.GetHashCode();
                    }
                    if (NodeClass == NodeClass.VariableType)
                    {
                        checksum = checksum * 31 + FullAttributes.Value.GetHashCode();
                    }

                    if (nodeTypeMetadata)
                    {
                        checksum = checksum * 31 + (FullAttributes.TypeDefinition?.Id?.GetHashCode() ?? 0);
                    }
                }
            }

            return checksum;
        }

        public override void Format(StringBuilder builder, int indent, bool writeParent = true, bool writeProperties = true)
        {
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Variable: {1}", new string(' ', indent), Name);
            builder.AppendLine();
            base.Format(builder, indent + 4, writeParent);

            var indt = new string(' ', indent + 4);
            if (FullAttributes.DataType != null)
            {
                FullAttributes.DataType.Format(builder, indent + 4, false);
            }
            if (FullAttributes.ValueRank != ValueRanks.Scalar)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}ValueRank: {1}", indt, FullAttributes.ValueRank);
                builder.AppendLine();
            }
            if (FullAttributes.ArrayDimensions != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}ArrayDimensions: {1}", indt,
                    string.Join(", ", FullAttributes.ArrayDimensions));
                builder.AppendLine();
            }
            if (FullAttributes.TypeDefinition != null && writeParent)
            {
                FullAttributes.TypeDefinition.Format(builder, indent + 4, false, false);
            }
            if (AsEvents)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Written as events to destinations", indt);
                builder.AppendLine();
            }
            if (FullAttributes.Historizing)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Historizing: {1}", indt, FullAttributes.Historizing);
                builder.AppendLine();
            }
            if (Value != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Value: {1}", indt, Value.Value);
                builder.AppendLine();
            }
        }

        #region serialization
        /// <summary>
        /// Set special timeseries attributes from metadata, as given by <paramref name="metaMap"/>.
        /// </summary>
        /// <param name="metaMap">Configured mapping from property name to one of the special timeseries attributes:
        /// description, name, unit or parentId</param>
        /// <param name="writePoco">TimeSeries to write to</param>
        /// <param name="parentIdHandler">Method called for each string mapped to parentId, should set
        /// parentId as dictated by external context.</param>
        private void HandleMetaMap(
            Dictionary<string, string>? metaMap,
            TimeSeriesCreate writePoco,
            Action<string> parentIdHandler,
            StringConverter converter)
        {
            if (Properties == null || !Properties.Any() || metaMap == null || metaMap.Count == 0) return;
            foreach (var prop in Properties)
            {
                if (prop is not UAVariable propVar) continue;
                if (metaMap.TryGetValue(prop.Name ?? "", out var mapped))
                {
                    var value = converter.ConvertToString(propVar.Value, propVar.FullAttributes.DataType.EnumValues);
                    if (string.IsNullOrWhiteSpace(value)) continue;
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
        /// <summary>
        /// Create a stateless timeseries, setting the AssetExternalId property, from this variable.
        /// </summary>
        /// <param name="dataSetId">Optional dataSetId</param>
        /// <param name="metaMap">Configured mapping from property name to timeseries attribute</param>
        /// <returns>Stateless timeseries to create or null.</returns>
        public StatelessTimeSeriesCreate? ToStatelessTimeSeries(
            FullConfig config,
            IUAClientAccess client,
            long? dataSetId,
            Dictionary<string, string>? metaMap)
        {
            string? externalId = GetUniqueId(client.Context);
            var writePoco = new StatelessTimeSeriesCreate
            {
                Description = FullAttributes.Description,
                ExternalId = externalId,
                AssetExternalId = client.GetUniqueId(ParentId),
                Name = Name,
                LegacyName = externalId,
                IsString = FullAttributes.DataType.IsString,
                IsStep = FullAttributes.DataType.IsStep,
                DataSetId = dataSetId
            };
            writePoco.Metadata = BuildMetadata(config, client, true);

            HandleMetaMap(metaMap, writePoco, value => writePoco.AssetExternalId = value, client.StringConverter);

            return writePoco;
        }
        /// <summary>
        /// Create a timeseries object, setting assetId based from <paramref name="nodeToAssetIds"/>.
        /// </summary>
        /// <param name="extractor">Active extractor, used for metadata.</param>
        /// <param name="dataSetId">Optional dataSetId</param>
        /// <param name="nodeToAssetIds">Mapping from ids to assets, used for creating AssetId</param>
        /// <param name="metaMap">Configured mapping from property name to timeseries attribute</param>
        /// <param name="minimal">True to only add minimal metadata.</param>
        /// <returns>Timeseries to create or null</returns>
        public TimeSeriesCreate ToTimeseries(
            FullConfig config,
            IUAClientAccess client,
            UAExtractor extractor,
            long? dataSetId,
            IDictionary<NodeId, long>? nodeToAssetIds,
            Dictionary<string, string>? metaMap)
        {
            string? externalId = GetUniqueId(client.Context);
            var writePoco = new TimeSeriesCreate
            {
                Description = FullAttributes.Description,
                ExternalId = externalId,
                Name = Name,
                LegacyName = externalId,
                IsString = FullAttributes.DataType.IsString,
                IsStep = FullAttributes.DataType.IsStep,
                DataSetId = dataSetId
            };

            if (nodeToAssetIds != null && nodeToAssetIds.TryGetValue(ParentId, out long parent))
            {
                writePoco.AssetId = parent;
            }

            writePoco.Metadata = BuildMetadata(config, client, true);

            HandleMetaMap(metaMap, writePoco, value =>
            {
                var id = extractor.State.GetNodeId(value);
                if (id != null && nodeToAssetIds != null && nodeToAssetIds.TryGetValue(id, out long assetId))
                {
                    writePoco.AssetId = assetId;
                }
            }, extractor.StringConverter);

            return writePoco;
        }

        public TimeSeriesCreate ToMinimalTimeseries(IUAClientAccess client, long? dataSetId)
        {
            string? externalId = GetUniqueId(client.Context);

            return new TimeSeriesCreate
            {
                ExternalId = externalId,
                IsString = FullAttributes.DataType.IsString,
                IsStep = FullAttributes.DataType.IsStep,
                DataSetId = dataSetId
            };
        }
        #endregion
    }

    public class UAVariableMember : UAVariable
    {
        public UAVariable TSParent { get; }
        public int Index { get; } = -1;
        public override string? Name => Index >= 0 ? $"{base.Name}[{Index}]" : base.Name;
        public UAVariableMember(UAVariable parent, int index)
            : base(parent)
        {
            Index = index;
            TSParent = parent;
        }

        public override string? GetUniqueId(SessionContext context)
        {
            return context.GetUniqueId(Id, Index);
        }

        public override (NodeId, int) DestinationId()
        {
            return (Id, Index);
        }
    }
}

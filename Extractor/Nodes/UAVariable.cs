using Cognite.OpcUa.Config;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Serilog.Debugging;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;

namespace Cognite.OpcUa.Nodes
{
    public class VariableAttributes : BaseNodeAttributes
    {
        public bool Historizing { get; private set; }
        public int ValueRank { get; private set; }
        public UADataType DataType { get; private set; } = null!;
        public int[]? ArrayDimensions { get; set; }
        public byte AccessLevel { get; private set; }
        public Variant? Value { get; private set; }

        public UAVariableType? TypeDefinition { get; private set; }

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
            if (!config.Subscriptions.DataPoints) return false;

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

            base.LoadFromSavedNode(node, typeManager);
        }

        public void LoadFromNodeState(BaseVariableState state, TypeManager typeManager)
        {
            Historizing = state.Historizing;
            ValueRank = state.ValueRank;
            DataType = typeManager.GetDataType(state.DataType);
            ArrayDimensions = state.ArrayDimensions.Cast<int>().ToArray();
            AccessLevel = state.AccessLevel;
            Value = state.WrappedValue;
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
        }

        public bool AllowTSMap(
            ILogger log,
            DataTypeConfig config,
            int? arraySizeOverride = null,
            bool overrideString = false)
        {
            return FullAttributes.DataType.AllowTSMap(this, log, config, arraySizeOverride, overrideString);
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

        public override Dictionary<string, string>? GetExtraMetadata(FullConfig config, IUAClientAccess client)
        {
            Dictionary<string, string>? fields = null;
            if (config.Extraction.NodeTypes.Metadata && FullAttributes.TypeDefinition?.Attributes?.DisplayName != null)
            {
                fields ??= new Dictionary<string, string>();
                fields["TypeDefinition"] = FullAttributes.TypeDefinition.Attributes.DisplayName;
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
                    fields["dataType"] = dt.Attributes.DisplayName ?? dt.GetUniqueId(client) ?? "null";
                }
            }
            return fields;
        }
    }

    public class UAVariableMember : UAVariable
    {
        public UAVariable TSParent { get; }
        public int Index { get; } = -1;
        public UAVariableMember(UAVariable parent, int index)
            : base(parent)
        {
            Index = index;
            TSParent = parent;
        }

        public override string? GetUniqueId(IUAClientAccess client)
        {
            return client.GetUniqueId(Id, Index);
        }

        public override (NodeId, int) DestinationId()
        {
            return (Id, Index);
        }
    }
}

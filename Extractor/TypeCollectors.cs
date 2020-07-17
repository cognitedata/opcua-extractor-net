using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Represents a simplified OPC-UA datatype, containing information relevant to us (isString, isStep)
    /// </summary>
    public class BufferedDataType
    {
        public uint Identifier { get; set; }
        public bool IsStep { get; set; }
        public bool IsString { get; set; }
        public NodeId Raw { get; }
        public IEnumerable<EnumValueType> EnumValues { get; set; }
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

        public BufferedDataType(NodeId rawDataType, BufferedDataType other) : this(rawDataType)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            IsStep = other.IsStep;
            IsString = other.IsString;
            Raw = rawDataType;
        }

        public override string ToString()
        {
            return "DataType: {\n" +
                $"    NodeId: {Raw}\n" +
                $"    isStep: {IsStep}\n" +
                $"    isString: {IsString}\n" +
                "}";
        }
    }

    public class DataTypeManager
    {
        private readonly ILogger log = Log.Logger.ForContext<DataTypeManager>();
        readonly UAClient uaClient;
        readonly Dictionary<NodeId, NodeId> parentIds = new Dictionary<NodeId, NodeId>();
        readonly Dictionary<NodeId, BufferedDataType> dataTypes = new Dictionary<NodeId, BufferedDataType>();
        readonly HashSet<NodeId> ignoreDataTypes = new HashSet<NodeId>();
        private readonly DataTypeConfig config;
        private readonly BufferedDataType defaultDataType = new BufferedDataType(NodeId.Null) { IsString = true };

        public DataTypeManager(UAClient client, DataTypeConfig config)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (config == null) throw new ArgumentNullException(nameof(config));
            uaClient = client;
            this.config = config;
        }
        public void Reconfigure()
        {
            if (config.CustomNumericTypes != null)
            {
                foreach (var type in config.CustomNumericTypes)
                {
                    var id = type.NodeId.ToNodeId(uaClient);
                    if (id == null)
                    {
                        log.Warning("Invalid datatype nodeId: {ns}: {identifier}", type.NodeId.NamespaceUri, type.NodeId.NodeId);
                        continue;
                    }
                    dataTypes[id] = new BufferedDataType(type, id);
                    log.Information("Add custom datatype: {id}", id);
                }
            }
            if (config.IgnoreDataTypes != null)
            {
                foreach (var type in config.IgnoreDataTypes)
                {
                    var id = type.ToNodeId(uaClient);
                    if (id == null)
                    {
                        log.Warning("Invalid ignore datatype nodeId: {ns}: {identifier}", type.NamespaceUri, type.NodeId);
                        continue;
                    }
                    ignoreDataTypes.Add(id);
                }
            }
        }

        private IEnumerable<NodeId> GetAncestors(NodeId id)
        {
            var ids = new List<NodeId>();
            while (parentIds.TryGetValue(id, out var parent))
            {
                ids.Add(parent);
                id = parent;
            }
            return ids;
        }

        private BufferedDataType GetType(NodeId id)
        {
            if (id == null || id.IsNullNodeId)
            {
                return new BufferedDataType(NodeId.Null)
                {
                    IsString = !config.NullAsNumeric
                };
            }
            if (id == DataTypes.Number) return new BufferedDataType(id) { IsString = false };
            if (id == DataTypes.Boolean) return new BufferedDataType(id) { IsString = false, IsStep = true };
            if (id == DataTypes.Enumeration) return new BufferedDataType(id)
            {
                IsString = config.EnumsAsStrings,
                IsStep = !config.EnumsAsStrings,
                EnumValues = Enumerable.Empty<EnumValueType>()
            };

            var ancestors = GetAncestors(id);
            foreach (var parent in ancestors)
            {
                if (parent != DataTypes.BaseDataType && dataTypes.TryGetValue(parent, out var dt)) return dt;
            }
            return new BufferedDataType(id);
        }
        public BufferedDataType GetDataType(NodeId id)
        {
            return dataTypes.GetValueOrDefault(id) ?? defaultDataType;
        }

        /// <summary>
        /// Returns true if the timeseries may be mapped based on rules of array size and datatypes.
        /// </summary>
        /// <param name="node">Variable to be tested</param>
        /// <returns>True if variable may be mapped to a timeseries</returns>
        public bool AllowTSMap(BufferedVariable node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            BufferedDataType dt;
            if (!dataTypes.TryGetValue(node.DataTypeId, out dt))
            {
                log.Warning("Missing datatype: {dt}", node.DataTypeId);
                dt = new BufferedDataType(node.DataTypeId);
            }

            if (dt.IsString && !config.AllowStringVariables)
            {
                log.Debug("Skipping variable {id} due to string datatype and allow-string-variables being set to false", node.Id);
                return false;
            }
            if (ignoreDataTypes.Contains(dt.Raw))
            {
                log.Debug("Skipping variable {id} due to raw datatype {raw} being in list of ignored data types", node.Id, dt.Raw);
                return false;
            }
            if (node.ValueRank == ValueRanks.Scalar || config.UnknownAsScalar
                && (node.ValueRank == ValueRanks.ScalarOrOneDimension || node.ValueRank == ValueRanks.Any)) return true;

            if (node.ArrayDimensions != null && node.ArrayDimensions.Count == 1)
            {
                int length = node.ArrayDimensions.First();
                if (config.MaxArraySize < 0 || length > 0 && length <= config.MaxArraySize)
                {
                    return true;
                }
                else
                {
                    log.Debug("Skipping variable {id} due to non-scalar ValueRank {rank} and too large dimension {dim}",
                        node.Id, node.ValueRank, length);
                }
            }
            else if (node.ArrayDimensions == null)
            {
                log.Debug("Skipping variable {id} due to non-scalar ValueRank {rank} and null ArrayDimensions", node.Id, node.ValueRank);
            }
            else
            {
                log.Debug("Skipping variable {id} due to non-scalar ValueRank {rank} and too high dimensionality {dim}",
                    node.Id, node.ArrayDimensions.Count);
            }

            return false;
        }

        public Dictionary<string, string> GetAdditionalMetadata(BufferedVariable variable)
        {
            if (variable == null) return null;
            if (!dataTypes.TryGetValue(variable.DataTypeId, out var dt)) return null;
            Dictionary<string, string> ret = null;
            if (dt.EnumValues != null && !config.EnumsAsStrings)
            {
                ret = new Dictionary<string, string>();
                foreach (var val in dt.EnumValues)
                {
                    ret[val.Value.ToString(CultureInfo.InvariantCulture)] = val.DisplayName.Text;
                }
            }
            if (config.DataTypeMetadata)
            {
                ret = ret ?? new Dictionary<string, string>();
                ret["dataType"] = uaClient.GetUniqueId(dt.Raw);
            }
            return ret;
        }

        public void InvestigateDataTypes(IEnumerable<NodeId> types, CancellationToken token)
        {
            var typeSet = new HashSet<NodeId>(types.Where(type => !dataTypes.ContainsKey(type)));
            if (!typeSet.Any()) return;

            if (!config.AutoIdentifyTypes)
            {
                foreach (var type in typeSet)
                {
                    dataTypes[type] = new BufferedDataType(type);
                }
                return;
            }

            void Callback(ReferenceDescription child, NodeId parent)
            {
                var id = uaClient.ToNodeId(child.NodeId);
                parentIds[id] = parent;
                if (dataTypes.ContainsKey(id)) return;
                dataTypes[id] = GetType(id);
            }

            uaClient.BrowseDirectory(new List<NodeId> { DataTypeIds.BaseDataType },
                Callback,
                token,
                ReferenceTypeIds.HasSubtype,
                (uint)NodeClass.DataType,
                false);

            var enumTypesToGet = typeSet.Where(type => dataTypes[type].EnumValues != null).ToList();

            if (enumTypesToGet.Any())
            {
                var enumPropMap = new Dictionary<NodeId, NodeId>();
                var children = uaClient.GetNodeChildren(enumTypesToGet, ReferenceTypes.HierarchicalReferences, (uint)NodeClass.Variable, token);
                foreach (var id in enumTypesToGet)
                {
                    if (!children.TryGetValue(id, out var properties)) continue;
                    if (properties == null) continue;
                    foreach (var prop in properties)
                    {
                        if (prop.BrowseName.Name == "EnumStrings" || prop.BrowseName.Name == "EnumValues")
                        {
                            enumPropMap[id] = id;
                            break;
                        }
                    }
                }
                if (!enumPropMap.Any()) return;
                var values = uaClient.ReadRawValues(enumPropMap.Values, token);
                foreach (var kvp in enumPropMap)
                {
                    var type = dataTypes[kvp.Key];
                    var value = values[kvp.Value];
                    if (value.Value is LocalizedText[] strings)
                    {
                        type.EnumValues = strings.Select((str, i) => new EnumValueType
                        {
                            Value = i,
                            DisplayName = str
                        }).ToArray();
                    }
                    else if (value.Value is EnumValueType[] enumValues)
                    {
                        type.EnumValues = enumValues;
                    }
                }
            }
        }
    }


    /// <summary>
    /// Collects the fields of a given list of eventIds. It does this by mapping out the entire event type hierarchy,
    /// and collecting the fields of each node on the way.
    /// </summary>
    public class EventFieldCollector
    {
        readonly UAClient uaClient;
        readonly Dictionary<NodeId, IEnumerable<ReferenceDescription>> properties = new Dictionary<NodeId, IEnumerable<ReferenceDescription>>();
        readonly Dictionary<NodeId, IEnumerable<ReferenceDescription>> localProperties = new Dictionary<NodeId, IEnumerable<ReferenceDescription>>();
        readonly IEnumerable<NodeId> targetEventIds;
        /// <summary>
        /// Construct the collector.
        /// </summary>
        /// <param name="parent">UAClient to be used for browse calls.</param>
        /// <param name="targetEventIds">Target event ids</param>
        public EventFieldCollector(UAClient parent, IEnumerable<NodeId> targetEventIds)
        {
            uaClient = parent;
            this.targetEventIds = targetEventIds;
        }
        /// <summary>
        /// Main collection function. Calls BrowseDirectory on BaseEventType, waits for it to complete, which should populate properties and localProperties,
        /// then collects the resulting fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).
        /// </summary>
        /// <returns>The collected fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).</returns>
        public Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> GetEventIdFields(CancellationToken token)
        {
            properties[ObjectTypeIds.BaseEventType] = new List<ReferenceDescription>();
            localProperties[ObjectTypeIds.BaseEventType] = new List<ReferenceDescription>();

            uaClient.BrowseDirectory(new List<NodeId> { ObjectTypeIds.BaseEventType },
                EventTypeCallback, token, ReferenceTypeIds.HierarchicalReferences, (uint)NodeClass.ObjectType | (uint)NodeClass.Variable);
            var propVariables = new Dictionary<ExpandedNodeId, (NodeId, QualifiedName)>();
            foreach (var kvp in localProperties)
            {
                foreach (var description in kvp.Value)
                {
                    if (!propVariables.ContainsKey(description.NodeId))
                    {
                        propVariables[description.NodeId] = (kvp.Key, description.BrowseName);
                    }
                }
            }
            return targetEventIds
                .Where(id => properties.ContainsKey(id))
                .ToDictionary(id => id, id => properties[id]
                    .Where(desc => propVariables.ContainsKey(desc.NodeId))
                    .Select(desc => propVariables[desc.NodeId]));
        }
        /// <summary>
        /// HandleNode callback for the event type mapping.
        /// </summary>
        /// <param name="child">Type or property to be handled</param>
        /// <param name="parent">Parent type id</param>
        private void EventTypeCallback(ReferenceDescription child, NodeId parent)
        {
            var id = uaClient.ToNodeId(child.NodeId);
            if (child.NodeClass == NodeClass.ObjectType && !properties.ContainsKey(id))
            {
                var parentProperties = new List<ReferenceDescription>();
                if (properties.ContainsKey(parent))
                {
                    foreach (var prop in properties[parent])
                    {
                        parentProperties.Add(prop);
                    }
                }
                properties[id] = parentProperties;
                localProperties[id] = new List<ReferenceDescription>();
            }
            if (child.ReferenceTypeId == ReferenceTypeIds.HasProperty)
            {
                properties[parent] = properties[parent].Append(child);
                localProperties[parent] = localProperties[parent].Append(child);
            }
        }

    }
}

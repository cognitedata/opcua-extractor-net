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

using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
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
        public IDictionary<long, string> EnumValues { get; set; }
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
        public BufferedDataPoint ToDataPoint(UAExtractor extractor, object value, DateTime timestamp, string id)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            if (IsString)
            {
                if (EnumValues != null)
                {
                    try
                    {
                        var longVal = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                        if (EnumValues.TryGetValue(longVal, out string enumVal))
                        {
                            return new BufferedDataPoint(timestamp, id, enumVal);
                        }
                    }
                    catch
                    {
                    }
                }
                return new BufferedDataPoint(timestamp, id, extractor.ConvertToString(value));
            }
            return new BufferedDataPoint(timestamp, id, UAClient.ConvertToDouble(value));
        }

        /// <summary>
        /// Construct datatype from config object ProtoDateType and NodeId of datatype. Used when datatypes are being overriden.
        /// </summary>
        /// <param name="protoDataType">Overriding propoDataType</param>
        /// <param name="rawDataType">NodeId of the datatype to be transformed into a BufferedDataType</param>
        public BufferedDataType(ProtoDataType protoDataType, NodeId rawDataType, DataTypeConfig config) : this(rawDataType)
        {
            if (protoDataType == null) throw new ArgumentNullException(nameof(protoDataType));
            if (config == null) throw new ArgumentNullException(nameof(config));
            IsStep = protoDataType.IsStep;
            IsString = config.EnumsAsStrings && protoDataType.Enum;
            if (protoDataType.Enum)
            {
                EnumValues = new Dictionary<long, string>();
                IsStep = !config.EnumsAsStrings;
            }
        }

        public BufferedDataType(NodeId rawDataType, BufferedDataType other) : this(rawDataType)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            IsStep = other.IsStep;
            IsString = other.IsString;
            Raw = rawDataType;
            if (other.EnumValues != null) EnumValues = new Dictionary<long, string>();
        }

        public override string ToString()
        {
            return "DataType: {\n" +
                $"    NodeId: {Raw}\n" +
                $"    isStep: {IsStep}\n" +
                $"    isString: {IsString}\n" +
                (EnumValues != null ? 
                $"    EnumValues: {string.Concat(EnumValues)}\n"
                : "") +
                "}";
        }
    }

    public class DataTypeManager
    {
        private readonly ILogger log = Log.Logger.ForContext<DataTypeManager>();
        private readonly UAClient uaClient;
        private readonly Dictionary<NodeId, NodeId> parentIds = new Dictionary<NodeId, NodeId>();
        private readonly Dictionary<NodeId, BufferedDataType> dataTypes = new Dictionary<NodeId, BufferedDataType>();
        private readonly Dictionary<NodeId, string> customTypeNames = new Dictionary<NodeId, string>();
        private readonly HashSet<NodeId> ignoreDataTypes = new HashSet<NodeId>();
        private readonly DataTypeConfig config;

        public DataTypeManager(UAClient client, DataTypeConfig config)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (config == null) throw new ArgumentNullException(nameof(config));
            uaClient = client;
            this.config = config;
        }
        public void Configure()
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
                    dataTypes[id] = new BufferedDataType(type, id, config);
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
            yield return id;
            while (parentIds.TryGetValue(id, out var parent))
            {
                yield return parent;
                id = parent;
            }
        }

        private BufferedDataType CreateDataType(NodeId id)
        {
            if (id == null || id.IsNullNodeId)
            {
                return new BufferedDataType(NodeId.Null)
                {
                    IsString = !config.NullAsNumeric
                };
            }

            foreach (var parent in GetAncestors(id))
            {
                if (parent != DataTypes.BaseDataType && dataTypes.TryGetValue(parent, out var dt))
                    return new BufferedDataType(id, dt);

                if (parent == DataTypes.Number) return new BufferedDataType(id) { IsString = false };
                if (parent == DataTypes.Boolean) return new BufferedDataType(id) { IsString = false, IsStep = true };
                if (parent == DataTypes.Enumeration) return new BufferedDataType(id)
                {
                    IsString = config.EnumsAsStrings,
                    IsStep = !config.EnumsAsStrings,
                    EnumValues = new Dictionary<long, string>()
                };
            }
            return new BufferedDataType(id);
        }
        public BufferedDataType GetDataType(NodeId id)
        {
            if (id == null) id = NodeId.Null;
            if (dataTypes.TryGetValue(id, out var dt)) return dt;
            dt = CreateDataType(id);
            dataTypes[id] = dt;
            return dt;
        }

        /// <summary>
        /// Returns true if the timeseries may be mapped based on rules of array size and datatypes.
        /// </summary>
        /// <param name="node">Variable to be tested</param>
        /// <returns>True if variable may be mapped to a timeseries</returns>
        public bool AllowTSMap(BufferedVariable node, int? arraySizeOverride = null, bool overrideString = false)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (node.DataType == null)
            {
                log.Warning("Skipping variable {id} due to missing datatype", node.Id);
                return false;
            }
            var dt = node.DataType;

            if (dt.IsString && !config.AllowStringVariables && !overrideString)
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
                if (config.MaxArraySize < 0 || length > 0 && length <= (arraySizeOverride ?? config.MaxArraySize))
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
            if (variable == null || variable.DataType == null) return null;
            var dt = variable.DataType;
            Dictionary<string, string> ret = null;
            if (dt.EnumValues != null && !config.EnumsAsStrings)
            {
                ret = new Dictionary<string, string>();
                foreach (var val in dt.EnumValues)
                {
                    ret[val.Key.ToString(CultureInfo.InvariantCulture)] = val.Value;
                }
            }
            if (config.DataTypeMetadata)
            {
                ret ??= new Dictionary<string, string>();
                if (dt.Raw.NamespaceIndex == 0)
                {
                    ret["dataType"] = DataTypes.GetBuiltInType(dt.Raw).ToString();
                }
                else
                {
                    ret["dataType"] = customTypeNames.GetValueOrDefault(dt.Raw) ?? uaClient.GetUniqueId(dt.Raw);
                }
            }
            return ret;
        }
        public async Task GetDataTypeMetadataAsync(IEnumerable<NodeId> types, CancellationToken token)
        {
            var typeSet = new HashSet<NodeId>(types.Where(type =>
                dataTypes.TryGetValue(type, out var dt)
                && dt.EnumValues != null
                && !dt.EnumValues.Any()));
            if (!typeSet.Any()) return;

            log.Information("Get enum properties for {cnt} enum types", typeSet.Count);
            var enumPropMap = new Dictionary<NodeId, NodeId>();
            var children = await Task.Run(() => uaClient.GetNodeChildren(typeSet, ReferenceTypes.HierarchicalReferences, (uint)NodeClass.Variable, token));

            foreach (var id in typeSet)
            {
                if (!children.TryGetValue(id, out var properties)) continue;
                if (properties == null) continue;
                foreach (var prop in properties)
                {
                    if (prop.BrowseName.Name == "EnumStrings" || prop.BrowseName.Name == "EnumValues")
                    {
                        enumPropMap[id] = uaClient.ToNodeId(prop.NodeId);
                        break;
                    }
                }
            }
            if (!enumPropMap.Any()) return;

            var values = await Task.Run(() => uaClient.ReadRawValues(enumPropMap.Values, token));
            foreach (var kvp in enumPropMap)
            {
                var type = dataTypes[kvp.Key];
                var value = values[kvp.Value];
                if (value.Value is LocalizedText[] strings)
                {
                    for (int i = 0; i < strings.Length; i++)
                    {
                        type.EnumValues[i] = strings[i].Text;
                    }
                }
                else if (value.Value is EnumValueType[] enumValues)
                {
                    foreach (var val in enumValues)
                    {
                        type.EnumValues[val.Value] = val.DisplayName.Text;
                    }
                }
                else if (value.Value is ExtensionObject[] exts)
                {
                    foreach (var ext in exts)
                    {
                        if (ext.Body is EnumValueType val)
                        {
                            type.EnumValues[val.Value] = val.DisplayName.Text;
                        }
                    }
                }
                else
                {
                    log.Warning("Unknown enum strings type: {type}", value.Value.GetType());
                }
            }
        }

        public async Task GetDataTypeStructureAsync(CancellationToken token)
        {
            if (!config.AutoIdentifyTypes) return;

            void Callback(ReferenceDescription child, NodeId parent)
            {
                var id = uaClient.ToNodeId(child.NodeId);
                parentIds[id] = parent;
                customTypeNames[id] = child.DisplayName?.Text;
            }

            await Task.Run(() => uaClient.BrowseDirectory(new List<NodeId> { DataTypeIds.BaseDataType },
                Callback,
                token,
                ReferenceTypeIds.HasSubtype,
                (uint)NodeClass.DataType,
                false), CancellationToken.None);
        }
    }


    /// <summary>
    /// Collects the fields of events. It does this by mapping out the entire event type hierarchy,
    /// and collecting the fields of each node on the way.
    /// </summary>
    public class EventFieldCollector
    {
        private readonly UAClient uaClient;
        private readonly Dictionary<NodeId, BufferedEventType> types = new Dictionary<NodeId, BufferedEventType>();
        private readonly EventConfig config;
        private readonly Regex ignoreFilter;
        /// <summary>
        /// Construct the collector.
        /// </summary>
        /// <param name="parent">UAClient to be used for browse calls.</param>
        /// <param name="targetEventIds">Target event ids</param>
        public EventFieldCollector(UAClient parent, EventConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            uaClient = parent;
            this.config = config;
            if (!string.IsNullOrEmpty(config.ExcludeEventFilter))
            {
                ignoreFilter = new Regex(config.ExcludeEventFilter, RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.CultureInvariant);
            }
        }
        /// <summary>
        /// Main collection function. Calls BrowseDirectory on BaseEventType, waits for it to complete, which should populate properties and localProperties,
        /// then collects the resulting fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).
        /// </summary>
        /// <returns>The collected fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).</returns>
        public Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> GetEventIdFields(CancellationToken token)
        {
            types[ObjectTypeIds.BaseEventType] = new BufferedEventType
            {
                Id = ObjectTypeIds.BaseEventType,
                CollectedFields = new List<ReferenceDescription>(),
                Properties = new List<ReferenceDescription>(),
                DisplayName = "BaseEventType",
                ParentId = NodeId.Null
            };

            uaClient.BrowseDirectory(new List<NodeId> { ObjectTypeIds.BaseEventType },
                EventTypeCallback, token, ReferenceTypeIds.HierarchicalReferences, (uint)NodeClass.ObjectType | (uint)NodeClass.Variable);

            var result = new Dictionary<NodeId, List<(NodeId, QualifiedName)>>();

            var excludeProperties = new HashSet<string>(config.ExcludeProperties);
            var baseExcludeProperties = new HashSet<string>(config.BaseExcludeProperties);

            var propVariables = new Dictionary<NodeId, (NodeId, QualifiedName)>();
            // Find reverse mappings from properties to their parents, along with their browse name
            foreach (var type in types.Values)
            {
                foreach (var description in type.Properties)
                {

                    if (!propVariables.ContainsKey(uaClient.ToNodeId(description.NodeId)))
                    {
                        propVariables[uaClient.ToNodeId(description.NodeId)] = (type.Id, description.BrowseName);
                    }
                }
            }

            HashSet<NodeId> whitelist = null;
            if (config.EventIds != null && config.EventIds.Any())
            {
                whitelist = new HashSet<NodeId>(config.EventIds.Select(proto => proto.ToNodeId(uaClient, ObjectTypeIds.BaseEventType)));
            }
            // Add mappings to result
            foreach (var type in types.Values)
            {
                if (ignoreFilter != null && ignoreFilter.IsMatch(type.DisplayName.Text)) continue;
                if (!config.AllEvents && type.Id.NamespaceIndex == 0) continue;
                if (whitelist != null && whitelist.Any() && !whitelist.Contains(type.Id)) continue;
                result[type.Id] = new List<(NodeId, QualifiedName)>();
                foreach (var desc in type.CollectedFields)
                {
                    if (excludeProperties.Contains(desc.BrowseName.Name)
                        || baseExcludeProperties.Contains(desc.BrowseName.Name) && type.Id == ObjectTypeIds.BaseEventType) continue;
                    result[type.Id].Add(propVariables[uaClient.ToNodeId(desc.NodeId)]);
                }
            }

            return result.ToDictionary(kvp => kvp.Key, kvp => (IEnumerable<(NodeId, QualifiedName)>)kvp.Value);
        }
        /// <summary>
        /// HandleNode callback for the event type mapping.
        /// </summary>
        /// <param name="child">Type or property to be handled</param>
        /// <param name="parent">Parent type id</param>
        private void EventTypeCallback(ReferenceDescription child, NodeId parent)
        {
            var id = uaClient.ToNodeId(child.NodeId);
            var parentType = types.GetValueOrDefault(parent);

            if (child.NodeClass == NodeClass.ObjectType)
            {
                types[id] = new BufferedEventType
                {
                    Id = id,
                    ParentId = parent,
                    Properties = new List<ReferenceDescription>(),
                    CollectedFields = parentType?.CollectedFields?.ToList() ?? new List<ReferenceDescription>(),
                    DisplayName = child.DisplayName
                };
            }
            if (child.ReferenceTypeId == ReferenceTypeIds.HasProperty)
            {
                if (parentType == null) return;
                parentType.Properties.Add(child);
                parentType.CollectedFields.Add(child);
            }
        }
        private class BufferedEventType
        {
            public NodeId Id { get; set; }
            public LocalizedText DisplayName { get; set; }
            public NodeId ParentId { get; set; }
            public List<ReferenceDescription> Properties { get; set; }
            public List<ReferenceDescription> CollectedFields { get; set; }
        }
    }



    public class BufferedReferenceType
    {
        public NodeId Id { get; }
        private string name;
        private string inverseName;
        public bool HasName => name != null;
        public BufferedReferenceType(NodeId id)
        {
            Id = id;
        }
        public void SetNames(string name, string inverseName)
        {
            this.name = name;
            this.inverseName = inverseName;
        }
        public string GetName(bool isInverse)
        {
            if (isInverse && !string.IsNullOrEmpty(inverseName)) return inverseName;
            return name;
        }
    }
    /// <summary>
    /// Maps out reference types, in order to find their names and inverse names
    /// </summary>
    public class ReferenceTypeManager
    {
        private readonly UAClient uaClient;
        private readonly UAExtractor extractor;
        private readonly Dictionary<NodeId, BufferedReferenceType> mappedTypes = new Dictionary<NodeId, BufferedReferenceType>();
        public ReferenceTypeManager(UAClient client, UAExtractor extractor)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            uaClient = client;
            this.extractor = extractor;
        }

        public BufferedReferenceType GetReferenceType(NodeId id)
        {
            if (id == null) id = NodeId.Null;
            if (mappedTypes.TryGetValue(id, out var type)) return type;
            type = new BufferedReferenceType(id);
            mappedTypes[id] = type;
            return type;
        }

        public async Task GetReferenceTypeDataAsync(CancellationToken token)
        {
            var toRead = mappedTypes.Values.Where(type => !type.HasName && !type.Id.IsNullNodeId).ToList();
            if (!toRead.Any()) return;

            var readValueIds = toRead.SelectMany(type => new[] {
                new ReadValueId { AttributeId = Attributes.DisplayName, NodeId = type.Id },
                new ReadValueId { AttributeId = Attributes.InverseName, NodeId = type.Id }
            });

            var values = await Task.Run(() => uaClient.ReadAttributes(new ReadValueIdCollection(readValueIds), toRead.Count, token), token);

            for (int i = 0; i < toRead.Count; i++)
            {
                var type = toRead[i];
                type.SetNames(
                    (values[i * 2].Value as LocalizedText)?.Text,
                    (values[i * 2 + 1].Value as LocalizedText)?.Text
                );
            }
        }

        public async Task<IEnumerable<BufferedReference>> GetReferencesAsync(IEnumerable<NodeId> nodes, NodeId referenceTypes, CancellationToken token)
        {
            if (!nodes.Any()) return Array.Empty<BufferedReference>();

            // We only care about references to objects or variables, at least for now.
            // Only references between objects represented in the extracted hierarchy are relevant.
            var references = await Task.Run(() => uaClient.GetNodeChildren(
                nodes,
                referenceTypes,
                (uint)NodeClass.Object | (uint)NodeClass.Variable,
                token,
                BrowseDirection.Both));

            var results = new List<BufferedReference>();

            foreach (var (parentId, children) in references)
            {
                var parentNode = extractor.State.GetActiveNode(parentId);
                if (parentNode == null) continue;
                foreach (var child in children)
                {
                    var childNode = extractor.State.GetActiveNode(uaClient.ToNodeId(child.NodeId));
                    if (childNode == null) continue;
                    results.Add(new BufferedReference(child, parentNode, childNode, this));
                }
            }

            return results;
        }
    }
}

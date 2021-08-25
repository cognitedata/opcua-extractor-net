/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

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

using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.TypeCollectors
{
    /// <summary>
    /// Maps out the data type hierarchy to differentiate between numeric and string datatypes,
    /// and fetch extra information used both for metadata, and for alternative ways to convert the
    /// raw datavalues to destination datapoints.
    /// </summary>
    public class DataTypeManager
    {
        private readonly ILogger log = Log.Logger.ForContext<DataTypeManager>();
        private readonly UAClient uaClient;
        private readonly Dictionary<NodeId, NodeId> parentIds = new Dictionary<NodeId, NodeId>();
        private readonly Dictionary<NodeId, UADataType> dataTypes = new Dictionary<NodeId, UADataType>();
        private readonly Dictionary<NodeId, string> customTypeNames = new Dictionary<NodeId, string>();
        private readonly HashSet<NodeId> ignoreDataTypes = new HashSet<NodeId>();
        private readonly DataTypeConfig config;

        public DataTypeManager(UAClient client, DataTypeConfig config)
        {
            uaClient = client;
            this.config = config;
        }
        /// <summary>
        /// Configure the manager from the config object.
        /// </summary>
        public void Configure()
        {
            if (config.CustomNumericTypes != null)
            {
                foreach (var type in config.CustomNumericTypes)
                {
                    if (type.NodeId == null) continue;
                    var id = type.NodeId.ToNodeId(uaClient);
                    if (id == null || id.IsNullNodeId)
                    {
                        log.Warning("Invalid datatype nodeId: {ns}: {identifier}", type.NodeId.NamespaceUri, type.NodeId.NodeId);
                        continue;
                    }
                    dataTypes[id] = new UADataType(type, id, config);
                    log.Information("Add custom datatype: {id}", id);
                }
            }
            if (config.IgnoreDataTypes != null)
            {
                foreach (var type in config.IgnoreDataTypes)
                {
                    var id = type.ToNodeId(uaClient);
                    if (id == null || id.IsNullNodeId)
                    {
                        log.Warning("Invalid ignore datatype nodeId: {ns}: {identifier}", type.NamespaceUri, type.NodeId);
                        continue;
                    }
                    ignoreDataTypes.Add(id);
                }
            }
        }
        /// <summary>
        /// Return a list of NodeIds for each of the ancestors of <paramref name="id"/>
        /// in reverse order, starting with the Id itself.
        /// Requires the hierarchy to be populated.
        /// </summary>
        /// <param name="id">NodeId to find ancestors for</param>
        /// <returns></returns>
        private IEnumerable<NodeId> GetAncestors(NodeId id)
        {
            yield return id;
            while (parentIds.TryGetValue(id, out var parent))
            {
                yield return parent;
                id = parent;
            }
        }
        /// <summary>
        /// Create a new datatype from <paramref name="id"/>, using built in types and knowledge of the hierarchy
        /// to configure it.
        /// </summary>
        /// <param name="id">Id to create datatype for</param>
        /// <returns>UADataType for <paramref name="id"/></returns>
        private UADataType CreateDataType(NodeId id)
        {
            if (id.IsNullNodeId)
            {
                return new UADataType(NodeId.Null)
                {
                    IsString = !config.NullAsNumeric
                };
            }

            foreach (var parent in GetAncestors(id))
            {
                if (parent != DataTypeIds.BaseDataType && dataTypes.TryGetValue(parent, out var dt))
                    return new UADataType(id, dt);

                if (parent == DataTypeIds.Number) return new UADataType(id) { IsString = false };
                if (parent == DataTypeIds.Boolean) return new UADataType(id) { IsString = false, IsStep = true };
                if (parent == DataTypeIds.Enumeration) return new UADataType(id)
                {
                    IsString = config.EnumsAsStrings,
                    IsStep = !config.EnumsAsStrings,
                    EnumValues = new Dictionary<long, string>()
                };
            }
            return new UADataType(id);
        }
        /// <summary>
        /// Get or create a <see cref="UADataType"/> from <paramref name="id"/>.
        /// </summary>
        /// <param name="id">Id to create or retrieve a datatype for.</param>
        /// <returns>UADataType for <paramref name="id"/></returns>
        public UADataType GetDataType(NodeId? id)
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
        public bool AllowTSMap(UAVariable node, int? arraySizeOverride = null, bool overrideString = false)
        {
            // We don't care about the data type of variable types except for as metadata.
            if (node.NodeClass == NodeClass.VariableType) return true;
            if (node.DataType == null)
            {
                log.Warning("Skipping variable {name} {id} due to missing datatype", node.DisplayName, node.Id);
                return false;
            }
            var dt = node.DataType;

            if (dt.IsString && !config.AllowStringVariables && !overrideString)
            {
                log.Debug("Skipping variable {name} {id} due to string datatype and allow-string-variables being set to false",
                    node.DisplayName, node.Id);
                return false;
            }
            if (ignoreDataTypes.Contains(dt.Raw))
            {
                log.Debug("Skipping variable {name} {id} due to raw datatype {raw} being in list of ignored data types",
                    node.DisplayName, node.Id, dt.Raw);
                return false;
            }
            if (node.ValueRank == ValueRanks.Scalar) return true;

            if (node.ArrayDimensions != null && node.ArrayDimensions.Length == 1)
            {
                int length = node.ArrayDimensions.First();
                int maxArraySize = arraySizeOverride.HasValue ? Math.Max(arraySizeOverride.Value, config.MaxArraySize) : config.MaxArraySize;
                if (config.MaxArraySize < 0 || length > 0 && length <= maxArraySize)
                {
                    return true;
                }
                else
                {
                    log.Debug("Skipping variable {name} {id} due to non-scalar ValueRank {rank} and too large dimension {dim}",
                        node.DisplayName, node.Id, node.ValueRank, length);
                    return false;
                }
            }
            else if (node.ArrayDimensions == null)
            {
                if (config.UnknownAsScalar && (node.ValueRank == ValueRanks.ScalarOrOneDimension
                    || node.ValueRank == ValueRanks.Any)) return true;
                log.Debug("Skipping variable {name} {id} due to non-scalar ValueRank {rank} and null ArrayDimensions",
                    node.DisplayName, node.Id, node.ValueRank);
                return false;
            }
            else
            {
                log.Debug("Skipping variable {name} {id} due to non-scalar ValueRank {rank} and too high dimensionality {dim}",
                    node.DisplayName, node.Id, node.ValueRank, node.ArrayDimensions.Length);
                return false;
            }
        }

        /// <summary>
        /// Build a dictionary of extra metadata for the datatype of the given variable.
        /// </summary>
        /// <param name="variable">Variable to get metadata for</param>
        /// <returns>Dictionary containing datatype-related metadata for the given variable.</returns>
        public Dictionary<string, string>? GetAdditionalMetadata(UAVariable variable)
        {
            if (variable == null || variable.DataType == null) return null;
            var dt = variable.DataType;
            Dictionary<string, string>? ret = null;
            if (dt.EnumValues != null)
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
                    ret["dataType"] = customTypeNames.GetValueOrDefault(dt.Raw) ?? uaClient.GetUniqueId(dt.Raw) ?? "null";
                }
            }
            return ret;
        }
        /// <summary>
        /// Retrieve metadata for the given list of datatypes.
        /// </summary>
        /// <param name="types">Ids of types to retrieve metadata for</param>
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

            var values = await Task.Run(() => uaClient.ReadRawValues(enumPropMap.Values.Distinct(), token));
            foreach (var kvp in enumPropMap)
            {
                SetEnumStrings(kvp.Key, values[kvp.Value].Value);
            }
        }
        /// <summary>
        /// Map out the datatype hierarchy and build the tree internally,
        /// necessary for automatic identification of datatypes later.
        /// </summary>
        public async Task GetDataTypeStructureAsync(CancellationToken token)
        {
            if (!config.AutoIdentifyTypes) return;

            log.Information("Map out datatype structure to automatically identify numeric datatypes");

            void Callback(ReferenceDescription child, NodeId parent)
            {
                var id = uaClient.ToNodeId(child.NodeId);
                parentIds[id] = parent;
                if (id.NamespaceIndex != 0)
                {
                    customTypeNames[id] = child.DisplayName.Text;
                }
            }

            await Task.Run(() => uaClient.BrowseDirectory(new List<NodeId> { DataTypeIds.BaseDataType },
                Callback,
                token,
                ReferenceTypeIds.HasSubtype,
                (uint)NodeClass.DataType,
                false,
                false), CancellationToken.None);
        }
        /// <summary>
        /// Manually register a type in the manager, used when type hierarchy is obtained from file
        /// </summary>
        /// <param name="id">NodeId of type</param>
        /// <param name="parent">NodeId of supertype, if present</param>
        /// <param name="name">Name of node</param>
        public void RegisterType(NodeId id, NodeId parent, string name)
        {
            parentIds[id] = parent;
            if (id.NamespaceIndex != 0)
            {
                customTypeNames[id] = name;
            }
            if (!dataTypes.ContainsKey(id))
            {
                GetDataType(id);
            }
        }
        /// <summary>
        /// Set the enumStrings property on the datatype given by <paramref name="node"/>
        /// </summary>
        /// <param name="node"></param>
        /// <param name="value"></param>
        public void SetEnumStrings(NodeId node, object value)
        {
            if (value is Variant variant) SetEnumStrings(node, variant.Value);
            var type = dataTypes[node];

            if (value is LocalizedText[] strings)
            {
                for (int i = 0; i < strings.Length; i++)
                {
                    type.EnumValues![i] = strings[i].Text;
                }
            }
            else if (value is ExtensionObject[] exts)
            {
                foreach (var ext in exts)
                {
                    if (ext.Body is EnumValueType val)
                    {
                        type.EnumValues![val.Value] = val.DisplayName.Text;
                    }
                }
            }
            else
            {
                log.Warning("Unknown enum strings type: {type}", value.GetType());
            }
        }
        /// <summary>
        /// Reset the manager, GetDataTypeStructure and GetDataTypeMetadata will need to be re-run.
        /// </summary>
        public void Reset()
        {
            parentIds.Clear();
            dataTypes.Clear();
            ignoreDataTypes.Clear();
            Configure();
        }
    }
}

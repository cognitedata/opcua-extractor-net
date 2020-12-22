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
                    dataTypes[id] = new UADataType(type, id, config);
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

        private UADataType CreateDataType(NodeId id)
        {
            if (id == null || id.IsNullNodeId)
            {
                return new UADataType(NodeId.Null)
                {
                    IsString = !config.NullAsNumeric
                };
            }

            foreach (var parent in GetAncestors(id))
            {
                if (parent != DataTypes.BaseDataType && dataTypes.TryGetValue(parent, out var dt))
                    return new UADataType(id, dt);

                if (parent == DataTypes.Number) return new UADataType(id) { IsString = false };
                if (parent == DataTypes.Boolean) return new UADataType(id) { IsString = false, IsStep = true };
                if (parent == DataTypes.Enumeration) return new UADataType(id)
                {
                    IsString = config.EnumsAsStrings,
                    IsStep = !config.EnumsAsStrings,
                    EnumValues = new Dictionary<long, string>()
                };
            }
            return new UADataType(id);
        }
        public UADataType GetDataType(NodeId id)
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

        public Dictionary<string, string> GetAdditionalMetadata(UAVariable variable)
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
}

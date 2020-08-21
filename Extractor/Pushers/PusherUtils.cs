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

using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Cognite.OpcUa.Pushers
{
    public static class PusherUtils
    {
        public static readonly DateTime CogniteMinTime = CogniteTime.FromUnixTimeMilliseconds(CogniteUtils.TimestampMin);
        public static readonly DateTime CogniteMaxTime = CogniteTime.FromUnixTimeMilliseconds(CogniteUtils.TimestampMax);

        /// <summary>
        /// Get the value of given object assumed to be a timestamp as the number of milliseconds since 1/1/1970
        /// </summary>
        /// <param name="value">Value of the object. Assumed to be a timestamp or numeric value</param>
        /// <returns>Milliseconds since epoch</returns>
        private static long GetTimestampValue(object value)
        {
            if (value is DateTime dt)
            {
                return new DateTimeOffset(dt).ToUnixTimeMilliseconds();
            }
            else
            {
                return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            }
        }
        public static Dictionary<string, string> PropertiesToMetadata(
            IEnumerable<BufferedVariable> properties,
            Dictionary<string, string> extras = null)
        {
            if (properties == null && extras == null) return new Dictionary<string, string>();

            var raw = new List<KeyValuePair<string, string>>();
            if (extras != null) raw.AddRange(extras);
            if (properties != null)
            {
                foreach (var prop in properties)
                {
                    if (prop != null && !string.IsNullOrEmpty(prop.DisplayName))
                    {
                        raw.Add(new KeyValuePair<string, string>(
                            ExtractorUtils.LimitUtf8ByteCount(prop.DisplayName, 128), ExtractorUtils.LimitUtf8ByteCount(prop.Value?.StringValue, 256)
                        ));

                        // Handles one layer of nested properties. This only happens if variables that have their own properties are mapped
                        // to properties.
                        if (prop.Properties != null)
                        {
                            raw.AddRange(prop.Properties
                                .Where(prop => prop != null && !string.IsNullOrEmpty(prop.DisplayName))
                                .Select(nestedProp => new KeyValuePair<string, string>(
                                    ExtractorUtils.LimitUtf8ByteCount($"{prop.DisplayName}_{nestedProp.DisplayName}", 128),
                                    ExtractorUtils.LimitUtf8ByteCount(nestedProp.Value?.StringValue, 256)))
                            );
                        }

                    }
                }

            }
            int count = 0;
            int byteCount = 0;
            raw = raw.TakeWhile(pair =>
            {
                count++;
                if (pair.Key != null) byteCount += Encoding.UTF8.GetByteCount(pair.Key);
                if (pair.Value != null) byteCount += Encoding.UTF8.GetByteCount(pair.Value);
                return count <= 256 && byteCount <= 10240;
            }).ToList();

            return raw.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        /// <summary>
        /// Converts BufferedNode into asset write poco.
        /// </summary>
        /// <param name="node">Node to be converted</param>
        /// <returns>Full asset write poco</returns>
        public static AssetCreate NodeToAsset(BufferedNode node, UAExtractor extractor, long? dataSetId, Dictionary<string, string> metaMap)
        {
            if (extractor == null || node == null) return null;
            var writePoco = new AssetCreate
            {
                Description = ExtractorUtils.Truncate(node.Description, 500),
                ExternalId = extractor.GetUniqueId(node.Id),
                Name = string.IsNullOrEmpty(node.DisplayName)
                    ? ExtractorUtils.Truncate(extractor.GetUniqueId(node.Id), 140) : ExtractorUtils.Truncate(node.DisplayName, 140),
                DataSetId = dataSetId
            };

            if (node.ParentId != null && !node.ParentId.IsNullNodeId)
            {
                writePoco.ParentExternalId = extractor.GetUniqueId(node.ParentId);
            }

            var extras = node is BufferedVariable variable ? extractor.DataTypeManager.GetAdditionalMetadata(variable) : null;
            writePoco.Metadata = PropertiesToMetadata(node.Properties, extras);
            if (node.Properties != null && node.Properties.Any() && (metaMap?.Any() ?? false))
            {
                foreach (var prop in node.Properties)
                {
                    if (!string.IsNullOrWhiteSpace(prop.Value?.StringValue) && metaMap.TryGetValue(prop.DisplayName, out var mapped))
                    {
                        var value = prop.Value.StringValue;
                        switch (mapped)
                        {
                            case "description": writePoco.Description = value; break;
                            case "name": writePoco.Name = value; break;
                            case "parentId": writePoco.ParentExternalId = value; break;
                        }
                    }
                }
            }
            
            return writePoco;
        }
        private static readonly HashSet<string> excludeMetaData = new HashSet<string> {
            "StartTime", "EndTime", "Type", "SubType"
        };
        /// <summary>
        /// Transform BufferedEvent into EventEntity to be sent to CDF.
        /// </summary>
        /// <param name="evt">Event to be transformed.</param>
        /// <returns>Final EventEntity object</returns>
        public static StatelessEventCreate EventToStatelessCDFEvent(BufferedEvent evt, UAExtractor extractor, long? dataSetId)
        {
            if (evt == null || extractor == null) return null;

            var parent = evt.SourceNode == null || evt.SourceNode.IsNullNodeId ? null : extractor.State.GetActiveNode(evt.SourceNode);
            var entity = new StatelessEventCreate
            {
                Description = ExtractorUtils.Truncate(evt.Message, 500),
                StartTime = evt.MetaData.TryGetValue("StartTime", out var rawStartTime)
                    ? GetTimestampValue(rawStartTime)
                    : evt.Time.ToUnixTimeMilliseconds(),
                EndTime = evt.MetaData.TryGetValue("EndTime", out var rawEndTime)
                    ? GetTimestampValue(rawEndTime)
                    : evt.Time.ToUnixTimeMilliseconds(),
                AssetExternalIds = parent == null
                    ? (IEnumerable<string>)Array.Empty<string>()
                    : new List<string> { extractor.GetUniqueId(parent.IsVariable ? parent.ParentId : parent.Id) },
                ExternalId = ExtractorUtils.Truncate(evt.EventId, 255),
                Type = ExtractorUtils.Truncate(evt.MetaData.TryGetValue("Type", out var rawType)
                    ? extractor.ConvertToString(rawType)
                    : extractor.GetUniqueId(evt.EventType), 64),
                DataSetId = dataSetId
            };
            var finalMetaData = new Dictionary<string, string>();
            int len = 1;
            finalMetaData["Emitter"] = extractor.GetUniqueId(evt.EmittingNode);
            if (!evt.MetaData.ContainsKey("SourceNode") && evt.SourceNode != null && !evt.SourceNode.IsNullNodeId)
            {
                finalMetaData["SourceNode"] = extractor.GetUniqueId(evt.SourceNode);
                len++;
            }
            if (evt.MetaData.ContainsKey("SubType"))
            {
                entity.Subtype = ExtractorUtils.Truncate(extractor.ConvertToString(evt.MetaData["SubType"]), 64);
            }

            foreach (var dt in evt.MetaData)
            {
                if (!excludeMetaData.Contains(dt.Key))
                {
                    finalMetaData[ExtractorUtils.Truncate(dt.Key, 32)] =
                        ExtractorUtils.Truncate(extractor.ConvertToString(dt.Value), 256);
                }

                if (len++ == 15) break;
            }

            if (finalMetaData.Any())
            {
                entity.Metadata = finalMetaData;
            }
            return entity;
        }

        /// <summary>
        /// Transform BufferedEvent into EventEntity to be sent to CDF.
        /// </summary>
        /// <param name="evt">Event to be transformed.</param>
        /// <returns>Final EventEntity object</returns>
        public static EventCreate EventToCDFEvent(BufferedEvent evt, UAExtractor extractor, long? dataSetId,
            IDictionary<NodeId, long> nodeToAssetIds)
        {
            if (evt == null || extractor == null) return null;
            EventCreate entity;
            entity = new EventCreate
            {
                Description = ExtractorUtils.Truncate(evt.Message, 500),
                StartTime = evt.MetaData.TryGetValue("StartTime", out var rawStartTime)
                    ? GetTimestampValue(rawStartTime)
                    : evt.Time.ToUnixTimeMilliseconds(),
                EndTime = evt.MetaData.TryGetValue("EndTime", out var rawEndTime)
                    ? GetTimestampValue(rawEndTime)
                    : evt.Time.ToUnixTimeMilliseconds(),
                ExternalId = ExtractorUtils.Truncate(evt.EventId, 255),
                Type = ExtractorUtils.Truncate(evt.MetaData.TryGetValue("Type", out var rawType)
                    ? extractor.ConvertToString(rawType)
                    : extractor.GetUniqueId(evt.EventType), 64),
                DataSetId = dataSetId
            };

            if (nodeToAssetIds != null && evt.SourceNode != null && !evt.SourceNode.IsNullNodeId
                && nodeToAssetIds.TryGetValue(evt.SourceNode, out var assetId)) {
                entity.AssetIds = new List<long> { assetId };
            }

            var finalMetaData = new Dictionary<string, string>();
            int len = 1;
            finalMetaData["Emitter"] = extractor.GetUniqueId(evt.EmittingNode);
            if (!evt.MetaData.ContainsKey("SourceNode") && evt.SourceNode != null && !evt.SourceNode.IsNullNodeId)
            {
                finalMetaData["SourceNode"] = extractor.GetUniqueId(evt.SourceNode);
                len++;
            }
            if (evt.MetaData.TryGetValue("SubType", out var rawSubType))
            {
                entity.Subtype = ExtractorUtils.Truncate(extractor.ConvertToString(rawSubType), 64);
            }

            foreach (var dt in evt.MetaData)
            {
                if (!excludeMetaData.Contains(dt.Key))
                {
                    finalMetaData[ExtractorUtils.Truncate(dt.Key, 32)] =
                        ExtractorUtils.Truncate(extractor.ConvertToString(dt.Value), 256);
                }

                if (len++ == 15) break;
            }

            if (finalMetaData.Any())
            {
                entity.Metadata = finalMetaData;
            }
            return entity;
        }
        /// <summary>
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="variable">Variable to be converted</param>
        /// <returns>Complete timeseries write poco</returns>
        public static StatelessTimeSeriesCreate VariableToStatelessTimeSeries(BufferedVariable variable,
            UAExtractor extractor, long? dataSetId, Dictionary<string, string> metaMap)
        {
            if (variable == null || extractor == null) return null;
            string externalId = extractor.GetUniqueId(variable.Id, variable.Index);
            var writePoco = new StatelessTimeSeriesCreate
            {
                Description = ExtractorUtils.Truncate(variable.Description, 1000),
                ExternalId = externalId,
                AssetExternalId = extractor.GetUniqueId(variable.ParentId),
                Name = ExtractorUtils.Truncate(variable.DisplayName, 255),
                LegacyName = externalId,
                IsString = variable.DataType.IsString,
                IsStep = variable.DataType.IsStep,
                DataSetId = dataSetId
            };

            var extra = extractor.DataTypeManager.GetAdditionalMetadata(variable);
            writePoco.Metadata = PropertiesToMetadata(variable.Properties, extra);
            if (variable.Properties != null && variable.Properties.Any() && (metaMap?.Any() ?? false))
            {
                foreach (var prop in variable.Properties)
                {
                    if (!string.IsNullOrWhiteSpace(prop.Value?.StringValue) && metaMap.TryGetValue(prop.DisplayName, out var mapped))
                    {
                        var value = prop.Value.StringValue;
                        switch (mapped)
                        {
                            case "description": writePoco.Description = value; break;
                            case "name": writePoco.Name = value; break;
                            case "unit": writePoco.Unit = value; break;
                            case "parentId": writePoco.AssetExternalId = value; break;
                        }
                    }
                }
            }
            return writePoco;
        }
        /// <summary>
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="variable">Variable to be converted</param>
        /// <returns>Complete timeseries write poco</returns>
        public static TimeSeriesCreate VariableToTimeseries(BufferedVariable variable, UAExtractor extractor, long? dataSetId,
            IDictionary<NodeId, long> nodeToAssetIds, Dictionary<string, string> metaMap, bool minimal = false)
        {
            if (variable == null
                || extractor == null) return null;

            string externalId = extractor.GetUniqueId(variable.Id, variable.Index);

            if (minimal)
            {
                return new TimeSeriesCreate
                {
                    ExternalId = externalId,
                    IsString = variable.DataType.IsString,
                    IsStep = variable.DataType.IsStep,
                    DataSetId = dataSetId
                };
            }

            var writePoco = new TimeSeriesCreate
            {
                Description = ExtractorUtils.Truncate(variable.Description, 1000),
                ExternalId = externalId,
                Name = ExtractorUtils.Truncate(variable.DisplayName, 255),
                LegacyName = externalId,
                IsString = variable.DataType.IsString,
                IsStep = variable.DataType.IsStep,
                DataSetId = dataSetId
            };

            if (nodeToAssetIds != null && nodeToAssetIds.TryGetValue(variable.ParentId, out long parent))
            {
                writePoco.AssetId = parent;
            }

            var extra = extractor.DataTypeManager.GetAdditionalMetadata(variable);
            writePoco.Metadata = PropertiesToMetadata(variable.Properties, extra);

            if (variable.Properties != null && variable.Properties.Any() && (metaMap?.Any() ?? false))
            {
                foreach (var prop in variable.Properties)
                {
                    if (!string.IsNullOrWhiteSpace(prop.Value?.StringValue) && metaMap.TryGetValue(prop.DisplayName, out var mapped))
                    {
                        var value = prop.Value.StringValue;
                        switch (mapped)
                        {
                            case "description": writePoco.Description = value; break;
                            case "name": writePoco.Name = value; break;
                            case "unit": writePoco.Unit = value; break;
                            case "parentId":
                                var id = extractor.State.GetNodeId(value);
                                if (id != null && nodeToAssetIds != null && nodeToAssetIds.TryGetValue(id, out long assetId))
                                {
                                    writePoco.AssetId = assetId;
                                }
                                break;
                        }
                    }
                }
            }
            return writePoco;
        }

        private static void UpdateIfModified(Dictionary<string, object> ret, RawRow raw, string newValue, string key)
        {
            if (raw.Columns.TryGetValue(key, out var column))
            {
                string oldValue = null;
                try
                {
                    oldValue = column.GetString();
                }
                catch (JsonException) { }
                if (string.IsNullOrWhiteSpace(oldValue) || !string.IsNullOrWhiteSpace(newValue) && newValue != oldValue)
                {
                    ret[key] = newValue;
                }
            }
            else
            {
                ret[key] = newValue;
            }
        }
        private static JsonElement? CreateRawUpdateCommon(
            UAExtractor extractor,
            BufferedNode node,
            RawRow raw,
            TypeUpdateConfig update,
            Dictionary<string, object> ret)
        {
            if (update.Description)
            {
                string newDescription = ExtractorUtils.Truncate(node.Description, 1000);
                UpdateIfModified(ret, raw, newDescription, "description");
            }

            if (update.Name)
            {
                string newName = ExtractorUtils.Truncate(node.DisplayName, 255);
                UpdateIfModified(ret, raw, newName, "name");
            }

            if (update.Metadata)
            {
                var extra = node is BufferedVariable variable ? extractor.DataTypeManager.GetAdditionalMetadata(variable) : null;
                var newMetaData = PropertiesToMetadata(node.Properties, extra);
                if (raw.Columns.TryGetValue("metadata", out var rawMetaData))
                {
                    Dictionary<string, string> oldMetaData = null;
                    try
                    {
                        oldMetaData = JsonSerializer.Deserialize<Dictionary<string, string>>(rawMetaData.ToString());
                    }
                    catch (JsonException) { }
                    if (oldMetaData == null || newMetaData != null && newMetaData.Any(kvp =>
                        !oldMetaData.TryGetValue(kvp.Key, out var field) || field != kvp.Value))
                    {
                        if (oldMetaData != null)
                        {
                            foreach (var field in oldMetaData)
                            {
                                if (!newMetaData.ContainsKey(field.Key))
                                {
                                    newMetaData[field.Key] = field.Value;
                                }
                            }
                        }
                        ret["metadata"] = newMetaData;
                        
                    }
                }
                else
                {
                    ret["metadata"] = newMetaData;
                }
            }
            if (!ret.Any()) return null;

            foreach (var kvp in raw.Columns)
            {
                if (!ret.ContainsKey(kvp.Key))
                {
                    ret[kvp.Key] = kvp.Value;
                }
            }
            return JsonDocument.Parse(JsonSerializer.SerializeToUtf8Bytes(ret)).RootElement;
        }

        public static JsonElement? CreateRawTsUpdate(
            BufferedVariable variable, 
            UAExtractor extractor,
            RawRow raw,
            TypeUpdateConfig update,
            Dictionary<string, string> metaMap)
        {
            if (variable == null || extractor == null || update == null) return null;

            if (raw == null)
            {
                var create = VariableToStatelessTimeSeries(variable, extractor, null, metaMap);
                return JsonDocument.Parse(JsonSerializer.Serialize(create,
                    new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase })).RootElement;
            }


            var ret = new Dictionary<string, object>();
            if (update.Context)
            {
                string newAssetExtId = extractor.GetUniqueId(variable.ParentId);
                UpdateIfModified(ret, raw, newAssetExtId, "assetExternalId");
            }
            return CreateRawUpdateCommon(extractor, variable, raw, update, ret);   
        }

        public static JsonElement? CreateRawAssetUpdate(
            BufferedNode node,
            UAExtractor extractor,
            RawRow raw,
            TypeUpdateConfig update,
            Dictionary<string, string> metaMap)
        {
            if (node == null || extractor == null || update == null) return null;

            if (raw == null)
            {
                var create = NodeToAsset(node, extractor, null, metaMap);
                return JsonDocument.Parse(JsonSerializer.Serialize(create,
                    new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase })).RootElement;
            }

            var ret = new Dictionary<string, object>();
            if (update.Context)
            {
                string newParentId = extractor.GetUniqueId(node.ParentId);
                UpdateIfModified(ret, raw, newParentId, "parentExternalId");
            }
            return CreateRawUpdateCommon(extractor, node, raw, update, ret);
        }

        public static IEnumerable<IEnumerable<string>> ChunkByHierarchy(IEnumerable<KeyValuePair<string, BufferedNode>> objects)
        {
            if (objects == null) return Array.Empty<IEnumerable<string>>();
            var level = new Dictionary<NodeId, int>();
            foreach (var obj in objects) level[obj.Value.Id] = 0;
            bool changed = false;
            do
            {
                changed = false;
                foreach (var obj in objects)
                {
                    if (obj.Value.ParentId == null || obj.Value.ParentId.IsNullNodeId || !level.ContainsKey(obj.Value.ParentId)) continue;
                    if (level[obj.Value.ParentId] <= level[obj.Value.Id])
                    {
                        changed = true;
                        level[obj.Value.ParentId] = level[obj.Value.Id] + 1;
                    }
                }
            } while (changed);
            return objects.GroupBy(obj => level[obj.Value.Id]).OrderByDescending(group => group.Key).Select(group => group.Select(kvp => kvp.Key));
        }
    }

    public class StatelessEventCreate : EventCreate
    {
        public IEnumerable<string> AssetExternalIds { get; set; }
    }

    public class StatelessTimeSeriesCreate : TimeSeriesCreate
    {
        public string AssetExternalId { get; set; }
    }
}

using Cognite.Extractor.Utils;
using CogniteSdk;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Pushers
{
    public static class PusherUtils
    {
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
        public static Dictionary<string, string> PropertiesToMetadata(IEnumerable<BufferedVariable> properties)
        {
            if (properties == null) return new Dictionary<string, string>();
            return properties
                .Where(prop => prop.Value != null)
                .Take(16)
                .ToDictionary(prop => ExtractorUtils.Truncate(prop.DisplayName, 32), prop => ExtractorUtils.Truncate(prop.Value.StringValue, 256));
        }

        /// <summary>
        /// Converts BufferedNode into asset write poco.
        /// </summary>
        /// <param name="node">Node to be converted</param>
        /// <returns>Full asset write poco</returns>
        public static AssetCreate NodeToAsset(BufferedNode node, UAExtractor extractor, long? dataSetId)
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
            if (node.Properties != null && node.Properties.Any())
            {
                writePoco.Metadata = PropertiesToMetadata(node.Properties);
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
                StartTime = evt.MetaData.ContainsKey("StartTime")
                    ? GetTimestampValue(evt.MetaData["StartTime"])
                    : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                EndTime = evt.MetaData.ContainsKey("EndTime")
                    ? GetTimestampValue(evt.MetaData["EndTime"])
                    : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                AssetExternalIds = parent == null
                    ? (IEnumerable<string>)Array.Empty<string>()
                    : new List<string> { extractor.GetUniqueId(parent.IsVariable ? parent.ParentId : parent.Id) },
                ExternalId = ExtractorUtils.Truncate(evt.EventId, 255),
                Type = ExtractorUtils.Truncate(evt.MetaData.ContainsKey("Type")
                    ? extractor.ConvertToString(evt.MetaData["Type"])
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
                StartTime = evt.MetaData.ContainsKey("StartTime")
                    ? GetTimestampValue(evt.MetaData["StartTime"])
                    : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                EndTime = evt.MetaData.ContainsKey("EndTime")
                    ? GetTimestampValue(evt.MetaData["EndTime"])
                    : new DateTimeOffset(evt.Time).ToUnixTimeMilliseconds(),
                ExternalId = ExtractorUtils.Truncate(evt.EventId, 255),
                Type = ExtractorUtils.Truncate(evt.MetaData.ContainsKey("Type")
                    ? extractor.ConvertToString(evt.MetaData["Type"])
                    : extractor.GetUniqueId(evt.EventType), 64),
                DataSetId = dataSetId
            };

            if (nodeToAssetIds != null && evt.SourceNode != null && !evt.SourceNode.IsNullNodeId && nodeToAssetIds.ContainsKey(evt.SourceNode)) {
                entity.AssetIds = new List<long> { nodeToAssetIds[evt.SourceNode] };
            }

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
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="variable">Variable to be converted</param>
        /// <returns>Complete timeseries write poco</returns>
        public static StatelessTimeSeriesCreate VariableToStatelessTimeSeries(BufferedVariable variable, UAExtractor extractor, long? dataSetId)
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
            if (variable.Properties != null && variable.Properties.Any())
            {
                writePoco.Metadata = PropertiesToMetadata(variable.Properties);
            }
            return writePoco;
        }
        /// <summary>
        /// Create timeseries poco to create this node in CDF
        /// </summary>
        /// <param name="variable">Variable to be converted</param>
        /// <returns>Complete timeseries write poco</returns>
        public static TimeSeriesCreate VariableToTimeseries(BufferedVariable variable, UAExtractor extractor, long? dataSetId,
            IDictionary<NodeId, long> nodeToAssetIds, bool minimal = false)
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
                    IsStep = variable.DataType.IsStep
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

            if (nodeToAssetIds != null && nodeToAssetIds.ContainsKey(variable.ParentId))
            {
                writePoco.AssetId = nodeToAssetIds[variable.ParentId];
            }

            if (variable.Properties != null && variable.Properties.Any())
            {
                writePoco.Metadata = PropertiesToMetadata(variable.Properties);
            }
            return writePoco;
        }

        private static void UpdateIfModified(Dictionary<string, object> ret, RawRow raw, string newValue, string key)
        {
            if (raw.Columns.ContainsKey(key))
            {
                string oldValue = null;
                try
                {
                    oldValue = raw.Columns[key].GetString();
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
                var newMetaData = PropertiesToMetadata(node.Properties);
                if (raw.Columns.ContainsKey("metadata"))
                {
                    Dictionary<string, string> oldMetaData = null;
                    try
                    {
                        oldMetaData = JsonSerializer.Deserialize<Dictionary<string, string>>(raw.Columns["metadata"].ToString());
                    }
                    catch (JsonException) { }
                    if (oldMetaData == null || newMetaData != null && newMetaData.Any(kvp =>
                        !oldMetaData.ContainsKey(kvp.Key) || oldMetaData[kvp.Key] != kvp.Value))
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
            TypeUpdateConfig update)
        {
            if (variable == null || extractor == null || update == null) return null;

            if (raw == null)
            {
                var create = VariableToStatelessTimeSeries(variable, extractor, null);
                return JsonDocument.Parse(JsonSerializer.Serialize(create,
                    new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase })).RootElement;
            }


            var ret = new Dictionary<string, object>();
            if (update.Context)
            {
                string newAssetExtId = extractor.GetUniqueId(variable.ParentId);
                UpdateIfModified(ret, raw, newAssetExtId, "assetExternalId");
            }
            return CreateRawUpdateCommon(variable, raw, update, ret);   
        }

        public static JsonElement? CreateRawAssetUpdate(
            BufferedNode node,
            UAExtractor extractor,
            RawRow raw,
            TypeUpdateConfig update)
        {
            if (node == null || extractor == null || update == null) return null;

            if (raw == null)
            {
                var create = NodeToAsset(node, extractor, null);
                return JsonDocument.Parse(JsonSerializer.Serialize(create,
                    new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase })).RootElement;
            }

            var ret = new Dictionary<string, object>();
            if (update.Context)
            {
                string newParentId = extractor.GetUniqueId(node.ParentId);
                UpdateIfModified(ret, raw, newParentId, "parentExternalId");
            }
            return CreateRawUpdateCommon(node, raw, update, ret);
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

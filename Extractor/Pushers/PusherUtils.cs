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

using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
        public static long GetTimestampValue(object value)
        {
            if (value is DateTime dt)
            {
                return dt.ToUnixTimeMilliseconds();
            }
            else
            {
                try
                {
                    return Convert.ToInt64(value, CultureInfo.InvariantCulture);
                }
                catch
                {
                    return 0;
                }
            }
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
            UANode node,
            RawRow raw,
            TypeUpdateConfig update,
            Dictionary<string, object> ret)
        {
            if (update.Description)
            {
                string newDescription = node.Description;
                UpdateIfModified(ret, raw, newDescription, "description");
            }

            if (update.Name)
            {
                string newName = node.DisplayName;
                UpdateIfModified(ret, raw, newName, "name");
            }

            if (update.Metadata)
            {
                var newMetaData = node.BuildMetadata(extractor);
                if (raw.Columns.TryGetValue("metadata", out var rawMetaData))
                {
                    Dictionary<string, string> oldMetaData = null;
                    try
                    {
                        oldMetaData = JsonSerializer.Deserialize<Dictionary<string, string>>(rawMetaData.ToString());
                    }
                    catch (JsonException) { }
                    if (oldMetaData == null || newMetaData != null
                        && newMetaData.Any(kvp => !oldMetaData.TryGetValue(kvp.Key, out var field) || field != kvp.Value))
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
            UAVariable variable,
            UAExtractor extractor,
            RawRow raw,
            TypeUpdateConfig update,
            Dictionary<string, string> metaMap)
        {
            if (variable == null || extractor == null || update == null) return null;

            if (raw == null)
            {
                var create = variable.ToStatelessTimeSeries(extractor, null, metaMap);
                // This is inefficient, but it seems like it might be difficult to do better given the SDK and System.Text.Json
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
            UANode node,
            UAExtractor extractor,
            RawRow raw,
            TypeUpdateConfig update,
            Dictionary<string, string> metaMap)
        {
            if (node == null || extractor == null || update == null) return null;

            if (raw == null)
            {
                var create = node.ToCDFAsset(extractor, null, metaMap);
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
        public static TimeSeriesUpdate GetTSUpdate(
            UAExtractor extractor,
            TimeSeries old,
            UAVariable newTs,
            TypeUpdateConfig update,
            IDictionary<NodeId, long> nodeToAssetIds)
        {
            if (update == null || newTs == null || nodeToAssetIds == null || old == null) return null;
            var tsUpdate = new TimeSeriesUpdate();
            if (update.Context)
            {
                if (newTs.ParentId != null && !newTs.ParentId.IsNullNodeId
                    && nodeToAssetIds.TryGetValue(newTs.ParentId, out long assetId))
                {
                    if (assetId != old.AssetId && assetId > 0)
                    {
                        tsUpdate.AssetId = new UpdateNullable<long?>(assetId);
                    }
                }
            }

            var newDesc = Sanitation.Truncate(newTs.Description, Sanitation.TimeSeriesDescriptionMax);
            if (update.Description && !string.IsNullOrEmpty(newDesc) && newDesc != old.Description)
                tsUpdate.Description = new UpdateNullable<string>(newDesc);

            var newName = Sanitation.Truncate(newTs.DisplayName, Sanitation.TimeSeriesNameMax);
            if (update.Name && !string.IsNullOrEmpty(newName) && newName != old.Name)
                tsUpdate.Name = new UpdateNullable<string>(newName);

            if (update.Metadata)
            {
                var newMetaData = newTs.BuildMetadata(extractor)
                    .Where(kvp => !string.IsNullOrEmpty(kvp.Value))
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
                    .SanitizeMetadata(
                        Sanitation.TimeSeriesMetadataMaxPerKey,
                        Sanitation.TimeSeriesMetadataMaxPairs,
                        Sanitation.TimeSeriesMetadataMaxPerValue,
                        Sanitation.TimeSeriesMetadataMaxBytes);

                if (old.Metadata == null && newMetaData.Any()
                    || newMetaData.Any(meta => !old.Metadata.ContainsKey(meta.Key) || old.Metadata[meta.Key] != meta.Value))
                {
                    tsUpdate.Metadata = new UpdateDictionary<string>(newMetaData, Array.Empty<string>());
                }
            }
            return tsUpdate;
        }

        public static AssetUpdate GetAssetUpdate(
            Asset old,
            UANode newAsset,
            UAExtractor extractor,
            TypeUpdateConfig update)
        {
            if (old == null || newAsset == null || extractor == null || update == null) return null;
            var assetUpdate = new AssetUpdate();
            if (update.Context && newAsset.ParentId != null && !newAsset.ParentId.IsNullNodeId)
            {
                var parentId = extractor.GetUniqueId(newAsset.ParentId);
                if (parentId != old.ParentExternalId)
                {
                    assetUpdate.ParentExternalId = new UpdateNullable<string>(parentId);
                }
            }

            if (update.Description && !string.IsNullOrEmpty(newAsset.Description) && newAsset.Description != old.Description)
                assetUpdate.Description = new UpdateNullable<string>(newAsset.Description);

            if (update.Name && !string.IsNullOrEmpty(newAsset.DisplayName) && newAsset.DisplayName != old.Name)
                assetUpdate.Name = new UpdateNullable<string>(newAsset.DisplayName);

            if (update.Metadata)
            {
                var newMetaData = newAsset.BuildMetadata(extractor)
                    .Where(kvp => !string.IsNullOrEmpty(kvp.Value))
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
                    .SanitizeMetadata(
                        Sanitation.AssetMetadataMaxPerKey,
                        Sanitation.AssetMetadataMaxPairs,
                        Sanitation.AssetMetadataMaxPerValue,
                        Sanitation.AssetMetadataMaxBytes);

                if (old.Metadata == null && newMetaData.Any()
                    || newMetaData.Any(meta => !old.Metadata.ContainsKey(meta.Key) || old.Metadata[meta.Key] != meta.Value))
                {
                    assetUpdate.Metadata = new UpdateDictionary<string>(newMetaData, Array.Empty<string>());
                }
            }
            return assetUpdate;
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

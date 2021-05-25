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
using Cognite.Extractor.StateStorage;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Opc.Ua;
using System;
using System.Buffers;
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
        /// <summary>
        /// Update the field in <paramref name="ret"/> by comparing the value in
        /// <paramref name="raw"/> with <paramref name="newValue"/>.
        /// If the new value is null or whitespace, it is ignored.
        /// </summary>
        /// <param name="ret"></param>
        /// <param name="raw"></param>
        /// <param name="newValue"></param>
        /// <param name="key"></param>
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
        /// <summary>
        /// Parse description, name and metadata for the given node,
        /// updating fields modified from the original raw row <paramref name="raw"/>,
        /// and converting the result to a json element.
        /// </summary>
        /// <param name="extractor">Active extractor, used for building metadata</param>
        /// <param name="node">Node to create update for</param>
        /// <param name="raw">Existing raw row</param>
        /// <param name="update">Configuration for which fields to update</param>
        /// <param name="ret">Dictionary containing values to be serialized</param>
        /// <returns>Converted json element, or null if updating was unnecessary.</returns>
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
                var newMetaData = node.MetadataToJson(extractor, extractor.StringConverter);
                if (raw.Columns.TryGetValue("metadata", out var rawMetaData))
                {
                    ret["metadata"] = JsonDocument.Parse(Merge(rawMetaData, newMetaData.RootElement));
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
        /// <summary>
        /// Adapted from https://stackoverflow.com/a/59574030/9946909. 
        /// Merge two JsonElements, producing a json encoded string with the merged contents.
        /// </summary>
        private static string Merge(JsonElement r1, JsonElement r2)
        {
            var outputBuffer = new ArrayBufferWriter<byte>();

            using (var jsonWriter = new Utf8JsonWriter(outputBuffer, new JsonWriterOptions { Indented = true }))
            {
                if (r1.ValueKind != JsonValueKind.Array && r1.ValueKind != JsonValueKind.Object || r1.ValueKind != r2.ValueKind)
                {
                    r2.WriteTo(jsonWriter);
                }
                else if (r1.ValueKind == JsonValueKind.Array)
                {
                    MergeArrays(jsonWriter, r1, r2);
                }
                else
                {
                    MergeObjects(jsonWriter, r1, r2);
                }
            }

            return Encoding.UTF8.GetString(outputBuffer.WrittenSpan);
        }

        /// <summary>
        /// Merge two json objects, write the result to <paramref name="jsonWriter"/>.
        /// </summary>
        /// <param name="jsonWriter">Output writer</param>
        /// <param name="root1">First object</param>
        /// <param name="root2">Second object</param>
        private static void MergeObjects(Utf8JsonWriter jsonWriter, JsonElement root1, JsonElement root2)
        {
            jsonWriter.WriteStartObject();
            foreach (JsonProperty property in root1.EnumerateObject())
            {
                string propertyName = property.Name;

                JsonValueKind newValueKind;

                if (root2.TryGetProperty(propertyName, out JsonElement newValue) && (newValueKind = newValue.ValueKind) != JsonValueKind.Null)
                {
                    jsonWriter.WritePropertyName(propertyName);

                    JsonElement originalValue = property.Value;
                    JsonValueKind originalValueKind = originalValue.ValueKind;

                    if (newValueKind == JsonValueKind.Object && originalValueKind == JsonValueKind.Object)
                    {
                        MergeObjects(jsonWriter, originalValue, newValue);
                    }
                    else if (newValueKind == JsonValueKind.Array && originalValueKind == JsonValueKind.Array)
                    {
                        MergeArrays(jsonWriter, originalValue, newValue);
                    }
                    else
                    {
                        newValue.WriteTo(jsonWriter);
                    }
                }
                else
                {
                    property.WriteTo(jsonWriter);
                }
            }

            // Write all the properties of the second document that are unique to it.
            foreach (JsonProperty property in root2.EnumerateObject())
            {
                if (!root1.TryGetProperty(property.Name, out _))
                {
                    property.WriteTo(jsonWriter);
                }
            }

            jsonWriter.WriteEndObject();
        }

        /// <summary>
        /// Merge two JSON arrays, write the result to <paramref name="jsonWriter"/>.
        /// </summary>
        /// <param name="jsonWriter">Output writer</param>
        /// <param name="root1">First array</param>
        /// <param name="root2">Second array</param>
        private static void MergeArrays(Utf8JsonWriter jsonWriter, JsonElement root1, JsonElement root2)
        {
            jsonWriter.WriteStartArray();

            foreach (JsonElement element in root1.EnumerateArray())
            {
                element.WriteTo(jsonWriter);
            }
            foreach (JsonElement element in root2.EnumerateArray())
            {
                element.WriteTo(jsonWriter);
            }

            jsonWriter.WriteEndArray();
        }

        /// <summary>
        /// Create Raw update for a variable as timeseries.
        /// updating fields modified from the original raw row<paramref name= "raw" />,
        /// and converting the result to a json element.
        /// </summary>
        /// <param name="variable">Node to create update for</param>
        /// <param name="extractor">Active extractor, used for building metadata</param>
        /// <param name="raw">Existing raw row</param>
        /// <param name="update">Configuration for which fields to update</param>
        /// <param name="metaMap">Map for special timeseries attributes to be sat based on metadata</param>
        /// <returns>Converted json element, or null if updating was unnecessary</returns>
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
        /// <summary>
        /// Create Raw update for a node as asset.
        /// updating fields modified from the original raw row<paramref name= "raw" />,
        /// and converting the result to a json element.
        /// </summary>
        /// <param name="node">Node to create update for</param>
        /// <param name="extractor">Active extractor, used for building metadata</param>
        /// <param name="raw">Existing raw row</param>
        /// <param name="update">Configuration for which fields to update</param>
        /// <param name="metaMap">Map for special asset attributes to be sat based on metadata</param>
        /// <returns>Converted json element, or null if updating was unnecessary</returns>
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
        /// <summary>
        /// Create timeseries update from existing timeseries and new OPC-UA variable.
        /// </summary>
        /// <param name="extractor">Active extractor, used for building metadata</param>
        /// <param name="old">Existing timeseries</param>
        /// <param name="newTs">New OPC-UA variable</param>
        /// <param name="update">Configuration for which fields to update</param>
        /// <param name="nodeToAssetIds">Map from NodeIds to assetIds, necessary for setting parents</param>
        /// <returns>Update object, or null if updating was unnecessary</returns>
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
                var newMetaData = newTs.BuildMetadata(extractor, extractor.StringConverter)
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
        
        /// <summary>
        /// Create asset update from existing asset and new OPC-UA node.
        /// </summary>
        /// <param name="extractor">Active extractor, used for building metadata</param>
        /// <param name="old">Existing asset</param>
        /// <param name="newAsset">New OPC-UA node</param>
        /// <param name="update">Configuration for which fields to update</param>
        /// <returns>Update object, or null if updating was unnecessary</returns>
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
                assetUpdate.Description = new UpdateNullable<string>(newAsset.Description.Truncate(Sanitation.AssetDescriptionMax));

            if (update.Name && !string.IsNullOrEmpty(newAsset.DisplayName) && newAsset.DisplayName != old.Name)
                assetUpdate.Name = new UpdateNullable<string>(newAsset.DisplayName.Truncate(Sanitation.AssetNameMax));

            if (update.Metadata)
            {
                var newMetaData = newAsset.BuildMetadata(extractor, extractor.StringConverter)
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
    /// <summary>
    /// EventCreate which can can be created without access to CDF Clean.
    /// </summary>
    public class StatelessEventCreate : EventCreate
    {
        public IEnumerable<string> AssetExternalIds { get; set; }
    }
    /// <summary>
    /// TimeSeriesCreate which can can be created without access to CDF Clean.
    /// </summary>
    public class StatelessTimeSeriesCreate : TimeSeriesCreate
    {
        public string AssetExternalId { get; set; }
        public new JsonDocument Metadata { get; set; }
    }
    public class AssetCreateJson : AssetCreate
    {
        public new JsonDocument Metadata { get; set; }
    }
}

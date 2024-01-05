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

using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.Logging;
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

        public static JsonElement? CreateRawUpdate(
            ILogger log,
            StringConverter converter,
            BaseUANode node,
            RawRow<Dictionary<string, JsonElement>>? raw,
            ConverterType type)
        {
            if (node == null) return null;
            var newObj = node.ToJson(log, converter, type);

            if (newObj == null || newObj.RootElement.ValueKind != JsonValueKind.Object) return null;
            if (raw == null) return newObj.RootElement;

            var fields = new HashSet<string>();
            foreach (var row in newObj.RootElement.EnumerateObject())
            {
                if (!raw.Columns.ContainsKey(row.Name) || raw.Columns[row.Name].ToString() != row.Value.ToString())
                {
                    return newObj.RootElement;
                }
                fields.Add(row.Name);
            }
            if (raw.Columns.Any(kvp => !fields.Contains(kvp.Key))) return newObj.RootElement;
            return null;
        }

        private static bool ShouldSetNewMetadata(FullConfig config, Dictionary<string, string> newMetadata, Dictionary<string, string>? oldMetadata)
        {
            if (newMetadata.Count != 0)
            {
                if (oldMetadata == null) return true;
                if (!newMetadata.All(kvp => oldMetadata.TryGetValue(kvp.Key, out var oldVal) && kvp.Value == oldVal)) return true;
            }
            if (config.Extraction.Deletes.Enabled
                && oldMetadata != null
                && oldMetadata.ContainsKey(config.Extraction.Deletes.DeleteMarker))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Create timeseries update from existing timeseries and new OPC-UA variable.
        /// </summary>
        /// <param name="old">Existing timeseries</param>
        /// <param name="newTs">New OPC-UA variable</param>
        /// <param name="update">Configuration for which fields to update</param>
        /// <param name="nodeToAssetIds">Map from NodeIds to assetIds, necessary for setting parents</param>
        /// <returns>Update object, or null if updating was unnecessary</returns>
        public static TimeSeriesUpdate? GetTSUpdate(
            FullConfig config,
            IUAClientAccess client,
            TimeSeries old,
            UAVariable newTs,
            TypeUpdateConfig update,
            IDictionary<NodeId, long> nodeToAssetIds)
        {
            if (update == null || newTs == null || nodeToAssetIds == null || old == null) return null;
            var tsUpdate = new TimeSeriesUpdate();
            if (update.Context)
            {
                if (!newTs.ParentId.IsNullNodeId && nodeToAssetIds.TryGetValue(newTs.ParentId, out long assetId))
                {
                    if (assetId != old.AssetId && assetId > 0)
                    {
                        tsUpdate.AssetId = new UpdateNullable<long?>(assetId);
                    }
                }
            }

            var newDesc = Sanitation.Truncate(newTs.FullAttributes.Description, Sanitation.TimeSeriesDescriptionMax);
            if (update.Description && !string.IsNullOrEmpty(newDesc) && newDesc != old.Description)
                tsUpdate.Description = new UpdateNullable<string>(newDesc);

            var newName = Sanitation.Truncate(newTs.Name, Sanitation.TimeSeriesNameMax);
            if (update.Name && !string.IsNullOrEmpty(newName) && newName != old.Name)
                tsUpdate.Name = new UpdateNullable<string>(newName);

            if (update.Metadata)
            {
                var newMetadata = newTs.BuildMetadata(config, client, true)
                    .Where(kvp => !string.IsNullOrEmpty(kvp.Value))
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
                    .SanitizeMetadata(
                        Sanitation.TimeSeriesMetadataMaxPerKey,
                        Sanitation.TimeSeriesMetadataMaxPairs,
                        Sanitation.TimeSeriesMetadataMaxPerValue,
                        Sanitation.TimeSeriesMetadataMaxBytes,
                        out _);

                if (ShouldSetNewMetadata(config, newMetadata, old.Metadata))
                {
                    tsUpdate.Metadata = new UpdateDictionary<string>(newMetadata);
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
        public static AssetUpdate? GetAssetUpdate(
            FullConfig config,
            Asset old,
            BaseUANode newAsset,
            UAExtractor extractor,
            TypeUpdateConfig update)
        {
            if (old == null || newAsset == null || extractor == null || update == null) return null;
            var assetUpdate = new AssetUpdate();
            if (update.Context && !newAsset.ParentId.IsNullNodeId)
            {
                var parentId = extractor.GetUniqueId(newAsset.ParentId);
                if (parentId != old.ParentExternalId)
                {
#pragma warning disable CS8604 // Possible null reference argument.
                    assetUpdate.ParentExternalId = new UpdateNullable<string>(parentId);
#pragma warning restore CS8604 // Possible null reference argument.
                }
            }

            if (update.Description && !string.IsNullOrEmpty(newAsset.Attributes.Description) && newAsset.Attributes.Description != old.Description)
                assetUpdate.Description = new UpdateNullable<string>(newAsset.Attributes.Description.Truncate(Sanitation.AssetDescriptionMax)!);

            if (update.Name && !string.IsNullOrEmpty(newAsset.Name) && newAsset.Name != old.Name)
                assetUpdate.Name = new UpdateNullable<string>(newAsset.Name.Truncate(Sanitation.AssetNameMax)!);

            if (update.Metadata)
            {
                var newMetadata = newAsset.BuildMetadata(config, extractor, true)
                    .Where(kvp => !string.IsNullOrEmpty(kvp.Value))
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
                    .SanitizeMetadata(
                        Sanitation.AssetMetadataMaxPerKey,
                        Sanitation.AssetMetadataMaxPairs,
                        Sanitation.AssetMetadataMaxPerValue,
                        Sanitation.AssetMetadataMaxBytes,
                        out _);

                if (ShouldSetNewMetadata(config, newMetadata, old.Metadata))
                {
                    assetUpdate.Metadata = new UpdateDictionary<string>(newMetadata);
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
        public IEnumerable<string>? AssetExternalIds { get; set; }
    }
    /// <summary>
    /// TimeSeriesCreate which can can be created without access to CDF Clean.
    /// </summary>
    public class StatelessTimeSeriesCreate : TimeSeriesCreate
    {
        public string? AssetExternalId { get; set; }
    }
}

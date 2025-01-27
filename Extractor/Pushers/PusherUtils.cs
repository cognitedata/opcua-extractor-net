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
        /// <param name="config">Extractor configuration object.</param>
        /// <param name="client">Interface for generating unique IDs using the client context.</param>
        /// <param name="old">Existing timeseries</param>
        /// <param name="newTs">New OPC-UA variable</param>
        /// <param name="nodeToAssetIds">Map from NodeIds to assetIds, necessary for setting parents</param>
        /// <returns>Update object, or null if updating was unnecessary</returns>
        public static TimeSeriesUpdate? GetTSUpdate(
            FullConfig config,
            IUAClientAccess client,
            TimeSeries old,
            UAVariable newTs,
            IDictionary<NodeId, long> nodeToAssetIds)
        {
            if (newTs == null || nodeToAssetIds == null || old == null) return null;
            var tsUpdate = new TimeSeriesUpdate();

            if (!newTs.ParentId.IsNullNodeId && nodeToAssetIds.TryGetValue(newTs.ParentId, out long assetId))
            {
                if (assetId != old.AssetId && assetId > 0)
                {
                    tsUpdate.AssetId = new UpdateNullable<long?>(assetId);
                }
            }

            var newDesc = Sanitation.Truncate(newTs.FullAttributes.Description, Sanitation.TimeSeriesDescriptionMax);
            if (!string.IsNullOrEmpty(newDesc) && newDesc != old.Description)
                tsUpdate.Description = new UpdateNullable<string>(newDesc);

            var newName = Sanitation.Truncate(newTs.Name, Sanitation.TimeSeriesNameMax);
            if (!string.IsNullOrEmpty(newName) && newName != old.Name)
                tsUpdate.Name = new UpdateNullable<string>(newName);

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

            return tsUpdate;
        }

        /// <summary>
        /// Create asset update from existing asset and new OPC-UA node.
        /// </summary>
        /// <param name="config">Extractor config.</param>
        /// <param name="old">Existing asset</param>
        /// <param name="newAsset">New OPC-UA node</param>
        /// <param name="extractor">Active extractor, used for building metadata</param>
        /// <returns>Update object, or null if updating was unnecessary</returns>
        public static AssetUpdate? GetAssetUpdate(
            FullConfig config,
            Asset old,
            BaseUANode newAsset,
            UAExtractor extractor)
        {
            if (old == null || newAsset == null || extractor == null) return null;
            var assetUpdate = new AssetUpdate();
            if (!newAsset.ParentId.IsNullNodeId)
            {
                var parentId = extractor.GetUniqueId(newAsset.ParentId);
                // Do not move an asset from root to non-root or the other way around.
                if (parentId != null
                    && old.ParentExternalId != null
                    && parentId != old.ParentExternalId)
                {
                    assetUpdate.ParentExternalId = new UpdateNullable<string?>(parentId);
                }
            }

            if (!string.IsNullOrEmpty(newAsset.Attributes.Description) && newAsset.Attributes.Description != old.Description)
                assetUpdate.Description = new UpdateNullable<string>(newAsset.Attributes.Description.Truncate(Sanitation.AssetDescriptionMax)!);

            if (!string.IsNullOrEmpty(newAsset.Name) && newAsset.Name != old.Name)
                assetUpdate.Name = new UpdateNullable<string>(newAsset.Name.Truncate(Sanitation.AssetNameMax)!);

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
            return assetUpdate;
        }

        public static DataPushResult ResultFromException(Exception? exc)
        {
            if (exc is not ResponseException rex) return DataPushResult.RecoverableFailure;
            // This is more than just the status codes we auto-retry on.
            // It includes errors that are likely to pop up during an extractor's life
            // that are easily remedied, it makes sense to buffer in this case.
            // We want these to be errors that are likely to also be reflected in the
            // "CanPush*" methods, so that we don't constantly read from the failure buffer.
            // This includes 401 and 403, as well as regular transient errors such as
            // 408, 5xx, and non-CDF errors.
            if (rex.Code == 429
                || rex.Code >= 500
                || rex.Code == 401
                || rex.Code == 403
                || rex.Code == 408) return DataPushResult.RecoverableFailure;
            return DataPushResult.UnrecoverableFailure;
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

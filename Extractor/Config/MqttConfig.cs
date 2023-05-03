/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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

using System.ComponentModel;

namespace Cognite.OpcUa.Config
{
    public class MqttPusherConfig : IPusherConfig
    {
        /// <summary>
        /// TCP Broker URL.
        /// </summary>
        public string? Host { get; set; }
        /// <summary>
        /// TCP Broker port.
        /// </summary>
        public int? Port { get; set; }
        /// <summary>
        /// MQTT broker username.
        /// </summary>
        public string? Username { get; set; }
        /// <summary>
        /// MQTT broker password.
        /// </summary>
        public string? Password { get; set; }
        /// <summary>
        /// True to enable TLS to the broker.
        /// </summary>
        public bool UseTls { get; set; }
        /// <summary>
        /// Allow untrusted SSL certificates.
        /// </summary>
        public bool AllowUntrustedCertificates { get; set; }
        /// <summary>
        /// Path to an optional cert file for a custom certificate authority.
        /// </summary>
        public string? CustomCertificateAuthority { get; set; }
        /// <summary>
        /// Mqtt client id. Should be unique for a given broker.
        /// </summary>
        [DefaultValue("cognite-opcua-extractor")]
        public string ClientId { get; set; } = "cognite-opcua-extractor";
        /// <summary>
        /// Data set to use for new objects. Existing objects will not be updated.
        /// </summary>
        public long? DataSetId { get; set; }
        /// <summary>
        /// Topic for assets.
        /// </summary>
        [DefaultValue("cognite/opcua/assets")]
        public string AssetTopic { get; set; } = "cognite/opcua/assets";
        /// <summary>
        /// Topic for timeseries.
        /// </summary>
        [DefaultValue("congite/opcua/timeseries")]
        public string TsTopic { get; set; } = "cognite/opcua/timeseries";
        /// <summary>
        /// Topic for events.
        /// </summary>
        [DefaultValue("cognite/opcua/events")]
        public string EventTopic { get; set; } = "cognite/opcua/events";
        /// <summary>
        /// Topic for datapoints.
        /// </summary>
        [DefaultValue("cognite/opcua/datapoints")]
        public string DatapointTopic { get; set; } = "cognite/opcua/datapoints";
        /// <summary>
        /// Topic for raw.
        /// </summary>
        [DefaultValue("cognite/opcua/raw")]
        public string RawTopic { get; set; } = "cognite/opcua/raw";
        /// <summary>
        /// Topic for relationships.
        /// </summary>
        [DefaultValue("cognite/opcua/relationships")]
        public string RelationshipTopic { get; set; } = "cognite/opcua/relationships";
        /// <summary>
        /// Set to enable storing a list of created assets/timeseries to local litedb.
        /// Requires the StateStorage.Location property to be set.
        /// If this is left empty, metadata will have to be read each time the extractor restarts.
        /// </summary>
        public string? LocalState { get; set; }
        /// <summary>
        /// Timestamp in ms since epoch to invalidate stored mqtt states.
        /// On extractor restart, assets/timeseries created before this will be attempted re-created in CDF.
        /// They will not be deleted or updated.
        /// </summary>
        public long InvalidateBefore { get; set; }
        /// <summary>
        /// DEPRECATED. If true, pusher will not push to target.
        /// </summary>
        public bool Debug { get; set; }
        public bool ReadExtractedRanges { get; set; }
        /// <summary>
        /// Do not push any metadata at all. If this is true, plain timeseries without metadata will be created,
        /// similarly to raw-metadata, and datapoints will be pushed. Nothing will be written to raw, and no assets will be created.
        /// Events will be created, but without asset context.
        /// </summary>
        public bool SkipMetadata { get; set; }
        /// <summary>
        /// Store assets and/or timeseries data in raw. Assets will not be created at all,
        /// timeseries will be created with just externalId, isStep and isString.
        /// Both timeseries and assets will be persisted in their entirety to raw.
        /// Datapoints are not affected, events will be created, but without asset context. The externalId
        /// of the source node is added to metadata if applicable.
        /// Use different table names for assets and timeseries.
        /// </summary>
        public RawMetadataConfig? RawMetadata { get; set; }
        /// <summary>
        /// Map metadata to asset/timeseries attributes. Each of "assets" and "timeseries" is a map from property DisplayName to
        /// CDF attribute. Legal attributes are "name, description, parentId" and "unit" for timeseries. "parentId" must somehow refer to
        /// an existing asset. For timeseries it must be a mapped asset, for assets it can be any asset.
        /// Example usage:
        /// timeseries:
        ///    "EngineeringUnits": "unit"
        ///    "EURange": "description"
        /// assets:
        ///    "Name": "name"
        /// </summary>
        public MetadataMapConfig? MetadataMapping { get; set; }
        /// <summary>
        /// Replacement for NaN values.
        /// </summary>
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set
            {
                if (value == null) return;
                nonFiniteReplacement = double.IsFinite(value.Value) ? value : null;
            }
        }
        private double? nonFiniteReplacement;
    }
}

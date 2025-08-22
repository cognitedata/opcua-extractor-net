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

using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json.Serialization;
using System.Linq;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa.Config
{
    /// <summary>
    /// Enum for MQTT JSON format types.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum MqttJsonFormat
    {
        /// <summary>
        /// Legacy timeseries format (existing format).
        /// </summary>
        Legacy,
        /// <summary>
        /// Polling snapshot object format with structured data.
        /// </summary>
        PollingSnapshotObject,
        /// <summary>
        /// Polling snapshot plain format with flat structure.
        /// </summary>
        PollingSnapshotPlain,
        /// <summary>
        /// Subscription format for individual tag updates.
        /// </summary>
        Subscription,
        /// <summary>
        /// Timeseries format (deprecated, use Legacy instead).
        /// </summary>
        Timeseries,
        /// <summary>
        /// Polling snapshot format (deprecated, use PollingSnapshotObject instead).
        /// </summary>
        PollingSnapshot
    }

    /// <summary>
    /// Enum for MQTT transmission strategy types.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum MqttTransmissionStrategy
    {
        /// <summary>
        /// Group data by root nodes configured in extraction.root-nodes.
        /// </summary>
        ROOT_NODE_BASED,
        /// <summary>
        /// Use chunking strategy with max-chunk-size (default behavior).
        /// </summary>
        CHUNK_BASED,
        /// <summary>
        /// Group data by specified tag lists.
        /// </summary>
        TAG_LIST_BASED,
        /// <summary>
        /// Send data based on OPC UA tag changes (subscription-based).
        /// </summary>
        TAG_CHANGE_BASED
    }

    /// <summary>
    /// Configuration for a single tag list group with custom name.
    /// </summary>
    public class TagListGroup
    {
        /// <summary>
        /// Custom name for this tag list group. If not specified, defaults to "tag_list_N".
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "tag-list-name")]
        public string? Name { get; set; }

        /// <summary>
        /// List of tag IDs that should be grouped together.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "tags")]
        public List<string>? Tags { get; set; }
    }

    /// <summary>
    /// Configuration for exclusion rules within a selector.
    /// </summary>
    public class ExcludeConfig
    {
        /// <summary>
        /// Specific tag IDs to exclude from the selection.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "tags")]
        public List<string>? Tags { get; set; }

        /// <summary>
        /// Regex patterns for tags to exclude from the selection.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "patterns")]
        public List<string>? Patterns { get; set; }
    }

    /// <summary>
    /// Configuration for a single selector within a publish group.
    /// </summary>
    public class SelectorConfig
    {
        /// <summary>
        /// Prefix string for matching node IDs (used for ROOT_NODE_BASED strategy).
        /// All nodes whose IDs start with this prefix will be included.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "prefix")]
        public string? Prefix { get; set; }

        /// <summary>
        /// Specific tag IDs to include in this selector.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "tags")]
        public List<string>? Tags { get; set; }

        /// <summary>
        /// Regex pattern for matching node IDs.
        /// All nodes whose IDs match this pattern will be included.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "pattern")]
        public string? Pattern { get; set; }

        /// <summary>
        /// Exclusion rules to filter out specific tags from this selector.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "exclude")]
        public ExcludeConfig? Exclude { get; set; }
    }

    /// <summary>
    /// Configuration for a single publish group.
    /// </summary>
    public class PublishGroup
    {
        /// <summary>
        /// Name of this publish group. This name will appear in the MQTT metadata.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "name")]
        public string? Name { get; set; }

        /// <summary>
        /// List of selectors that define which nodes belong to this group.
        /// For ROOT_NODE_BASED strategy: Uses "most specific rule wins" logic.
        /// For TAG_LIST_BASED strategy: Uses "first matching rule wins" logic.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "selectors")]
        public List<SelectorConfig>? Selectors { get; set; }
    }

    /// <summary>
    /// Configuration for MQTT transmission strategy.
    /// Supports both legacy flat format and new nested format.
    /// </summary>
    public class MqttTransmissionStrategyConfig
    {
        /// <summary>
        /// Data grouping strategy for MQTT transmission.
        /// ROOT_NODE_BASED: Group by extraction.root-nodes configuration
        /// CHUNK_BASED: Use existing chunking strategy (default)
        /// TAG_LIST_BASED: Group by specified tag lists
        /// TAG_CHANGE_BASED: Send based on OPC UA tag changes (subscription-based)
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "data-group-by")]
        public MqttTransmissionStrategy DataGroupBy { get; set; } = MqttTransmissionStrategy.CHUNK_BASED;

        /// <summary>
        /// Configuration for tag list grouping when DataGroupBy is TAG_LIST_BASED.
        /// Each list represents a group of tags that should be sent together in one JSON message.
        /// Legacy format: List of string arrays.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "tag-lists")]
        public List<List<string>>? TagLists { get; set; }

        /// <summary>
        /// Advanced configuration for tag list grouping with custom names when DataGroupBy is TAG_LIST_BASED.
        /// New format: List of TagListGroup objects with custom names.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "tag-list-groups")]
        public List<TagListGroup>? TagListGroups { get; set; }

        /// <summary>
        /// New unified publish groups configuration that replaces tag-list-groups.
        /// This provides advanced selector and exclusion capabilities for both ROOT_NODE_BASED and TAG_LIST_BASED strategies.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "publish-groups")]
        public List<PublishGroup>? PublishGroups { get; set; }

        /// <summary>
        /// Get the effective tag lists with their names.
        /// Returns a dictionary where key is the group name and value is the list of tags.
        /// </summary>
        public Dictionary<string, List<string>> GetEffectiveTagListsWithNames()
        {
            var result = new Dictionary<string, List<string>>();

            // Process new format first (tag-list-groups)
            if (TagListGroups != null && TagListGroups.Any())
            {
                for (int i = 0; i < TagListGroups.Count; i++)
                {
                    var group = TagListGroups[i];
                    if (group.Tags != null && group.Tags.Any())
                    {
                        var groupName = !string.IsNullOrEmpty(group.Name) ? group.Name : $"tag_list_{i + 1}";
                        result[groupName] = group.Tags;
                    }
                }
            }
            // Fall back to legacy format (tag-lists)
            else if (TagLists != null && TagLists.Any())
            {
                for (int i = 0; i < TagLists.Count; i++)
                {
                    var tagList = TagLists[i];
                    if (tagList != null && tagList.Any())
                    {
                        var groupName = $"tag_list_{i + 1}";
                        result[groupName] = tagList;
                    }
                }
            }

            return result;
        }
    }

    public class MqttPusherConfig : IPusherConfig
    {
        /// <summary>
        /// Set to true to enable this destination
        /// </summary>
        public bool Enabled { get; set; }

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

        /// <summary>
        /// Whether to use gRPC/Protobuf serialization for datapoints.
        /// If true, datapoints will be serialized using Protobuf (gRPC format).
        /// If false, datapoints will be serialized using JSON format.
        /// Default is true for better performance and compatibility with MQTTCDFBridge.
        /// </summary>
        [DefaultValue(true)]
        public bool UseGrpc { get; set; } = true;

        /// <summary>
        /// Timestamp format for JSON serialization.
        /// "epoch" - Unix timestamp in milliseconds since epoch (default)
        /// "iso8601" - ISO 8601 format (YYYY-MM-DDTHH:mm:ss.SSS+09:00)
        /// </summary>
        [DefaultValue("epoch")]
        public string TimestampFormat { get; set; } = "epoch";

        /// <summary>
        /// Timezone offset for ISO8601 timestamp format.
        /// Format: "+09:00" for Korea time, "+00:00" for UTC, etc.
        /// Only used when TimestampFormat is "iso8601".
        /// </summary>
        [DefaultValue("+00:00")]
        public string TimezoneOffset { get; set; } = "+00:00";

        /// <summary>
        /// JSON format to use when UseGrpc is false.
        /// </summary>
        [DefaultValue(MqttJsonFormat.Legacy)]
        public MqttJsonFormat JsonFormatType { get; set; } = MqttJsonFormat.Legacy;

        /// <summary>
        /// Whether to include metadata object in JSON output.
        /// </summary>
        [DefaultValue(true)]
        public bool IncludeMetadata { get; set; } = true;

        /// <summary>
        /// Whether to include msgRecvStartTimestamp and msgRecvEndTimestamp in metadata.
        /// Only applicable when IncludeMetadata is true.
        /// </summary>
        [DefaultValue(true)]
        public bool IncludeMessageTimestamps { get; set; } = true;

        /// <summary>
        /// Whether to include data type (dt) field for each tag.
        /// </summary>
        [DefaultValue(true)]
        public bool IncludeDataType { get; set; } = true;

        /// <summary>
        /// Whether to include status code (sc) field for each tag.
        /// </summary>
        [DefaultValue(true)]
        public bool IncludeStatusCode { get; set; } = true;

        /// <summary>
        /// Maximum number of timeseries to send in a single datapoint message.
        /// </summary>
        [DefaultValue(1000)]
        public int DatapointsPerMessage { get; set; } = 1000;

        /// <summary>
        /// Maximum MQTT message size in bytes for adaptive chunking.
        /// Messages larger than this will be split into smaller chunks.
        /// </summary>
        [DefaultValue(1048576)] // 1MB
        public int MaxMessageSize { get; set; } = 1048576;

        /// <summary>
        /// MQTT transmission strategy for grouping and sending data.
        /// ROOT_NODE_BASED: Group by extraction.root-nodes configuration
        /// CHUNK_BASED: Use existing chunking strategy (default)
        /// TAG_LIST_BASED: Group by specified tag lists
        /// TAG_CHANGE_BASED: Send based on OPC UA tag changes (subscription-based)
        /// </summary>
        [DefaultValue(MqttTransmissionStrategy.CHUNK_BASED)]
        public MqttTransmissionStrategy TransmissionStrategy { get; set; } = MqttTransmissionStrategy.CHUNK_BASED;

        /// <summary>
        /// New nested transmission strategy configuration.
        /// If specified, this will override the legacy TransmissionStrategy and TagLists properties.
        /// </summary>
        [YamlDotNet.Serialization.YamlMember(Alias = "mqtt-transmission-strategy")]
        public MqttTransmissionStrategyConfig? TransmissionStrategyConfig { get; set; }

        /// <summary>
        /// Configuration for tag list grouping when TransmissionStrategy is TAG_LIST_BASED.
        /// Each list represents a group of tags that should be sent together in one JSON message.
        /// </summary>
        public List<List<string>>? TagLists { get; set; }

        /// <summary>
        /// Maximum number of data points in a single chunk for adaptive chunking.
        /// This acts as an upper limit regardless of message size.
        /// </summary>
        [DefaultValue(10000)]
        public int MaxChunkSize { get; set; } = 10000;

        /// <summary>
        /// Minimum number of data points in a single chunk for adaptive chunking.
        /// This acts as a lower limit to prevent too many small messages.
        /// </summary>
        [DefaultValue(10)]
        public int MinChunkSize { get; set; } = 10;

        /// <summary>
        /// Maximum number of concurrent chunk processing for parallel MQTT publishing.
        /// Higher values may improve throughput but can overwhelm the MQTT broker.
        /// </summary>
        [DefaultValue(4)]
        public int MaxConcurrency { get; set; } = 4;

        /// <summary>
        /// Get the effective transmission strategy.
        /// Returns the strategy from nested config if available, otherwise falls back to legacy property.
        /// </summary>
        public MqttTransmissionStrategy GetEffectiveTransmissionStrategy()
        {
            return TransmissionStrategyConfig?.DataGroupBy ?? TransmissionStrategy;
        }

        /// <summary>
        /// Get the effective tag lists.
        /// Returns tag lists from nested config if available, otherwise falls back to legacy property.
        /// </summary>
        public List<List<string>>? GetEffectiveTagLists()
        {
            return TransmissionStrategyConfig?.TagLists ?? TagLists;
        }

        /// <summary>
        /// Set the transmission strategy using the new nested format.
        /// This will create the nested config if it doesn't exist.
        /// </summary>
        public void SetTransmissionStrategy(MqttTransmissionStrategy strategy, List<List<string>>? tagLists = null)
        {
            TransmissionStrategyConfig ??= new MqttTransmissionStrategyConfig();
            TransmissionStrategyConfig.DataGroupBy = strategy;
            TransmissionStrategyConfig.TagLists = tagLists;
        }
    }
}

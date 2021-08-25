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

using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.TypeCollectors;
using CogniteSdk;
using Newtonsoft.Json;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents an opcua node.
    /// </summary>
    public class UANode
    {
        /// <summary>
        /// Raw NodeId in OPC-UA.
        /// </summary>
        public NodeId Id { get; }
        /// <summary>
        /// DisplayName in OPC-UA
        /// </summary>
        public string DisplayName { get; }
        /// <summary>
        /// Node Class in OPC-UA
        /// </summary>
        public NodeClass NodeClass => Attributes.NodeClass;
        /// <summary>
        /// NodeId of the parent. May be NodeId.Null for rootNodes
        /// </summary>
        public NodeId ParentId { get; }
        public virtual NodeAttributes Attributes { get; }
        /// <summary>
        /// True if the properties of this object has been read.
        /// </summary>
        public bool PropertiesRead => Attributes.PropertiesRead;
        /// <summary>
        /// True if node data has been read
        /// </summary>
        public bool DataRead => Attributes.DataRead;
        /// <summary>
        /// Description in OPC-UA
        /// </summary>
        [MaybeNull]
        public string Description => Attributes.Description;
        /// <summary>
        /// Whether to subscribe to this node, independent of reading history.
        /// </summary>
        public bool ShouldSubscribe => Attributes.ShouldSubscribe;
        /// <summary>
        /// True if the node has been modified after pushing.
        /// </summary>
        public bool Changed { get; set; }
        /// <summary>
        /// Raw OPC-UA EventNotifier attribute.
        /// </summary>
        public byte EventNotifier => Attributes.EventNotifier;
        /// <summary>
        /// OPC-UA node type
        /// </summary>
        [MaybeNull]
        public UANodeType? NodeType => Attributes.NodeType;
        /// <summary>
        /// True if node is a property
        /// </summary>
        public bool IsProperty => Attributes.IsProperty;
        /// <summary>
        /// Parent node
        /// </summary>
        [MaybeNull]
        public UANode? Parent { get; set; }
        /// <summary>
        /// True if this node should not be pushed to destinations.
        /// </summary>
        public bool Ignore => Attributes.Ignore;

        private static readonly ILogger log = Log.Logger.ForContext(typeof(UANode));

        /// <summary>
        /// Where this node was first generated from
        /// </summary>
        public NodeSource Source { get; set; } = NodeSource.OPCUA;
        /// <summary>
        /// Return a string description, for logging
        /// </summary>
        /// <returns>Descriptive string</returns>
        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.AppendFormat(CultureInfo.InvariantCulture, "Object: {0}\n", DisplayName);
            builder.AppendFormat(CultureInfo.InvariantCulture, "Id: {0}\n", Id);
            if (ParentId != null && !ParentId.IsNullNodeId)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "ParentId: {0}\n", ParentId);
            }
            if (Description != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "Description: {0}\n", Description);
            }
            if (EventNotifier != 0)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "EventNotifier: {0}\n", EventNotifier);
            }
            if (NodeType != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "NodeType: {0}\n", NodeType.Name ?? NodeType.Id);
            }

            if (Properties != null && Properties.Any())
            {
                var meta = BuildMetadataBase(null, new StringConverter(null, null));
                builder.Append("Properties: {\n");
                foreach (var prop in meta)
                {
                    builder.AppendFormat(CultureInfo.InvariantCulture, "    {0}: {1}\n", prop.Key, prop.Value ?? "??");
                }
                builder.Append('}');
            }
            return builder.ToString();
        }
        /// <summary>
        /// Properties in OPC-UA
        /// </summary>
        [MaybeNull]
        public IEnumerable<UANode> Properties => Attributes.Properties;
        public UANode(NodeId id, string displayName, NodeId parentId, NodeClass nodeClass) : this(id, displayName, parentId)
        {
            Attributes = new NodeAttributes(nodeClass);
        }
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        protected UANode(NodeId id, string displayName, NodeId parentId)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        {
            Id = id;
            DisplayName = displayName;
            ParentId = parentId;
        }
        /// <summary>
        /// Build checksum based on current properties, used for efficiently checking for changes without
        /// having to store the entire event.
        /// </summary>
        /// <param name="update">Configuration for which fields should be considered.</param>
        /// <param name="dataTypeMetadata">True if the dataType should be considered</param>
        /// <param name="nodeTypeMetadata">True if the nodeType should be considered.</param>
        /// <returns>Integer checksum</returns>
        public int GetUpdateChecksum(TypeUpdateConfig update, bool dataTypeMetadata, bool nodeTypeMetadata)
        {
            if (update == null || !update.AnyUpdate) return 0;
            int checksum = 0;
            unchecked
            {
                if (update.Context)
                {
                    checksum += (ParentId?.GetHashCode() ?? 0);
                }
                if (update.Description)
                {
                    checksum = checksum * 31 + (Description?.GetHashCode(StringComparison.InvariantCulture) ?? 0);
                }
                if (update.Name)
                {
                    checksum = checksum * 31 + (DisplayName?.GetHashCode(StringComparison.InvariantCulture) ?? 0);
                }
                if (update.Metadata)
                {
                    int metaHash = 0;
                    if (Properties != null)
                    {
                        foreach (var prop in Properties.OrderBy(prop => prop.DisplayName))
                        {
                            metaHash *= 31;
                            if (prop.DisplayName == null) continue;
                            if (prop is UAVariable propVariable)
                            {
                                metaHash += (prop.DisplayName, propVariable.Value.Value).GetHashCode();
                            }
                            if (prop.Properties?.Any() ?? false)
                            {
                                metaHash += prop.GetUpdateChecksum(new TypeUpdateConfig { Metadata = true }, false, false);
                            }
                        }
                    }
                    if (this is UAVariable variable)
                    {
                        if (dataTypeMetadata)
                        {
                            metaHash = metaHash * 31 + variable.DataType.Raw.GetHashCode();
                        }
                        if (NodeClass == NodeClass.VariableType)
                        {
                            metaHash = metaHash * 31 + variable.Value.GetHashCode();
                        }
                    }

                    if (nodeTypeMetadata)
                    {
                        metaHash = metaHash * 31 + (NodeType?.Id?.GetHashCode() ?? 0);
                    }
                    checksum = checksum * 31 + metaHash;
                }
            }
            return checksum;
        }
        [return: MaybeNull]
        public Dictionary<string, string> GetExtraMetadata(ExtractionConfig config, DataTypeManager manager, StringConverter converter)
        {
            Dictionary<string, string>? fields = null;
            if (this is UAVariable variable)
            {
                fields = manager.GetAdditionalMetadata(variable);
                if (variable.NodeClass == NodeClass.VariableType)
                {
                    fields ??= new Dictionary<string, string>();
                    fields["Value"] = converter.ConvertToString(variable.Value, variable.DataType?.EnumValues);
                }
            }
            if (config.NodeTypes.Metadata)
            {
                fields ??= new Dictionary<string, string>();
                if (NodeType?.Name != null)
                {
                    fields["TypeDefinition"] = NodeType.Name;
                }
            }
            return fields;
        }

        protected Dictionary<string, string> BuildMetadataBase(Dictionary<string, string>? extras, StringConverter converter)
        {
            var result = extras ?? new Dictionary<string, string>();

            if (Properties == null) return result;

            foreach (var prop in Properties)
            {
                if (prop != null && !string.IsNullOrEmpty(prop.DisplayName))
                {
                    if (prop is UAVariable variable)
                    {
                        result[prop.DisplayName] = converter.ConvertToString(variable.Value, variable.DataType?.EnumValues)
                            ?? variable.Value.ToString();
                    }

                    if (prop.Properties != null)
                    {
                        var nestedProperties = prop.BuildMetadataBase(null, converter);
                        foreach (var sprop in nestedProperties)
                        {
                            result[$"{prop.DisplayName}_{sprop.Key}"] = sprop.Value;
                        }
                    }
                }
            }
            return result;
        }


        /// <summary>
        /// Return a dictionary of metadata fields for this node.
        /// </summary>
        /// <param name="config">Extraction config object</param>
        /// <param name="manager">DataTypeManager used to get information about the datatype</param>
        /// <param name="converter">StringConverter used for building metadata</param>
        /// <param name="getExtras">True to get extra metadata</param>
        /// <returns>Created metadata dictionary.</returns>
        public Dictionary<string, string> BuildMetadata(ExtractionConfig config, DataTypeManager manager, StringConverter converter, bool getExtras)
        {
            Dictionary<string, string>? extras = null;
            if (getExtras)
            {
                extras = GetExtraMetadata(config, manager, converter);
            }
            return BuildMetadataBase(extras, converter);
        }

        /// <summary>
        /// Retrieve a full list of properties for this node,
        /// recursively fetching properties of properties.
        /// </summary>
        /// <returns>Full list of properties</returns>
        public IEnumerable<UANode> GetAllProperties()
        {
            if (Properties == null) return Enumerable.Empty<UANode>();
            var result = new List<UANode>();
            result.AddRange(Properties);
            foreach (var prop in Properties)
            {
                result.AddRange(prop.GetAllProperties());
            }
            return result;
        }
        private void PopulateAssetCreate(
            IUAClientAccess client,
            StringConverter converter,
            long? dataSetId,
            Dictionary<string, string> metaMap,
            AssetCreate asset)
        {
            var id = client.GetUniqueId(Id);
            asset.Description = Description;
            asset.ExternalId = id;
            asset.Name = string.IsNullOrEmpty(DisplayName) ? id : DisplayName;
            asset.DataSetId = dataSetId;
            if (ParentId != null && !ParentId.IsNullNodeId)
            {
                asset.ParentExternalId = client.GetUniqueId(ParentId);
            }
            if (Properties != null && Properties.Any() && (metaMap?.Any() ?? false))
            {
                foreach (var prop in Properties)
                {
                    if (!(prop is UAVariable propVar)) continue;
                    if (metaMap.TryGetValue(prop.DisplayName, out var mapped))
                    {
                        var value = converter.ConvertToString(propVar.Value, propVar.DataType.EnumValues);
                        if (string.IsNullOrWhiteSpace(value)) continue;
                        switch (mapped)
                        {
                            case "description": asset.Description = value; break;
                            case "name": asset.Name = value; break;
                            case "parentId": asset.ParentExternalId = value; break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Convert to CDF Asset.
        /// </summary>
        /// <param name="config">Active configuration object</param>
        /// <param name="client">Access to OPC-UA session</param>
        /// <param name="converter">StringConverter for converting fields</param>
        /// <param name="dataSetId">Optional dataSetId</param>
        /// <param name="metaMap">Map from metadata to asset attributes.</param>
        /// <returns></returns>
        public AssetCreate ToCDFAsset(
            ExtractionConfig config,
            IUAClientAccess client,
            StringConverter converter,
            DataTypeManager manager,
            long? dataSetId,
            Dictionary<string, string> metaMap)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (converter == null) throw new ArgumentNullException(nameof(converter));
            var asset = new AssetCreate();
            PopulateAssetCreate(client, converter, dataSetId, metaMap, asset);
            asset.Metadata = BuildMetadata(config, manager, converter, true);

            return asset;
        }
        [return: MaybeNull]
        public JsonDocument ToJson(StringConverter converter, ConverterType type)
        {
            // This is inefficient. A better solution would use System.Text.Json directly, but that requires .NET 6
            // for WriteRaw in Utf8JsonWriter.
            var serializer = new Newtonsoft.Json.JsonSerializer();
            converter.AddConverters(serializer, type);
            using var stream = new MemoryStream();
            var sw = new StreamWriter(stream);
            using var writer = new JsonTextWriter(sw);

            serializer.Serialize(writer, this);
            writer.Flush();
            stream.Seek(0, SeekOrigin.Begin);

            try
            {
                return JsonDocument.Parse(stream);
            }
            catch (Exception ex)
            {
                stream.Seek(0, SeekOrigin.Begin);
                log.Error("Failed to parse JSON data: {msg}, invalid JSON: {data}", ex.Message, Encoding.UTF8.GetString(stream.ToArray()));
                return null;
            }
        }
        /// <summary>
        /// Add property to list, creating the list if it does not exist.
        /// </summary>
        /// <param name="prop">Property to add</param>
        public void AddProperty(UANode prop)
        {
            if (Attributes.Properties == null)
            {
                Attributes.Properties = new List<UANode> { prop };
                return;
            }
            if (Properties.Any(oldProp => oldProp.Id == prop.Id)) return;
            Attributes.Properties.Add(prop);
        }
        /// <summary>
        /// Set the node type to one given by <paramref name="nodeId"/>
        /// </summary>
        /// <param name="client">Active UAClient</param>
        /// <param name="nodeId">NodeId of node type to set.</param>
        public void SetNodeType(UAClient client, ExpandedNodeId nodeId)
        {
            if (nodeId == null || nodeId.IsNull) return;
            if (client == null) throw new ArgumentNullException(nameof(client));
            var id = client.ToNodeId(nodeId);
            Attributes.NodeType = client.ObjectTypeManager.GetObjectType(id, NodeClass == NodeClass.Variable);
            if (NodeClass == NodeClass.Variable && id == VariableTypeIds.PropertyType)
            {
                Attributes.IsProperty = true;
                Attributes.PropertiesRead = true;
            }
        }
    }
}

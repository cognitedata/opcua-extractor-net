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
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents a single OPC-UA event, with especially relevant information stored as properties and the rest in a dictionary.
    /// </summary>
    public class UAEvent
    {
        /// <summary>
        /// Message sent with the original event.
        /// </summary>
        public string? Message { get; set; }
        /// <summary>
        /// Transformed ID of the event. The Raw id is a byte-string. This is the byte-string transformed into Base64 and prepended the globalprefix.
        /// </summary>
        public string? EventId { get; set; } // Base64
        /// <summary>
        /// NodeId of the SourceNode
        /// </summary>
        public NodeId? SourceNode { get; set; }
        /// <summary>
        /// Time this event triggered.
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// NodeId of the eventType of this event.
        /// </summary>
        public UAObjectType? EventType { get; set; }
        /// <summary>
        /// Id of the node that emitted the event in opc-ua
        /// </summary>
        public NodeId EmittingNode { get; set; } = null!;

        public Dictionary<RawTypeField, EventFieldValue>? Values { get; private set; }

        private StringConverter? valuesConverter;
        private ILogger? valuesLogger;

        private Dictionary<string, string>? cachedMetadata;
        public Dictionary<string, string>? MetaData
        {
            get
            {
                if (cachedMetadata == null && valuesConverter != null && valuesLogger != null && Values != null)
                {
                    cachedMetadata = GetMetadata(valuesConverter, Values.Values, valuesLogger);
                }
                return cachedMetadata;
            }
            set => cachedMetadata = value;
        }

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.AppendFormat(CultureInfo.InvariantCulture, "Event: {0}", EventId);
            builder.AppendLine();
            builder.AppendFormat(CultureInfo.InvariantCulture, "Time: {0}", Time);
            builder.AppendLine();
            builder.AppendFormat(CultureInfo.InvariantCulture, "Type: {0}", EventType?.Name);
            builder.AppendLine();
            builder.AppendFormat(CultureInfo.InvariantCulture, "Emitter: {0}", EmittingNode);
            builder.AppendLine();
            if (Message != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "Message: {0}", Message);
                builder.AppendLine();
            }
            if (SourceNode != null && !SourceNode.IsNullNodeId)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "SourceNode: {0}", SourceNode);
                builder.AppendLine();
            }

            if (MetaData != null && MetaData.Count != 0)
            {
                builder.Append("MetaData: {");
                builder.AppendLine();
                foreach (var kvp in MetaData)
                {
                    builder.AppendFormat(CultureInfo.InvariantCulture, "    {0}: {1}", kvp.Key, kvp.Value);
                    builder.AppendLine();
                }
                builder.Append('}');
            }

            return builder.ToString();
        }
        public void SetMetadata(StringConverter converter, IEnumerable<EventFieldValue> values, ILogger log)
        {
            Values = values.ToDictionary(v => v.Field);
            valuesConverter = converter;
            valuesLogger = log;
        }
        [return: NotNullIfNotNull("values")]
        private static Dictionary<string, string>? GetMetadata(StringConverter converter, IEnumerable<EventFieldValue> values, ILogger log)
        {
            if (values == null) return null;
            var parents = new Dictionary<string, EventFieldNode>();
            foreach (var field in values)
            {
                if (field.IsBuiltIn) continue;
                IDictionary<string, EventFieldNode> next = parents;
                EventFieldNode? current = null;
                for (int i = 0; i < field.Field.BrowsePath.Count; i++)
                {
                    if (!next.TryGetValue(field.Field.BrowsePath[i].Name, out current))
                    {
                        next[field.Field.BrowsePath[i].Name] = current = new EventFieldNode();
                    }
                    next = current!.Children;
                }
                if (current != null)
                {
                    current.Value = field.Value;
                }
            }
            var options = new JsonSerializerOptions();
            options.Converters.Add(new EventFieldConverter(converter, log));

            var results = new Dictionary<string, string>();
            foreach (var kvp in parents)
            {
                string result = JsonSerializer.Serialize(kvp.Value, options);
                if (result.StartsWith('"') && result.EndsWith('"'))
                {
                    result = result[1..^1];
                }
                results[kvp.Key] = result;
            }

            return results;
        }
        /// <summary>
        /// Converts event into array of bytes which may be written to file.
        /// The format is [ushort length][string message][string eventId][string sourceNode][long timestamp][string type]
        /// [string emitter][ushort metadata count][[string key][string value]...]
        /// Strings are stored on the format [ushort length][string]
        /// </summary>
        /// <param name="extractor">Extractor to use for nodeId conversions</param>
        /// <returns>Array of converted bytes</returns>
        public byte[] ToStorableBytes(UAExtractor extractor)
        {
            var outputBuffer = new List<byte>();
            outputBuffer.AddRange(CogniteUtils.StringToStorable(Message));
            outputBuffer.AddRange(CogniteUtils.StringToStorable(EventId));
            outputBuffer.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(SourceNode)));
            outputBuffer.AddRange(BitConverter.GetBytes(Time.ToBinary()));
            outputBuffer.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(EventType?.Id)));
            outputBuffer.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(EmittingNode)));
            if (Values == null)
            {
                outputBuffer.AddRange(BitConverter.GetBytes(0));
            }
            else
            {
                outputBuffer.AddRange(BitConverter.GetBytes(Values.Count));
                foreach (var v in Values.Values)
                {
                    v.ToStorableBytes(outputBuffer, extractor);
                }
            }
            return outputBuffer.ToArray();
        }
        /// <summary>
        /// Read event from given stream. See BufferedEvent.ToStorableBytes for details.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="extractor">Extractor to use for nodeId conversions</param>
        /// <returns>Converted event</returns>
        public static UAEvent? FromStream(Stream stream, UAExtractor extractor, ILogger log)
        {
            var evt = new UAEvent
            {
                Message = CogniteUtils.StringFromStream(stream),
                EventId = CogniteUtils.StringFromStream(stream),
                SourceNode = extractor.State.GetNodeId(CogniteUtils.StringFromStream(stream))
            };
            var buffer = new byte[sizeof(long)];

            if (stream.Read(buffer, 0, sizeof(long)) < sizeof(long)) return null;
            long dt = BitConverter.ToInt64(buffer, 0);
            evt.Time = DateTime.FromBinary(dt);
            var typeId = extractor.State.GetNodeId(CogniteUtils.StringFromStream(stream));
            evt.EventType = extractor.State.ActiveEvents.GetValueOrDefault(typeId);
            evt.EmittingNode = extractor.State.GetNodeId(CogniteUtils.StringFromStream(stream));

            if (stream.Read(buffer, 0, sizeof(int)) < sizeof(int)) return null;
            int count = BitConverter.ToInt32(buffer, 0);
            var values = new List<EventFieldValue>(count);
            for (int i = 0; i < count; i++)
            {
                var r = EventFieldValue.FromStream(stream, extractor);
                if (r == null) return null;
                values.Add(r);
            }
            evt.SetMetadata(extractor.StringConverter, values, log);
            evt.Values = values.ToDictionary(v => v.Field);

            return evt;
        }
        /// <summary>
        /// Convert common properties to a CDF event, this does not handle
        /// assetId, as that will depend on whether we are converting to <see cref="StatelessEventCreate"/>
        /// or <see cref="EventCreate"/>.
        /// </summary>
        /// <param name="client">Access to client for converting properties to string</param>
        /// <param name="evt">Event to populate</param>
        /// <param name="dataSetId">Optional dataSetId to set</param>
        private void ToCDFEventBase(IUAClientAccess client, EventCreate evt, long? dataSetId)
        {
            evt.Description = Message;
            evt.StartTime = MetaData != null && MetaData.TryGetValue("StartTime", out var rawStartTime)
                ? PusherUtils.GetTimestampValue(rawStartTime)
                : Time.ToUnixTimeMilliseconds();
            evt.EndTime = MetaData != null && MetaData.TryGetValue("EndTime", out var rawEndTime)
                ? PusherUtils.GetTimestampValue(rawEndTime)
                : Time.ToUnixTimeMilliseconds();
            evt.ExternalId = EventId;
            evt.Type = MetaData != null && MetaData.TryGetValue("Type", out var rawType)
                ? client.StringConverter.ConvertToString(rawType)
                : client.GetUniqueId(EventType?.Id);
            evt.DataSetId = dataSetId;

            var finalMetaData = new Dictionary<string, string>
            {
                { "Emitter", client.GetUniqueId(EmittingNode) ?? "null" },
                { "TypeName", EventType?.Name ?? "null" }
            };
            if (MetaData == null)
            {
                evt.Metadata = finalMetaData;
                return;
            }
            if (!MetaData.ContainsKey("SourceNode") && SourceNode != null && !SourceNode.IsNullNodeId)
            {
                finalMetaData["SourceNode"] = client.GetUniqueId(SourceNode)!;
            }
            if (MetaData.TryGetValue("SubType", out var subtype))
            {
                evt.Subtype = client.StringConverter.ConvertToString(subtype);
            }

            foreach (var dt in MetaData)
            {
                if (!excludeMetaData.Contains(dt.Key))
                {
                    finalMetaData[dt.Key] = dt.Value;
                }
            }

            if (finalMetaData.Count != 0)
            {
                evt.Metadata = finalMetaData;
            }
        }

        private static readonly HashSet<string> excludeMetaData = new HashSet<string> {
            "StartTime", "EndTime", "Type", "SubType"
        };
        /// <summary>
        /// Convert event to stateless CDF event.
        /// </summary>
        /// <param name="client">Access to OPC-UA client for converting to string</param>
        /// <param name="dataSetId">Optional dataSetId</param>
        /// <param name="parentIdMap">Map from parent NodeIds to externalIds</param>
        /// <returns>Converted event or null</returns>
        public StatelessEventCreate ToStatelessCDFEvent(
            IUAClientAccess client,
            long? dataSetId,
            IDictionary<NodeId, string?> parentIdMap)
        {
            string? sourceId = null;
            if (SourceNode != null && !SourceNode.IsNullNodeId)
            {
                if (parentIdMap != null && parentIdMap.TryGetValue(SourceNode, out var parentId))
                {
                    sourceId = parentId;
                }
                else
                {
                    sourceId = client.GetUniqueId(SourceNode);
                }
            }

            var evt = new StatelessEventCreate
            {
                AssetExternalIds = sourceId == null
                    ? Enumerable.Empty<string>()
                    : new string[] { sourceId }
            };
            ToCDFEventBase(client, evt, dataSetId);

            return evt;
        }
        /// <summary>
        /// Convert event to CDF event.
        /// </summary>
        /// <param name="client">Access to OPC-UA client for converting to string</param>
        /// <param name="dataSetId">Optional dataSetId</param>
        /// <param name="nodeToAssetIds">Map from parent NodeIds to internalIds</param>
        /// <returns>Converted event or null</returns>
        public EventCreate ToCDFEvent(
            IUAClientAccess client,
            long? dataSetId,
            IDictionary<NodeId, long> nodeToAssetIds)
        {
            var evt = new EventCreate();

            if (nodeToAssetIds != null
                && SourceNode != null
                && !SourceNode.IsNullNodeId
                && nodeToAssetIds.TryGetValue(SourceNode, out var assetId))
            {
                evt.AssetIds = new List<long> { assetId };
            }
            ToCDFEventBase(client, evt, dataSetId);
            return evt;
        }
    }
    public class EventFieldValue
    {
        public RawTypeField Field { get; }
        public Variant Value { get; }

        public bool IsBuiltIn
        {
            get
            {
                return Field.Name == "Message" || Field.Name == "EventId"
                    || Field.Name == "SourceNode" || Field.Name == "Time"
                    || Field.Name == "EventType";
            }
        }
        public EventFieldValue(RawTypeField field, Variant value)
        {
            Field = field;
            Value = value;
        }

        public void ToStorableBytes(List<byte> outputBuffer, UAExtractor extractor)
        {
            Field.ToStorableBytes(outputBuffer);
            var encoder = new BinaryEncoder(extractor.Context.MessageContext);
            encoder.WriteVariant(null, Value);
            outputBuffer.AddRange(encoder.CloseAndReturnBuffer());
        }

        public static EventFieldValue? FromStream(Stream stream, UAExtractor extractor)
        {
            var field = RawTypeField.FromStream(stream);
            if (field == null) return null;
            try
            {
                var decoder = new BinaryDecoder(stream, extractor.Context.MessageContext);
                var variant = decoder.ReadVariant(null);
                return new EventFieldValue(field, variant);
            }
            catch
            {
                return null;
            }
        }
    }
    internal class EventFieldNode
    {
        public IDictionary<string, EventFieldNode> Children { get; } = new Dictionary<string, EventFieldNode>();
        public Variant? Value { get; set; }
    }

    internal class EventFieldConverter : JsonConverter<EventFieldNode>
    {
        private readonly ILogger log;
        private readonly StringConverter converter;
        public EventFieldConverter(StringConverter converter, ILogger log)
        {
            this.converter = converter;
            this.log = log;
        }

        public override EventFieldNode? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }

        private void WriteValueSafe(Utf8JsonWriter writer, EventFieldNode field)
        {
            var value = converter.ConvertToString(field.Value, null, null, StringConverterMode.Json);
            try
            {
                writer.WriteRawValue(value);
            }
            catch (Exception ex)
            {
                log.LogWarning("Serialization of value on event field: {Message}, {Json}",
                    ex.Message, value);
                writer.WriteNullValue();
            }
        }

        public override void Write(Utf8JsonWriter writer, EventFieldNode value, JsonSerializerOptions options)
        {
            if (value.Children.Any())
            {
                writer.WriteStartObject();
                int valIdx = 1;
                foreach (var kvp in value.Children)
                {
                    string key = kvp.Key;
                    if (key == "Value")
                    {
                        do
                        {
                            key = $"Value{valIdx++}";
                        } while (value.Children.ContainsKey(key));
                    }
                    writer.WritePropertyName(key);
                    JsonSerializer.Serialize(writer, kvp.Value, options);
                }
                if (value.Value.HasValue)
                {
                    writer.WritePropertyName("Value");
                    WriteValueSafe(writer, value);
                }
                writer.WriteEndObject();
            }
            else if (value.Value.HasValue)
            {
                WriteValueSafe(writer, value);
            }
        }
    }
}

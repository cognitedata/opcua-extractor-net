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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Cognite.Extensions;
using Opc.Ua;

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
        public string Message { get; set; }
        /// <summary>
        /// Transformed ID of the event. The Raw id is a byte-string. This is the byte-string transformed into Base64 and prepended the globalprefix.
        /// </summary>
        public string EventId { get; set; } // Base64
        /// <summary>
        /// NodeId of the SourceNode
        /// </summary>
        public NodeId SourceNode { get; set; }
        /// <summary>
        /// Time this event triggered.
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// NodeId of the eventType of this event.
        /// </summary>
        public NodeId EventType { get; set; }
        /// <summary>
        /// string->object dictionary of the remaining properties that haven't been filtered out.
        /// </summary>
        public Dictionary<string, object> MetaData { get; set; }
        /// <summary>
        /// Id of the node that emitted the event in opc-ua
        /// </summary>
        public NodeId EmittingNode { get; set; }

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.AppendFormat(CultureInfo.InvariantCulture, "Event: {0}\n", EventId);
            builder.AppendFormat(CultureInfo.InvariantCulture, "Time: {0}\n", Time);
            builder.AppendFormat(CultureInfo.InvariantCulture, "Type: {0}\n", EventType);
            builder.AppendFormat(CultureInfo.InvariantCulture, "Emitter: {0}\n", EmittingNode);
            if (Message != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "Message: {0}\n", Message);
            }
            if (SourceNode != null && !SourceNode.IsNullNodeId)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "SourceNode: {0}\n", SourceNode);
            }
            if (MetaData != null && MetaData.Any())
            {
                builder.Append("MetaData: {\n");
                foreach (var kvp in MetaData)
                {
                    builder.AppendFormat(CultureInfo.InvariantCulture, "    {0}: {1}\n", kvp.Key, kvp.Value);
                }
                builder.Append("}\n");
            }

            return builder.ToString();
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
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            var bytes = new List<byte>();
            bytes.AddRange(CogniteUtils.StringToStorable(Message));
            bytes.AddRange(CogniteUtils.StringToStorable(EventId));
            bytes.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(SourceNode)));
            bytes.AddRange(BitConverter.GetBytes(Time.ToBinary()));
            bytes.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(EventType)));
            bytes.AddRange(CogniteUtils.StringToStorable(extractor.GetUniqueId(EmittingNode)));
            var metaDataBytes = new List<byte>();
            ushort count = 0;
            if (MetaData != null)
            {
                foreach (var kvp in MetaData)
                {
                    count++;
                    metaDataBytes.AddRange(CogniteUtils.StringToStorable(kvp.Key));
                    metaDataBytes.AddRange(CogniteUtils.StringToStorable(extractor.ConvertToString(kvp.Value)));
                }
            }

            bytes.AddRange(BitConverter.GetBytes(count));
            bytes.AddRange(metaDataBytes);
            return bytes.ToArray();
        }
        /// <summary>
        /// Read event from given stream. See BufferedEvent.ToStorableBytes for details.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="extractor">Extractor to use for nodeId conversions</param>
        /// <returns>Converted event</returns>
        public static UAEvent FromStream(Stream stream, UAExtractor extractor)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            var evt = new UAEvent();
            evt.Message = CogniteUtils.StringFromStream(stream);
            evt.EventId = CogniteUtils.StringFromStream(stream);
            evt.SourceNode = extractor.State.GetNodeId(CogniteUtils.StringFromStream(stream));
            var buffer = new byte[sizeof(long)];

            if (stream.Read(buffer, 0, sizeof(long)) < sizeof(long)) return null;
            long dt = BitConverter.ToInt64(buffer, 0);
            evt.Time = DateTime.FromBinary(dt);
            evt.EventType = extractor.State.GetNodeId(CogniteUtils.StringFromStream(stream));
            evt.EmittingNode = extractor.State.GetEmitterState(CogniteUtils.StringFromStream(stream))?.SourceId ?? NodeId.Null;

            if (stream.Read(buffer, 0, sizeof(ushort)) < sizeof(ushort)) return null;
            ushort count = BitConverter.ToUInt16(buffer, 0);

            evt.MetaData = new Dictionary<string, object>();

            for (int i = 0; i < count; i++)
            {
                string key = CogniteUtils.StringFromStream(stream);
                string value = CogniteUtils.StringFromStream(stream);
                evt.MetaData[key] = value;
            }

            return evt;
        }
    }
}

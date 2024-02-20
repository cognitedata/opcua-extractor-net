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

using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;

namespace Cognite.OpcUa.Config
{
    public class EventConfig
    {
        /// <summary>
        /// Event ids to map, with full namespace-uri, and node identifier on the form "i=123" or "s=somestring"
        /// Custom events must be subtypes of the BaseEventType.
        /// This is used to specify which specific events should be extracted, instead of just extracting all events.
        /// </summary>
        public IEnumerable<ProtoNodeId>? EventIds { get; set; }
        /// <summary>
        /// Id of nodes to be observed as event emitters. Empty Namespace/NodeId defaults to the server node.
        /// This is used to add extra emitters that are not in the extracted node hierarchy, or that does not
        /// correctly specify the EventNotifier property. 
        /// </summary>
        public IEnumerable<ProtoNodeId>? EmitterIds { get; set; }
        /// <summary>
        /// Subset of the emitter-ids property. Used to make certain emitters historical.
        /// Requires the events.history property to be true
        /// </summary>
        public IEnumerable<ProtoNodeId>? HistorizingEmitterIds { get; set; }
        /// <summary>
        /// True to enable reading events from the server.
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// Automatically treat nodes with suitable EventNotifier as emitters.
        /// </summary>
        [DefaultValue(true)]
        public bool DiscoverEmitters { get; set; } = true;
        /// <summary>
        /// Enable reading both custom events and base opc-ua events.
        /// </summary>
        [DefaultValue(true)]
        public bool AllEvents { get; set; } = true;
        /// <summary>
        /// True to read event history if it is enabled.
        /// </summary>
        public bool History { get; set; }
        /// <summary>
        /// Regex filter on event type DisplayName, matches will not be extracted.
        /// </summary>
        public string? ExcludeEventFilter { get; set; }
        /// <summary>
        /// True to also check the server node when looking for event emitters, default true.
        /// </summary>
        [DefaultValue(true)]
        public bool ReadServer { get; set; } = true;
        /// <summary>
        /// List of BrowseName for properties to be excluded from automatic mapping to destination metadata.
        /// All event properties are read, by default only "Time" and "Severity" are used from the base event.
        /// Be aware that a maximum of 16 metadata entries are allowed in CDF.
        /// </summary>
        public IEnumerable<string> ExcludeProperties { get => excludeProperties; set => excludeProperties = value ?? excludeProperties; }
        private IEnumerable<string> excludeProperties = new List<string>();
        public IEnumerable<string> BaseExcludeProperties
        {
            get => baseExcludeProperties;
            set => baseExcludeProperties = value ?? baseExcludeProperties;
        }
        private IEnumerable<string> baseExcludeProperties = new List<string> { "LocalTime", "ReceiveTime", "ConditionClassId", "ConditionClassName", "ConditionSubClassId", "ConditionSubClassName" };
        /// <summary>
        /// Map source browse names to other values in the destination. For CDF, internal properties may be overwritten, by default
        /// "Message" is mapped to description, "SourceNode" is used for context and "EventType" is used for type.These may also be excluded or replaced by 
        /// overrides in DestinationNameMap. If multiple properties are mapped to the same value, the first non-null is used.
        ///
        /// If "StartTime", "EndTime" or "SubType" are specified, either directly or through the map, these are used as event properties instead of metadata.
        /// StartTime and EndTime should be either DateTime, or a number corresponding to the number of milliseconds since January 1 1970.
        /// If no StartTime or EndTime are specified, both are set to the "Time" property of BaseEventType.
        /// "Type" may be overriden case-by-case using "NameOverrides" in Extraction configuration, or in a dynamic way here. If no "Type" is specified,
        /// it is generated from Event NodeId in the same way ExternalIds are generated for normal nodes.
        /// </summary>
        public Dictionary<string, string> DestinationNameMap { get => destinationNameMap; set => destinationNameMap = value ?? destinationNameMap; }
        private Dictionary<string, string> destinationNameMap = new Dictionary<string, string>();

        public HashSet<NodeId>? GetWhitelist(SessionContext context, ILogger logger)
        {
            if (EventIds == null || !EventIds.Any()) return null;
            var whitelist = new HashSet<NodeId>();
            foreach (var proto in EventIds)
            {
                var id = proto.ToNodeId(context);
                if (id.IsNullNodeId)
                {
                    throw new ConfigurationException($"Failed to convert event id {proto.NamespaceUri} {proto.NodeId} to NodeId");
                }

                whitelist.Add(id);
            }
            return whitelist;
        }

        public HashSet<NodeId> GetEmitterIds(SessionContext context, ILogger logger)
        {
            if (EmitterIds == null || !EmitterIds.Any()) return new HashSet<NodeId>();
            var ids = new HashSet<NodeId>();
            foreach (var proto in EmitterIds)
            {
                var id = proto.ToNodeId(context);
                if (id.IsNullNodeId)
                {
                    throw new ConfigurationException($"Failed to convert emitter id {proto.NamespaceUri} {proto.NodeId} to NodeId");
                }

                ids.Add(id);
            }
            return ids;
        }

        public HashSet<NodeId> GetHistorizingEmitterIds(SessionContext context, ILogger logger)
        {
            if (HistorizingEmitterIds == null || !HistorizingEmitterIds.Any()) return new HashSet<NodeId>();
            var ids = new HashSet<NodeId>();
            foreach (var proto in HistorizingEmitterIds)
            {
                var id = proto.ToNodeId(context);
                if (id.IsNullNodeId)
                {
                    throw new ConfigurationException($"Failed to convert historizing emitter id {proto.NamespaceUri} {proto.NodeId} to NodeId");
                }
                ids.Add(id);
            }
            return ids;
        }
    }
}

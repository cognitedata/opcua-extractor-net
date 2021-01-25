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

using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

namespace Cognite.OpcUa.TypeCollectors
{
    public class EventField
    {
        public NodeId TypeId { get; }
        public QualifiedName BrowseName { get; }
        public EventField(NodeId typeId, QualifiedName browseName)
        {
            TypeId = typeId;
            BrowseName = browseName;
        }
        // The default hash code of browsename does not include the namespaceindex for some reason.
        public override int GetHashCode()
        {
            return HashCode.Combine(TypeId, BrowseName.Name, BrowseName.NamespaceIndex);
        }
        public override bool Equals(object other)
        {
            if (!(other is EventField otherField)) return false;
            return otherField.TypeId == TypeId
                && otherField.BrowseName.Name == BrowseName.Name
                && otherField.BrowseName.NamespaceIndex == BrowseName.NamespaceIndex;
        }
    }

    /// <summary>
    /// Collects the fields of events. It does this by mapping out the entire event type hierarchy,
    /// and collecting the fields of each node on the way.
    /// </summary>
    public class EventFieldCollector
    {
        private readonly UAClient uaClient;
        private readonly Dictionary<NodeId, UAEventType> types = new Dictionary<NodeId, UAEventType>();
        private readonly EventConfig config;
        private readonly Regex ignoreFilter;
        private HashSet<string> excludeProperties;
        private HashSet<string> baseExcludeProperties;
        /// <summary>
        /// Construct the collector.
        /// </summary>
        /// <param name="parent">UAClient to be used for browse calls.</param>
        /// <param name="targetEventIds">Target event ids</param>
        public EventFieldCollector(UAClient parent, EventConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            uaClient = parent;
            this.config = config;
            if (!string.IsNullOrEmpty(config.ExcludeEventFilter))
            {
                ignoreFilter = new Regex(config.ExcludeEventFilter, RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.CultureInvariant);
            }
            excludeProperties = new HashSet<string>(config.ExcludeProperties);
            baseExcludeProperties = new HashSet<string>(config.BaseExcludeProperties);
        }
        /// <summary>
        /// Main collection function. Calls BrowseDirectory on BaseEventType, waits for it to complete, which should populate properties and localProperties,
        /// then collects the resulting fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).
        /// </summary>
        /// <returns>The collected fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).</returns>
        public Dictionary<NodeId, HashSet<EventField>> GetEventIdFields(CancellationToken token)
        {
            types[ObjectTypeIds.BaseEventType] = new UAEventType
            {
                Id = ObjectTypeIds.BaseEventType,
                DisplayName = "BaseEventType"
            };

            uaClient.BrowseDirectory(new List<NodeId> { ObjectTypeIds.BaseEventType },
                EventTypeCallback, token, ReferenceTypeIds.HierarchicalReferences, (uint)NodeClass.ObjectType | (uint)NodeClass.Variable);

            var result = new Dictionary<NodeId, HashSet<EventField>>();

            HashSet<NodeId> whitelist = null;
            if (config.EventIds != null && config.EventIds.Any())
            {
                whitelist = new HashSet<NodeId>(config.EventIds.Select(proto => proto.ToNodeId(uaClient, ObjectTypeIds.BaseEventType)));
            }

            foreach (var type in types.Values)
            {
                if (ignoreFilter != null && ignoreFilter.IsMatch(type.DisplayName.Text)) continue;
                if (whitelist != null && whitelist.Any())
                {
                    if (!whitelist.Contains(type.Id)) continue;
                }
                else if (!config.AllEvents && type.Id.NamespaceIndex == 0) continue;
                result[type.Id] = new HashSet<EventField>(type.CollectedFields);
            }

            return result;
        }
        /// <summary>
        /// HandleNode callback for the event type mapping.
        /// </summary>
        /// <param name="child">Type or property to be handled</param>
        /// <param name="parent">Parent type id</param>
        private void EventTypeCallback(ReferenceDescription child, NodeId parent)
        {
            var id = uaClient.ToNodeId(child.NodeId);
            var parentType = types.GetValueOrDefault(parent);

            if (child.NodeClass == NodeClass.ObjectType)
            {
                types[id] = new UAEventType
                {
                    Id = id,
                    Parent = parentType,
                    DisplayName = child.DisplayName
                };
            }
            else if (child.ReferenceTypeId == ReferenceTypeIds.HasProperty)
            {
                if (parentType == null) return;
                if (parent == ObjectTypeIds.BaseEventType && baseExcludeProperties.Contains(child.BrowseName.Name)
                    || excludeProperties.Contains(child.BrowseName.Name)) return;
                parentType.AddField(child);
            }
        }
        private class UAEventType
        {
            public NodeId Id { get; set; }
            public LocalizedText DisplayName { get; set; }
            public UAEventType Parent { get; set; }
            public void AddField(ReferenceDescription desc)
            {
                fields.Add(new EventField(Id, desc.BrowseName));
            }
            public IEnumerable<EventField> CollectedFields { get => Parent?.CollectedFields?.Concat(fields) ?? fields; }
            private List<EventField> fields = new List<EventField>();
        }
    }
}

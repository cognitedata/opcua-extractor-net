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
    /// <summary>
    /// Collects the fields of events. It does this by mapping out the entire event type hierarchy,
    /// and collecting the fields of each node on the way.
    /// </summary>
    public class EventFieldCollector
    {
        private readonly UAClient uaClient;
        private readonly Dictionary<NodeId, BufferedEventType> types = new Dictionary<NodeId, BufferedEventType>();
        private readonly EventConfig config;
        private readonly Regex ignoreFilter;
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
        }
        /// <summary>
        /// Main collection function. Calls BrowseDirectory on BaseEventType, waits for it to complete, which should populate properties and localProperties,
        /// then collects the resulting fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).
        /// </summary>
        /// <returns>The collected fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).</returns>
        public Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> GetEventIdFields(CancellationToken token)
        {
            types[ObjectTypeIds.BaseEventType] = new BufferedEventType
            {
                Id = ObjectTypeIds.BaseEventType,
                CollectedFields = new List<ReferenceDescription>(),
                Properties = new List<ReferenceDescription>(),
                DisplayName = "BaseEventType",
                ParentId = NodeId.Null
            };

            uaClient.BrowseDirectory(new List<NodeId> { ObjectTypeIds.BaseEventType },
                EventTypeCallback, token, ReferenceTypeIds.HierarchicalReferences, (uint)NodeClass.ObjectType | (uint)NodeClass.Variable);

            var result = new Dictionary<NodeId, List<(NodeId, QualifiedName)>>();

            var excludeProperties = new HashSet<string>(config.ExcludeProperties);
            var baseExcludeProperties = new HashSet<string>(config.BaseExcludeProperties);

            var propVariables = new Dictionary<NodeId, (NodeId, QualifiedName)>();
            // Find reverse mappings from properties to their parents, along with their browse name
            foreach (var type in types.Values)
            {
                foreach (var description in type.Properties)
                {

                    if (!propVariables.ContainsKey(uaClient.ToNodeId(description.NodeId)))
                    {
                        propVariables[uaClient.ToNodeId(description.NodeId)] = (type.Id, description.BrowseName);
                    }
                }
            }

            HashSet<NodeId> whitelist = null;
            if (config.EventIds != null && config.EventIds.Any())
            {
                whitelist = new HashSet<NodeId>(config.EventIds.Select(proto => proto.ToNodeId(uaClient, ObjectTypeIds.BaseEventType)));
            }
            // Add mappings to result
            foreach (var type in types.Values)
            {
                if (ignoreFilter != null && ignoreFilter.IsMatch(type.DisplayName.Text)) continue;
                if (!config.AllEvents && type.Id.NamespaceIndex == 0) continue;
                if (whitelist != null && whitelist.Any() && !whitelist.Contains(type.Id)) continue;
                result[type.Id] = new List<(NodeId, QualifiedName)>();
                foreach (var desc in type.CollectedFields)
                {
                    if (excludeProperties.Contains(desc.BrowseName.Name)
                        || baseExcludeProperties.Contains(desc.BrowseName.Name) && type.Id == ObjectTypeIds.BaseEventType) continue;
                    result[type.Id].Add(propVariables[uaClient.ToNodeId(desc.NodeId)]);
                }
            }

            return result.ToDictionary(kvp => kvp.Key, kvp => (IEnumerable<(NodeId, QualifiedName)>)kvp.Value);
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
                types[id] = new BufferedEventType
                {
                    Id = id,
                    ParentId = parent,
                    Properties = new List<ReferenceDescription>(),
                    CollectedFields = parentType?.CollectedFields?.ToList() ?? new List<ReferenceDescription>(),
                    DisplayName = child.DisplayName
                };
            }
            if (child.ReferenceTypeId == ReferenceTypeIds.HasProperty)
            {
                if (parentType == null) return;
                parentType.Properties.Add(child);
                parentType.CollectedFields.Add(child);
            }
        }
        private class BufferedEventType
        {
            public NodeId Id { get; set; }
            public LocalizedText DisplayName { get; set; }
            public NodeId ParentId { get; set; }
            public List<ReferenceDescription> Properties { get; set; }
            public List<ReferenceDescription> CollectedFields { get; set; }
        }
    }
}

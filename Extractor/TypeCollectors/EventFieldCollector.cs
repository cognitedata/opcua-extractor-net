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

using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.TypeCollectors
{
    /// <summary>
    /// Represents a single property with value in the event hierarchy.
    /// </summary>
    public class EventField
    {
        public QualifiedNameCollection BrowsePath { get; }
        public string Name => BrowsePath.Last().Name;
        public EventField(QualifiedName browseName)
        {
            BrowsePath = new QualifiedNameCollection() { browseName };
        }
        public EventField(QualifiedNameCollection browsePath)
        {
            BrowsePath = browsePath;
        }
        // The default hash code of browsename does not include the namespaceindex for some reason.
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 0;
                foreach (var name in BrowsePath)
                {
                    hash *= 31;
                    hash += HashCode.Combine(name.Name, name.NamespaceIndex);
                }
                return hash;
            }
        }
        public override bool Equals(object obj)
        {
            if (obj is not EventField otherField) return false;
            if (BrowsePath.Count != otherField.BrowsePath.Count) return false;

            for (int i = 0; i < BrowsePath.Count; i++)
            {
                if (BrowsePath[i].Name != otherField.BrowsePath[i].Name
                    || BrowsePath[i].NamespaceIndex != otherField.BrowsePath[i].NamespaceIndex) return false;
            }

            return true;
        }
    }

    public interface IEventFieldSource
    {
        Task<Dictionary<NodeId, UAEventType>> GetEventIdFields(CancellationToken token);
    }

    /// <summary>
    /// Collects the fields of events. It does this by mapping out the entire event type hierarchy,
    /// and collecting the fields of each node on the way.
    /// </summary>
    public partial class EventFieldCollector : IEventFieldSource
    {
        private readonly UAClient uaClient;
        private readonly Dictionary<NodeId, UAEventType> types = new Dictionary<NodeId, UAEventType>();
        private readonly Dictionary<NodeId, ChildNode> nodes = new Dictionary<NodeId, ChildNode>();
        private readonly EventConfig config;
        private readonly Regex? ignoreFilter;
        private readonly HashSet<string> excludeProperties;
        private readonly HashSet<string> baseExcludeProperties;
        private readonly ILogger log;
        /// <summary>
        /// Construct the collector.
        /// </summary>
        /// <param name="parent">UAClient to be used for browse calls.</param>
        /// <param name="config">Event configuration to use</param>
        public EventFieldCollector(ILogger log, UAClient parent, EventConfig config)
        {
            this.log = log;
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
        /// Main collection function. Browses the hierarchy and builds an internal tree of event types,
        /// then builds a list of eventfields for each type by recursively collecting the properties of all parents.
        /// </summary>
        /// <returns>The collected fields in a dictionary on the form EventTypeId -> (SourceTypeId, BrowseName).</returns>
        public async Task<Dictionary<NodeId, UAEventType>> GetEventIdFields(CancellationToken token)
        {
            types[ObjectTypeIds.BaseEventType] = new UAEventType(ObjectTypeIds.BaseEventType, "BaseEventType");

            log.LogInformation("Browse event type hierarchy to map out event types");
            await uaClient.Browser.BrowseDirectory(
                new List<NodeId> { ObjectTypeIds.BaseEventType },
                EventTypeCallback,
                token,
                ReferenceTypeIds.HierarchicalReferences,
                (uint)NodeClass.ObjectType | (uint)NodeClass.Variable | (uint)NodeClass.Object,
                doFilter: false,
                purpose: "the event type hierarchy");

            var result = new Dictionary<NodeId, UAEventType>();

            HashSet<NodeId>? whitelist = config.GetWhitelist(uaClient, log);

            foreach (var type in types.Values)
            {
                if (ignoreFilter != null && ignoreFilter.IsMatch(type.DisplayName.Text)) continue;
                if (whitelist != null && whitelist.Any())
                {
                    if (!whitelist.Contains(type.Id)) continue;
                }
                else if (!config.AllEvents && type.Id.NamespaceIndex == 0) continue;
                result[type.Id] = type;
            }

            return result;
        }
        /// <summary>
        /// HandleNode callback for the event type mapping.
        /// Adds new types to the internal tree, adds objects and variables
        /// to their parents.
        /// </summary>
        /// <param name="child">Type or property to be handled</param>
        /// <param name="parent">Parent type id</param>
        private void EventTypeCallback(ReferenceDescription child, NodeId parent, bool visited)
        {
            var id = uaClient.ToNodeId(child.NodeId);

            if (child.NodeClass == NodeClass.ObjectType)
            {
                var parentType = types.GetValueOrDefault(parent);
                types[id] = new UAEventType(id, child.DisplayName)
                {
                    Parent = parentType
                };
            }
            else if (child.NodeClass == NodeClass.Object || child.NodeClass == NodeClass.Variable)
            {
                ChildNode node;
                if (types.TryGetValue(parent, out var parentType))
                {
                    if (parent == ObjectTypeIds.BaseEventType && baseExcludeProperties.Contains(child.BrowseName.Name)
                        || excludeProperties.Contains(child.BrowseName.Name)) return;
                    node = parentType.AddChild(child);
                }
                else if (nodes.TryGetValue(parent, out var parentNode))
                {
                    node = parentNode.AddChild(child);
                }
                else
                {
                    return;
                }
                if (child.NodeClass != NodeClass.Variable
                    || child.TypeDefinition != VariableTypeIds.PropertyType)
                {
                    nodes[id] = node;
                }
            }
        }
    }
}

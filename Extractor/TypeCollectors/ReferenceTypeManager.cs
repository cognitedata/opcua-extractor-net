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

using Cognite.OpcUa.History;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.TypeCollectors
{
    /// <summary>
    /// Maps out reference types, in order to find their names and inverse names
    /// </summary>
    public class ReferenceTypeManager
    {
        private readonly ILogger<ReferenceTypeManager> log;
        private readonly UAClient uaClient;
        private readonly UAExtractor extractor;
        private readonly Dictionary<NodeId, UAReferenceType> mappedTypes = new Dictionary<NodeId, UAReferenceType>();
        public ReferenceTypeManager(ILogger<ReferenceTypeManager> log, UAClient client, UAExtractor extractor)
        {
            this.log = log;
            uaClient = client;
            this.extractor = extractor;
        }
        /// <summary>
        /// Gets or creates a <see cref="UAReferenceType"/> for <paramref name="id"/>.
        /// </summary>
        /// <param name="id">Id to get type for</param>
        /// <returns>Unique type given by <paramref name="id"/></returns>
        public UAReferenceType GetReferenceType(NodeId id)
        {
            if (id == null) id = NodeId.Null;
            if (mappedTypes.TryGetValue(id, out var type)) return type;
            type = new UAReferenceType(id);
            mappedTypes[id] = type;
            return type;
        }
        /// <summary>
        /// Fetch reference type metadata for all retrieved types.
        /// </summary>
        public async Task GetReferenceTypeDataAsync(CancellationToken token)
        {
            var toRead = mappedTypes.Values.Where(type => !type.HasName && !type.Id.IsNullNodeId).ToList();
            log.LogInformation("Get reference type metadata for {Count} types", toRead.Count);
            if (!toRead.Any()) return;

            var readValueIds = toRead.SelectMany(type => new[] {
                new ReadValueId { AttributeId = Attributes.DisplayName, NodeId = type.Id },
                new ReadValueId { AttributeId = Attributes.InverseName, NodeId = type.Id }
            });

            var values = await uaClient.ReadAttributes(new ReadValueIdCollection(readValueIds), toRead.Count, token);

            for (int i = 0; i < toRead.Count; i++)
            {
                var type = toRead[i];
                type.SetNames(
                    (values[i * 2].Value as LocalizedText)?.Text,
                    (values[i * 2 + 1].Value as LocalizedText)?.Text
                );
            }
        }
        /// <summary>
        /// Get all references between nodes in <paramref name="nodes"/> with reference type as subtype of
        /// <paramref name="referenceTypes"/>.
        /// </summary>
        /// <param name="nodes">Nodes to fetch references for</param>
        /// <param name="referenceTypes">ReferenceType filter</param>
        /// <returns>List of found references</returns>
        public async Task<IEnumerable<UAReference>> GetReferencesAsync(IEnumerable<UANode> nodes, NodeId referenceTypes, CancellationToken token)
        {
            if (!nodes.Any()) return Enumerable.Empty<UAReference>();

            var nodeMap = nodes.ToDictionary(node => node.Id);

            // We only care about references to objects or variables, at least for now.
            // Only references between objects represented in the extracted hierarchy are relevant.
            log.LogInformation("Get extra references from the server for {Count} nodes", nodeMap.Count);

            var browseNodes = nodeMap.Keys.Select(node => new BrowseNode(node)).ToDictionary(node => node.Id);

            var baseParams = new BrowseParams
            {
                BrowseDirection = BrowseDirection.Both,
                NodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable,
                ReferenceTypeId = referenceTypes,
                Nodes = browseNodes
            };

            var references = await Task.Run(() => uaClient.Browser.BrowseLevel(baseParams, token));

            var results = new List<UAReference>();

            foreach (var (parentId, children) in references)
            {
                if (!nodeMap.TryGetValue(parentId, out var parentNode)) continue;
                if (!extractor.State.IsMappedNode(parentId)) continue;
                if (parentNode is UAVariable parentVar && parentVar.IsProperty) continue;
                foreach (var child in children)
                {
                    var childId = uaClient.ToNodeId(child.NodeId);
                    if (!extractor.State.IsMappedNode(childId)) continue;
                    if (child.TypeDefinition == VariableTypeIds.PropertyType) continue;

                    VariableExtractionState? childState = null;
                    if (child.NodeClass == NodeClass.Variable)
                    {
                        childState = extractor.State.GetNodeState(childId);
                    }
                    results.Add(new UAReference(
                        child.ReferenceTypeId,
                        child.IsForward,
                        parentId,
                        childId,
                        parentNode is UAVariable pVar && !pVar.IsObject,
                        childState != null && !childState.IsArray,
                        this));
                }
            }

            log.LogInformation("Found {Count} extra references", results.Count);

            return results;
        }
    }
}

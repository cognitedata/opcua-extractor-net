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

using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
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
        private readonly FullConfig config;
        public ReferenceTypeManager(FullConfig config, ILogger<ReferenceTypeManager> log, UAClient client, UAExtractor extractor)
        {
            this.log = log;
            uaClient = client;
            this.extractor = extractor;
            this.config = config;
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

            var values = await uaClient.ReadAttributes(new ReadValueIdCollection(readValueIds), toRead.Count, token,
                "reference types");

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
        public async Task<IEnumerable<UAReference>> GetReferencesAsync(
            IEnumerable<NodeId> nodes,
            NodeId referenceTypes,
            CancellationToken token,
            uint nodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable)
        {
            if (!nodes.Any()) return Enumerable.Empty<UAReference>();

            log.LogInformation("Get extra references from the server for {Count} nodes", nodes.Count());

            var browseNodes = nodes.Select(node => new BrowseNode(node)).ToDictionary(node => node.Id);

            if (config.Extraction.NodeTypes?.AsNodes ?? false)
            {
                nodeClassMask |= ((uint)NodeClass.ObjectType) | ((uint)NodeClass.VariableType);
            }

            var baseParams = new BrowseParams
            {
                BrowseDirection = BrowseDirection.Both,
                NodeClassMask = nodeClassMask,
                ReferenceTypeId = referenceTypes,
                Nodes = browseNodes
            };

            var references = await uaClient.Browser.BrowseLevel(baseParams, token, purpose: "references");

            var results = new List<UAReference>();
            foreach (var (parentId, children) in references)
            {
                var parentNode = extractor.State.GetMappedNode(parentId);
                if (parentNode == null) continue;
                foreach (var child in children)
                {
                    var childId = uaClient.ToNodeId(child.NodeId);
                    var childNode = extractor.State.GetMappedNode(childId);

                    results.Add(new UAReference(
                        type: child.ReferenceTypeId,
                        isForward: child.IsForward,
                        source: parentId,
                        target: childId,
                        sourceTs: !parentNode.IsObject,
                        targetTs: childNode != null && !childNode.IsObject,
                        isHierarchical: false,
                        manager: this));
                }
            }

            log.LogInformation("Found {Count} extra references", results.Count);

            return results;
        }
    }
}

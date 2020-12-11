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
using Serilog;
using System;
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
        private ILogger log = Log.Logger.ForContext<ReferenceTypeManager>();
        private readonly UAClient uaClient;
        private readonly UAExtractor extractor;
        private readonly Dictionary<NodeId, BufferedReferenceType> mappedTypes = new Dictionary<NodeId, BufferedReferenceType>();
        public ReferenceTypeManager(UAClient client, UAExtractor extractor)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            uaClient = client;
            this.extractor = extractor;
        }

        public BufferedReferenceType GetReferenceType(NodeId id)
        {
            if (id == null) id = NodeId.Null;
            if (mappedTypes.TryGetValue(id, out var type)) return type;
            type = new BufferedReferenceType(id);
            mappedTypes[id] = type;
            return type;
        }

        public async Task GetReferenceTypeDataAsync(CancellationToken token)
        {
            var toRead = mappedTypes.Values.Where(type => !type.HasName && !type.Id.IsNullNodeId).ToList();
            log.Information("Get reference type metadata for {cnt} types", toRead.Count);
            if (!toRead.Any()) return;

            var readValueIds = toRead.SelectMany(type => new[] {
                new ReadValueId { AttributeId = Attributes.DisplayName, NodeId = type.Id },
                new ReadValueId { AttributeId = Attributes.InverseName, NodeId = type.Id }
            });

            var values = await Task.Run(() => uaClient.ReadAttributes(new ReadValueIdCollection(readValueIds), toRead.Count, token), token);

            for (int i = 0; i < toRead.Count; i++)
            {
                var type = toRead[i];
                type.SetNames(
                    (values[i * 2].Value as LocalizedText)?.Text,
                    (values[i * 2 + 1].Value as LocalizedText)?.Text
                );
            }
        }

        public async Task<IEnumerable<BufferedReference>> GetReferencesAsync(IEnumerable<BufferedNode> nodes, NodeId referenceTypes, CancellationToken token)
        {
            if (!nodes.Any()) return Array.Empty<BufferedReference>();

            var nodeMap = nodes.ToDictionary(node => node.Id);

            // We only care about references to objects or variables, at least for now.
            // Only references between objects represented in the extracted hierarchy are relevant.
            log.Information("Get extra references from the server");
            var references = await Task.Run(() => uaClient.GetNodeChildren(
                nodeMap.Keys,
                referenceTypes,
                (uint)NodeClass.Object | (uint)NodeClass.Variable,
                token,
                BrowseDirection.Both));

            var results = new List<BufferedReference>();

            foreach (var (parentId, children) in references)
            {
                if (!nodeMap.TryGetValue(parentId, out var parentNode)) continue;
                if (parentNode is BufferedVariable parentVar && parentVar.IsProperty) continue;
                foreach (var child in children)
                {
                    var childId = uaClient.ToNodeId(child.NodeId);
                    if (child.TypeDefinition == VariableTypeIds.PropertyType) continue;
                    if (!extractor.State.IsMappedNode(childId)) continue;

                    NodeExtractionState childState = null;
                    if (child.NodeClass == NodeClass.Variable)
                    {
                        childState = extractor.State.GetNodeState(childId);
                    }
                    results.Add(new BufferedReference(child, parentNode, childId, childState, this));
                }
            }

            log.Information("Found {cnt} extra references", results.Count);

            return results;
        }
    }
}

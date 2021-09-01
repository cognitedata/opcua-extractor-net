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

using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.TypeCollectors
{
    /// <summary>
    /// Handles the type definitions of objects and variables.
    /// </summary>
    public class NodeTypeManager
    {
        private readonly ILogger log = Log.Logger.ForContext<NodeTypeManager>();
        private readonly UAClient uaClient;
        private readonly Dictionary<NodeId, UANodeType> mappedTypes = new Dictionary<NodeId, UANodeType>();

        public NodeTypeManager(UAClient client)
        {
            uaClient = client;
        }
        /// <summary>
        /// Get or create UANodeType for <paramref name="id"/>.
        /// </summary>
        /// <param name="id">TypeDefinitionId to use</param>
        /// <param name="isVariableType">True if this should be a variableType</param>
        /// <returns>Unique UANodeType given by <paramref name="id"/></returns>
        public UANodeType GetObjectType(NodeId id, bool isVariableType)
        {
            if (id == null) id = NodeId.Null;
            if (mappedTypes.TryGetValue(id, out var type)) return type;
            type = new UANodeType(id, isVariableType);
            mappedTypes[id] = type;
            return type;
        }
        /// <summary>
        /// Fetch the names and other metadata for all constructed node types.
        /// </summary>
        public async Task GetObjectTypeMetadataAsync(CancellationToken token)
        {
            var toRead = mappedTypes.Values.Where(type => !type.Id.IsNullNodeId && type.Name == null).ToList();
            log.Information("Get object type metadata for {cnt} types", toRead.Count);
            if (!toRead.Any()) return;

            var readValueIds = toRead.Select(read => new ReadValueId
            {
                AttributeId = Attributes.DisplayName,
                NodeId = read.Id
            });

            var values = await Task.Run(() => uaClient.ReadAttributes(new ReadValueIdCollection(readValueIds), toRead.Count, token), token);

            for (int i = 0; i < toRead.Count; i++)
            {
                toRead[i].Name = (values[i].Value as LocalizedText)?.Text;
            }
        }
    }
}

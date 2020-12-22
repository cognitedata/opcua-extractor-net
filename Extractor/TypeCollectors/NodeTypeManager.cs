using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            if (client == null) throw new ArgumentNullException(nameof(client));
            uaClient = client;
        }
        public UANodeType GetObjectType(NodeId id, bool isVariableType)
        {
            if (id == null) id = NodeId.Null;
            if (mappedTypes.TryGetValue(id, out var type)) return type;
            type = new UANodeType(id, isVariableType);
            mappedTypes[id] = type;
            return type;
        }
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

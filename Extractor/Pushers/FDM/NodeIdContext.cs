using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Pushers.FDM
{
    public interface INodeIdConverter
    {
        string NodeIdToString(NodeId id);
    }

    public class NodeIdExternalIdConverter : INodeIdConverter
    {
        private IUAClientAccess client;
        public NodeIdExternalIdConverter(IUAClientAccess client)
        {
            this.client = client;
        }

        public string NodeIdToString(NodeId nodeId)
        {
            return client.GetUniqueId(nodeId) ?? "i=0";
        }
    }

    public class NodeIdContext : INodeIdConverter
    {
        private Dictionary<ushort, ushort> NamespaceIndexMap { get; }

        public NodeIdContext(Dictionary<ushort, ushort> namespaceIndexMap)
        {
            NamespaceIndexMap = namespaceIndexMap;
        }

        public NodeIdContext(List<string> finalNamespaces, IEnumerable<string> serverNamespaces)
        {
            var namespaceIndexMap = new Dictionary<ushort, ushort>();

            foreach (var (ns, idx) in serverNamespaces.Select((v, i) => (v, i)))
            {
                var mappedIndex = finalNamespaces.IndexOf(ns);
                if (mappedIndex == -1) throw new InvalidOperationException("Failed to map namespace indices");
                namespaceIndexMap.Add((ushort)idx, (ushort)mappedIndex);
            }
            NamespaceIndexMap = namespaceIndexMap;
        }


        public string NodeIdToString(NodeId id)
        {
            var buf = new StringBuilder();
            var idx = NamespaceIndexMap[id.NamespaceIndex];
            NodeId.Format(buf, id.Identifier, id.IdType, idx);
            return buf.ToString();
        }
    }
}

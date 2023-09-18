﻿using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class NodeIdContext
    {
        private Dictionary<ushort, ushort> namespaceIndexMap { get; }

        public NodeIdContext(Dictionary<ushort, ushort> namespaceIndexMap)
        {
            this.namespaceIndexMap = namespaceIndexMap;
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
            this.namespaceIndexMap = namespaceIndexMap;
        }


        public string NodeIdToString(NodeId id)
        {
            var buf = new StringBuilder();
            var idx = namespaceIndexMap[id.NamespaceIndex];
            NodeId.Format(buf, id.Identifier, id.IdType, idx);
            return buf.ToString();
        }
    }
}

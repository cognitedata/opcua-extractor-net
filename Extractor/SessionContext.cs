using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa
{
    /// <summary>
    /// OPC-UA server context detached from the server itself.
    /// Having this be separate makes it easier to keep running the extractor
    /// even when connection to the server is lost.
    /// </summary>
    public class SessionContext
    {
        public IServiceMessageContext MessageContext { get; private set; }
        public ISystemContext SystemContext { get; private set; }
        public NamespaceTable NamespaceTable { get; private set; }

        private readonly FullConfig config;
        private readonly ILogger log;

        private readonly Dictionary<NodeId, string> nodeOverrides = new();
        private readonly Dictionary<ushort, string> nsPrefixMap = new();

        public SessionContext(FullConfig config, ILogger log)
        {
            NamespaceTable = new NamespaceTable(new[] {
                "http://opcfoundation.org/UA/"
            });
            MessageContext = new DummyMessageContext(NamespaceTable);
            SystemContext = new DummySystemContext(NamespaceTable);
            this.config = config;
            this.log = log;
        }

        private void InitNodeOverrides()
        {
            if (config.Extraction.NodeMap != null)
            {
                foreach (var kvp in config.Extraction.NodeMap)
                {
                    var nodeId = kvp.Value.ToNodeId(this);
                    if (nodeId.IsNullNodeId) throw new ConfigurationException($"Failed to convert nodeId override {kvp.Value.NamespaceUri} {kvp.Value.NodeId} to NodeId");

                    nodeOverrides[nodeId] = kvp.Key;
                }
            }
        }

        public void AddExternalNamespaces(string[] table)
        {
            if (table == null) return;
            foreach (var ns in table)
            {
                NamespaceTable.GetIndexOrAppend(ns);
            }
            InitNodeOverrides();
        }

        public void UpdateFromSession(ISession session)
        {
            MessageContext = session.MessageContext;
            SystemContext = session.SystemContext;
            NamespaceTable = session.NamespaceUris;
            InitNodeOverrides();
        }

        /// <summary>
        /// Converts an ExpandedNodeId into a NodeId using the Session
        /// </summary>
        /// <param name="nodeid"></param>
        /// <returns>Resulting NodeId</returns>
        public NodeId ToNodeId(ExpandedNodeId nodeid)
        {
            if (nodeid == null || nodeid.IsNull || NamespaceTable == null) return NodeId.Null;
            return ExpandedNodeId.ToNodeId(nodeid, NamespaceTable);
        }

        public NodeId ToNodeId(string? identifier, string? namespaceUri)
        {
            if (identifier == null || namespaceUri == null || NamespaceTable == null) return NodeId.Null;
            int idx = NamespaceTable.GetIndex(namespaceUri);
            if (idx < 0)
            {
                log.LogInformation("Namespace {NS} not found in {Table}", namespaceUri, NamespaceTable.ToArray());
                if (config.Extraction.NamespaceMap.ContainsValue(namespaceUri))
                {
                    string readNs = config.Extraction.NamespaceMap.First(kvp => kvp.Value == namespaceUri).Key;
                    idx = NamespaceTable.GetIndex(readNs);
                    if (idx < 0) return NodeId.Null;
                }
                else
                {
                    return NodeId.Null;
                }
            }

            string nsString = "ns=" + idx;
            return new NodeId(nsString + ";" + identifier);
        }

        /// <summary>
        /// Returns consistent unique string representation of a <see cref="NodeId"/> given its namespaceUri
        /// </summary>
        /// <remarks>
        /// NodeId is, according to spec, unique in combination with its namespaceUri. We use this to generate a consistent, unique string
        /// to be used for mapping assets and timeseries in CDF to opcua nodes.
        /// To avoid having to send the entire namespaceUri to CDF, we allow mapping Uris to prefixes in the config file.
        /// </remarks>
        /// <param name="id">Nodeid to be converted</param>
        /// <returns>Unique string representation</returns>
        public string? GetUniqueId(ExpandedNodeId id, int index = -1)
        {
            var nodeId = ToNodeId(id);
            if (nodeId.IsNullNodeId) return null;
            if (nodeOverrides.TryGetValue(nodeId, out var nodeOverride))
            {
                if (index <= -1) return nodeOverride;
                return $"{nodeOverride}[{index}]";
            }

            // ExternalIds shorter than 32 chars are unlikely, this will generally avoid at least 1 re-allocation of the buffer,
            // and usually boost performance.
            var buffer = new StringBuilder(config.Extraction.IdPrefix, 32);

            if (!nsPrefixMap.TryGetValue(nodeId.NamespaceIndex, out var prefix))
            {
                var namespaceUri = id.NamespaceUri ?? NamespaceTable!.GetString(nodeId.NamespaceIndex);
                string newPrefix;
                if (namespaceUri is not null)
                {
                    if (config.Extraction.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode))
                    {
                        newPrefix = prefixNode;
                    }
                    else
                    {
                        newPrefix = namespaceUri + ":";
                    }
                }
                else
                {
                    log.LogWarning("NodeID received with null NamespaceUri, and index not in NamespaceTable. This is likely a bug in the server. {Id}, {Index}",
                        nodeId, nodeId.NamespaceIndex);
                    newPrefix = $"UNKNOWN_NS_{nodeId.NamespaceIndex}";
                }
                nsPrefixMap[nodeId.NamespaceIndex] = prefix = newPrefix;
            }

            buffer.Append(prefix);
            // Use 0 as namespace-index. This means that the namespace is not appended, as the string representation
            // of a base namespace nodeId does not include the namespace-index, which fits our use-case.
            NodeId.Format(buffer, nodeId.Identifier, nodeId.IdType, 0);

            TrimEnd(buffer);

            if (index > -1)
            {
                // Modifying buffer.Length effectively removes the last few elements, but more efficiently than modifying strings,
                // StringBuilder is just a char array.
                // 255 is max length, Log10(Max(1, index)) + 3 is the length of the index suffix ("[123]").
                buffer.Length = Math.Min(buffer.Length, 255 - ((int)Math.Log10(Math.Max(1, index)) + 3));
                buffer.AppendFormat(CultureInfo.InvariantCulture, "[{0}]", index);
            }
            else
            {
                buffer.Length = Math.Min(buffer.Length, 255);
            }
            return buffer.ToString();
        }
        /// <summary>
        /// Used to trim the whitespace off the end of a StringBuilder
        /// </summary> 
        private static void TrimEnd(StringBuilder sb)
        {
            if (sb == null || sb.Length == 0) return;

            int i = sb.Length - 1;
            for (; i >= 0; i--)
                if (!char.IsWhiteSpace(sb[i]))
                    break;

            if (i < sb.Length - 1)
                sb.Length = i + 1;

            return;
        }

        /// <summary>
        /// Append NodeId and namespace to the given StringBuilder.
        /// </summary>
        /// <param name="buffer">Builder to append to</param>
        /// <param name="nodeId">NodeId to append</param>
        private void AppendNodeId(StringBuilder buffer, NodeId nodeId)
        {
            if (nodeOverrides.TryGetValue(nodeId, out var nodeOverride))
            {
                buffer.Append(nodeOverride);
                return;
            }

            if (!nsPrefixMap.TryGetValue(nodeId.NamespaceIndex, out var prefix))
            {
                var namespaceUri = NamespaceTable!.GetString(nodeId.NamespaceIndex);
                string newPrefix = config.Extraction.NamespaceMap.TryGetValue(namespaceUri, out string prefixNode) ? prefixNode : (namespaceUri + ":");
                nsPrefixMap[nodeId.NamespaceIndex] = prefix = newPrefix;
            }

            buffer.Append(prefix);

            NodeId.Format(buffer, nodeId.Identifier, nodeId.IdType, 0);

            TrimEnd(buffer);
        }

        /// <summary>
        /// Get string representation of NodeId on the form i=123 or s=string, etc.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private string GetNodeIdString(NodeId id)
        {
            var buffer = new StringBuilder();
            AppendNodeId(buffer, id);
            return buffer.ToString();
        }

        /// <summary>
        /// Get the unique reference id, on the form [prefix][reference-name];[sourceId];[targetId]
        /// </summary>
        /// <param name="reference">Reference to get id for</param>
        /// <returns>String reference id</returns>
        public string GetRelationshipId(UAReference reference)
        {
            var buffer = new StringBuilder(config.Extraction.IdPrefix, 64);
            buffer.Append(reference.Type.GetName(!reference.IsForward));
            buffer.Append(';');
            AppendNodeId(buffer, reference.Source.Id);
            buffer.Append(';');
            AppendNodeId(buffer, reference.Target.Id);

            if (buffer.Length > 255)
            {
                // This is an edge-case. If the id overflows, it is most sensible to cut from the
                // start of the id, as long ids are likely (from experience) to be similar to
                // system.subsystem.sensor.measurement...
                // so cutting from the start is less likely to cause conflicts
                var overflow = (int)Math.Ceiling((buffer.Length - 255) / 2.0);
                buffer = new StringBuilder(config.Extraction.IdPrefix, 255);
                buffer.Append(reference.Type.GetName(!reference.IsForward));
                buffer.Append(';');
                buffer.Append(GetNodeIdString(reference.Source.Id).AsSpan(overflow));
                buffer.Append(';');
                buffer.Append(GetNodeIdString(reference.Target.Id).AsSpan(overflow));
            }
            return buffer.ToString();
        }
    }
}

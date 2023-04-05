using Cognite.OpcUa.Nodes;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System;
using Cognite.OpcUa.Config;
using System.Threading.Tasks;
using System.Threading;
using CogniteSdk;
using System.Linq;

namespace Cognite.OpcUa.TypeCollectors
{
    public class TypeManager
    {
        public Dictionary<NodeId, BaseUANode> NodeMap { get; } = new();


        private readonly ILogger log;
        private readonly FullConfig config;
        private readonly UAClient client;

        private bool eventTypesRead = false;
        private bool dataTypesRead = false;
        public TypeManager(FullConfig config, UAClient client, ILogger log)
        {
            this.log = log;
            this.config = config;
            this.client = client;
        }

        public bool IsTypeNodeClass(NodeClass nodeClass)
        {
            return nodeClass == NodeClass.ObjectType
                || nodeClass == NodeClass.VariableType
                || nodeClass == NodeClass.ReferenceType
                || nodeClass == NodeClass.DataType;
        }

        private async Task ReadTypeData(CancellationToken token)
        {
            var toRead = new List<BaseUANode>();
            var toReadValues = new List<UAVariable>();
            foreach (var tp in NodeMap.Values)
            {
                if (tp.Attributes.IsDataRead) continue;
                if (tp.NodeClass == NodeClass.ObjectType && !config.Extraction.NodeTypes.Metadata
                    && (tp is not UAObjectType otp || !otp.IsEventType())) continue;
                if (tp.NodeClass == NodeClass.VariableType && !config.Extraction.NodeTypes.Metadata) continue;
                if (tp.NodeClass == NodeClass.DataType && !config.Extraction.DataTypes.DataTypeMetadata
                    && !config.Extraction.DataTypes.AutoIdentifyTypes) continue;
                if (tp.NodeClass == NodeClass.ReferenceType && !config.Extraction.Relationships.Enabled) continue;
                toRead.Add(tp);
                if (tp.NodeClass == NodeClass.Variable && tp is UAVariable variable) toReadValues.Add(variable);
            }
            await client.ReadNodeData(toRead, this, token);
            await client.ReadNodeValues(toReadValues, this, token);
        }

        private async Task ReadTypeHiearchies(CancellationToken token)
        {
            var rootNodes = new List<NodeId>();
            var mask = (uint)NodeClass.Variable | (uint)NodeClass.Object;
            if (config.Extraction.DataTypes.AutoIdentifyTypes && !dataTypesRead)
            {
                mask |= (uint)NodeClass.DataType;
                rootNodes.Add(DataTypeIds.BaseDataType);
            }
            if (config.Events.Enabled && !eventTypesRead)
            {
                mask |= (uint)NodeClass.ObjectType;
                rootNodes.Add(ObjectTypeIds.BaseEventType);
            }
            if (!rootNodes.Any()) return;
            await client.Browser.BrowseDirectory(rootNodes, HandleNode, token, ReferenceTypeIds.HierarchicalReferences,
                mask, doFilter: false, purpose: "the type hierarchy");
        }

        public async Task LoadTypeData(CancellationToken token)
        {
            await ReadTypeHiearchies(token);
            await ReadTypeData(token);
        }

        private void HandleNode(ReferenceDescription node, NodeId parentId, bool visited)
        {
            if (visited) return;

            var parent = NodeMap.GetValueOrDefault(parentId);

            var result = BaseUANode.Create(node, parentId, parent, client, this);

            if (result == null)
            {
                log.LogWarning("Node of unexpected type received: {Type}, {Id}", node.NodeClass, node.NodeId);
                return;
            }

            NodeMap[result.Id] = result;
            log.LogTrace("Handle node {Name}, {Id}: {Class}", result.Attributes.DisplayName, result.Id, result.NodeClass);
        }

        public UADataType GetDataType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UADataType dt)
                {
                    log.LogWarning("Requested data type {Type}, but it was not a data type: {Class} {Name}",
                        nodeId, node.NodeClass, node.Attributes.DisplayName);
                    // This is a bug in the server, but instead of crashing we return a fresh node. The extracted data may be incomplete,
                    // but not incorrect.
                    return new UADataType(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = new UADataType(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }

        public UAReferenceType GetReferenceType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UAReferenceType dt)
                {
                    log.LogWarning("Requested reference type {Type}, but it was not a reference type: {Class} {Name}",
                        nodeId, node.NodeClass, node.Attributes.DisplayName);
                    return new UAReferenceType(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = new UAReferenceType(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }

        public UAObjectType GetObjectType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UAObjectType dt)
                {
                    log.LogWarning("Requested object type {Type}, but it was not an object type: {Class} {Name}",
                        nodeId, node.NodeClass, node.Attributes.DisplayName);
                    return new UAObjectType(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = new UAObjectType(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }

        public UAVariableType GetVariableType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UAVariableType dt)
                {
                    log.LogWarning("Requested variable type {Type}, but it was not a variable type: {Class} {Name}",
                        nodeId, node.NodeClass, node.Attributes.DisplayName);
                    return new UAVariableType(nodeId);
                }
                return dt;
            }
            else
            {
                var dt = new UAVariableType(nodeId);
                NodeMap[nodeId] = dt;
                return dt;
            }
        }
    }
}

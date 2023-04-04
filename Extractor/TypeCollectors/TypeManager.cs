using Cognite.OpcUa.Nodes;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System;

namespace Cognite.OpcUa.TypeCollectors
{
    public class TypeManager
    {
        public Dictionary<NodeId, BaseUANode> NodeMap { get; } = new();


        private readonly ILogger log;
        public TypeManager(ILogger log)
        {
            this.log = log;
        }

        public bool IsTypeNodeClass(NodeClass nodeClass)
        {
            return nodeClass == NodeClass.ObjectType
                || nodeClass == NodeClass.VariableType
                || nodeClass == NodeClass.ReferenceType
                || nodeClass == NodeClass.DataType;
        }

        public BaseUANode BuildType(UAClient client, ReferenceDescription desc, BaseUANode parent)
        {
            var id = client.ToNodeId(desc.NodeId);

            if (NodeMap.TryGetValue(id, out var type))
            {
                if (type is UAVariableType vt) vt.Initialize(desc, parent);
                else if (type is UAObjectType ot) ot.Initialize(desc, parent);
                else if (type is UAReferenceType rt) rt.Initialize(desc, parent);
                else if (type is UADataType dt) dt.Initialize(desc, parent);
                return type;
            }


            var name = desc.DisplayName?.Text;

            BaseUANode node;
            switch (desc.NodeClass)
            {
                case NodeClass.VariableType: node = new UAVariableType(id, name, parent); break;
                case NodeClass.ObjectType: node = new UAObjectType(id, name, parent); break;
                case NodeClass.ReferenceType: node = new UAReferenceType(id, name, parent); break;
                case NodeClass.DataType: node = new UADataType(id, name, parent); break;
                default:
                    throw new ArgumentException($"Non-type node passed to {nameof(BuildType)}", nameof(desc));
            }

            NodeMap[node.Id] = node;
            return node;
        }

        public UADataType GetDataType(NodeId nodeId)
        {
            if (NodeMap.TryGetValue(nodeId, out var node))
            {
                if (node is not UADataType dt)
                {
                    log.LogWarning("Requested data type {Type}, but it was not a data type: {Class} {Name}",
                        nodeId, node.NodeClass, node.DisplayName);
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
                        nodeId, node.NodeClass, node.DisplayName);
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
                        nodeId, node.NodeClass, node.DisplayName);
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
                        nodeId, node.NodeClass, node.DisplayName);
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

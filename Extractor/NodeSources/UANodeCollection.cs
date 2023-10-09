using Cognite.OpcUa.Nodes;
using Opc.Ua;
using System.Collections;
using System.Collections.Generic;

namespace Cognite.OpcUa.NodeSources
{
    /// <summary>
    /// A map from a node ID to its node. Notably, this
    /// collection preserves insertion order, which is important when browsing, as the
    /// node hierarchy builder assumes that parents appear before children when iterating.
    /// 
    /// Performance concerns:
    /// Roughly equivalent to dictionary, but may be faster when iterating.
    /// Removal is very inefficient, and should be avoided.
    /// </summary>
    public class UANodeCollection : IEnumerable<BaseUANode>
    {
        private readonly Dictionary<NodeId, BaseUANode> inner = new();
        private readonly List<BaseUANode> sequential = new();

        public bool TryGetValue(NodeId id, out BaseUANode node)
        {
            return inner.TryGetValue(id, out node);
        }

        public IEnumerator<BaseUANode> GetEnumerator()
        {
            return sequential.GetEnumerator();
        }

        public int Count => sequential.Count;

        public BaseUANode? GetValueOrDefault(NodeId nodeId)
        {
            if (inner.TryGetValue(nodeId, out var node)) return node;
            return null;
        }

        public bool TryAdd(BaseUANode node)
        {
            if (!inner.TryAdd(node.Id, node)) return false;
            sequential.Add(node);
            return true;
        }

        public void Add(BaseUANode node)
        {
            inner.Add(node.Id, node);
            sequential.Add(node);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}

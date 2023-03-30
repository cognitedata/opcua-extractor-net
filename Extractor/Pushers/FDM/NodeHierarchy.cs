using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class NodeHierarchy
    {
        public Dictionary<NodeId, IEnumerable<UAReference>> ReferencesByTargetId { get; }
        public Dictionary<NodeId, IEnumerable<UAReference>> ReferencesBySourceId { get; }
        public Dictionary<NodeId, UANode> NodeMap { get; }
        public HashSet<NodeId> KnownTypeDefinitions { get; }

        public NodeHierarchy(IEnumerable<UAReference> references, IEnumerable<UANode> nodes)
        {
            ReferencesByTargetId = references.GroupBy(rf => rf.Target.Id).ToDictionary(group => group.Key, group => (IEnumerable<UAReference>)group);
            ReferencesBySourceId = references.GroupBy(rf => rf.Source.Id).ToDictionary(group => group.Key, group => (IEnumerable<UAReference>)group);
            NodeMap = nodes.ToDictionary(node => node.Id);
            KnownTypeDefinitions = new HashSet<NodeId>(nodes.Where(node => !IsChildOfType(node)).SelectNonNull(s => s.NodeType?.Id));
        }

        private bool IsChildOfType(UANode node)
        {
            if (node.NodeClass == NodeClass.ObjectType || node.NodeClass == NodeClass.VariableType)
            {
                return true;
            }
            if (node.Parent == null) return false;
            return IsChildOfType(node.Parent);
        }

        public IEnumerable<UAReference> BySource(NodeId id)
        {
            return ReferencesBySourceId.GetValueOrDefault(id) ?? Enumerable.Empty<UAReference>();
        }

        public IEnumerable<UAReference> ByTarget(NodeId id)
        {
            return ReferencesByTargetId.GetValueOrDefault(id) ?? Enumerable.Empty<UAReference>();
        }

        public UANode Get(NodeId id)
        {
            return NodeMap[id];
        }
    }
}

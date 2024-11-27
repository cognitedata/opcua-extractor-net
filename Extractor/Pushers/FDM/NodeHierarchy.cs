using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Cognite.OpcUa.Utils;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class NodeHierarchy
    {
        public IReadOnlyDictionary<NodeId, IEnumerable<UAReference>> ReferencesByTargetId { get; }
        public IReadOnlyDictionary<NodeId, IEnumerable<UAReference>> ReferencesBySourceId { get; }
        public IReadOnlyDictionary<NodeId, BaseUANode> NodeMap { get; }
        public HashSet<NodeId> KnownTypeDefinitions { get; }

        public NodeHierarchy(IEnumerable<UAReference> references, IEnumerable<BaseUANode> nodes)
            : this(references, nodes.ToDictionary(node => node.Id))
        {
        }

        public NodeHierarchy(IEnumerable<UAReference> references, IReadOnlyDictionary<NodeId, BaseUANode> nodes)
        {
            NodeMap = nodes;
            ReferencesByTargetId = references
                .Where(rf => rf.IsForward && NodeMap.ContainsKey(rf.Source.Id) && NodeMap.ContainsKey(rf.Target.Id))
                .GroupBy(rf => rf.Target.Id)
                .ToDictionary(group => group.Key, group => (IEnumerable<UAReference>)group);
            ReferencesBySourceId = references.Where(rf => rf.IsForward).GroupBy(rf => rf.Source.Id).ToDictionary(group => group.Key, group => (IEnumerable<UAReference>)group);
            KnownTypeDefinitions = new HashSet<NodeId>(nodes.Values.Where(node => node.Id.NamespaceIndex > 0 || !node.IsChildOfType).SelectNonNull(s => s.TypeDefinition));
        }

        public IEnumerable<UAReference> BySource(NodeId id)
        {
            return ReferencesBySourceId.GetValueOrDefault(id) ?? Enumerable.Empty<UAReference>();
        }

        public IEnumerable<UAReference> ByTarget(NodeId id)
        {
            return ReferencesByTargetId.GetValueOrDefault(id) ?? Enumerable.Empty<UAReference>();
        }

        public BaseUANode Get(NodeId id)
        {
            return NodeMap[id];
        }
    }
}

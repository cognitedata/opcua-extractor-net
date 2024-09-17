using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class TraverseResult
    {
        public IReadOnlyDictionary<NodeId, BaseUANode> MappedNodes { get; }
        public IEnumerable<UAReference> MappedReferences { get; }

        public TraverseResult(IReadOnlyDictionary<NodeId, BaseUANode> mappedNodes, IEnumerable<UAReference> mappedReferences)
        {
            MappedNodes = mappedNodes;
            MappedReferences = mappedReferences;
        }
    }

    public class SimpleTypeCollector
    {
        private readonly HashSet<NodeId> knownTypes;

        private readonly Dictionary<NodeId, BaseUANode> mappedNodes = new();
        private readonly HashSet<UAReference> mappedReferences = new();

        private readonly NodeHierarchy typeHierarchy;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0052:Remove unread private members", Justification = "Logger is handy")]
        private readonly ILogger log;
        public SimpleTypeCollector(ILogger log, IEnumerable<BaseUANode> nodes, IEnumerable<UAReference> references, NodeHierarchy typeHierarchy)
        {
            knownTypes = new HashSet<NodeId>();
            foreach (var node in nodes)
            {
                if (node is UAVariable vb) knownTypes.Add(vb.FullAttributes.DataType.Id);
                else if (node is UAVariableType vbt) knownTypes.Add(vbt.FullAttributes.DataType.Id);
            }
            foreach (var rf in references)
            {
                knownTypes.Add(rf.Type.Id);
            }
            this.typeHierarchy = typeHierarchy;
            this.log = log;
        }

        public TraverseResult CollectReferencedTypes()
        {
            foreach (var id in knownTypes)
            {
                if (id.IsNullNodeId) continue;
                TraverseNodeTypeUp(id, null);
            }

            return new TraverseResult(mappedNodes, mappedReferences);
        }

        private void TraverseNodeTypeUp(NodeId nodeId, UAReference? reference)
        {
            var node = typeHierarchy.Get(nodeId);

            if (!node.IsType) return;

            if (reference != null) mappedReferences.Add(reference);
            if (!mappedNodes.TryAdd(node.Id, node)) return;

            if (nodeId == DataTypeIds.BaseDataType || nodeId == ReferenceTypeIds.References) return;

            var parentRef = typeHierarchy.ByTarget(node.Id).FirstOrDefault(n => n.Type.Id == ReferenceTypeIds.HasSubtype);

            if (parentRef != null)
            {
                TraverseNodeTypeUp(parentRef.Source.Id, parentRef);
            }
        }

    }
}

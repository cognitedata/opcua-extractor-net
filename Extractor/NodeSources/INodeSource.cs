using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    public enum NodeSource
    {
        CDF,
        OPCUA
    }
    /// <summary>
    /// A map from a node ID to its node. Notably, this
    /// collection preserves insertion order, which is important when browsing, as the
    /// node hierarchy builder assumes that parents appear before children when iterating.
    /// 
    /// Performance concerns:
    /// Roughly equivalent to dictionary, but may be faster when iterating.
    /// Removal is very inefficient, and should be avoided.
    /// </summary>
    public class UANodeCollection : KeyedCollection<NodeId, BaseUANode>
    {
        protected override NodeId GetKeyForItem(BaseUANode item)
        {
            return item.Id;
        }

        public BaseUANode? GetValueOrDefault(NodeId nodeId)
        {
            if (TryGetValue(nodeId, out var node)) return node;
            return null;
        }

        public bool TryAdd(BaseUANode node)
        {
            if (Contains(node)) return false;
            Add(node);
            return true;
        }
    }


    public class NodeLoadResult
    {
        public UANodeCollection Nodes { get; }
        public IEnumerable<UAReference> References { get; }
        public bool AssumeFullyTransformed { get; }
        public bool ShouldBackgroundBrowse { get; }

        public NodeLoadResult(
            UANodeCollection nodes,
            IEnumerable<UAReference> references,
            bool assumeFullyTransformed,
            bool shouldBackgroundBrowse)
        {
            Nodes = nodes;
            References = references;
            AssumeFullyTransformed = assumeFullyTransformed;
            ShouldBackgroundBrowse = shouldBackgroundBrowse;
        }
    }

    public enum HierarchicalReferenceMode
    {
        Disabled,
        Forward,
        Both
    }

    public interface INodeSource
    {
        Task Initialize(CancellationToken token);
        Task<NodeLoadResult> LoadNodes(
            IEnumerable<NodeId> nodesToBrowse,
            uint nodeClassMask,
            HierarchicalReferenceMode hierarchicalReferences,
            CancellationToken token);
        Task<NodeLoadResult> LoadNonHierarchicalReferences(
            IReadOnlyDictionary<NodeId, BaseUANode> parentNodes,
            bool getTypeReferences,
            bool initUnknownNodes,
            CancellationToken token);
    }

    public interface ITypeAndNodeSource: INodeSource
    {
        Task LoadTypeMetadata(IEnumerable<BaseUANode> nodes, DataTypeConfig config, CancellationToken token);
    }
}

using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    public enum NodeSource
    {
        CDF,
        OPCUA
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
        /// <summary>
        /// Load nodes from the source.
        /// </summary>
        /// <param name="nodesToBrowse">Root nodes to start browsing from.</param>
        /// <param name="nodeClassMask">Node class mask to filter nodes.</param>
        /// <param name="hierarchicalReferences">Mode for hierarchical references.</param>
        /// <param name="purpose">Purpose description for logging.</param>
        /// <param name="token">Cancellation token.</param>
        Task<NodeLoadResult> LoadNodes(
            IEnumerable<NodeId> nodesToBrowse,
            uint nodeClassMask,
            HierarchicalReferenceMode hierarchicalReferences,
            string purpose,
            CancellationToken token);
        Task<NodeLoadResult> LoadNonHierarchicalReferences(
            IReadOnlyDictionary<NodeId, BaseUANode> parentNodes,
            bool getTypeReferences,
            bool initUnknownNodes,
            string purpose,
            CancellationToken token);
    }

    public interface ITypeAndNodeSource : INodeSource
    {
        Task LoadTypeMetadata(IEnumerable<BaseUANode> nodes, DataTypeConfig config, CancellationToken token);
    }
}

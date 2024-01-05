using Cognite.OpcUa.Nodes;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    internal class CDFNodeSourceWithFallback : INodeSource
    {
        private readonly CDFNodeSource cdfSource;
        private readonly INodeSource fallbackSource;

        private bool usingFallback = false;
        public CDFNodeSourceWithFallback(CDFNodeSource cdfSource, INodeSource fallback)
        {
            this.cdfSource = cdfSource;
            fallbackSource = fallback;
        }

        public async Task Initialize(CancellationToken token)
        {
            await cdfSource.Initialize(token);
        }

        public async Task<NodeLoadResult> LoadNodes(IEnumerable<NodeId> nodesToBrowse, uint nodeClassMask, HierarchicalReferenceMode hierarchicalReferences, string purpose, CancellationToken token)
        {
            var res = await cdfSource.LoadNodes(nodesToBrowse, nodeClassMask, hierarchicalReferences, purpose, token);
            if (res.Nodes.Count == 0)
            {
                usingFallback = true;
                return await fallbackSource.LoadNodes(nodesToBrowse, nodeClassMask, hierarchicalReferences, purpose, token);
            }
            return res;
        }

        public async Task<NodeLoadResult> LoadNonHierarchicalReferences(IReadOnlyDictionary<NodeId, BaseUANode> parentNodes, bool getTypeReferences, bool initUnknownNodes, string purpose, CancellationToken token)
        {
            if (usingFallback)
            {
                return await fallbackSource.LoadNonHierarchicalReferences(parentNodes, getTypeReferences, initUnknownNodes, purpose, token);
            }
            return await cdfSource.LoadNonHierarchicalReferences(parentNodes, getTypeReferences, initUnknownNodes, purpose, token);
        }
    }
}

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

        public async Task<NodeLoadResult> LoadNodes(IEnumerable<NodeId> nodesToBrowse, uint nodeClassMask, HierarchicalReferenceMode hierarchicalReferences, CancellationToken token)
        {
            var res = await cdfSource.LoadNodes(nodesToBrowse, nodeClassMask, hierarchicalReferences, token);
            if (!res.Nodes.Any())
            {
                usingFallback = true;
                return await fallbackSource.LoadNodes(nodesToBrowse, nodeClassMask, hierarchicalReferences, token);
            }
            return res;
        }

        public async Task<NodeLoadResult> LoadNonHierarchicalReferences(IReadOnlyDictionary<NodeId, BaseUANode> parentNodes, bool getTypeReferences, bool initUnknownNodes, CancellationToken token)
        {
            if (usingFallback)
            {
                return await fallbackSource.LoadNonHierarchicalReferences(parentNodes, getTypeReferences, initUnknownNodes, token);
            }
            return await cdfSource.LoadNonHierarchicalReferences(parentNodes, getTypeReferences, initUnknownNodes, token);
        }
    }
}

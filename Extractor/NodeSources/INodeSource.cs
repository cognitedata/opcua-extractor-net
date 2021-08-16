using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Cognite.OpcUa.NodeSources
{
    public interface INodeSource
    {
        public Task<BrowseResult> ParseResults(CancellationToken token);
    }
}

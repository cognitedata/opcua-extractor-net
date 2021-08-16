using Cognite.OpcUa.Pushers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    public class CDFNodeSource : INodeSource
    {
        private readonly FullConfig config;
        private readonly UAExtractor extractor;
        private readonly UAClient client;
        private readonly CDFPusher pusher;

        public CDFNodeSource(FullConfig config, UAExtractor extractor, UAClient client, CDFPusher pusher)
        {
            this.config = config;
            this.extractor = extractor;
            this.client = client;
            this.pusher = pusher;
        }

        public async Task ReadRawNodes(CancellationToken token)
        {
        }


        public async Task<BrowseResult> ParseResults(CancellationToken token)
        {
            return null;
        }
    }
}

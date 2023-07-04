using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.Destinations.Interfaces;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Destinations
{
    public class RelationshipsWriter : IRelationshipsWriter
    {
        private ILogger<RelationshipsWriter> _logger;
        private FullConfig _config;

        public FullConfig config => _config;

        public RelationshipsWriter(ILogger<RelationshipsWriter> logger, dynamic config, CancellationToken token) {
            _logger = logger;
            _config = config;
        }

        public Task PushReferences(IEnumerable<UAReference> references, BrowseReport report)
        {
            throw new System.NotImplementedException();
        }
    }
}

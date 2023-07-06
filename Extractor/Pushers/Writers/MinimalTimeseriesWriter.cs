using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class MinimalTimeseriesWriter : TimeseriesWriter, ITimeseriesWriter
    {
        public MinimalTimeseriesWriter(
            ILogger<TimeseriesWriter> logger,
            CogniteDestination destination,
            FullConfig config
        )
            : base(logger, destination, config) { }
        protected override bool createMinimalTimeseries =>  true; 
    }
}

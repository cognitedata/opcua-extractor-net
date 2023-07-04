using Cognite.OpcUa.Pushers.Writers.Interfaces;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class CDFWriter : ICDFWriter
    {
        public IRawWriter raw { get; }
        public ITimeseriesWriter timeseries { get; }
        public IAssetsWriter assets { get; }
        public IRelationshipsWriter relationships{ get; }

        public CDFWriter(
            IRawWriter rawWriter,
            ITimeseriesWriter timeseriesWriter,
            IAssetsWriter assetsWriter,
            IRelationshipsWriter relationshipsWriter
        )
        {
            this.raw = rawWriter;
            this.timeseries = timeseriesWriter;
            this.assets = assetsWriter;
            this.relationships = relationshipsWriter;
        }
    }
}

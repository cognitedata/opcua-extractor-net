using Cognite.OpcUa.Pushers.Writers.Interfaces;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class CDFWriter : ICDFWriter
    {
        public IRawWriter Raw { get; }
        public ITimeseriesWriter Timeseries { get; }
        public IAssetsWriter Assets { get; }
        public IRelationshipsWriter Relationships{ get; }
        public ITimeseriesWriter MinimalTimeseries { get; }

        public CDFWriter(
            IRawWriter rawWriter,
            ITimeseriesWriter timeseriesWriter,
            IAssetsWriter assetsWriter,
            IRelationshipsWriter relationshipsWriter,
            ITimeseriesWriter minimalTimeSeriesWriter
        )
        {
            this.Raw = rawWriter;
            this.Timeseries = timeseriesWriter;
            this.Assets = assetsWriter;
            this.Relationships = relationshipsWriter;
            this.MinimalTimeseries = minimalTimeSeriesWriter;
        }
    }
}

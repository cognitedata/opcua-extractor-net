using Cognite.OpcUa.Pushers.FDM;
using Cognite.OpcUa.Pushers.Writers.Interfaces;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class CDFWriter : ICDFWriter
    {
        public IRawWriter? Raw { get; }
        public ITimeseriesWriter Timeseries { get; }
        public IAssetsWriter? Assets { get; }
        public IRelationshipsWriter? Relationships { get; }
        public FDMWriter? FDM { get; }

        public CDFWriter(
            ITimeseriesWriter timeseriesWriter,
            IRawWriter? rawWriter,
            IAssetsWriter? assetsWriter,
            IRelationshipsWriter? relationshipsWriter,
            FDMWriter? fdmWriter
        )
        {
            Raw = rawWriter;
            Timeseries = timeseriesWriter;
            Assets = assetsWriter;
            Relationships = relationshipsWriter;
            FDM = fdmWriter;
        }
    }
}

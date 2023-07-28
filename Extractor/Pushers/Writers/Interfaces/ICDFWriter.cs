using Cognite.OpcUa.Pushers.FDM;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface ICDFWriter
    {
        IRawWriter? Raw { get; }
        ITimeseriesWriter Timeseries { get; }
        IAssetsWriter? Assets { get; }
        IRelationshipsWriter? Relationships { get; }
        FDMWriter? FDM { get; }
    }
}

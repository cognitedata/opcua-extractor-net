namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface ICDFWriter
    {
        IRawWriter raw { get; }
        ITimeseriesWriter timeseries { get; }
        IAssetsWriter assets { get; }
        IRelationshipsWriter relationships { get; }
    }
}

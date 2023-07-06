namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface ICDFWriter
    {
        IRawWriter Raw { get; }
        ITimeseriesWriter Timeseries { get; }
        ITimeseriesWriter MinimalTimeseries { get; }
        IAssetsWriter Assets { get; }
        IRelationshipsWriter Relationships { get; }
    }
}

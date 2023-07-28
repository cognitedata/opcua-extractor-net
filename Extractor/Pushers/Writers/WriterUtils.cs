using System.Threading;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.FDM;
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{
    public static class WriterUtils
    {
        public static void AddWriters(this IServiceCollection services, CancellationToken token, FullConfig config)
        {
            services.AddSingleton<ITimeseriesWriter>(provider => {
                var destination = provider.GetRequiredService<CogniteDestination>();
                var config = provider.GetRequiredService<FullConfig>();
                return (config.Cognite?.MetadataTargets?.Clean?.Timeseries ?? false) 
                    ? new TimeseriesWriter(provider.GetRequiredService<ILogger<TimeseriesWriter>>(), destination, config)
                    : new MinimalTimeseriesWriter(provider.GetRequiredService<ILogger<MinimalTimeseriesWriter>>(), destination, config);
            });
            if (config.Cognite?.MetadataTargets?.Clean?.Assets ?? false)
            {
                services.AddSingleton<IAssetsWriter, AssetsWriter>(provider => {
                    var destination = provider.GetRequiredService<CogniteDestination>();
                    return new AssetsWriter(
                        provider.GetRequiredService<ILogger<AssetsWriter>>(),
                        destination,
                        config
                    );
                });
            }
            if (config.Cognite?.MetadataTargets?.Clean?.Relationships ?? false)
            {
                services.AddSingleton<IRelationshipsWriter, RelationshipsWriter>(provider => {
                    var destination = provider.GetRequiredService<CogniteDestination>();
                    return new RelationshipsWriter(
                        provider.GetRequiredService<ILogger<RelationshipsWriter>>(),
                        destination,
                        config
                    );
                });
            }
            if (config.Cognite?.MetadataTargets?.Raw is not null)
            {
                services.AddSingleton<IRawWriter, RawWriter>(provider => {
                    var destination = provider.GetRequiredService<CogniteDestination>();
                    return new RawWriter(
                        provider.GetRequiredService<ILogger<RawWriter>>(),
                        destination,
                        config
                    );
                });
            }
            if (config.Cognite?.MetadataTargets?.DataModels != null && config.Cognite.MetadataTargets.DataModels.Enabled)
            {
                services.AddSingleton<FDMWriter>(provider => {
                    var destination = provider.GetRequiredService<CogniteDestination>();
                    return new FDMWriter(provider.GetRequiredService<FullConfig>(), destination,
                        provider.GetRequiredService<ILogger<FDMWriter>>());
                });
            }
            services.AddSingleton<ICDFWriter, CDFWriter>(provider =>
            {
                return new CDFWriter(
                    provider.GetRequiredService<ITimeseriesWriter>(),
                    provider.GetService<IRawWriter>(),
                    provider.GetService<IAssetsWriter>(),
                    provider.GetService<IRelationshipsWriter>(),
                    provider.GetService<FDMWriter>()
                );
            });
        }
    }
}

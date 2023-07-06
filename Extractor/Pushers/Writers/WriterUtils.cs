using System.Threading;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.Writers.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{
    public static class WriterUtils
    {
        public static void AddWriters(this IServiceCollection services, CancellationToken token)
        {
            services.AddSingleton<ICDFWriter, CDFWriter>(provider =>
            {
                var dest = provider.GetRequiredService<CogniteDestination>();
                var config = provider.GetRequiredService<FullConfig>();
                return new CDFWriter(
                    new RawWriter(provider.GetRequiredService<ILogger<RawWriter>>(), dest, config),
                    new TimeseriesWriter(
                        provider.GetRequiredService<ILogger<TimeseriesWriter>>(),
                        dest,
                        config
                    ),
                    new AssetsWriter(
                        provider.GetRequiredService<ILogger<AssetsWriter>>(),
                        dest,
                        config
                    ),
                    new RelationshipsWriter(
                        provider.GetRequiredService<ILogger<RelationshipsWriter>>(),
                        dest,
                        config
                    ),
                    new MinimalTimeseriesWriter(
                        provider.GetRequiredService<ILogger<MinimalTimeseriesWriter>>(),
                        dest,
                        config
                    )
                );
            });
        }
    }
}

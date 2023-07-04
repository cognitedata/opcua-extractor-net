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
            services.AddSingleton<IAssetsWriter, AssetsWriter>(provider =>
            {
                var config = provider.GetRequiredService<FullConfig>();
                var logger = provider.GetRequiredService<ILogger<AssetsWriter>>();
                var dest = provider.GetRequiredService<CogniteDestination>();
                var extractor = provider.GetRequiredService<UAExtractor>();
                return new AssetsWriter(logger, token, dest, config, extractor);
            });
            services.AddSingleton<IRawWriter, RawWriter>(provider =>
            {
                var config = provider.GetRequiredService<FullConfig>();
                var logger = provider.GetRequiredService<ILogger<RawWriter>>();
                var dest = provider.GetRequiredService<CogniteDestination>();
                var extractor = provider.GetRequiredService<UAExtractor>();
                return new RawWriter(logger, token, dest, config, extractor);
            });
            services.AddSingleton<ITimeseriesWriter, TimeseriesWriter>(provider =>
            {
                var config = provider.GetRequiredService<FullConfig>();
                var logger = provider.GetRequiredService<ILogger<TimeseriesWriter>>();
                var dest = provider.GetRequiredService<CogniteDestination>();
                var extractor = provider.GetRequiredService<UAExtractor>();
                return new TimeseriesWriter(logger, token, dest, config, extractor);
            });
        }
    }
}

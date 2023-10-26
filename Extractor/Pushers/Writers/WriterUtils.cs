using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.FDM;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{
    public static class WriterUtils
    {
        public static void AddWriters(this IServiceCollection services, FullConfig config)
        {
            services.AddSingleton<BaseTimeseriesWriter>(provider =>
            {
                var destination = provider.GetRequiredService<CogniteDestination>();
                var config = provider.GetRequiredService<FullConfig>();
                return (config.Cognite?.MetadataTargets?.Clean?.Timeseries ?? false)
                    ? new TimeseriesWriter(provider.GetRequiredService<ILogger<TimeseriesWriter>>(), destination, config)
                    : new MinimalTimeseriesWriter(provider.GetRequiredService<ILogger<MinimalTimeseriesWriter>>(), destination, config);
            });
            if (config.Cognite?.MetadataTargets?.Clean is not null)
            {
                services.AddSingleton(provider =>
                {
                    var destination = provider.GetRequiredService<CogniteDestination>();
                    return new CleanWriter(
                        provider.GetRequiredService<ILogger<CleanWriter>>(),
                        destination,
                        config
                    );
                });
            }
            if (config.Cognite?.MetadataTargets?.Raw is not null)
            {
                services.AddSingleton(provider =>
                {
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
                services.AddSingleton(provider =>
                {
                    var destination = provider.GetRequiredService<CogniteDestination>();
                    return new FDMWriter(provider.GetRequiredService<FullConfig>(), destination,
                        provider.GetRequiredService<ILogger<FDMWriter>>());
                });
            }
            services.AddSingleton(provider =>
            {
                return new CDFWriter(
                    provider.GetRequiredService<BaseTimeseriesWriter>(),
                    provider.GetService<RawWriter>(),
                    provider.GetService<CleanWriter>(),
                    provider.GetService<FDMWriter>(),
                    provider.GetRequiredService<FullConfig>(),
                    provider.GetRequiredService<ILogger<CDFWriter>>()
                );
            });
        }

        public static async Task<IEnumerable<RawRow<Dictionary<string, JsonElement>>>> GetRawRows(
            string dbName,
            string tableName,
            CogniteDestination destination,
            IEnumerable<string>? columns,
            ILogger log,
            CancellationToken token
        )
        {
            string? cursor = null;
            var rows = new List<RawRow<Dictionary<string, JsonElement>>>();
            do
            {
                try
                {
                    var result = await destination.CogniteClient.Raw.ListRowsAsync<
                        Dictionary<string, JsonElement>
                    >(
                        dbName,
                        tableName,
                        new RawRowQuery
                        {
                            Cursor = cursor,
                            Limit = 10_000,
                            Columns = columns
                        },
                        null,
                        token
                    );
                    rows.AddRange(result.Items);
                    cursor = result.NextCursor;
                }
                catch (ResponseException ex) when (ex.Code == 404)
                {
                    log.LogWarning("Table or database not found: {Message}", ex.Message);
                    break;
                }
            } while (cursor != null);
            return rows;
        }
    }
}

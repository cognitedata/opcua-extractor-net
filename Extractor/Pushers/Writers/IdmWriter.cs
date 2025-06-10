using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources;
using CogniteSdk.Resources.DataModels;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Pushers.Writers
{

    class SourceSystemResource : BaseDataModelResource<SourceSystem>
    {

        public SourceSystemResource(DataModelsResource resource) : base(resource)
        {
        }

        public override ViewIdentifier View => new ViewIdentifier("cdf_cdm", "CogniteSourceSystem", "v1");
    }

    class SourceSystem : CogniteSourceSystem { }

    public class IdmWriter
    {
        private readonly ILogger log;
        private readonly FullConfig config;
        private readonly CogniteDestinationWithIDM destination;
        private readonly CleanMetadataTargetConfig cleanConfig;
        private readonly string space;

        public bool Assets => cleanConfig.Assets;
        public bool Timeseries => cleanConfig.Timeseries;
        public bool Relationships => cleanConfig.Relationships;

        private readonly BaseDataModelResource<SourceSystem> sources;

        private readonly string sourceId;

        public IdmWriter(ILogger<IdmWriter> logger, CogniteDestinationWithIDM destination, FullConfig config)
        {
            log = logger;
            this.config = config;
            this.destination = destination;
            cleanConfig = config.Cognite?.MetadataTargets?.Clean ?? throw new ArgumentException("Attempted to initialize IDM writer without clean config");
            if (cleanConfig.Space == null) throw new ArgumentException("Attempted to initialize IDM writer without space ID");
            space = cleanConfig.Space;
            sources = new SourceSystemResource(destination.CogniteClient.DataModels);
            var sourceId = cleanConfig.Source;
            if (string.IsNullOrWhiteSpace(sourceId))
            {
                if (!string.IsNullOrWhiteSpace(config.Source.EndpointUrl))
                {
                    sourceId = $"OPC_UA:{config.Source.EndpointUrl}";
                }
                else if (config.Source.NodeSetSource?.NodeSets?.Any() ?? false)
                {
                    var lastNs = config.Source.NodeSetSource.NodeSets.Last();
                    sourceId = $"OPC_UA_NODESET:{lastNs.FileName ?? lastNs.Url?.ToString()}";
                }
                else
                {
                    // Should be impossible, currently.
                    sourceId = "OPC_UA";
                }
            }
            this.sourceId = sourceId.TruncateBytes(256);
        }

        public async Task Init(
            IUAClientAccess client,
            CancellationToken token
        )
        {
            var buildInfo = client.SourceInfo;
            // Initialize source system
            var source = new SourceSystem
            {
                Name = buildInfo.Name,
                Manufacturer = buildInfo.Manufacturer,
                Version = buildInfo.Version,
                Description = buildInfo.ToString(),
            };

            var item = new SourcedNodeWrite<SourceSystem>
            {
                ExternalId = sourceId,
                Space = cleanConfig.Space,
                Properties = source
            };
            await sources.UpsertAsync(new[] { item }, new UpsertOptions(), token);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "Temp interface until implemented")]
        public Task<bool> PushAssets(
            IUAClientAccess client,
            IDictionary<string, BaseUANode> nodes,
            TypeUpdateConfig update,
            BrowseReport report,
            CancellationToken token
        )
        {
            if (!cleanConfig.Assets) return Task.FromResult(true);
            log.LogWarning("Writing assets to the core data models is not yet implemented");
            return Task.FromResult(true);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "Temp interface until implemented")]
        public Task<bool> PushReferences(
            IUAClientAccess client,
            IEnumerable<UAReference> references,
            BrowseReport report,
            CancellationToken token
        )
        {
            if (!cleanConfig.Relationships) return Task.FromResult(true);
            log.LogWarning("Writing references to the core data models is not yet implemented");
            return Task.FromResult(true);
        }

        public async Task<bool> PushTimeseries(
            IUAClientAccess client,
            IDictionary<string, UAVariable> nodes,
            HashSet<Identity> missingTimeseries,
            BrowseReport report,
            CancellationToken token
        )
        {
            if (!cleanConfig.Timeseries) throw new ConfigurationException("If space is set, clean.timeseries must also be true");
            var timeseries = nodes.Values.Select(v => v.ToIdmTimeSeries(client, space, sourceId, config, config.Cognite?.MetadataMapping?.Timeseries));

            try
            {
                var res = await destination.UpsertTimeSeriesAsync(timeseries, RetryMode.OnError, SanitationMode.Clean, token);

                if (missingTimeseries.Count > 0)
                {
                    foreach (var ts in timeseries)
                    {
                        missingTimeseries.Remove(Identity.Create(new InstanceIdentifier(ts.Space, ts.ExternalId)));
                    }
                }

                log.LogResult(res, RequestType.CreateTimeSeries, false);
                res.ThrowOnFatal();

                report.AssetsCreated += nodes.Count;

                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push timeseries to data models: {Message}", ex.Message);
                return false;
            }
        }

        public async Task DeleteIdmNodes(
            DeletedNodes deletes,
            CancellationToken token
        )
        {
            var toDelete = new List<InstanceIdentifierWithType>();
            if (Assets)
            {
                foreach (var obj in deletes.Objects)
                {
                    toDelete.Add(new InstanceIdentifierWithType(InstanceType.node, new InstanceIdentifier(space, obj.Id)));
                }
            }
            if (Timeseries)
            {
                foreach (var vr in deletes.Variables)
                {
                    toDelete.Add(new InstanceIdentifierWithType(InstanceType.node, new InstanceIdentifier(space, vr.Id)));
                }
            }
            if (Relationships)
            {
                foreach (var rf in deletes.References)
                {
                    toDelete.Add(new InstanceIdentifierWithType(InstanceType.edge, new InstanceIdentifier(space, rf.Id)));
                }
            }

            // TODO: Use config
            var chunks = toDelete.ChunkBy(config.Cognite!.CdfChunking.Instances).ToList();

            var generators = chunks
                .Select<IEnumerable<InstanceIdentifierWithType>, Func<Task>>((c, idx) => async () =>
                {
                    await destination.CogniteClient.CoreDataModel.TimeSeries<CogniteExtractorTimeSeries>()
                        .DeleteAsync(c, token);
                });

            int taskNum = 0;
            await generators.RunThrottled(
                config.Cognite!.CdfThrottling.Instances,
                (_) =>
                {
                    if (chunks.Count > 1)
                    {
                        log.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(DeleteIdmNodes), ++taskNum, chunks.Count);
                    }
                },
                token
            );

        }
    }
}

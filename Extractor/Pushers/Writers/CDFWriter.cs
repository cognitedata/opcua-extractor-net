using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.FDM;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class CDFWriter
    {
        private readonly RawWriter? raw;
        private readonly BaseTimeseriesWriter timeseries;
        private readonly CleanWriter? clean;
        private readonly FDMWriter? fdm;

        private readonly FullConfig config;
        public Dictionary<NodeId, long> NodeToAssetIds { get; } = new();
        public HashSet<string> MismatchedTimeseries { get; } = new();

        private readonly ILogger log;
        public CDFWriter(
            BaseTimeseriesWriter timeseriesWriter,
            RawWriter? rawWriter,
            CleanWriter? cleanWriter,
            FDMWriter? fdmWriter,
            FullConfig config,
            ILogger log
        )
        {
            raw = rawWriter;
            timeseries = timeseriesWriter;
            clean = cleanWriter;
            fdm = fdmWriter;
            this.config = config;
            this.log = log;
        }

        public async Task PushNodesAndReferences(
            IEnumerable<BaseUANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            BrowseReport report,
            UpdateConfig update,
            PushResult result,
            UAExtractor extractor,
            CancellationToken token)
        {
            if (config.DryRun)
            {
                if (fdm != null)
                {
                    await fdm.PushNodes(objects, variables, references, extractor, token);
                }

                return;
            }

            var assetMap = objects
                .Where(node => node.Source != NodeSources.NodeSource.CDF)
                .ToDictionary(obj => obj.GetUniqueId(extractor.Context)!);
            var timeseriesMap = variables
                .ToDictionary(obj => obj.GetUniqueId(extractor.Context)!);

            // Start by initializing clean assets, if necessary.
            if (clean != null)
            {
                result.Objects &= await clean.PushAssets(extractor, assetMap, NodeToAssetIds,
                    update.Objects, report, token);
            }

            // Next, push timeseries
            result.Variables &= await timeseries.PushVariables(extractor, timeseriesMap, NodeToAssetIds,
                MismatchedTimeseries, update.Variables, report, token);

            // Finally, push the various other resources as needed.
            var tasks = new List<Task>();

            // Relationships
            if (references.Any() && (clean != null || raw != null))
            {
                var relationships = references
                    .Select(rf => rf.ToRelationship(config.Cognite?.DataSet?.Id, extractor))
                    .DistinctBy(rel => rel.ExternalId);
                if (clean != null)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        result.References &= await clean.PushReferences(relationships, report, token);
                    }));
                }
                if (raw != null)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        result.RawReferences &= await raw.PushReferences(relationships, report, token);
                    }));
                }
            }

            // Raw assets and timeseries
            if (assetMap.Count != 0 && raw != null)
            {
                tasks.Add(Task.Run(async () =>
                {
                    result.RawObjects &= await raw.PushAssets(extractor, assetMap, update.Objects, report, token);
                }));
            }

            if (timeseriesMap.Count != 0 && raw != null)
            {
                tasks.Add(Task.Run(async () =>
                {
                    result.RawVariables &= await raw.PushTimeseries(extractor, timeseriesMap, update.Variables, report, token);
                }));
            }

            // FDM
            if (fdm != null)
            {
                tasks.Add(PushFdm(objects, variables, references, result, extractor, token));
            }

            await Task.WhenAll(tasks);
        }


        public async Task ExecuteDeletes(DeletedNodes deletes, UAExtractor extractor, CancellationToken token)
        {
            var tasks = new List<Task>();
            if (raw != null)
            {
                tasks.Add(raw.MarkDeleted(deletes, token));
            }
            if (clean != null)
            {
                tasks.Add(clean.MarkDeleted(deletes, token));
            }
            tasks.Add(timeseries.MarkTimeseriesDeleted(deletes.Variables.Select(d => d.Id), token));
            if (fdm != null)
            {
                tasks.Add(fdm.DeleteInFdm(deletes, extractor.Context, token));
            }

            await Task.WhenAll(tasks);
        }
        private async Task PushFdm(
            IEnumerable<BaseUANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            PushResult result,
            UAExtractor extractor,
            CancellationToken token)
        {
            bool pushResult;
            try
            {
                pushResult = await fdm!.PushNodes(objects, variables, references, extractor, token);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to push nodes to CDF Data Models: {Message}", ex.Message);
                if (ex is ResponseException rex && rex.Code < 500)
                {
                    log.LogWarning("Failed to push nodes to Data Models with a non-transient error, pushing will not be retried.");
                    pushResult = true;
                }
                else
                {
                    pushResult = false;
                }
            }
            result.Variables &= pushResult;
            result.Objects &= pushResult;
            result.References &= pushResult;
        }
    }
}

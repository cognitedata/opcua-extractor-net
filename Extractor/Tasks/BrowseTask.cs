using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Pushers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Tasks
{
    public class BrowseTask : BaseSchedulableTask
    {
        private readonly UAClient client;
        private readonly UAExtractor extractor;
        private readonly IServiceProvider provider;
        private readonly FullConfig config;

        private NodeSetNodeSource? nodeSetSource;

        public BrowseTask(UAExtractor extractor, UAClient client, FullConfig config, IServiceProvider provider)
        {
            this.extractor = extractor;
            this.client = client;
            this.config = config;
            this.provider = provider;
        }

        private object nodesLock = new object();
        private HashSet<NodeId> nodesToBrowse = new HashSet<NodeId>();
        private bool isFull;
        private bool isInitial = true;

        public override ITimeSpanProvider? Schedule => null;

        public bool AddNodesToBrowse(IEnumerable<NodeId> nodes, bool isFull)
        {
            lock (nodesLock)
            {
                if (this.isFull) return false; // Already scheduled a full browse.
                // If it's a full browse, just clear any we already enqueued,
                // since in this case we'll be browsing from the root of the node hierarchy anyway.
                if (isFull) nodesToBrowse.Clear();
                foreach (var node in nodes) nodesToBrowse.Add(node);
                this.isFull |= isFull;
                return true;
            }
        }

        public override bool ErrorIsFatal => true;

        public override string Name => "Browse OPC-UA Node Hierarchy";

        public override TaskMetadata Metadata => new TaskMetadata(CogniteSdk.Alpha.TaskType.batch);

        public override bool CanRunNow()
        {
            return extractor.IsReadyToBrowse;
        }

        private (INodeSource nodeSource, ITypeAndNodeSource typeSource) GetSources(
            bool initial,
            UANodeSource uaNodeSource)
        {
            INodeSource? nodeSource = null;
            ITypeAndNodeSource? typeSource = null;

            if ((config.Cognite?.RawNodeBuffer?.Enable ?? false) && initial)
            {
                var cdfSource = new CDFNodeSource(provider.GetRequiredService<ILogger<CDFNodeSource>>(),
                    config,
                    extractor,
                    extractor.Pusher as CDFPusher ?? throw new InvalidOperationException("Attempt to read from CDF without a configured CDF source"),
                    extractor.TypeManager);
                if (config.Cognite.RawNodeBuffer.BrowseOnEmpty)
                {
                    nodeSource = new CDFNodeSourceWithFallback(cdfSource, uaNodeSource);
                }
                else
                {
                    nodeSource = cdfSource;
                }
            }

            if ((config.Source.NodeSetSource?.NodeSets?.Any() ?? false) && initial)
            {
                if (config.Source.NodeSetSource.Instance || config.Source.NodeSetSource.Types)
                {
                    nodeSetSource ??= new NodeSetNodeSource(
                            provider.GetRequiredService<ILogger<NodeSetNodeSource>>(),
                            config, extractor, client, extractor.TypeManager);
                }

                if (config.Source.NodeSetSource.Instance)
                {
                    if (nodeSource != null) throw new ConfigurationException(
                        "Combining node-set-source.instance with cognite.raw-node-buffer is not allowed");
                    nodeSource = nodeSetSource;
                }
                if (config.Source.NodeSetSource.Types)
                {
                    typeSource = nodeSetSource;
                }
            }

            return (nodeSource ?? uaNodeSource, typeSource ?? uaNodeSource);
        }

        public override async Task<TaskUpdatePayload?> Run(BaseErrorReporter task, CancellationToken token)
        {
            var batch = new List<NodeId>(nodesToBrowse.Count);
            bool batchIsFull;
            bool batchIsInitial;
            lock (nodesLock)
            {
                batch = nodesToBrowse.ToList();
                nodesToBrowse.Clear();
                batchIsFull = isFull;
                batchIsInitial = isInitial;
                isFull = false;
                isInitial = false;

                // If the batch was empty, schedule a full rebrowse.
                // This was triggered by a periodic schedule.
                if (batch.Count == 0)
                {
                    batch = extractor.RootNodes.ToList();
                    batchIsFull = true;
                }
            }

            if (batch.Count == 0) return null;

            var uaSource = new UANodeSource(
                provider.GetRequiredService<ILogger<UANodeSource>>(),
                extractor, client, extractor.TypeManager);
            var (nodeSource, typeSource) = GetSources(batchIsInitial, uaSource);

            var builder = new NodeHierarchyBuilder(
                nodeSource,
                typeSource,
                config,
                batch,
                client,
                extractor,
                extractor.Transformations,
                provider.GetRequiredService<ILogger<NodeHierarchyBuilder>>());

            var result = await builder.LoadNodeHierarchy(batchIsFull, token);
            extractor.Streamer.AllowData = extractor.State.NodeStates.Count != 0;
            var toPush = await PusherInput.FromNodeSourceResult(result, extractor.Context, extractor.DeletesManager, token);

            await extractor.PushNodes(toPush, batchIsInitial);

            extractor.OnNodeHierarchyRead();
            // Changed flag means that it already existed, so we avoid synchronizing these.
            extractor.CreateSubscriptions(result.SourceVariables.Where(var => !var.Changed));

            if (result.ShouldBackgroundBrowse && batchIsInitial)
            {
                extractor.ScheduleRebrowse();
            }

            var sourceName = "the OPC-UA server";
            if (nodeSource is CDFNodeSource cdfSource)
            {
                sourceName = "CDF raw";
            }
            else if (nodeSource is NodeSetNodeSource nodeSetSource)
            {
                sourceName = "local NodeSet2 XML files";
            }

            return new TaskUpdatePayload
            {
                Message = $"Reading nodes from {sourceName} resulted in {result.Describe()}",
            };
        }
    }
}
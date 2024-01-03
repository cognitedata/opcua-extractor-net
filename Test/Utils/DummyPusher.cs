using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Test.Utils
{
    public class DummyPusherConfig : IPusherConfig
    {
        public bool Debug { get; set; }
        public bool ReadExtractedRanges { get; set; } = true;
        public double? NonFiniteReplacement { get; set; }
    }
    public sealed class DummyPusher : IPusher
    {
        public bool DataFailing { get; set; }
        public bool EventsFailing { get; set; }
        public bool Initialized { get; set; }
        public bool NoInit { get; set; }

        public bool? TestConnectionResult { get; set; } = true;
        public bool PushNodesResult { get; set; } = true;
        public bool InitDpRangesResult { get; set; } = true;
        public bool InitEventRangesResult { get; set; } = true;
        public bool? PushDataPointResult { get; set; } = true;
        public bool? PushEventResult { get; set; } = true;
        public bool PushReferenceResult { get; set; } = true;
        public bool DeleteResult { get; set; } = true;
        public ManualResetEvent OnReset { get; } = new ManualResetEvent(false);

        private readonly object dpLock = new object();
        private readonly object eventLock = new object();
        public Dictionary<(NodeId, int), List<UADataPoint>> DataPoints { get; }
            = new Dictionary<(NodeId, int), List<UADataPoint>>();
        public Dictionary<NodeId, List<UAEvent>> Events { get; }
            = new Dictionary<NodeId, List<UAEvent>>();

        public Dictionary<string, (NodeId, int)> UniqueToNodeId { get; } = new Dictionary<string, (NodeId, int)>();

        public IPusherConfig BaseConfig => config;
        private readonly DummyPusherConfig config;

        public UAExtractor Extractor { get; set; }

        public DeletedNodes LastDeleteReq { get; set; }

        public Dictionary<NodeId, BaseUANode> PushedNodes { get; }
            = new Dictionary<NodeId, BaseUANode>();
        public Dictionary<(NodeId, int), UAVariable> PushedVariables { get; }
            = new Dictionary<(NodeId, int), UAVariable>();
        public HashSet<UAReference> PushedReferences { get; }
            = new HashSet<UAReference>();

        public PusherInput PendingNodes { get; set; }

        public DummyPusher(DummyPusherConfig config)
        {
            this.config = config;
        }

        public void Dispose()
        {
        }

        public Task<bool?> TestConnection(FullConfig fullConfig, CancellationToken token)
        {
            return Task.FromResult(TestConnectionResult);
        }

        public Task<PushResult> PushNodes(
            IEnumerable<BaseUANode> objects,
            IEnumerable<UAVariable> variables,
            IEnumerable<UAReference> references,
            UpdateConfig update,
            CancellationToken token)
        {
            var result = new PushResult
            {
                Objects = PushNodesResult,
                Variables = PushNodesResult,
                References = PushReferenceResult
            };

            if (objects != null && PushNodesResult)
            {
                foreach (var obj in objects)
                {
                    UniqueToNodeId[Extractor.GetUniqueId(obj.Id)] = (obj.Id, -1);
                    PushedNodes[obj.Id] = obj;
                }
            }
            if (references != null && PushReferenceResult)
            {
                foreach (var rel in references)
                {
                    PushedReferences.Add(rel);
                }
            }

            lock (dpLock)
            {
                if (variables != null && PushNodesResult)
                {
                    foreach (var variable in variables)
                    {
                        if (!DataPoints.ContainsKey(variable.DestinationId()))
                        {
                            DataPoints[variable.DestinationId()] = new List<UADataPoint>();
                        }
                        UniqueToNodeId[variable.GetUniqueId(Extractor.Context)] = variable.DestinationId();
                        PushedVariables[variable.DestinationId()] = variable;
                    }
                }
            }

            return Task.FromResult(result);
        }

        public Task<bool> InitExtractedRanges(
            IEnumerable<VariableExtractionState> states,
            bool backfillEnabled,
            CancellationToken token)
        {
            if (!config.ReadExtractedRanges) return Task.FromResult(true);
            if (!InitDpRangesResult) return Task.FromResult(InitDpRangesResult);
            if (states == null || !states.Any()) return Task.FromResult(true);
            lock (dpLock)
            {
                foreach (var state in states)
                {
                    int idx = state.IsArray ? 0 : -1;

                    if (DataPoints.TryGetValue((state.SourceId, idx), out var dps) && dps.Count != 0)
                    {
                        var (min, max) = dps.MinMax(dp => dp.Timestamp);
                        if (backfillEnabled)
                        {
                            state.InitExtractedRange(min, max);
                        }
                        else
                        {
                            state.InitExtractedRange(CogniteTime.DateTimeEpoch, max);
                        }
                    }
                    else
                    {
                        state.InitToEmpty();
                    }
                }
            }
            return Task.FromResult(InitDpRangesResult);
        }

        public Task<bool> InitExtractedEventRanges(
            IEnumerable<EventExtractionState> states,
            bool backfillEnabled,
            CancellationToken token)
        {
            if (!config.ReadExtractedRanges) return Task.FromResult(true);
            if (!InitEventRangesResult) return Task.FromResult(InitEventRangesResult);
            if (states == null || !states.Any()) return Task.FromResult(true);
            lock (eventLock)
            {
                foreach (var state in states)
                {
                    if (Events.TryGetValue(state.SourceId, out var events))
                    {
                        var (min, max) = events.MinMax(evt => evt.Time);
                        if (backfillEnabled)
                        {
                            state.InitExtractedRange(min, max);
                        }
                        else
                        {
                            state.InitExtractedRange(CogniteTime.DateTimeEpoch, max);
                        }
                    }
                    else
                    {
                        state.InitToEmpty();
                    }
                }
            }
            return Task.FromResult(InitEventRangesResult);
        }

        public Task<bool?> PushEvents(IEnumerable<UAEvent> events, CancellationToken token)
        {
            if (!PushEventResult ?? false) return Task.FromResult(PushEventResult);
            if (events == null || !events.Any()) return Task.FromResult<bool?>(null);
            lock (eventLock)
            {
                var groups = events.GroupBy(evt => evt.EmittingNode);
                foreach (var group in groups)
                {
                    if (!Events.TryGetValue(group.Key, out var stored))
                    {
                        Events[group.Key] = stored = new List<UAEvent>();
                    }
                    stored.AddRange(group);
                }
            }


            return Task.FromResult(PushEventResult);
        }

        public Task<bool?> PushDataPoints(IEnumerable<UADataPoint> points, CancellationToken token)
        {
            if (!PushDataPointResult ?? false) return Task.FromResult(PushDataPointResult);
            if (points == null || !points.Any()) return Task.FromResult<bool?>(null);
            lock (dpLock)
            {
                // Missing nodes here is unacceptable
                foreach (var dp in points)
                {
                    DataPoints[UniqueToNodeId[dp.Id]].Add(dp);
                }
            }

            return Task.FromResult(PushDataPointResult);
        }

        public Task<bool> ExecuteDeletes(DeletedNodes deletes, CancellationToken token)
        {
            LastDeleteReq = deletes;

            return Task.FromResult(DeleteResult);
        }

        public void Reset()
        {
            OnReset.Set();
        }
        public void Wipe()
        {
            PushedNodes.Clear();
            PushedReferences.Clear();
            PushedVariables.Clear();
            DataPoints.Clear();
            Events.Clear();
            UniqueToNodeId.Clear();
        }
    }
}

using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Test.Utils
{
    public class DummyPusherConfig : IPusherConfig
    {
        public bool Debug { get; set; }
        public bool ReadExtractedRanges { get; set; } = true;
        public double? NonFiniteReplacement { get; set; }

        public IPusher ToPusher(IServiceProvider _)
        {
            return new DummyPusher(this);
        }
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

        public ManualResetEvent OnReset { get; } = new ManualResetEvent(false);

        public Dictionary<NodeId, TimeRange> EventTimeRange { get; set; }

        private object dpLock = new object();
        private object eventLock = new object();
        public Dictionary<(NodeId, int), List<BufferedDataPoint>> DataPoints { get; }
            = new Dictionary<(NodeId, int), List<BufferedDataPoint>>();
        public Dictionary<NodeId, List<BufferedEvent>> Events { get; }
            = new Dictionary<NodeId, List<BufferedEvent>>();

        public Dictionary<string, (NodeId, int)> UniqueToNodeId { get; } = new Dictionary<string, (NodeId, int)>();

        public IPusherConfig BaseConfig => config;
        private DummyPusherConfig config;

        public UAExtractor Extractor { get; set; }


        public Dictionary<NodeId, BufferedNode> PushedNodes { get; }
            = new Dictionary<NodeId, BufferedNode>();
        public Dictionary<(NodeId, int), BufferedVariable> PushedVariables { get; }
            = new Dictionary<(NodeId, int), BufferedVariable>();
        public HashSet<BufferedReference> PushedReferences { get; }
            = new HashSet<BufferedReference>();

        public List<BufferedNode> PendingNodes { get; } = new List<BufferedNode>();

        public List<BufferedReference> PendingReferences { get; } = new List<BufferedReference>();

        public DummyPusher(DummyPusherConfig config)
        {
            this.config = config;
        }

        public void Dispose()
        {
        }

        public Task<bool?> TestConnection(FullConfig config, CancellationToken token)
        {
            return Task.FromResult(TestConnectionResult);
        }

        public Task<bool> PushNodes(
            IEnumerable<BufferedNode> objects,
            IEnumerable<BufferedVariable> variables,
            UpdateConfig _,
            CancellationToken __)
        {
            if (!PushNodesResult) return Task.FromResult(false);
            if (objects != null)
            {
                foreach (var obj in objects)
                {
                    UniqueToNodeId[Extractor.GetUniqueId(obj.Id)] = (obj.Id, -1);
                    PushedNodes[obj.Id] = obj;
                }
            }
            lock (dpLock)
            {
                if (variables != null)
                {
                    foreach (var variable in variables)
                    {
                        if (!DataPoints.ContainsKey((variable.Id, variable.Index)))
                        {
                            DataPoints[(variable.Id, variable.Index)] = new List<BufferedDataPoint>();
                        }
                        UniqueToNodeId[Extractor.GetUniqueId(variable.Id, variable.Index)] = (variable.Id, variable.Index);
                        PushedVariables[(variable.Id, variable.Index)] = variable;
                    }
                }
            }

            return Task.FromResult(PushNodesResult);
        }

        public Task<bool> InitExtractedRanges(
            IEnumerable<NodeExtractionState> states,
            bool backfillEnabled,
            bool initMissing,
            CancellationToken _)
        {
            if (!config.ReadExtractedRanges) return Task.FromResult(true);
            if (!InitDpRangesResult) return Task.FromResult(InitDpRangesResult);
            if (states == null || !states.Any()) return Task.FromResult(true);
            lock (dpLock)
            {
                foreach (var state in states)
                {
                    int idx = state.IsArray ? 0 : -1;

                    if (DataPoints.TryGetValue((state.SourceId, idx), out var dps) && dps.Any())
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
                    else if (initMissing)
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
            bool initMissing,
            CancellationToken token)
        {
            if (!config.ReadExtractedRanges) return Task.FromResult(true);
            if (!InitEventRangesResult) return Task.FromResult(InitEventRangesResult);
            if (states == null || !states.Any()) return Task.FromResult(true);
            lock (eventLock)
            {
                foreach (var state in states)
                {
                    if (EventTimeRange.TryGetValue(state.SourceId, out var range))
                    {
                        if (backfillEnabled)
                        {
                            state.InitExtractedRange(range.First, range.Last);
                        }
                        else
                        {
                            state.InitExtractedRange(CogniteTime.DateTimeEpoch, range.Last);
                        }
                    }
                    else if (initMissing)
                    {
                        state.InitToEmpty();
                    }
                }
            }
            return Task.FromResult(InitEventRangesResult);
        }

        public Task<bool?> PushEvents(IEnumerable<BufferedEvent> events, CancellationToken token)
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
                        Events[group.Key] = stored = new List<BufferedEvent>();
                    }
                    stored.AddRange(group);
                }
            }


            return Task.FromResult(PushEventResult);
        }

        public Task<bool?> PushDataPoints(IEnumerable<BufferedDataPoint> points, CancellationToken token)
        {
            if (!PushDataPointResult ?? false) return Task.FromResult(PushDataPointResult);
            if (points == null || !points.Any()) return Task.FromResult<bool?>(null);
            Console.WriteLine($"Push {points.Count()} dps");
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

        public Task<bool> PushReferences(IEnumerable<BufferedReference> references, CancellationToken token)
        {
            if (!PushReferenceResult) return Task.FromResult(PushReferenceResult);
            if (references == null || !references.Any()) return Task.FromResult(true);

            foreach (var rel in references)
            {
                PushedReferences.Add(rel);
            }

            return Task.FromResult(true);
        }
        public void Reset()
        {
            OnReset.Set();
        }
    }
}

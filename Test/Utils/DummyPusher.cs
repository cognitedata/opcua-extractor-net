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
    class DummyPusher : IPusher
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

        public Dictionary<NodeId, TimeRange> EventTimeRange { get; set; }

        private object dpLock = new object();
        private object eventLock = new object();
        public Dictionary<(NodeId, int), List<BufferedDataPoint>> DataPoints { get; }
            = new Dictionary<(NodeId, int), List<BufferedDataPoint>>();
        public Dictionary<NodeId, List<BufferedEvent>> Events { get; }
            = new Dictionary<NodeId, List<BufferedEvent>>();

        private Dictionary<string, (NodeId, int)> uniqueToNodeId = new Dictionary<string, (NodeId, int)>();

        public IPusherConfig BaseConfig => config;
        private DummyPusherConfig config;

        public UAExtractor Extractor { get; set; }


        public Dictionary<NodeId, BufferedNode> PushedNodes { get; }
            = new Dictionary<NodeId, BufferedNode>();
        public Dictionary<(NodeId, int), BufferedVariable> PushedVariables { get; }
            = new Dictionary<(NodeId, int), BufferedVariable>();

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
                    uniqueToNodeId[Extractor.GetUniqueId(obj.Id)] = (obj.Id, -1);
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
                        uniqueToNodeId[Extractor.GetUniqueId(variable.Id, variable.Index)] = (variable.Id, variable.Index);
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
            lock (dpLock)
            {
                foreach (var state in states)
                {
                    int idx = state.IsArray ? 0 : -1;

                    if (DataPoints.TryGetValue((state.SourceId, idx), out var dps))
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
            lock (dpLock)
            {
                // Missing nodes here is unacceptable
                foreach (var dp in points)
                {
                    DataPoints[uniqueToNodeId[dp.Id]].Add(dp);
                }
            }

            return Task.FromResult(PushDataPointResult);
        }

        public void Reset()
        {
        }
    }
}

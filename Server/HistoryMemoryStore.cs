using System;
using System.Collections.Generic;
using System.Linq;
using Opc.Ua;
using Serilog;

namespace Server
{
    class HistoryMemoryStore
    {
        private const int maxHistoryDatapoints = 100000;
        private const int maxHistoryEvents = 10000;
        private readonly Dictionary<NodeId, List<DataValue>> historyStorage = new Dictionary<NodeId, List<DataValue>>();
        private readonly Dictionary<NodeId, List<BaseEventState>> eventHistoryStorage = new Dictionary<NodeId, List<BaseEventState>>();
        private readonly ILogger log = Log.Logger.ForContext(typeof(HistoryMemoryStore));
        public void AddHistorizingNode(BaseVariableState state)
        {
            state.Historizing = true;
            state.AccessLevel |= AccessLevels.HistoryRead;
            state.UserAccessLevel |= AccessLevels.HistoryRead;
            historyStorage[state.NodeId] = new List<DataValue>();
            log.Information("Historizing node: {id}", state.NodeId);
        }

        public void AddEventHistorizingEmitter(NodeId emitter)
        {
            log.Information("Historizing emitter: {id}", emitter);
            eventHistoryStorage[emitter] = new List<BaseEventState>();
        }

        public void HistorizeDataValue(NodeId id, DataValue value)
        {
            historyStorage[id].Add(value);
        }

        public void UpdateNode(BaseVariableState state)
        {
            var extractedValue = new DataValue
            {
                SourceTimestamp = state.Timestamp,
                Value = state.Value,
                ServerTimestamp = state.Timestamp,
                StatusCode = state.StatusCode
            };
            historyStorage[state.NodeId].Add(extractedValue);
        }

        public IEnumerable<DataValue> GetFullHistory(NodeId id)
        {
            return historyStorage[id];
        }

        public IEnumerable<BaseEventState> GetFullEventHistory(NodeId id)
        {
            return eventHistoryStorage[id];
        }

        public void HistorizeEvent(NodeId emitter, BaseEventState evt)
        {
            eventHistoryStorage[emitter].Add(evt);
        }

        public (IEnumerable<DataValue>, bool) ReadHistory(InternalHistoryRequest request)
        {
            int idx = request.MemoryIndex;
            var data = historyStorage[request.Id].OrderBy(dp => dp.SourceTimestamp).ToList();
            var result = new List<DataValue>();
            int count = 0;
            bool final = false;
            if (request.IsReverse)
            {
                if (idx < 0)
                {
                    idx = request.StartTime == DateTime.MinValue ? data.Count : data.FindLastIndex(vl => vl.SourceTimestamp <= request.StartTime);
                    log.Information("Read data backwards from index {idx}/{cnt}, time {start}", idx, data.Count - 1, request.StartTime);
                }
                else
                {
                    log.Information("Read data backwards from index {idx}/{cnt}", idx, data.Count - 1);
                }

                while (true)
                {
                    if (idx < 0 || idx >= data.Count || (request.EndTime != DateTime.MinValue && data[idx].SourceTimestamp < request.EndTime))
                    {
                        final = true;
                        break;
                    }

                    if (request.NumValuesPerNode > 0 && count >= request.NumValuesPerNode || count >= maxHistoryDatapoints && maxHistoryDatapoints > 0)
                    {
                        request.MemoryIndex = idx;
                        break;
                    }

                    result.Add(data[idx]);
                    count++;
                    idx--;
                }

                return (result, final);
            }

            if (idx < 0)
            {
                idx = data.FindIndex(vl => vl.SourceTimestamp >= request.StartTime);
                log.Information("Read data from index {idx}/{cnt}, time {start}", idx, data.Count - 1, request.StartTime);
            }
            else
            {
                log.Information("Read data from index {idx}/{cnt}", idx, data.Count - 1);
            }

            while (true)
            {
                if (idx >= data.Count || idx < 0 || (request.EndTime != DateTime.MinValue && data[idx].SourceTimestamp > request.EndTime))
                {
                    final = true;
                    break;
                }

                if (request.NumValuesPerNode > 0 && count >= request.NumValuesPerNode || count >= maxHistoryDatapoints && maxHistoryDatapoints > 0)
                {
                    request.MemoryIndex = idx;
                    break;
                }

                result.Add(data[idx]);
                idx++;
                count++;
            }

            return (result, final);
        }

        public (IEnumerable<BaseEventState>, bool) ReadEventHistory(InternalEventHistoryRequest request)
        {
            int idx = request.MemoryIndex;
            var data = eventHistoryStorage[request.Id].OrderBy(evt => evt.Time.Value).ToList();
            var result = new List<BaseEventState>();
            int count = 0;
            bool final = false;
            if (request.IsReverse)
            {
                if (idx < 0)
                {
                    idx = request.StartTime == DateTime.MinValue ? data.Count : data.FindLastIndex(vl => vl.Time.Value <= request.StartTime);
                    log.Information("Read events backwards from index {idx}/{cnt}, time {start}", idx, data.Count - 1, request.StartTime);
                }
                else
                {
                    log.Information("Read events backwards from index {idx}/{cnt}", idx, data.Count - 1);
                }

                while (true)
                {
                    if (idx < 0 || idx >= data.Count || (request.EndTime != DateTime.MinValue && data[idx].Time.Value < request.EndTime))
                    {
                        final = true;
                        break;
                    }

                    if (request.NumValuesPerNode > 0 && count >= request.NumValuesPerNode || count >= maxHistoryEvents && maxHistoryEvents > 0)
                    {
                        request.MemoryIndex = idx;
                        break;
                    }

                    result.Add(data[idx]);
                    idx--;
                    count++;
                }

                return (result, final);
            }

            if (idx < 0)
            {
                idx = data.FindIndex(vl => vl.Time.Value >= request.StartTime);
                log.Information("Read events from index {idx}/{cnt}, time {start}", idx, data.Count - 1, request.StartTime);
            }
            else
            {
                log.Information("Read events from index {idx}/{cnt}", idx, data.Count - 1);
            }

            while (true)
            {
                if (idx >= data.Count || idx < 0 || (request.EndTime != DateTime.MinValue && data[idx].Time.Value > request.EndTime))
                {
                    final = true;
                    break;
                }

                if (request.NumValuesPerNode > 0 && count >= request.NumValuesPerNode || count >= maxHistoryEvents && maxHistoryEvents > 0)
                {
                    request.MemoryIndex = idx;
                    break;
                }

                result.Add(data[idx]);
                idx++;
                count++;
            }

            return (result, final);
        }
        public void WipeHistory(NodeId id)
        {
            historyStorage[id].Clear();
        }
        public void WipeHistory()
        {
            foreach (var kvp in historyStorage)
            {
                kvp.Value.Clear();
            }
        }
        public void WipeEventHistory(NodeId id)
        {
            eventHistoryStorage[id].Clear();
        }
        public void WipeEventHistory()
        {
            foreach (var kvp in eventHistoryStorage)
            {
                kvp.Value.Clear();
            }
        }
    }
}

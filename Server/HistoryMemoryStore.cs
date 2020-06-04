using System;
using System.Collections.Generic;
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
        public void AddHistorizingNode(BaseVariableState state)
        {
            state.Historizing = true;
            state.AccessLevel |= AccessLevels.HistoryRead;
            state.UserAccessLevel |= AccessLevels.HistoryRead;
            historyStorage[state.NodeId] = new List<DataValue>();
            Log.Information("Historizing node: {id}", state.NodeId);
        }

        public void AddEventHistorizingEmitter(NodeId emitter)
        {
            Log.Information("Historizing emitter: {id}", emitter);
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
            var data = historyStorage[request.Id];
            var result = new List<DataValue>();
            int count = 0;
            bool final = false;
            if (request.IsReverse)
            {
                if (idx < 0)
                {
                    idx = request.StartTime == DateTime.MinValue ? data.Count : data.FindLastIndex(vl => vl.SourceTimestamp < request.StartTime);
                    if (idx < 0)
                    {
                        final = true;
                        return (result, final);
                    }
                }

                while (true)
                {
                    idx--;
                    if (idx < 0 || idx >= data.Count || data[idx].SourceTimestamp < request.EndTime)
                    {
                        final = true;
                        break;
                    }

                    if (request.NumValuesPerNode > 0 && count >= request.NumValuesPerNode || count >= maxHistoryDatapoints && maxHistoryDatapoints > 0)
                    {
                        request.MemoryIndex = idx + 1;
                        break;
                    }

                    result.Add(data[idx]);
                    count++;
                }

                return (result, final);
            }

            if (idx < 0)
            {
                idx = data.FindIndex(vl => vl.SourceTimestamp > request.StartTime) - 1;
                if (idx < 0)
                {
                    final = true;
                    return (result, final);
                }
            }

            while (true)
            {
                idx++;
                if (idx >= data.Count || idx < 0 || data[idx].SourceTimestamp > request.EndTime)
                {
                    final = true;
                    break;
                }

                if (request.NumValuesPerNode > 0 && count >= request.NumValuesPerNode || count >= maxHistoryDatapoints && maxHistoryDatapoints > 0)
                {
                    request.MemoryIndex = idx - 1;
                    break;
                }

                result.Add(data[idx]);
                count++;
            }

            return (result, final);
        }

        public (IEnumerable<BaseEventState>, bool) ReadEventHistory(InternalEventHistoryRequest request)
        {
            int idx = request.MemoryIndex;
            var data = eventHistoryStorage[request.Id];
            var result = new List<BaseEventState>();
            int count = 0;
            bool final = false;
            if (request.IsReverse)
            {
                if (idx < 0)
                {
                    idx = request.StartTime == DateTime.MinValue ? data.Count : data.FindLastIndex(vl => vl.Time.Value < request.StartTime);
                    Log.Information("Read events backwards from index {idx}/{cnt}, time {start}", idx, data.Count - 1, request.StartTime);
                    var evt = data[0];
                    Log.Information("Time: {t}", evt.Time.Value);
                    if (idx < 0)
                    {
                        final = true;
                        return (result, final);
                    }
                }

                while (true)
                {
                    idx--;
                    if (idx < 0 || idx >= data.Count || data[idx].Time.Value < request.EndTime)
                    {
                        final = true;
                        break;
                    }

                    if (request.NumValuesPerNode > 0 && count >= request.NumValuesPerNode || count >= maxHistoryEvents && maxHistoryEvents > 0)
                    {
                        request.MemoryIndex = idx + 1;
                        break;
                    }

                    result.Add(data[idx]);
                    count++;
                }

                return (result, final);
            }

            if (idx < 0)
            {
                idx = data.FindIndex(vl => vl.Time.Value > request.StartTime) - 1;
                Log.Information("Read events from index {idx}/{cnt}, time {start}", idx, data.Count - 1, request.StartTime);
                var evt = data[0];
                Log.Information("Time: {t}", evt.Time.Value);
                if (idx < 0)
                {
                    final = true;
                    return (result, final);
                }
            }

            while (true)
            {
                idx++;
                if (idx >= data.Count || idx < 0 || data[idx].Time.Value > request.EndTime)
                {
                    final = true;
                    break;
                }

                if (request.NumValuesPerNode > 0 && count >= request.NumValuesPerNode || count >= maxHistoryEvents && maxHistoryEvents > 0)
                {
                    request.MemoryIndex = idx - 1;
                    break;
                }

                result.Add(data[idx]);
                count++;
            }

            return (result, final);
        }
    }
}

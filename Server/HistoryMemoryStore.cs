﻿/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Server
{
    /// <summary>
    /// Class managing history in memory. Contains methods for reading and writing both datapoint and event history.
    /// </summary>
    internal sealed class HistoryMemoryStore
    {
        private const int maxHistoryDatapoints = 100000;
        private const int maxHistoryEvents = 10000;
        private readonly Dictionary<NodeId, List<DataValue>> historyStorage = new Dictionary<NodeId, List<DataValue>>();
        private readonly Dictionary<NodeId, List<BaseEventState>> eventHistoryStorage = new Dictionary<NodeId, List<BaseEventState>>();
        private readonly ILogger log;

        public HistoryMemoryStore(ILogger log)
        {
            this.log = log;
        }

        public void AddHistorizingNode(BaseVariableState state)
        {
            state.Historizing = true;
            state.AccessLevel |= AccessLevels.HistoryRead;
            state.UserAccessLevel |= AccessLevels.HistoryRead;
            historyStorage[state.NodeId] = new List<DataValue>();
            log.LogDebug("Historizing node: {Id}", state.NodeId);
        }

        public void AddEventHistorizingEmitter(NodeId emitter)
        {
            log.LogDebug("Historizing emitter: {Id}", emitter);
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

        /// <summary>
        /// Read datapoint history
        /// </summary>
        /// <param name="request">Internal abstraction of history request</param>
        /// <returns>A list of datavalues, and a boolean value indicating whether or not this is the final read.</returns>
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
                    log.LogInformation("Read data backwards from index {Id} {Idx}/{Cnt}, time {Start} {End}",
                        request.Id, idx, data.Count - 1, request.StartTime, request.EndTime);
                }
                else
                {
                    log.LogInformation("Read data backwards from index {Id} {Idx}/{Cnt}, time {Start} {End}",
                        request.Id, idx, data.Count - 1, request.StartTime, request.EndTime);
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
                log.LogInformation("Read data from index {Id} {Idx}/{Cnt}, time {Start} {End}",
                    request.Id, idx, data.Count - 1, request.StartTime, request.EndTime);
            }
            else
            {
                log.LogInformation("Read data from index {Id} {Idx}/{Cnt}, time {Start} {End}",
                    request.Id, idx, data.Count - 1, request.StartTime, request.EndTime);
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

        /// <summary>
        /// Read event history
        /// </summary>
        /// <param name="request">Internal abstraction of history request</param>
        /// <returns>A list of events, and a boolean value indicating whether or not this is the final read.</returns>
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
                    log.LogInformation("Read events backwards from index {Idx}/{Cnt}, time {Start} {End}",
                        idx, data.Count - 1, request.StartTime, request.EndTime);
                }
                else
                {
                    log.LogInformation("Read events backwards from index {Idx}/{Cnt}, time {Start} {End}",
                        idx, data.Count - 1, request.StartTime, request.EndTime);
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
                log.LogInformation("Read events from index {Idx}/{Cnt}, time {Start} {End}",
                    idx, data.Count - 1, request.StartTime, request.EndTime);
            }
            else
            {
                log.LogInformation("Read events from index {Idx}/{Cnt}, time {Start} {End}",
                    idx, data.Count - 1, request.StartTime, request.EndTime);
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

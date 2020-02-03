﻿/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;

namespace Cognite.OpcUa
{
    public interface IPusher : IDisposable
    {
        int Index { get; set; }
        PusherConfig BaseConfig { get; }

        /// <summary>
        /// Parent extractor
        /// </summary>
        Extractor Extractor { get; set; }

        /// <summary>
        /// Push nodes, emptying the queue
        /// </summary>
        Task<bool> PushNodes(IEnumerable<BufferedNode> objects, IEnumerable<BufferedVariable> variables, CancellationToken token)
        {
            return Task.FromResult(true);
        }
        /// <summary>
        /// Test the connection to the destination, should return false on failure
        /// </summary>
        Task<bool> TestConnection(CancellationToken token);
        /// <summary>
        /// Get earliest and latest timestamps in destination system, if possible
        /// </summary>
        Task<bool> InitExtractedRanges(IEnumerable<NodeExtractionState> states, bool backfillEnabled, CancellationToken token)
        {
            return Task.FromResult(true);
        }
        /// <summary>
        /// Get the earliest and latest timestamps for events in the destination system, if possible.
        /// </summary>
        /// <param name="states">States to initialize for</param>
        /// <param name="backfillEnabled">True if backfill is enabled</param>
        /// <returns>true on success</returns>
        Task<bool> InitExtractedEventRanges(IEnumerable<EventExtractionState> states, IEnumerable<NodeId> nodes, bool backfillEnabled,
            CancellationToken token)
        {
            return Task.FromResult(true);
        }
        /// <summary>
        /// Push events, emptying the event queue
        /// </summary>
        Task<IEnumerable<BufferedEvent>> PushEvents(CancellationToken token)
        {
            BufferedEventQueue.Clear();
            return Task.FromResult((IEnumerable<BufferedEvent>)Array.Empty<BufferedEvent>());
        }
        /// <summary>
        /// Push data points, emptying the queue
        /// </summary>
        /// <param name="dataPointQueue">Data points to be pushed</param>
        /// <returns>A list of datapoints that failed to be inserted</returns>
        Task<IEnumerable<BufferedDataPoint>> PushDataPoints(CancellationToken token)
        {
            BufferedDPQueue.Clear();
            return Task.FromResult((IEnumerable<BufferedDataPoint>)Array.Empty<BufferedDataPoint>());
        }
        /// <summary>
        /// Reset relevant persistent information in the pusher, preparing it to be restarted
        /// </summary>
        void Reset()
        {
            BufferedDPQueue.Clear();
            BufferedEventQueue.Clear();
        }
        ConcurrentQueue<BufferedDataPoint> BufferedDPQueue { get; }
        ConcurrentQueue<BufferedEvent> BufferedEventQueue { get; }
    }
}

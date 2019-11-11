/* Cognite Extractor for OPC-UA
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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public interface IPusher
    {
        PusherConfig BaseConfig { get; }
        /// <summary>
        /// Parent extractor
        /// </summary>
        Extractor Extractor { set; }

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
        /// Get latest timestamp in destination system, if possible
        /// </summary>
        Task<bool> InitLatestTimestamps(IEnumerable<NodeExtractionState> states, CancellationToken token)
        {
            return Task.FromResult(true);
        }
        /// <summary>
        /// Push events, emptying the event queue
        /// </summary>
        Task PushEvents(CancellationToken token)
        {
            BufferedEventQueue.Clear();
            return Task.CompletedTask;
        }
        /// <summary>
        /// Push data points, emptying the queue
        /// </summary>
        /// <param name="dataPointQueue">Data points to be pushed</param>
        Task PushDataPoints(CancellationToken token)
        {
            BufferedDPQueue.Clear();
            return Task.CompletedTask;
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

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
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public interface IPusher
    {
        /// <summary>
        /// Parent extractor
        /// </summary>
        Extractor Extractor { set; }
        /// <summary>
        /// The UAClient to use as source
        /// </summary>
        UAClient UAClient { set; }
        /// <summary>
        /// Push data points, emptying the queue
        /// </summary>
        /// <param name="dataPointQueue">Data points to be pushed</param>
        Task PushDataPoints(ConcurrentQueue<BufferedDataPoint> dataPointQueue, CancellationToken token);
        /// <summary>
        /// Push nodes, emptying the queue
        /// </summary>
        /// <param name="nodeQueue">Nodes to be pushed</param>
        Task<bool> PushNodes(ConcurrentQueue<BufferedNode> nodeQueue, CancellationToken token);
        /// <summary>
        /// Reset relevant persistent information in the pusher, preparing it to be restarted
        /// </summary>
        void Reset();
    }
}

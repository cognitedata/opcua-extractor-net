/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.Types;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public interface IPusher : IDisposable
    {
        bool DataFailing { get; set; }
        bool EventsFailing { get; set; }
        bool Initialized { get; set; }
        bool NoInit { get; set; }
        IPusherConfig BaseConfig { get; }

        /// <summary>
        /// Parent extractor
        /// </summary>
        UAExtractor Extractor { get; set; }
        /// <summary>
        /// Nodes not yet pushed due to pusher failure, should be cleared to free up memory after a successfull push.
        /// </summary>
        List<UANode> PendingNodes { get; }
        /// <summary>
        /// References not yet pushed due to pusher failure.
        /// </summary>
        List<UAReference> PendingReferences { get; }

        /// <summary>
        /// Push nodes, emptying the queue
        /// </summary>
        Task<bool> PushNodes(
            IEnumerable<UANode> objects,
            IEnumerable<UAVariable> variables,
            UpdateConfig update,
            CancellationToken token)
        {
            return Task.FromResult(true);
        }
        /// <summary>
        /// Test the connection to the destination, should return false on failure, can return null if 
        /// the test is somehow unable to be completed with meaningful results.
        /// </summary>
        Task<bool?> TestConnection(FullConfig config, CancellationToken token);
        /// <summary>
        /// Get earliest and latest timestamps for datapoints in destination system, if possible
        /// </summary>
        /// <param name="states">States to initialize for</param>
        /// <param name="backfillEnabled">True if backfill is enabled and earliest timestamps should be read</param>
        /// <returns>True on success</returns>
        Task<bool> InitExtractedRanges(
            IEnumerable<VariableExtractionState> states,
            bool backfillEnabled,
            CancellationToken token)
        {
            return Task.FromResult(true);
        }
        /// <summary>
        /// Get the earliest and latest timestamps for events in the destination system, if possible.
        /// </summary>
        /// <param name="states">States to initialize for</param>
        /// <param name="backfillEnabled">True if backfill is enabled and earliest timestamps should be read</param>
        /// <returns>True on success</returns>
        Task<bool> InitExtractedEventRanges(
            IEnumerable<EventExtractionState> states,
            bool backfillEnabled,
            CancellationToken token)
        {
            return Task.FromResult(true);
        }
        /// <summary>
        /// Push given list of events
        /// </summary>
        /// <param name="events">Events to push to destination</param>
        /// <returns>True on success, false on failure, null if no events were valid for this destination.</returns>
        Task<bool?> PushEvents(IEnumerable<UAEvent> events, CancellationToken token)
        {
            return Task.FromResult((bool?)true);
        }
        /// <summary>
        /// Push given list of datapoints.
        /// </summary>
        /// <param name="points">Data points to be pushed</param>
        /// <returns>True on success, false on failure, null if no events were valid for this destination.</returns>
        Task<bool?> PushDataPoints(IEnumerable<UADataPoint> points, CancellationToken token)
        {
            return Task.FromResult((bool?)true);
        }
        /// <summary>
        /// Push given list of references.
        /// </summary>
        /// <param name="references">References to push</param>
        /// <returns>True on success</returns>
        Task<bool> PushReferences(IEnumerable<UAReference> references, CancellationToken token)
        {
            return Task.FromResult(true);
        }

        /// <summary>
        /// Reset relevant persistent information in the pusher, preparing it to be restarted
        /// </summary>
        void Reset()
        {
        }
    }
}

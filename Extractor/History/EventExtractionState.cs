/* Cognite Extractor for OPC-UA
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

using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.History
{
    /// <summary>
    /// State of a node currently being extracted for events. Contains information about the data,
    /// a thread-safe last timestamp in destination systems,
    /// and a buffer for subscribed values arriving while HistoryRead is running.
    /// </summary>
    public sealed class EventExtractionState : UAHistoryExtractionState
    {
        /// <summary>
        /// Last known timestamp of events from OPC-UA.
        /// </summary>
        private IList<UAEvent>? buffer;
        public bool ShouldSubscribe { get; }

        public EventExtractionState(
            IUAClientAccess client,
            NodeId emitterId,
            bool frontfill, bool backfill, bool subscription)
            : base(client, emitterId, frontfill, backfill)
        {
            if (frontfill)
            {
                buffer = new List<UAEvent>();
            }
            ShouldSubscribe = subscription;
        }

        /// <summary>
        /// Update timestamp and buffer from stream.
        /// </summary>
        /// <param name="points">Event received for current stream iteration</param>
        public void UpdateFromStream(UAEvent? evt)
        {
            if (evt == null) return;
            UpdateFromStream(evt.Time, evt.Time);
            lock (Mutex)
            {
                if (IsFrontfilling)
                {
                    buffer?.Add(evt);
                }
            }
        }
        private void RefreshBuffer()
        {
            if (buffer == null) return;
            lock (Mutex)
            {
                buffer = buffer.Where(evt => !SourceExtractedRange.Contains(evt.Time)).ToList();
            }
        }
        public override void UpdateFromBackfill(DateTime first, bool final)
        {
            base.UpdateFromBackfill(first, final);
            if (!final)
            {
                RefreshBuffer();
            }
        }

        public override void UpdateFromFrontfill(DateTime last, bool final)
        {
            base.UpdateFromFrontfill(last, final);
            if (!final)
            {
                RefreshBuffer();
            }
        }
        /// <summary>
        /// Retrieve contents of the buffer after final historyRead iteration
        /// </summary>
        /// <returns>The contents of the buffer</returns>
        public IEnumerable<UAEvent> FlushBuffer()
        {
            if (IsFrontfilling || buffer == null || !buffer.Any()) return Array.Empty<UAEvent>();
            lock (Mutex)
            {
                var result = buffer.Where(evt => !SourceExtractedRange.Contains(evt.Time)).ToList();
                buffer.Clear();
                return result;
            }
        }
    }
}

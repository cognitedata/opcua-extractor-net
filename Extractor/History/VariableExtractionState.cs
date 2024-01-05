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

using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Cognite.OpcUa.History
{
    /// <summary>
    /// State of a node currently being extracted for data. Contains information about the data,
    /// a thread-safe last timestamp in destination systems,
    /// and a buffer for subscribed values arriving while HistoryRead is running.
    /// Represents a single OPC-UA variable, not necessarily a destination timeseries.
    /// </summary>
    public sealed class VariableExtractionState : UAHistoryExtractionState
    {
        /// <summary>
        /// Description of the OPC-UA datatype for the node
        /// </summary>
        public UADataType DataType { get; }
        /// <summary>
        /// Each entry in the array defines the fixed size of the given dimension of the variable.
        /// The extractor generally requires fixed dimensions in order to push arrays to destination systems.
        /// </summary>
        public int[]? ArrayDimensions { get; }
        public bool ShouldSubscribe { get; }
        public string DisplayName { get; }
        public bool AsEvents { get; }

        private readonly List<UADataPoint>? buffer;

        [MemberNotNullWhen(true, nameof(ArrayDimensions))]
        public bool IsArray => ArrayDimensions != null && ArrayDimensions.Length == 1 && ArrayDimensions[0] > 0;

        /// <summary>
        /// Constructor. Copies relevant data from BufferedVariable, initializes the buffer if Historizing is true.
        /// </summary>
        /// <param name="variable">Variable to be used as base</param>
        public VariableExtractionState(IUAClientAccess client, UAVariable variable, bool frontfill, bool backfill, bool subscription)
            : base(client, variable.Id, frontfill, backfill)
        {
            DataType = variable.FullAttributes.DataType;
            ArrayDimensions = variable.ArrayDimensions;
            DisplayName = variable.Name ?? "";
            ShouldSubscribe = subscription;
            AsEvents = variable.AsEvents;
            if (frontfill)
            {
                buffer = new List<UADataPoint>();
            }
        }
        /// <summary>
        /// Update time range and buffer from stream.
        /// </summary>
        /// <param name="points">Points received for current stream iteration</param>
        public void UpdateFromStream(IEnumerable<UADataPoint> points)
        {
            if (!points.Any()) return;
            UpdateFromStream(DateTime.MaxValue, points.Max(pt => pt.Timestamp));
            lock (Mutex)
            {
                if (IsFrontfilling)
                {
                    buffer?.AddRange(points);
                }
            }
        }
        /// <summary>
        /// Update last known timestamp from HistoryRead results. Empties the buffer if final is false.
        /// </summary>
        /// <param name="last">Latest timestamp in received values</param>
        /// <param name="final">True if this is the final iteration of history read</param>
        public override void UpdateFromFrontfill(DateTime last, bool final)
        {
            lock (Mutex)
            {
                SourceExtractedRange = SourceExtractedRange.Extend(null, last);
                if (!final)
                {
                    buffer?.Clear();
                }
                else
                {
                    IsFrontfilling = false;
                }
            }
        }

        /// <summary>
        /// Retrieve the buffer after the final iteration of HistoryRead. Filters out data received before the last known timestamp.
        /// </summary>
        /// <returns>The contents of the buffer once called.</returns>
        public IEnumerable<UADataPoint> FlushBuffer()
        {
            if (IsFrontfilling || buffer == null || buffer.Count == 0) return Array.Empty<UADataPoint>();
            lock (Mutex)
            {
                var result = buffer.Where(pt => pt.Timestamp > SourceExtractedRange.Last).ToList();
                buffer.Clear();
                return result;
            }
        }
    }
}

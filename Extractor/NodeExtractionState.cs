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

using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Cognite.OpcUa
{
    public class UAHistoryExtractionState : HistoryExtractionState
    {
        public NodeId SourceId { get; }
        public UAHistoryExtractionState(UAExtractor extractor, NodeId id, bool frontfill, bool backfill)
            : base(extractor?.GetUniqueId(id), frontfill, backfill)
        {
            SourceId = id;
        }
        public UAHistoryExtractionState(UAClient client, NodeId id, bool frontfill, bool backfill)
            : base(client?.GetUniqueId(id), frontfill, backfill)
        {
            SourceId = id;
        }

        public void InitToEmpty()
        {
            lock (_mutex)
            {
                if (!FrontfillEnabled || BackfillEnabled)
                {
                    SourceExtractedRange = DestinationExtractedRange = new TimeRange(DateTime.UtcNow, DateTime.UtcNow);
                }
                else
                {
                    SourceExtractedRange = DestinationExtractedRange = new TimeRange(CogniteTime.DateTimeEpoch, CogniteTime.DateTimeEpoch);
                }
            }
        }
    }


    /// <summary>
    /// State of a node currently being extracted for data. Contains information about the data,
    /// a thread-safe last timestamp in destination systems,
    /// and a buffer for subscribed values arriving while HistoryRead is running.
    /// Represents a single OPC-UA variable, not necessarily a destination timeseries.
    /// </summary>
    public sealed class NodeExtractionState : UAHistoryExtractionState
    {
        /// <summary>
        /// Description of the OPC-UA datatype for the node
        /// </summary>
        public BufferedDataType DataType { get; }
        /// <summary>
        /// Each entry in the array defines the fixed size of the given dimension of the variable.
        /// The extractor generally requires fixed dimensions in order to push arrays to destination systems.
        /// </summary>
        public Collection<int> ArrayDimensions { get; }
        public string DisplayName { get; }

        private readonly IList<IEnumerable<BufferedDataPoint>> buffer;

        public bool IsArray => ArrayDimensions != null && ArrayDimensions.Count == 1 && ArrayDimensions[0] > 0;

        /// <summary>
        /// Constructor. Copies relevant data from BufferedVariable, initializes the buffer if Historizing is true.
        /// </summary>
        /// <param name="variable">Variable to be used as base</param>
        public NodeExtractionState(UAExtractor extractor, BufferedVariable variable, bool frontfill, bool backfill, bool stateStore)
            : base(extractor, variable?.Id, frontfill, backfill)
        {
            if (variable == null) throw new ArgumentNullException(nameof(variable));
            DataType = variable.DataType;
            ArrayDimensions = variable.ArrayDimensions;
            DisplayName = variable.DisplayName;
            if (stateStore)
            {
                buffer = new List<IEnumerable<BufferedDataPoint>>();
            }
        }

        public NodeExtractionState(UAClient client, BufferedVariable variable, bool frontfill, bool backfill, bool stateStore)
            : base(client, variable?.Id, frontfill, backfill)
        {
            if (variable == null) throw new ArgumentNullException(nameof(variable));
            DataType = variable.DataType;
            ArrayDimensions = variable.ArrayDimensions;
            DisplayName = variable.DisplayName;
            if (stateStore)
            {
                buffer = new List<IEnumerable<BufferedDataPoint>>();
            }
        }
        /// <summary>
        /// Update time range and buffer from stream.
        /// </summary>
        /// <param name="points">Points received for current stream iteration</param>
        public void UpdateFromStream(IEnumerable<BufferedDataPoint> points)
        {
            if (!points.Any()) return;
            UpdateFromStream(DateTime.MaxValue, points.Max(pt => pt.Timestamp));
            lock (_mutex)
            {
                if (IsFrontfilling)
                {
                    buffer?.Add(points);
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
            lock (_mutex)
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
        public IEnumerable<IEnumerable<BufferedDataPoint>> FlushBuffer()
        {
            if (IsFrontfilling || buffer == null || !buffer.Any()) return Array.Empty<IEnumerable<BufferedDataPoint>>();
            lock (_mutex)
            {
                var result = buffer.Where(arr => arr.Max(pt => pt.Timestamp) > SourceExtractedRange.Last);
                buffer.Clear();
                return result;
            }
        }
    }
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
        private IList<BufferedEvent> buffer;

        public EventExtractionState(UAExtractor extractor, NodeId emitterId, bool frontfill, bool backfill, bool stateStore)
            : base(extractor, emitterId, frontfill, backfill)
        {
            if (stateStore)
            {
                buffer = new List<BufferedEvent>();
            }
        }

        /// <summary>
        /// Update timestamp and buffer from stream.
        /// </summary>
        /// <param name="points">Event received for current stream iteration</param>
        public void UpdateFromStream(BufferedEvent evt)
        {
            if (evt == null) return;
            UpdateFromStream(evt.Time, evt.Time);
            lock (_mutex)
            {
                if (IsFrontfilling)
                {
                    buffer?.Add(evt);
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
            lock (_mutex)
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
        /// Retrieve contents of the buffer after final historyRead iteration
        /// </summary>
        /// <returns>The contents of the buffer</returns>
        public IEnumerable<BufferedEvent> FlushBuffer()
        {
            if (IsFrontfilling || buffer == null || !buffer.Any()) return Array.Empty<BufferedEvent>();
            lock (_mutex)
            {
                var result = buffer.Where(evt => !SourceExtractedRange.Contains(evt.Time));
                buffer.Clear();
                return result;
            }
        }
    }

    public enum InfluxBufferType
    {
        StringType, DoubleType, EventType
    }
    /// <summary>
    /// Represents the state of a variable in the influxdb failureBuffer.
    /// </summary>
    public sealed class InfluxBufferState : BaseExtractionState
    {
        public InfluxBufferType Type { get; set; }
        public bool Historizing { get; }

        public InfluxBufferState(BaseExtractionState other) : base(other?.Id)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            DestinationExtractedRange = TimeRange.Empty;
            if (other is EventExtractionState)
            {
                Type = InfluxBufferType.EventType;
            }
            else if (other is NodeExtractionState state)
            {
                Historizing = state.FrontfillEnabled;
                Type = state.DataType.IsString ? InfluxBufferType.StringType : InfluxBufferType.DoubleType;
            }
            else if (other is InfluxBufferState iState)
            {
                Type = iState.Type;
                Historizing = iState.Historizing;
            }
        }
        /// <summary>
        /// Completely clear the ranges, after data has been written to all destinations.
        /// </summary>
        public void ClearRanges()
        {
            lock (_mutex)
            {
                DestinationExtractedRange = TimeRange.Empty;
            }
        }
        public void SetComplete()
        {
            InitExtractedRange(TimeRange.Complete.First, TimeRange.Complete.Last);
        }

        public override void InitExtractedRange(DateTime first, DateTime last)
        {
            base.InitExtractedRange(first, last);
        }
    }
}

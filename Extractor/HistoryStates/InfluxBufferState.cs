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

namespace Cognite.OpcUa.HistoryStates
{
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
        public NodeId SourceId { get; }

        public InfluxBufferState(BaseExtractionState other) : base(other?.Id)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            DestinationExtractedRange = TimeRange.Empty;
            if (other is EventExtractionState eState)
            {
                Type = InfluxBufferType.EventType;
                SourceId = eState.SourceId;
            }
            else if (other is VariableExtractionState state)
            {
                Type = state.DataType.IsString ? InfluxBufferType.StringType : InfluxBufferType.DoubleType;
                SourceId = state.SourceId;
            }
            else if (other is InfluxBufferState iState)
            {
                Type = iState.Type;
                SourceId = iState.SourceId;
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

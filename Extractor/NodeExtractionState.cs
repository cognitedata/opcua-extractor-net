﻿using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Cognite.OpcUa
{
    public abstract class BaseExtractionState
    {
        /// <summary>
        /// Id of the corresponding node in OPC-UA
        /// </summary>
        public NodeId Id { get; }
        /// <summary>
        /// True if the state has been modified since last time it was written to influxdb.
        /// </summary>
        public bool IsDirty { get; set; }
        /// <summary>
        /// Start- and endpoint of extracted range as known to be the case locally
        /// </summary>
        public TimeRange SourceExtractedRange { get; }
        /// <summary>
        /// Most conservative common extracted range for all destinations
        /// </summary>
        public TimeRange DestinationExtractedRange { get; }
        protected object RangeMutex { get; } = new object();
        /// <summary>
        /// True if backfill has been finished for this state
        /// </summary>
        public bool BackfillDone { get; set; }
        /// <summary>
        /// True if this state has ever been persisted to the influxdb state-storage.
        /// </summary>
        public bool StatePersisted { get; set; }
        /// <summary>
        /// True if state represents a historizing node.
        /// </summary>
        public virtual bool Historizing { get; set; }
        /// <summary>
        /// True if the node is currently passing live data from subscriptions into the pushers.
        /// </summary>
        public bool IsStreaming { get; protected set; }
        /// <summary>
        /// Construct from nodeId, initializes ranges to default values.
        /// </summary>
        /// <param name="id">NodeId of associated variable</param>
        protected BaseExtractionState(NodeId id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            Id = id;
            SourceExtractedRange = new TimeRange(DateTime.MinValue, DateTime.MaxValue);
            DestinationExtractedRange = new TimeRange(DateTime.MinValue, DateTime.MaxValue);
            BackfillDone = false;
        }
        /// <summary>
        /// Called when initializing extracted range from destinations and state storage.
        /// This will always shrink the believed range.
        /// </summary>
        /// <param name="first"></param>
        /// <param name="last"></param>
        public void InitExtractedRange(DateTime first, DateTime last)
        {
            lock (RangeMutex)
            {
                if (last < DestinationExtractedRange.End)
                {
                    DestinationExtractedRange.End = last;
                    SourceExtractedRange.End = last;
                }

                if (first > DestinationExtractedRange.Start)
                {
                    DestinationExtractedRange.Start = first;
                    SourceExtractedRange.Start = first;
                }
            }
        }
        /// <summary>
        /// Called after range initialization to set uninitialized ranges to proper default values depending on whether
        /// backfill is enabled or not.
        /// </summary>
        /// <param name="backfill">True if backfill is enabled</param>
        public void FinalizeRangeInit(bool backfill)
        {
            lock (RangeMutex)
            {
                if (SourceExtractedRange.Start == DateTime.MinValue && SourceExtractedRange.End == DateTime.MaxValue)
                {
                    if (backfill)
                    {
                        SourceExtractedRange.Start = DateTime.UtcNow;
                        SourceExtractedRange.End = DateTime.UtcNow;
                        DestinationExtractedRange.Start = DateTime.UtcNow;
                        DestinationExtractedRange.End = DateTime.UtcNow;
                    }
                    else
                    {
                        SourceExtractedRange.End = DateTime.MinValue;
                        DestinationExtractedRange.Start = DateTime.MaxValue;
                        DestinationExtractedRange.End = DateTime.MinValue;
                    }
                }
            }
        }
        /// <summary>
        /// Update start of source range directly from history backfill.
        /// </summary>
        /// <param name="first">Earliest timestamp in backfill chunk</param>
        /// <param name="final">True if this is the end of history</param>
        public void UpdateFromBackfill(DateTime first, bool final)
        {
            lock (RangeMutex)
            {
                if (first < SourceExtractedRange.Start)
                {
                    SourceExtractedRange.Start = first;
                }

                BackfillDone |= final;
            }
        }

        /// <summary>
        /// Use results of push to destinations to update the record of newest/latest points pushed to destinations.
        /// </summary>
        /// <param name="update">New range, will only be used to grow the destination range in the state</param>
        public void UpdateDestinationRange(TimeRange update)
        {
            if (update == null) return;
            lock (RangeMutex)
            {
                // Avoid updating destination range outside of the known range if this is a historizing node.
                // For non-historizing nodes it doesn't matter; we just want as many points as possible.
                // For historizing nodes this will only happen on extractor restart, in which case
                // writing outside of the extracted range could cause points to be lost if the extractor went down again before
                // that point in history was reached. It is an edge case, but it could happen.
                // There is a question as to what this means if history is sub-optimal and buffering historizing nodes is enabled,
                // but that is an edge case of an edge case and I think this is sufficient.
                if (update.End > DestinationExtractedRange.End
                    && (!Historizing || IsStreaming || update.End <= SourceExtractedRange.End))
                {
                    IsDirty = true;
                    DestinationExtractedRange.End = update.End;
                }

                if (update.Start < DestinationExtractedRange.Start
                    && (!Historizing || BackfillDone || update.Start >= SourceExtractedRange.Start))
                {
                    IsDirty = true;
                    DestinationExtractedRange.Start = update.Start;
                }
                if (BackfillDone && DestinationExtractedRange.Start == SourceExtractedRange.Start)
                {
                    DestinationExtractedRange.Start = DateTime.MinValue;
                }
            }
        }
        /// <summary>
        /// Reset the state based on a restart of history. This sets backfillDone to false, disables streaming
        /// and resets the source ranges to destination ranges.
        /// </summary>
        public void RestartHistory()
        {
            lock (RangeMutex)
            {
                IsStreaming = !Historizing;
                BackfillDone = false;
                SourceExtractedRange.Start = new DateTime(Math.Min(DestinationExtractedRange.Start.Ticks + 1000, DateTime.MaxValue.Ticks));
                SourceExtractedRange.End = new DateTime(Math.Max(DestinationExtractedRange.End.Ticks - 1000, DateTime.MinValue.Ticks));
            }

        }
    }
    /// <summary>
    /// State of a node currently being extracted for data. Contains information about the data,
    /// a thread-safe last timestamp in destination systems,
    /// and a buffer for subscribed values arriving while HistoryRead is running.
    /// Represents a single OPC-UA variable, not necessarily a destination timeseries.
    /// </summary>
    public sealed class NodeExtractionState : BaseExtractionState
    {
        /// <summary>
        /// True if there is historical data associated with this node
        /// </summary>
        public override bool Historizing { get; set; }
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
        /// <summary>
        /// Constructor. Copies relevant data from BufferedVariable, initializes the buffer if Historizing is true.
        /// </summary>
        /// <param name="variable">Variable to be used as base</param>
        public NodeExtractionState(BufferedVariable variable) : base(variable?.Id)
        {
            if (variable == null) throw new ArgumentNullException(nameof(variable));
            DataType = variable.DataType;
            ArrayDimensions = variable.ArrayDimensions;
            DisplayName = variable.DisplayName;
            BackfillDone = false;
            if (variable.Historizing)
            {
                Historizing = true;
                IsStreaming = false;
                buffer = new List<IEnumerable<BufferedDataPoint>>();
            }
            else
            {
                Historizing = false;
                IsStreaming = true;
            }

            IsStreaming = !Historizing;
        }
        /// <summary>
        /// Update time range and buffer from stream.
        /// </summary>
        /// <param name="points">Points received for current stream iteration</param>
        public void UpdateFromStream(IEnumerable<BufferedDataPoint> points)
        {
            if (!points.Any()) return;
            var last = points.Max(pt => pt.Timestamp);
            lock (RangeMutex)
            {
                if (last > SourceExtractedRange.End && IsStreaming)
                {
                    SourceExtractedRange.End = last;
                }
                else if (!IsStreaming)
                {
                    buffer.Add(points);
                }
            }
        }
        /// <summary>
        /// Update last known timestamp from HistoryRead results. Empties the buffer if final is false.
        /// </summary>
        /// <param name="last">Latest timestamp in received values</param>
        /// <param name="final">True if this is the final iteration of history read</param>
        public void UpdateFromFrontfill(DateTime last, bool final)
        {
            lock (RangeMutex)
            {
                if (last > SourceExtractedRange.End)
                {
                    SourceExtractedRange.End = last;
                }
                if (!final)
                {
                    buffer.Clear();
                }
                else
                {
                    IsStreaming = true;
                }
            }
        }

        /// <summary>
        /// Retrieve the buffer after the final iteration of HistoryRead. Filters out data received before the last known timestamp.
        /// </summary>
        /// <returns>The contents of the buffer once called.</returns>
        public IEnumerable<IEnumerable<BufferedDataPoint>> FlushBuffer()
        {
            if (!IsStreaming) throw new InvalidOperationException("Flush non-streaming buffer");
            if (buffer == null || !buffer.Any()) return new List<BufferedDataPoint[]>();
            lock (RangeMutex)
            {
                var result = buffer.Where(arr => arr.Max(pt => pt.Timestamp) > SourceExtractedRange.End);
                buffer.Clear();
                return result;
            }
        }

        /// <summary>
        /// Resets the state by disabling streaming for historizing timeseries and clearing the buffer.
        /// </summary>
        public void ResetStreamingState()
        {
            IsStreaming = !Historizing;
            BackfillDone = false;
            buffer?.Clear();
        }
    }
    /// <summary>
    /// State of a node currently being extracted for events. Contains information about the data,
    /// a thread-safe last timestamp in destination systems,
    /// and a buffer for subscribed values arriving while HistoryRead is running.
    /// </summary>
    public sealed class EventExtractionState : BaseExtractionState
    {
        private bool historizing;
        /// <summary>
        /// True if there are historical events in OPC-UA, and this is configured as a Historizing emitter.
        /// </summary>
        public override bool Historizing {
            get => historizing;
            set 
            {
                IsStreaming = !value;
                BackfillDone = !value;
                historizing = value;
                if (!historizing || buffer != null) return;
                lock (RangeMutex)
                {
                    buffer = new List<BufferedEvent>();
                }
            }
        }
        /// <summary>
        /// Last known timestamp of events from OPC-UA.
        /// </summary>
        private IList<BufferedEvent> buffer;

        public EventExtractionState(NodeId emitterId) : base(emitterId)
        {
            Historizing = false;
        }

        /// <summary>
        /// Update timestamp and buffer from stream.
        /// </summary>
        /// <param name="points">Event received for current stream iteration</param>
        public void UpdateFromStream(BufferedEvent evt)
        {
            if (evt == null) return;
            lock (RangeMutex)
            {
                if (evt.Time > SourceExtractedRange.End && evt.Time > SourceExtractedRange.Start && IsStreaming)
                {
                    SourceExtractedRange.End = evt.ReceivedTime;
                }
                else if (!IsStreaming)
                {
                    buffer.Add(evt);
                }
            }
        }
        /// <summary>
        /// Update last known timestamp from HistoryRead results. Empties the buffer if final is false.
        /// </summary>
        /// <param name="last">Latest timestamp in received values</param>
        /// <param name="final">True if this is the final iteration of history read</param>
        public void UpdateFromFrontfill(DateTime last, bool final)
        {
            lock (RangeMutex)
            {
                if (last > SourceExtractedRange.End)
                {
                    SourceExtractedRange.End = last;
                }
                if (!final)
                {
                    buffer.Clear();
                }
                else
                {
                    IsStreaming = true;
                }
            }
        }
        /// <summary>
        /// Retrieve contents of the buffer after final historyRead iteration
        /// </summary>
        /// <returns>The contents of the buffer</returns>
        public IEnumerable<BufferedEvent> FlushBuffer()
        {
            if (!IsStreaming) throw new InvalidOperationException("Flush non-streaming buffer");
            if (buffer == null || !buffer.Any()) return new List<BufferedEvent>();
            lock (RangeMutex)
            {
                var result = buffer.Where(evt => evt.ReceivedTime > SourceExtractedRange.End);
                buffer.Clear();
                return result;
            }
        }
        /// <summary>
        /// Resets the state by disabling streaming for historizing emitters and clearing the buffer.
        /// </summary>
        public void ResetStreamingState()
        {
            IsStreaming = !Historizing;
            BackfillDone = false;
            buffer?.Clear();
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
        public InfluxBufferType Type { get; }
        public override bool Historizing { get; set; }

        public InfluxBufferState(NodeExtractionState other, bool events) : base(other?.Id)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            DestinationExtractedRange.Start = DateTime.MaxValue;
            DestinationExtractedRange.End = DateTime.MinValue;
            if (events)
            {
                Type = InfluxBufferType.EventType;
            }
            else
            {
                Historizing = other.Historizing;
                Type = other.DataType.IsString ? InfluxBufferType.StringType : InfluxBufferType.DoubleType;
            }
        }

        public InfluxBufferState(NodeId objectId) : base(objectId)
        {
            Type = InfluxBufferType.EventType;
            DestinationExtractedRange.Start = DateTime.MaxValue;
            DestinationExtractedRange.End = DateTime.MinValue;
        }
        /// <summary>
        /// Completely clear the ranges, after data has been written to all destinations.
        /// </summary>
        public void ClearRanges()
        {
            lock (RangeMutex)
            {
                IsDirty = true;
                DestinationExtractedRange.Start = DateTime.MaxValue;
                DestinationExtractedRange.End = DateTime.MinValue;
            }
        }
    }
}

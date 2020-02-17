using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Cognite.OpcUa
{
    public interface IExtractionState
    {
        NodeId Id { get; }
        bool IsDirty { get; set; }
        bool Historizing { get; }
        TimeRange SourceExtractedRange { get; }
        TimeRange DestinationExtractedRange { get; }
        void InitExtractedRange(DateTime first, DateTime last);
        void FinalizeRangeInit(bool backfill);
    }
    /// <summary>
    /// State of a node currently being extracted for data. Contains information about the data,
    /// a thread-safe last timestamp in destination systems,
    /// and a buffer for subscribed values arriving while HistoryRead is running.
    /// Represents a single OPC-UA variable, not necessarily a destination timeseries.
    /// </summary>
    public class NodeExtractionState : IExtractionState
    {
        private readonly object rangeMutex = new object();
        /// <summary>
        /// True if the node is currently passing live data from subscriptions into the pushers.
        /// </summary>
        public bool IsStreaming { get; private set; }
        /// <summary>
        /// Id of the node in OPC-UA
        /// </summary>
        public NodeId Id { get; }
        /// <summary>
        /// True if there is historical data associated with this node
        /// </summary>
        public bool Historizing { get; }
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
        /// <summary>
        /// Earliest of the latest timestamps and latest of the earliest timestamps from destination systems.
        /// Represents the last common value for the pushers, not safe, as it updates before
        /// the pushers are done pushing. Each pusher should keep track of its own range as needed.
        /// </summary>
        public TimeRange SourceExtractedRange { get; }
        public TimeRange DestinationExtractedRange { get; }
        public bool BackfillDone { get; private set; }
        public bool IsDirty { get; set; }

        private readonly IList<IEnumerable<BufferedDataPoint>> buffer;
        /// <summary>
        /// Constructor. Copies relevant data from BufferedVariable, initializes the buffer if Historizing is true.
        /// </summary>
        /// <param name="variable">Variable to be used as base</param>
        public NodeExtractionState(BufferedVariable variable)
        {
            if (variable == null) throw new ArgumentNullException(nameof(variable));
            SourceExtractedRange = new TimeRange(DateTime.MinValue, DateTime.MaxValue);
            DestinationExtractedRange = new TimeRange(DateTime.MinValue, DateTime.MaxValue);
            Id = variable.Id;
            Historizing = variable.Historizing;
            DataType = variable.DataType;
            ArrayDimensions = variable.ArrayDimensions;
            DisplayName = variable.DisplayName;
            BackfillDone = false;
            if (!variable.Historizing)
            {
                IsStreaming = true;
            }
            else
            {
                buffer = new List<IEnumerable<BufferedDataPoint>>();
            }
        }

        public void InitExtractedRange(DateTime first, DateTime last)
        {
            lock (rangeMutex)
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
        /// Update time range and buffer from stream.
        /// </summary>
        /// <param name="points">Points received for current stream iteration</param>
        public void UpdateFromStream(IEnumerable<BufferedDataPoint> points)
        {
            if (!points.Any()) return;
            var last = points.Max(pt => pt.Timestamp);
            lock (rangeMutex)
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
            lock (rangeMutex)
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

        public void UpdateFromBackfill(DateTime first, bool final)
        {
            lock (rangeMutex)
            {
                if (first < SourceExtractedRange.Start)
                {
                    SourceExtractedRange.Start = first;
                }

                BackfillDone |= final;
            }
        }
        /// <summary>
        /// Retrieve the buffer after the final iteration of HistoryRead. Filters out data received before the last known timestamp.
        /// </summary>
        /// <returns>The contents of the buffer once called.</returns>
        public IEnumerable<IEnumerable<BufferedDataPoint>> FlushBuffer()
        {
            if (!IsStreaming) throw new InvalidOperationException("Flush non-streaming buffer");
            if (!buffer.Any()) return new List<BufferedDataPoint[]>();
            lock (rangeMutex)
            {
                var result = buffer.Where(arr => arr.Max(pt => pt.Timestamp) > SourceExtractedRange.End);
                buffer.Clear();
                return result;
            }
        }
        /// <summary>
        /// Use results of push to destinations to update the record of newest/latest points pushed to destinations.
        /// </summary>
        /// <param name="first"></param>
        /// <param name="last"></param>
        public void UpdateDestinationRange(TimeRange update)
        {
            if (update == null) return;
            lock (rangeMutex)
            {
                if (update.End > DestinationExtractedRange.End)
                {
                    IsDirty = true;
                    DestinationExtractedRange.End = update.End;
                }

                if (update.Start < DestinationExtractedRange.Start)
                {
                    IsDirty = true;
                    DestinationExtractedRange.Start = update.Start;
                }
            }
        }

        public void ResetStreamingState()
        {
            IsStreaming = !Historizing;
            BackfillDone = false;
            buffer?.Clear();
        }

        public void FinalizeRangeInit(bool backfill)
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
    /// State of a node currently being extracted for events. Contains information about the data,
    /// a thread-safe last timestamp in destination systems,
    /// and a buffer for subscribed values arriving while HistoryRead is running.
    /// </summary>
    public class EventExtractionState : IExtractionState
    {
        private readonly object rangeMutex = new object();
        /// <summary>
        /// True if the emitter in currently streaming live events into destination systems.
        /// </summary>
        public bool IsStreaming { get; private set; } = true;
        /// <summary>
        /// Id of the emitting node in OPC-UA
        /// </summary>
        public NodeId Id { get; }
        private bool historizing;
        /// <summary>
        /// True if there are historical events in OPC-UA, and this is configured as a Historizing emitter.
        /// </summary>
        public bool Historizing {
            get => historizing;
            set 
            {
                IsStreaming = !value;
                BackfillDone = !value;
                historizing = value;
                if (!historizing || buffer != null) return;
                lock (rangeMutex)
                {
                    buffer = new List<BufferedEvent>();
                }
            }
        }
        /// <summary>
        /// Earliest of the latest timestamps and latest of the earliest timestamps from destination systems.
        /// Represents the last common value for the pushers, not safe, as it updates before
        /// the pushers are done pushing. Each pusher should keep track of its own range as needed.
        /// </summary>
        public TimeRange SourceExtractedRange { get; }
        public TimeRange DestinationExtractedRange { get; }
        public bool IsDirty { get; set; }

        public bool BackfillDone { get; set; } = true;
        /// <summary>
        /// Last known timestamp of events from OPC-UA.
        /// </summary>
        private IList<BufferedEvent> buffer;
        public EventExtractionState(NodeId emitterId)
        {
            SourceExtractedRange = new TimeRange(DateTime.MinValue, DateTime.MaxValue);
            DestinationExtractedRange = new TimeRange(DateTime.MinValue, DateTime.MaxValue);

            Id = emitterId;
        }
        /// <summary>
        /// Set the time range if it is more conservative than the previous last known time range.
        /// </summary>
        /// <param name="last">Timestamp to be set</param>
        public void InitExtractedRange(DateTime first, DateTime last)
        {
            lock (rangeMutex)
            {
                if (last < SourceExtractedRange.End)
                {
                    SourceExtractedRange.End = last;
                    DestinationExtractedRange.End = last;
                }
                if (first > SourceExtractedRange.Start)
                {
                    SourceExtractedRange.Start = first;
                    DestinationExtractedRange.Start = first;
                }
            }
        }
        /// <summary>
        /// Update timestamp and buffer from stream.
        /// </summary>
        /// <param name="points">Event received for current stream iteration</param>
        public void UpdateFromStream(BufferedEvent evt)
        {
            if (evt == null) return;
            lock (rangeMutex)
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
            lock (rangeMutex)
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

        public void UpdateFromBackfill(DateTime first, bool final)
        {
            lock (rangeMutex)
            {
                if (first < SourceExtractedRange.Start)
                {
                    SourceExtractedRange.Start = first;
                }

                BackfillDone |= final;
            }
        }
        /// <summary>
        /// Retrieve contents of the buffer after final historyRead iteration
        /// </summary>
        /// <returns>The contents of the buffer</returns>
        public IEnumerable<BufferedEvent> FlushBuffer()
        {
            if (!IsStreaming) throw new InvalidOperationException("Flush non-streaming buffer");
            if (!buffer.Any()) return new List<BufferedEvent>();
            lock (rangeMutex)
            {
                var result = buffer.Where(evt => evt.ReceivedTime > SourceExtractedRange.End);
                buffer.Clear();
                return result;
            }
        }

        /// <summary>
        /// Use results of push to destinations to update the record of newest/latest points pushed to destinations.
        /// </summary>
        public void UpdateDestinationRange(TimeRange update)
        {
            if (update == null) return;
            lock (rangeMutex)
            {
                if (update.End > DestinationExtractedRange.End)
                {
                    DestinationExtractedRange.End = update.End;
                    IsDirty = true;
                }

                if (update.Start < DestinationExtractedRange.Start)
                {
                    DestinationExtractedRange.Start = update.Start;
                    IsDirty = true;
                }
            }
        }

        public void ResetStreamingState()
        {
            IsStreaming = !Historizing;
            BackfillDone = false;
            buffer?.Clear();
        }

        public void FinalizeRangeInit(bool backfill)
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
                    SourceExtractedRange.Start = DateTime.MaxValue;
                    DestinationExtractedRange.Start = DateTime.MaxValue;
                    DestinationExtractedRange.End = DateTime.MinValue;
                }
            }
        }
    }
}

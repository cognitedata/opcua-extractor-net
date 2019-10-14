using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa
{
    /// <summary>
    /// State of a node currently being extracted for data. Contains information about the data,
    /// a thread-safe last timestamp in destination systems,
    /// and a buffer for subscribed values arriving while HistoryRead is running.
    /// Represents a single OPC-UA variable, not necessarily a destination timeseries.
    /// </summary>
    public class NodeExtractionState
    {
        private readonly object lastMutex = new object();
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
        public int[] ArrayDimensions { get; }
        public string DisplayName { get; }
        /// <summary>
        /// Earliest of the latest timestamp of the destination systems.
        /// </summary>
        public DateTime DestLatestTimestamp { get; private set; }
        private readonly IList<IEnumerable<BufferedDataPoint>> buffer;
        /// <summary>
        /// Constructor. Copies relevant data from BufferedVariable, initializes the buffer if Historizing is true.
        /// </summary>
        /// <param name="variable">Variable to be used as base</param>
        public NodeExtractionState(BufferedVariable variable)
        {
            DestLatestTimestamp = DateTime.MinValue;
            Id = variable.Id;
            Historizing = variable.Historizing;
            DataType = variable.DataType;
            ArrayDimensions = variable.ArrayDimensions;
            DisplayName = variable.DisplayName;
            if (!variable.Historizing)
            {
                IsStreaming = true;
            }
            else
            {
                buffer = new List<IEnumerable<BufferedDataPoint>>();
            }
        }
        /// <summary>
        /// Set the timestamp if it is more conservative than the previous last known timestamp.
        /// </summary>
        /// <param name="last">Timestamp to be set</param>
        public void InitTimestamp(DateTime last)
        {
            lock (lastMutex)
            {
                if (last < DestLatestTimestamp || DestLatestTimestamp == DateTime.MinValue)
                {
                    DestLatestTimestamp = last;
                }
            }
        }
        /// <summary>
        /// Update timestamp and buffer from stream.
        /// </summary>
        /// <param name="points">Points received for current stream iteration</param>
        public void UpdateFromStream(IEnumerable<BufferedDataPoint> points)
        {
            if (!points.Any()) return;
            var last = points.Max(pt => pt.timestamp);
            lock (lastMutex)
            {
                if (last > DestLatestTimestamp && IsStreaming)
                {
                    DestLatestTimestamp = last;
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
            lock (lastMutex)
            {
                if (last > DestLatestTimestamp)
                {
                    DestLatestTimestamp = last;
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
            if (!IsStreaming) throw new Exception("Flush non-streaming buffer");
            if (!buffer.Any()) new List<BufferedDataPoint[]>();
            lock (lastMutex)
            {
                var result = buffer.Where(arr => arr.Max(pt => pt.timestamp) > DestLatestTimestamp);
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
    public class EventExtractionState
    {
        private readonly object lastMutex = new object();
        /// <summary>
        /// True if the emitter in currently streaming live events into destination systems.
        /// </summary>
        public bool IsStreaming { get; private set; } = true;
        /// <summary>
        /// Id of the emitting node in OPC-UA
        /// </summary>
        public NodeId Id { get; }
        private bool _historizing;
        /// <summary>
        /// True if there are historical events in OPC-UA, and this is configured as a Historizing emitter.
        /// </summary>
        public bool Historizing {
            get {
                return _historizing;
            }
            set {
                IsStreaming = !value;
                _historizing = value;
                if (_historizing && buffer == null)
                {
                    buffer = new List<BufferedEvent>();
                }
            }
        }
        /// <summary>
        /// Last known timestamp of events from OPC-UA.
        /// </summary>
        public DateTime DestLatestTimestamp { get; private set; }
        private IList<BufferedEvent> buffer;
        public EventExtractionState(NodeId emitterId)
        {
            DestLatestTimestamp = DateTime.MinValue;
            Id = emitterId;
        }
        /// <summary>
        /// Set the timestamp if it is more conservative than the previous last known timestamp.
        /// </summary>
        /// <param name="last">Timestamp to be set</param>
        public void InitTimestamp(DateTime last)
        {
            if (last < DestLatestTimestamp || DestLatestTimestamp == DateTime.MinValue)
            {
                DestLatestTimestamp = last;
            }
        }
        /// <summary>
        /// Update timestamp and buffer from stream.
        /// </summary>
        /// <param name="points">Event received for current stream iteration</param>
        public void UpdateFromStream(BufferedEvent evt)
        {
            lock (lastMutex)
            {
                if (evt.Time > DestLatestTimestamp && IsStreaming)
                {
                    DestLatestTimestamp = evt.ReceivedTime;
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
            lock (lastMutex)
            {
                if (last > DestLatestTimestamp)
                {
                    DestLatestTimestamp = last;
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
        /// Retrieve contents of the buffer after final historyRead iteration, filters out events triggered after latest known timestamp.
        /// </summary>
        /// <returns>The contents of the buffer</returns>
        public IEnumerable<BufferedEvent> FlushBuffer()
        {
            if (!IsStreaming) throw new Exception("Flush non-streaming buffer");
            if (!buffer.Any()) new List<BufferedDataPoint[]>();
            lock (lastMutex)
            {
                var result = buffer.Where(evt => evt.ReceivedTime > DestLatestTimestamp);
                buffer.Clear();
                return result;
            }
        }
    }
}

using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa
{
    public class NodeExtractionState
    {
        private readonly object lastMutex = new object();
        public bool IsStreaming { get; private set; }
        public NodeId Id { get; }
        public bool Historizing { get; }
        public BufferedDataType DataType { get; }
        public int ValueRank { get; }
        public int[] ArrayDimensions { get; }
        public DateTime DestLatestTimestamp { get; private set; }
        private readonly IList<IEnumerable<BufferedDataPoint>> buffer;
        public NodeExtractionState(BufferedVariable variable)
        {
            DestLatestTimestamp = Utils.Epoch;
            Id = variable.Id;
            ValueRank = variable.ValueRank;
            Historizing = variable.Historizing;
            DataType = variable.DataType;
            ArrayDimensions = variable.ArrayDimensions;
            if (!variable.Historizing)
            {
                IsStreaming = true;
            }
            else
            {
                buffer = new List<IEnumerable<BufferedDataPoint>>();
            }
        }
        public void InitTimestamp(DateTime last)
        {
            lock (lastMutex)
            {
                if (last < DestLatestTimestamp || DestLatestTimestamp == Utils.Epoch)
                {
                    DestLatestTimestamp = last;
                }
            }
        }
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
    public class EventExtractionState
    {
        private readonly object lastMutex = new object();
        public bool IsStreaming { get; private set; } = true;
        public NodeId Id { get; }
        private bool _historizing;
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
        public DateTime DestLatestTimestamp { get; private set; }
        private IList<BufferedEvent> buffer;
        public EventExtractionState(NodeId emitterId)
        {
            DestLatestTimestamp = Utils.Epoch;
            Id = emitterId;
        }
        public void InitTimestamp(DateTime last)
        {
            if (last < DestLatestTimestamp || DestLatestTimestamp == Utils.Epoch)
            {
                DestLatestTimestamp = last;
            }
        }
        public void UpdateFromStream(BufferedEvent evt)
        {
            lock (lastMutex)
            {
                if (evt.Time > DestLatestTimestamp && IsStreaming)
                {
                    DestLatestTimestamp = evt.Time;
                }
                else if (!IsStreaming)
                {
                    buffer.Add(evt);
                }
            }
        }
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
        public IEnumerable<BufferedEvent> FlushBuffer()
        {
            if (!IsStreaming) throw new Exception("Flush non-streaming buffer");
            if (!buffer.Any()) new List<BufferedDataPoint[]>();
            lock (lastMutex)
            {
                var result = buffer.Where(evt => evt.Time > DestLatestTimestamp);
                buffer.Clear();
                return result;
            }
        }
    }
}

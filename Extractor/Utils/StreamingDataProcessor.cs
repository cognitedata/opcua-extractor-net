using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Streaming data processor that processes data in chunks to reduce memory usage
    /// </summary>
    public class StreamingDataProcessor
    {
        private readonly ILogger _log;
        private readonly Channel<UADataPoint> _dataChannel;
        private readonly ChannelWriter<UADataPoint> _writer;
        private readonly ChannelReader<UADataPoint> _reader;
        private readonly int _batchSize;
        private readonly TimeSpan _batchTimeout;

        public StreamingDataProcessor(ILogger log, int batchSize = 1000, TimeSpan? batchTimeout = null)
        {
            _log = log;
            _batchSize = batchSize;
            _batchTimeout = batchTimeout ?? TimeSpan.FromMilliseconds(100);
            
            var options = new BoundedChannelOptions(batchSize * 10)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            };
            
            _dataChannel = Channel.CreateBounded<UADataPoint>(options);
            _writer = _dataChannel.Writer;
            _reader = _dataChannel.Reader;
        }

        /// <summary>
        /// Add a data point to the streaming processor
        /// </summary>
        public async Task<bool> AddDataPointAsync(UADataPoint dataPoint, CancellationToken token = default)
        {
            try
            {
                await _writer.WriteAsync(dataPoint, token);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        /// <summary>
        /// Add multiple data points to the streaming processor
        /// </summary>
        public async Task<bool> AddDataPointsAsync(IEnumerable<UADataPoint> dataPoints, CancellationToken token = default)
        {
            try
            {
                foreach (var dp in dataPoints)
                {
                    await _writer.WriteAsync(dp, token);
                }
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        /// <summary>
        /// Process data points in streaming fashion
        /// </summary>
        public async IAsyncEnumerable<IEnumerable<UADataPoint>> ProcessStreamAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token = default)
        {
            var batch = new List<UADataPoint>(_batchSize);
            var lastBatchTime = DateTime.UtcNow;

            await foreach (var dataPoint in _reader.ReadAllAsync(token))
            {
                batch.Add(dataPoint);

                var shouldFlush = batch.Count >= _batchSize || 
                                 (DateTime.UtcNow - lastBatchTime) > _batchTimeout;

                if (shouldFlush)
                {
                    yield return batch.ToList(); // Create a copy
                    batch.Clear();
                    lastBatchTime = DateTime.UtcNow;
                }
            }

            // Yield any remaining items
            if (batch.Count > 0)
            {
                yield return batch;
            }
        }

        /// <summary>
        /// Process data points grouped by tag ID
        /// </summary>
        public async IAsyncEnumerable<IDictionary<string, IEnumerable<UADataPoint>>> ProcessGroupedStreamAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token = default)
        {
            var groupedData = new Dictionary<string, List<UADataPoint>>();
            var lastBatchTime = DateTime.UtcNow;

            await foreach (var dataPoint in _reader.ReadAllAsync(token))
            {
                if (!groupedData.TryGetValue(dataPoint.Id, out var list))
                {
                    list = new List<UADataPoint>();
                    groupedData[dataPoint.Id] = list;
                }
                list.Add(dataPoint);

                var totalCount = groupedData.Values.Sum(l => l.Count);
                var shouldFlush = totalCount >= _batchSize || 
                                 (DateTime.UtcNow - lastBatchTime) > _batchTimeout;

                if (shouldFlush)
                {
                    // Create a copy with IEnumerable values
                    var result = groupedData.ToDictionary(
                        kvp => kvp.Key, 
                        kvp => (IEnumerable<UADataPoint>)kvp.Value.ToList()
                    );
                    
                    yield return result;
                    groupedData.Clear();
                    lastBatchTime = DateTime.UtcNow;
                }
            }

            // Yield any remaining items
            if (groupedData.Count > 0)
            {
                var result = groupedData.ToDictionary(
                    kvp => kvp.Key, 
                    kvp => (IEnumerable<UADataPoint>)kvp.Value
                );
                yield return result;
            }
        }

        /// <summary>
        /// Complete the data input
        /// </summary>
        public void CompleteAdding()
        {
            _writer.Complete();
        }

        /// <summary>
        /// Get the current queue count
        /// </summary>
        public int QueueCount => _dataChannel.Reader.CanCount ? _dataChannel.Reader.Count : -1;
    }
} 
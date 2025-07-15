using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Adaptive chunker that adjusts chunk sizes based on message size and network conditions
    /// </summary>
    public class AdaptiveChunker
    {
        private readonly ILogger _log;
        private readonly int _maxMessageSize;
        private readonly int _minChunkSize;
        private readonly int _maxChunkSize;
        private readonly JsonSerializerOptions _jsonOptions;
        
        private int _currentChunkSize;
        private int _successfulChunks;
        private int _failedChunks;
        private DateTime _lastAdjustment;
        private readonly object _adjustmentLock = new object();

        public AdaptiveChunker(ILogger log, int maxMessageSize = 1024 * 1024, int minChunkSize = 10, int maxChunkSize = 10000)
        {
            _log = log;
            _maxMessageSize = maxMessageSize;
            _minChunkSize = minChunkSize;
            _maxChunkSize = maxChunkSize;
            _currentChunkSize = Math.Min(1000, maxChunkSize); // Start with moderate size
            _lastAdjustment = DateTime.UtcNow;
            
            _jsonOptions = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
        }

        /// <summary>
        /// Create adaptive chunks based on message size
        /// </summary>
        public IEnumerable<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>> CreateAdaptiveChunks(
            IDictionary<string, IEnumerable<UADataPoint>> dataPoints, 
            Func<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>, object?> payloadCreator)
        {
            var dataList = dataPoints.ToList();
            var chunks = new List<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>>();
            
            int currentIndex = 0;
            
            while (currentIndex < dataList.Count)
            {
                var chunk = new List<KeyValuePair<string, IEnumerable<UADataPoint>>>();
                int currentChunkSize = GetCurrentChunkSize();
                int estimatedSize = 0;
                
                // Build chunk up to size limit or count limit
                while (currentIndex < dataList.Count && 
                       chunk.Count < currentChunkSize)
                {
                    var item = dataList[currentIndex];
                    chunk.Add(item);
                    currentIndex++;
                    
                    // Estimate size every few items to avoid expensive calculations
                    if (chunk.Count % 10 == 0)
                    {
                        estimatedSize = EstimatePayloadSize(chunk, payloadCreator);
                        if (estimatedSize > _maxMessageSize)
                        {
                            // Remove last item and break
                            chunk.RemoveAt(chunk.Count - 1);
                            currentIndex--;
                            break;
                        }
                    }
                }
                
                if (chunk.Count > 0)
                {
                    chunks.Add(chunk);
                }
            }
            
            return chunks;
        }

        /// <summary>
        /// Report successful chunk transmission
        /// </summary>
        public void ReportSuccess(int chunkSize, TimeSpan processingTime)
        {
            lock (_adjustmentLock)
            {
                _successfulChunks++;
                
                // If processing is fast and no failures, try to increase chunk size
                if (processingTime < TimeSpan.FromMilliseconds(100) && 
                    _failedChunks == 0 && 
                    _successfulChunks > 5)
                {
                    AdjustChunkSize(true);
                }
            }
        }

        /// <summary>
        /// Report failed chunk transmission
        /// </summary>
        public void ReportFailure(int chunkSize, Exception? exception = null)
        {
            lock (_adjustmentLock)
            {
                _failedChunks++;
                
                // If we have failures, reduce chunk size
                if (_failedChunks > 0)
                {
                    AdjustChunkSize(false);
                }
                
                _log.LogWarning("Chunk transmission failed with size {ChunkSize}. Exception: {Exception}", 
                    chunkSize, exception?.Message);
            }
        }

        /// <summary>
        /// Get current optimal chunk size
        /// </summary>
        public int GetCurrentChunkSize()
        {
            lock (_adjustmentLock)
            {
                return _currentChunkSize;
            }
        }

        /// <summary>
        /// Estimate payload size without full serialization
        /// </summary>
        private int EstimatePayloadSize(
            IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk,
            Func<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>, object?> payloadCreator)
        {
            try
            {
                var payload = payloadCreator(chunk);
                if (payload == null) return 0;
                
                var bytes = JsonSerializer.SerializeToUtf8Bytes(payload, _jsonOptions);
                return bytes.Length;
            }
            catch (Exception ex)
            {
                _log.LogWarning("Failed to estimate payload size: {Exception}", ex.Message);
                // Return a conservative estimate
                return chunk.Sum(kvp => kvp.Value.Count()) * 200; // Rough estimate per data point
            }
        }

        /// <summary>
        /// Adjust chunk size based on performance
        /// </summary>
        private void AdjustChunkSize(bool increase)
        {
            var now = DateTime.UtcNow;
            
            // Don't adjust too frequently
            if (now - _lastAdjustment < TimeSpan.FromSeconds(5))
            {
                return;
            }
            
            var oldSize = _currentChunkSize;
            
            if (increase)
            {
                // Increase by 20% but don't exceed max
                _currentChunkSize = Math.Min(_maxChunkSize, (int)(_currentChunkSize * 1.2));
            }
            else
            {
                // Decrease by 30% but don't go below min
                _currentChunkSize = Math.Max(_minChunkSize, (int)(_currentChunkSize * 0.7));
            }
            
            if (_currentChunkSize != oldSize)
            {
                _log.LogInformation("Adjusted chunk size from {OldSize} to {NewSize} (increase: {Increase})", 
                    oldSize, _currentChunkSize, increase);
                
                _lastAdjustment = now;
                
                // Reset counters after adjustment
                _successfulChunks = 0;
                _failedChunks = 0;
            }
        }

        /// <summary>
        /// Get performance statistics
        /// </summary>
        public (int CurrentChunkSize, int SuccessfulChunks, int FailedChunks, DateTime LastAdjustment) GetStats()
        {
            lock (_adjustmentLock)
            {
                return (_currentChunkSize, _successfulChunks, _failedChunks, _lastAdjustment);
            }
        }

        /// <summary>
        /// Reset performance counters
        /// </summary>
        public void ResetStats()
        {
            lock (_adjustmentLock)
            {
                _successfulChunks = 0;
                _failedChunks = 0;
                _lastAdjustment = DateTime.UtcNow;
            }
        }
    }
} 
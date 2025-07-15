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
            _currentChunkSize = _maxChunkSize; // Set to max chunk size since we only split when exceeding this
            _lastAdjustment = DateTime.UtcNow;
            
            // Log adaptive chunker configuration
            _log.LogInformation("[AdaptiveChunker] Initialized with MaxMessageSize={MaxMessageSize}MB, MinChunkSize={MinChunkSize}, MaxChunkSize={MaxChunkSize}",
                _maxMessageSize / (1024 * 1024), _minChunkSize, _maxChunkSize);
            
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
            
            int totalDataPoints = dataList.Sum(item => item.Value.Count());
            _log.LogInformation("[AdaptiveChunker] Starting chunking process: TotalNodes={TotalNodes}, TotalDataPoints={TotalDataPoints}, MaxChunkSize={MaxChunkSize}",
                dataList.Count, totalDataPoints, _maxChunkSize);
            
            // If total data points is within max chunk size, create a single chunk
            if (totalDataPoints <= _maxChunkSize)
            {
                _log.LogInformation("[AdaptiveChunker] Total data points ({TotalDataPoints}) within max chunk size ({MaxChunkSize}), creating single chunk",
                    totalDataPoints, _maxChunkSize);
                chunks.Add(dataList);
                return chunks;
            }
            
            // If exceeds max chunk size, split into multiple chunks
            _log.LogInformation("[AdaptiveChunker] Total data points ({TotalDataPoints}) exceeds max chunk size ({MaxChunkSize}), splitting into multiple chunks",
                totalDataPoints, _maxChunkSize);
            
            int currentIndex = 0;
            int chunkNumber = 0;
            
            while (currentIndex < dataList.Count)
            {
                var chunk = new List<KeyValuePair<string, IEnumerable<UADataPoint>>>();
                int estimatedSize = 0;
                
                // Build chunk up to max chunk size or message size limit
                int currentDataPointCount = 0;
                while (currentIndex < dataList.Count && 
                       currentDataPointCount < _maxChunkSize)
                {
                    var item = dataList[currentIndex];
                    int itemDataPointCount = item.Value.Count();
                    
                    // Check if adding this item would exceed the max chunk size
                    if (currentDataPointCount + itemDataPointCount > _maxChunkSize && chunk.Count > 0)
                    {
                        break; // Don't add this item, finish current chunk
                    }
                    
                    chunk.Add(item);
                    currentDataPointCount += itemDataPointCount;
                    currentIndex++;
                    
                    // Estimate size every few items to avoid expensive calculations
                    if (chunk.Count % 10 == 0)
                    {
                        estimatedSize = EstimatePayloadSize(chunk, payloadCreator);
                        if (estimatedSize > _maxMessageSize)
                        {
                            // Remove last item and break
                            currentDataPointCount -= itemDataPointCount;
                            chunk.RemoveAt(chunk.Count - 1);
                            currentIndex--;
                            _log.LogWarning("[AdaptiveChunker] Chunk {ChunkNumber} exceeded message size limit: EstimatedSize={EstimatedSize}MB, MaxSize={MaxSize}MB",
                                chunkNumber + 1, estimatedSize / (1024 * 1024), _maxMessageSize / (1024 * 1024));
                            break;
                        }
                    }
                }
                
                if (chunk.Count > 0)
                {
                    chunkNumber++;
                    _log.LogInformation("[AdaptiveChunker] Created chunk {ChunkNumber}: Nodes={NodeCount}, DataPoints={DataPointCount}, EstimatedSize={EstimatedSize}KB",
                        chunkNumber, chunk.Count, currentDataPointCount, estimatedSize / 1024);
                    chunks.Add(chunk);
                }
            }
            
            _log.LogInformation("[AdaptiveChunker] Chunking completed: TotalChunks={TotalChunks}, ProcessedDataPoints={ProcessedDataPoints}/{TotalDataPoints}",
                chunks.Count, chunks.Sum(c => c.Sum(item => item.Value.Count())), totalDataPoints);
            
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
        /// Process chunks in parallel with adaptive chunking
        /// </summary>
        /// <param name="dataPoints">Data points to process</param>
        /// <param name="payloadCreator">Function to create payload from chunk</param>
        /// <param name="chunkProcessor">Function to process each chunk</param>
        /// <param name="maxConcurrency">Maximum number of concurrent chunk processing (default: 4)</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if all chunks processed successfully</returns>
        public async Task<bool> ProcessChunksParallel<T>(
            IDictionary<string, IEnumerable<UADataPoint>> dataPoints,
            Func<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>, object?> payloadCreator,
            Func<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>, object?, Task<T>> chunkProcessor,
            int maxConcurrency = 4,
            CancellationToken token = default)
        {
            var chunks = CreateAdaptiveChunks(dataPoints, payloadCreator);
            var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
            var tasks = new List<Task<bool>>();

            foreach (var chunk in chunks)
            {
                var task = ProcessChunkWithSemaphore(chunk, payloadCreator, chunkProcessor, semaphore, token);
                tasks.Add(task);
            }

            var results = await Task.WhenAll(tasks);
            return results.All(result => result);
        }

        /// <summary>
        /// Process a single chunk with semaphore for concurrency control
        /// </summary>
        private async Task<bool> ProcessChunkWithSemaphore<T>(
            IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> chunk,
            Func<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>, object?> payloadCreator,
            Func<IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>>, object?, Task<T>> chunkProcessor,
            SemaphoreSlim semaphore,
            CancellationToken token)
        {
            await semaphore.WaitAsync(token);
            try
            {
                var startTime = DateTime.UtcNow;
                var payload = payloadCreator(chunk);

                if (payload == null) return true;

                var result = await chunkProcessor(chunk, payload);
                var processingTime = DateTime.UtcNow - startTime;

                // Report success or failure based on result
                if (result != null)
                {
                    ReportSuccess(chunk.Count(), processingTime);
                    return true;
                }
                else
                {
                    ReportFailure(chunk.Count());
                    return false;
                }
            }
            catch (Exception ex)
            {
                ReportFailure(chunk.Count(), ex);
                return false;
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
} 
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Manages connection recovery with enhanced data buffering and retry logic
    /// </summary>
    public class ConnectionRecoveryManager
    {
        private readonly ILogger _log;
        private readonly ConcurrentQueue<BufferedDataBatch> _dataBuffer;
        private readonly int _maxBufferSize;
        private readonly TimeSpan _retryInterval;
        private readonly int _maxRetries;
        private readonly SemaphoreSlim _recoverySemaphore;
        
        private volatile bool _isRecovering;
        private int _currentRetryCount;
        private DateTime _lastConnectionAttempt;
        private int _bufferedItemCount;

        public ConnectionRecoveryManager(ILogger log, int maxBufferSize = 100000, TimeSpan? retryInterval = null, int maxRetries = 10)
        {
            _log = log;
            _dataBuffer = new ConcurrentQueue<BufferedDataBatch>();
            _maxBufferSize = maxBufferSize;
            _retryInterval = retryInterval ?? TimeSpan.FromSeconds(5);
            _maxRetries = maxRetries;
            _recoverySemaphore = new SemaphoreSlim(1, 1);
            _currentRetryCount = 0;
            _lastConnectionAttempt = DateTime.MinValue;
        }

        /// <summary>
        /// Buffer data during connection failures
        /// </summary>
        public bool BufferData(IEnumerable<UADataPoint> dataPoints, string source = "unknown")
        {
            if (_bufferedItemCount >= _maxBufferSize)
            {
                _log.LogWarning("Data buffer is full ({BufferSize} items), dropping data from {Source}", 
                    _bufferedItemCount, source);
                return false;
            }

            var batch = new BufferedDataBatch
            {
                DataPoints = dataPoints.ToList(),
                Source = source,
                Timestamp = DateTime.UtcNow,
                RetryCount = 0
            };

            _dataBuffer.Enqueue(batch);
            Interlocked.Add(ref _bufferedItemCount, batch.DataPoints.Count);

            _log.LogDebug("Buffered {Count} data points from {Source}. Total buffered: {Total}", 
                batch.DataPoints.Count, source, _bufferedItemCount);

            return true;
        }

        /// <summary>
        /// Buffer data with grouped format
        /// </summary>
        public bool BufferGroupedData(IDictionary<string, IEnumerable<UADataPoint>> groupedData, string source = "unknown")
        {
            var totalCount = groupedData.Values.Sum(v => v.Count());
            
            if (_bufferedItemCount + totalCount >= _maxBufferSize)
            {
                _log.LogWarning("Data buffer would exceed capacity, dropping grouped data from {Source}", source);
                return false;
            }

            var batch = new BufferedDataBatch
            {
                GroupedData = groupedData.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToList()),
                Source = source,
                Timestamp = DateTime.UtcNow,
                RetryCount = 0
            };

            _dataBuffer.Enqueue(batch);
            Interlocked.Add(ref _bufferedItemCount, totalCount);

            _log.LogDebug("Buffered {Count} grouped data points from {Source}. Total buffered: {Total}", 
                totalCount, source, _bufferedItemCount);

            return true;
        }

        /// <summary>
        /// Attempt to recover connection and replay buffered data
        /// </summary>
        public async Task<bool> AttemptRecovery(
            Func<Task<bool>> connectionTest,
            Func<IEnumerable<UADataPoint>, CancellationToken, Task<bool?>> dataPointSender,
            Func<IDictionary<string, IEnumerable<UADataPoint>>, CancellationToken, Task<bool?>> groupedDataSender,
            CancellationToken token = default)
        {
            if (_isRecovering)
            {
                _log.LogDebug("Recovery already in progress, skipping");
                return false;
            }

            await _recoverySemaphore.WaitAsync(token);
            try
            {
                _isRecovering = true;

                // Check if we should attempt recovery based on retry interval
                var now = DateTime.UtcNow;
                if (now - _lastConnectionAttempt < _retryInterval)
                {
                    return false;
                }

                _lastConnectionAttempt = now;

                // Test connection
                _log.LogInformation("Attempting connection recovery (attempt {Attempt}/{MaxRetries})", 
                    _currentRetryCount + 1, _maxRetries);

                bool connectionRestored = await connectionTest();
                
                if (!connectionRestored)
                {
                    _currentRetryCount++;
                    if (_currentRetryCount >= _maxRetries)
                    {
                        _log.LogError("Connection recovery failed after {MaxRetries} attempts. Clearing buffer to prevent memory issues.", 
                            _maxRetries);
                        ClearBuffer();
                        _currentRetryCount = 0;
                    }
                    return false;
                }

                _log.LogInformation("Connection restored successfully. Replaying {Count} buffered batches", 
                    _dataBuffer.Count);

                // Connection restored, replay buffered data
                var successfulBatches = 0;
                var failedBatches = 0;
                var replayedDataPoints = 0;

                var batches = new List<BufferedDataBatch>();
                while (_dataBuffer.TryDequeue(out var batch))
                {
                    batches.Add(batch);
                }

                foreach (var batch in batches)
                {
                    try
                    {
                        bool? result = null;
                        
                        if (batch.DataPoints != null)
                        {
                            result = await dataPointSender(batch.DataPoints, token);
                            replayedDataPoints += batch.DataPoints.Count;
                        }
                        else if (batch.GroupedData != null)
                        {
                            var groupedData = batch.GroupedData.ToDictionary(
                                kvp => kvp.Key, 
                                kvp => (IEnumerable<UADataPoint>)kvp.Value
                            );
                            result = await groupedDataSender(groupedData, token);
                            replayedDataPoints += batch.GroupedData.Values.Sum(v => v.Count);
                        }

                        if (result == true)
                        {
                            successfulBatches++;
                            Interlocked.Add(ref _bufferedItemCount, 
                                -(batch.DataPoints?.Count ?? batch.GroupedData?.Values.Sum(v => v.Count) ?? 0));
                        }
                        else
                        {
                            failedBatches++;
                            batch.RetryCount++;
                            
                            // Re-queue failed batch if under retry limit
                            if (batch.RetryCount < 3)
                            {
                                _dataBuffer.Enqueue(batch);
                            }
                            else
                            {
                                _log.LogWarning("Dropping batch from {Source} after {RetryCount} retries", 
                                    batch.Source, batch.RetryCount);
                                Interlocked.Add(ref _bufferedItemCount, 
                                    -(batch.DataPoints?.Count ?? batch.GroupedData?.Values.Sum(v => v.Count) ?? 0));
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Error replaying batch from {Source}: {Message}", batch.Source, ex.Message);
                        failedBatches++;
                        
                        // Re-queue with retry limit
                        batch.RetryCount++;
                        if (batch.RetryCount < 3)
                        {
                            _dataBuffer.Enqueue(batch);
                        }
                        else
                        {
                            Interlocked.Add(ref _bufferedItemCount, 
                                -(batch.DataPoints?.Count ?? batch.GroupedData?.Values.Sum(v => v.Count) ?? 0));
                        }
                    }
                }

                _log.LogInformation("Recovery completed. Replayed {ReplayedDataPoints} data points. " +
                    "Successful batches: {SuccessfulBatches}, Failed batches: {FailedBatches}", 
                    replayedDataPoints, successfulBatches, failedBatches);

                _currentRetryCount = 0;
                return true;
            }
            finally
            {
                _isRecovering = false;
                _recoverySemaphore.Release();
            }
        }

        /// <summary>
        /// Clear all buffered data
        /// </summary>
        public void ClearBuffer()
        {
            var clearedCount = 0;
            while (_dataBuffer.TryDequeue(out var batch))
            {
                clearedCount += batch.DataPoints?.Count ?? batch.GroupedData?.Values.Sum(v => v.Count) ?? 0;
            }
            
            Interlocked.Exchange(ref _bufferedItemCount, 0);
            _log.LogWarning("Cleared {Count} buffered data points", clearedCount);
        }

        /// <summary>
        /// Get current buffer statistics
        /// </summary>
        public (int BufferedBatches, int BufferedDataPoints, bool IsRecovering, int RetryCount) GetStats()
        {
            return (_dataBuffer.Count, _bufferedItemCount, _isRecovering, _currentRetryCount);
        }

        /// <summary>
        /// Check if buffer is near capacity
        /// </summary>
        public bool IsBufferNearCapacity(double threshold = 0.8)
        {
            return _bufferedItemCount >= (_maxBufferSize * threshold);
        }

        /// <summary>
        /// Force a recovery attempt
        /// </summary>
        public async Task<bool> ForceRecovery(
            Func<Task<bool>> connectionTest,
            Func<IEnumerable<UADataPoint>, CancellationToken, Task<bool?>> dataPointSender,
            Func<IDictionary<string, IEnumerable<UADataPoint>>, CancellationToken, Task<bool?>> groupedDataSender,
            CancellationToken token = default)
        {
            _lastConnectionAttempt = DateTime.MinValue; // Reset to force immediate attempt
            return await AttemptRecovery(connectionTest, dataPointSender, groupedDataSender, token);
        }
    }

    /// <summary>
    /// Represents a batch of buffered data
    /// </summary>
    public class BufferedDataBatch
    {
        public List<UADataPoint>? DataPoints { get; set; }
        public Dictionary<string, List<UADataPoint>>? GroupedData { get; set; }
        public string Source { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public int RetryCount { get; set; }
    }
} 
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Cognite.OpcUa.Types;
using System.Collections.Generic; // Added for List

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Cache for data formatting operations to avoid repeated calculations
    /// </summary>
    public class DataFormatCache
    {
        private readonly ConcurrentDictionary<long, string> _timestampCache = new();
        private readonly ConcurrentDictionary<DataTypeCacheKey, string> _dataTypeCache = new();
        private readonly int _maxCacheSize;
        private readonly string _timezoneOffset;
        private readonly bool _useIso8601;

        public DataFormatCache(int maxCacheSize = 10000, string timezoneOffset = "+00:00", bool useIso8601 = true)
        {
            _maxCacheSize = maxCacheSize;
            _timezoneOffset = timezoneOffset;
            _useIso8601 = useIso8601;
        }

        /// <summary>
        /// Get formatted timestamp with caching
        /// </summary>
        public string GetFormattedTimestamp(long timestampMs)
        {
            return _timestampCache.GetOrAdd(timestampMs, ts =>
            {
                // Clean cache if it gets too large
                if (_timestampCache.Count > _maxCacheSize)
                {
                    ClearOldTimestamps();
                }

                if (_useIso8601)
                {
                    // Parse timezone offset to get hours and minutes
                    var offset = TimeSpan.Zero;
                    if (!string.IsNullOrEmpty(_timezoneOffset) && _timezoneOffset != "+00:00")
                    {
                        try
                        {
                            // Parse timezone offset format like "+09:00" or "-05:00"
                            if (_timezoneOffset.Length >= 6)
                            {
                                var sign = _timezoneOffset[0] == '+' ? 1 : -1;
                                var hours = int.Parse(_timezoneOffset.Substring(1, 2));
                                var minutes = int.Parse(_timezoneOffset.Substring(4, 2));
                                offset = new TimeSpan(sign * hours, sign * minutes, 0);
                            }
                        }
                        catch
                        {
                            // If parsing fails, default to UTC
                            offset = TimeSpan.Zero;
                        }
                    }

                    // Create DateTimeOffset with proper timezone conversion
                    var utcDateTime = DateTimeOffset.FromUnixTimeMilliseconds(ts);
                    var localDateTime = utcDateTime.ToOffset(offset);
                    return localDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffzzz");
                }
                else
                {
                    return ts.ToString();
                }
            });
        }

        /// <summary>
        /// Get data type string with caching
        /// </summary>
        public string GetDataTypeString(UADataPoint dp)
        {
            var key = new DataTypeCacheKey(dp.IsString, dp.DoubleValue ?? 0);
            return _dataTypeCache.GetOrAdd(key, k =>
            {
                // Clean cache if it gets too large
                if (_dataTypeCache.Count > _maxCacheSize)
                {
                    ClearOldDataTypes();
                }

                if (k.IsString) return "string";
                
                var value = k.DoubleValue;
                if (value == Math.Floor(value) && value >= int.MinValue && value <= int.MaxValue)
                    return "int32";
                return "float";
            });
        }

        /// <summary>
        /// Clear old timestamp entries to prevent memory growth
        /// </summary>
        private void ClearOldTimestamps()
        {
            var currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var cutoffTime = currentTime - TimeSpan.FromHours(1).Ticks / TimeSpan.TicksPerMillisecond;

            var keysToRemove = new List<long>();
            foreach (var kvp in _timestampCache)
            {
                if (kvp.Key < cutoffTime)
                {
                    keysToRemove.Add(kvp.Key);
                }
            }

            foreach (var key in keysToRemove)
            {
                _timestampCache.TryRemove(key, out _);
            }
        }

        /// <summary>
        /// Clear old data type entries to prevent memory growth
        /// </summary>
        private void ClearOldDataTypes()
        {
            var itemsToRemove = _dataTypeCache.Count / 2; // Remove half
            var removed = 0;
            
            foreach (var kvp in _dataTypeCache)
            {
                if (removed >= itemsToRemove) break;
                if (_dataTypeCache.TryRemove(kvp.Key, out _))
                {
                    removed++;
                }
            }
        }

        /// <summary>
        /// Clear all caches
        /// </summary>
        public void ClearAll()
        {
            _timestampCache.Clear();
            _dataTypeCache.Clear();
        }

        /// <summary>
        /// Get cache statistics
        /// </summary>
        public (int TimestampCount, int DataTypeCount) GetCacheStats()
        {
            return (_timestampCache.Count, _dataTypeCache.Count);
        }
    }

    /// <summary>
    /// Cache key for data type caching
    /// </summary>
    public readonly struct DataTypeCacheKey : IEquatable<DataTypeCacheKey>
    {
        public readonly bool IsString;
        public readonly double DoubleValue;

        public DataTypeCacheKey(bool isString, double doubleValue)
        {
            IsString = isString;
            DoubleValue = isString ? 0 : doubleValue; // Don't cache double value for strings
        }

        public bool Equals(DataTypeCacheKey other)
        {
            return IsString == other.IsString && 
                   (IsString || Math.Abs(DoubleValue - other.DoubleValue) < 0.0001);
        }

        public override bool Equals(object? obj)
        {
            return obj is DataTypeCacheKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            return IsString ? IsString.GetHashCode() : HashCode.Combine(IsString, DoubleValue);
        }
    }
} 
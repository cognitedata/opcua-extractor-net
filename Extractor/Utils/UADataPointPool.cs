using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Cognite.OpcUa.Types;
using Opc.Ua;

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Object pool for reducing UADataPoint allocations by reusing Lists and Dictionaries
    /// </summary>
    public class DataPointCollectionPool
    {
        private readonly ConcurrentQueue<List<UADataPoint>> _listPool = new();
        private readonly ConcurrentQueue<Dictionary<string, List<UADataPoint>>> _dictPool = new();
        private readonly int _maxPoolSize;
        private int _currentListPoolSize = 0;
        private int _currentDictPoolSize = 0;

        public DataPointCollectionPool(int maxPoolSize = 1000)
        {
            _maxPoolSize = maxPoolSize;
        }

        /// <summary>
        /// Get a List from the pool or create a new one
        /// </summary>
        public List<UADataPoint> GetList()
        {
            if (_listPool.TryDequeue(out var pooledList))
            {
                Interlocked.Decrement(ref _currentListPoolSize);
                return pooledList;
            }

            return new List<UADataPoint>();
        }

        /// <summary>
        /// Get a Dictionary from the pool or create a new one
        /// </summary>
        public Dictionary<string, List<UADataPoint>> GetDictionary()
        {
            if (_dictPool.TryDequeue(out var pooledDict))
            {
                Interlocked.Decrement(ref _currentDictPoolSize);
                return pooledDict;
            }

            return new Dictionary<string, List<UADataPoint>>();
        }

        /// <summary>
        /// Return a List to the pool
        /// </summary>
        public void ReturnList(List<UADataPoint> list)
        {
            if (_currentListPoolSize < _maxPoolSize)
            {
                list.Clear();
                _listPool.Enqueue(list);
                Interlocked.Increment(ref _currentListPoolSize);
            }
        }

        /// <summary>
        /// Return a Dictionary to the pool
        /// </summary>
        public void ReturnDictionary(Dictionary<string, List<UADataPoint>> dict)
        {
            if (_currentDictPoolSize < _maxPoolSize)
            {
                // Return all lists to the pool before clearing the dictionary
                foreach (var kvp in dict)
                {
                    ReturnList(kvp.Value);
                }
                dict.Clear();
                _dictPool.Enqueue(dict);
                Interlocked.Increment(ref _currentDictPoolSize);
            }
        }

        /// <summary>
        /// Clear all pools
        /// </summary>
        public void Clear()
        {
            while (_listPool.TryDequeue(out _))
            {
                Interlocked.Decrement(ref _currentListPoolSize);
            }
            while (_dictPool.TryDequeue(out _))
            {
                Interlocked.Decrement(ref _currentDictPoolSize);
            }
        }

        /// <summary>
        /// Get current pool sizes
        /// </summary>
        public (int Lists, int Dictionaries) CurrentPoolSizes => (_currentListPoolSize, _currentDictPoolSize);
    }
} 
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using Prometheus;

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Abstraction over a blocking producing/consuming queue.
    /// Has an optional max capacity, and methods for consuming values
    /// efficiently.
    /// </summary>
    /// <typeparam name="T">Type stored in the queue</typeparam>
    public class AsyncBlockingQueue<T>
    {
        private readonly Queue<T> queue = new();
        private readonly AsyncLock queueMutex = new AsyncLock();

        private readonly AsyncConditionVariable queueNotFull;
        private readonly AsyncConditionVariable queueNotEmpty;

        public event EventHandler? OnQueueOverflow;

        public int Capacity { get; }

        public string Name { get; }

        public int Count => queue.Count;

        private ILogger log;

        private static readonly Gauge queueLength = Metrics
            .CreateGauge("opcua_extractor_queue_length", "Length of the upload queues", "type");

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="capacity">Max capacity, set to -1 to have no size limit</param>
        /// <param name="name">Name of the queue, for logging.</param>
        /// <param name="log">Logger</param>
        public AsyncBlockingQueue(int capacity, string name, ILogger log)
        {
            Name = name;
            this.log = log;
            Capacity = capacity;
            queueNotFull = new AsyncConditionVariable(queueMutex);
            queueNotEmpty = new AsyncConditionVariable(queueMutex);
        }

        private void NotifyOverflow()
        {
            OnQueueOverflow?.Invoke(this, new EventArgs());
        }

        private void UpdateMetrics()
        {
            queueLength.WithLabels(Name).Set(queue.Count);
        }

        /// <summary>
        /// Enqeueue an item, blocking until the queue has capacity to accept the item.
        /// </summary>
        /// <param name="item">Item to enqueue</param>
        /// <param name="token">Optional cancellation token</param>
        public void Enqueue(T item, CancellationToken token = default)
        {
            using (queueMutex.Lock(token))
            {
                while (Capacity > 0 && queue.Count >= Capacity)
                {
                    log.LogTrace("{} queue is full", Name);
                    queueNotFull.Wait(token);
                }
                queue.Enqueue(item);
                UpdateMetrics();
                queueNotEmpty.Notify();
                if (Capacity > 0 && queue.Count >= Capacity) NotifyOverflow();
            }
        }

        /// <summary>
        /// Enqeueue a list of items, blocking until the queue has capacity to accept them.
        /// </summary>
        /// <param name="items">Items to enqueue</param>
        /// <param name="token">Optional cancellation token</param>
        public void Enqueue(IEnumerable<T> items, CancellationToken token = default)
        {
            using (queueMutex.Lock(token))
            {
                foreach (var item in items)
                {
                    while (Capacity > 0 && queue.Count >= Capacity)
                    {
                        log.LogTrace("{} queue is full", Name);
                        UpdateMetrics();
                        queueNotFull.Wait(token);
                    }
                    queue.Enqueue(item);
                    queueNotEmpty.Notify();
                    if (Capacity > 0 && queue.Count >= Capacity) NotifyOverflow();
                }
                UpdateMetrics();
            }
        }

        /// <summary>
        /// Enqeueue an item, blocking until the queue has capacity to accept the item.
        /// </summary>
        /// <param name="item">Item to enqueue</param>
        /// <param name="token">Optional cancellation token</param>
        public async Task EnqueueAsync(T item, CancellationToken token = default)
        {
            using (await queueMutex.LockAsync(token))
            {
                while (Capacity > 0 && queue.Count >= Capacity)
                {
                    log.LogTrace("{} queue is full", Name);
                    await queueNotFull.WaitAsync(token);
                }
                queue.Enqueue(item);
                UpdateMetrics();
                queueNotEmpty.Notify();
                if (Capacity > 0 && queue.Count >= Capacity) NotifyOverflow();
            }
        }

        /// <summary>
        /// Enqeueue a list of items, blocking until the queue has capacity to accept them.
        /// </summary>
        /// <param name="items">Items to enqueue</param>
        /// <param name="token">Optional cancellation token</param>
        public async Task EnqueueAsync(IEnumerable<T> items, CancellationToken token = default)
        {
            using (await queueMutex.LockAsync(token))
            {
                foreach (var item in items)
                {
                    while (Capacity > 0 && queue.Count >= Capacity)
                    {
                        log.LogTrace("{} queue is full", Name);
                        UpdateMetrics();
                        await queueNotFull.WaitAsync(token);
                    }
                    queue.Enqueue(item);
                    queueNotEmpty.Notify();
                    if (Capacity > 0 && queue.Count >= Capacity) NotifyOverflow();
                }
                UpdateMetrics();
            }
        }

        /// <summary>
        /// Drain the queue, returning an async enumerable over the items.
        /// This will only lock once, then completely empty the queue.
        /// </summary>
        /// <param name="token">Optional cancellation token</param>
        public async IAsyncEnumerable<T> DrainAsync([EnumeratorCancellation] CancellationToken token = default)
        {
            using (await queueMutex.LockAsync(token))
            {
                while (queue.TryDequeue(out var item))
                {
                    yield return item;
                }
                UpdateMetrics();
                queueNotFull.NotifyAll();
            }
        }

        /// <summary>
        /// Drain the queue, returning an enumerable over the items.
        /// This will only lock once, then completely empty the queue.
        /// </summary>
        /// <param name="token">Optional cancellation token</param>
        public IEnumerable<T> Drain(CancellationToken token = default)
        {
            using (queueMutex.Lock(token))
            {
                while (queue.TryDequeue(out var item))
                {
                    yield return item;
                }
                UpdateMetrics();
                queueNotFull.NotifyAll();
            }
        }

        /// <summary>
        /// Try to remove a single item from the queue, returning immediately
        /// even if the queue is empty.
        /// </summary>
        /// <param name="item">Out item</param>
        /// <param name="token">Optional cancellation token</param>
        /// <returns>True if an item was returned</returns>
        public bool TryDequeue(out T? item, CancellationToken token = default)
        {
            item = default;
            using (queueMutex.Lock(token))
            {
                var r = queue.TryDequeue(out item);
                UpdateMetrics();
                if (r) queueNotFull.Notify();
                return r;
            }
        }

        /// <summary>
        /// Try to remove a single item from the queue, returning immediately
        /// even if the queue is empty.
        /// </summary>
        /// <param name="token">Optional cancellation token</param>
        /// <returns>The retrieved item, or default if the queue was empty</returns>
        public async Task<T?> TryDequeueAsync(CancellationToken token = default)
        {
            using (await queueMutex.LockAsync(token))
            {
                var r = queue.TryDequeue(out var item);
                UpdateMetrics();
                if (r) queueNotFull.Notify();
                else return default;
                return item;
            }
        }

        /// <summary>
        /// Dequeue a single item from the queue, waiting until one is available
        /// if the queue is empty.
        /// </summary>
        /// <param name="token">Optional cancellation token</param>
        public T Dequeue(CancellationToken token = default)
        {
            using (queueMutex.Lock(token))
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    var r = queue.TryDequeue(out var item);
                    UpdateMetrics();
                    if (r)
                    {
                        queueNotFull.Notify();
                        return item;
                    }
                    queueNotEmpty.Wait(token);
                }
            }
        }

        /// <summary>
        /// Dequeue a single item from the queue, waiting until one is available
        /// if the queue is empty.
        /// </summary>
        /// <param name="token">Optional cancellation token</param>
        public async Task<T> DequeueAsync(CancellationToken token = default)
        {
            using (await queueMutex.LockAsync(token))
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    var r = queue.TryDequeue(out var item);
                    UpdateMetrics();
                    if (r)
                    {
                        queueNotFull.Notify();
                        return item;
                    }
                    await queueNotEmpty.WaitAsync(token);
                }
            }
        }

        /// <summary>
        /// Empty the queue, ignoring the contents.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        public async Task Clear(CancellationToken token = default)
        {
            using (await queueMutex.LockAsync(token))
            {
                queue.Clear();
                UpdateMetrics();
                queueNotFull.NotifyAll();
            }
        }
    }
}

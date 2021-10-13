using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Essentially an inverse semaphore, blocks if and only if the counter is not at 0.
    /// Any thread that increments the counter MUST also decrement it. Use the OperationInstance class.
    /// </summary>
    sealed internal class OperationWaiter : IDisposable
    {
        private readonly object sLock = new object();
        private readonly SemaphoreSlim sem = new SemaphoreSlim(0);
        private int counter;
        private int waiting;
        public void Increment()
        {
            Interlocked.Increment(ref counter);
        }
        public void Decrement()
        {
            lock (sLock)
            {
                // If there are threads waiting, release them all here
                counter--;
                if (counter == 0 && waiting > 0)
                {
                    sem.Release(waiting);
                }
            }
        }
        public async Task<bool> Wait(int msTimeout, CancellationToken token)
        {
            lock (sLock)
            {
                // If there are no current operations, return without waiting.
                if (counter == 0) return true;
                // If not, increment the waiting token
                waiting++;
            }
            // Then wait for the semaphore to be released
            return await sem.WaitAsync(msTimeout, token);
        }

        public void Dispose()
        {
            lock (sLock)
            {
                if (waiting > 0)
                {
                    sem.Release(waiting);
                }
                sem.Dispose();
            }
        }
        public OperationInstance GetInstance()
        {
            return new OperationInstance(this);
        }
        sealed internal class OperationInstance : IDisposable
        {
            private readonly OperationWaiter waiter;
            internal OperationInstance(OperationWaiter waiter)
            {
                this.waiter = waiter;
                waiter.Increment();
            }

            public void Dispose()
            {
                waiter.Decrement();
            }
        }
    }
    
}

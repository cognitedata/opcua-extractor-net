using System;
using System.Collections.Generic;

namespace Cognite.OpcUa.Subscriptions
{
    public enum SubscriptionName
    {
        Audit,
        DataPoints,
        Events,
        ServiceLevel,
        NodeMetrics,
        RebrowseTriggers,
    }

    public interface ISubscriptionState
    {
        public uint SubscriptionId { get; }
        public SubscriptionName Name { get; }
        public DateTime LastModifiedTime { get; }
        public int NumMonitoredItems { get; }
    }

    public class SubscriptionStateCache
    {
        private class SubscriptionState : ISubscriptionState
        {
            public uint SubscriptionId { get; private set; }
            public SubscriptionName Name { get; private set; }
            public DateTime LastModifiedTime { get; private set; }
            public int NumMonitoredItems { get; private set; }

            public SubscriptionState(SubscriptionName name, uint id)
            {
                Name = name;
                SubscriptionId = id;
                LastModifiedTime = DateTime.UtcNow;
            }

            public void IncrementMonitoredItems(int num)
            {
                NumMonitoredItems += num;
                LastModifiedTime = DateTime.UtcNow;
            }
        }

        private readonly Dictionary<SubscriptionName, SubscriptionState> subscriptions = new();

        private readonly object subLock = new();

        public void InitSubscription(SubscriptionName name, uint id)
        {
            lock (subLock)
            {
                subscriptions[name] = new SubscriptionState(name, id);
            }
        }

        public void IncrementMonitoredItems(SubscriptionName name, int num)
        {
            lock (subLock)
            {
                if (!subscriptions.TryGetValue(name, out var sub)) return;
                sub.IncrementMonitoredItems(num);
            }
        }

        public ISubscriptionState? GetSubscriptionState(SubscriptionName name)
        {
            lock (subLock)
            {
                return subscriptions.GetValueOrDefault(name);
            }
        }
    }
}

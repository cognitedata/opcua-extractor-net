using Cognite.Extractor.Common;
using Cognite.OpcUa;
using Cognite.OpcUa.Subscriptions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Test.Utils
{
    public class DummyClientCallbacks : IClientCallbacks
    {
        public PeriodicScheduler PeriodicScheduler { get; }
        public bool Connected { get; set; }
        public int ServiceLevelCbCount { get; set; }
        public int LowServiceLevelCbCount { get; set; }
        public int ReconnectCbCount { get; set; }
        public int DisconnectCbCount { get; set; }
        public HashSet<SubscriptionName> ActivelyFailedSubscriptions { get; } = new();

        public DummyClientCallbacks(CancellationToken token)
        {
            PeriodicScheduler = new PeriodicScheduler(token);
        }

        public void OnServerDisconnect(UAClient source)
        {
            Connected = false;
            DisconnectCbCount++;
        }

        public Task OnServerReconnect(UAClient source)
        {
            Connected = true;
            ReconnectCbCount++;
            return Task.CompletedTask;
        }

        public Task OnServiceLevelAboveThreshold(UAClient source)
        {
            ServiceLevelCbCount++;
            return Task.CompletedTask;
        }

        public Task OnServicelevelBelowThreshold(UAClient source)
        {
            LowServiceLevelCbCount++;
            return Task.CompletedTask;
        }

        public void Reset()
        {
            DisconnectCbCount = 0;
            ReconnectCbCount = 0;
            ServiceLevelCbCount = 0;
            LowServiceLevelCbCount = 0;
        }

        public void OnSubscriptionFailure(SubscriptionName subscription)
        {
            ActivelyFailedSubscriptions.Add(subscription);
        }

        public void OnCreatedSubscription(SubscriptionName subscription)
        {
            ActivelyFailedSubscriptions.Remove(subscription);
        }

        public void ScheduleTask(Func<CancellationToken, Task> task, SchedulerTaskResult staticResult, string name)
        {
            PeriodicScheduler.ScheduleTask(name, task);
        }
    }
}

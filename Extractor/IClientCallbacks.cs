using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable;
using Cognite.OpcUa.Subscriptions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public interface IClientCallbacks
    {
        PeriodicScheduler PeriodicScheduler { get; }

        void ScheduleTask(Func<CancellationToken, Task> task, SchedulerTaskResult staticResult, string name);

        /// <summary>
        /// Invoked when client loses connection to server.
        /// </summary>
        void OnServerDisconnect(UAClient source);

        /// <summary>
        /// Invoked whenever the session reconnects to the server.
        /// </summary>
        Task OnServerReconnect(UAClient source);

        /// <summary>
        /// Invoked whenever the client service level goes from below to above
        /// the configured threshold.
        /// </summary>
        Task OnServiceLevelAboveThreshold(UAClient source);

        /// <summary>
        /// Invoked whenever the client service level goes from above to below
        /// the configured threshold.
        /// </summary>
        Task OnServicelevelBelowThreshold(UAClient source);

        /// <summary>
        /// Invoked when a subscription is dropped.
        /// </summary>
        /// <param name="subscription">Dropped subscription</param>
        void OnSubscriptionFailure(SubscriptionName subscription);

        /// <summary>
        /// Invoked when a subscription is created or recreated.
        /// </summary>
        /// <param name="subscription"></param>
        void OnCreatedSubscription(SubscriptionName subscription);
    }
}

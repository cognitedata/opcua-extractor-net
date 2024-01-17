using Cognite.Extractor.Common;
using Cognite.OpcUa.Subscriptions;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public interface IClientCallbacks
    {
        PeriodicScheduler TaskScheduler { get; }

        /// <summary>
        /// Invoked when client loses connection to server.
        /// </summary>
        Task OnServerDisconnect(UAClient source);

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

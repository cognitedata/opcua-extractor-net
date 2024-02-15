using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;

namespace Cognite.OpcUa.Subscriptions
{
    internal class RecreateSubscriptionTask : BaseCreateSubscriptionTask<MonitoredItem>
    {
        private readonly Subscription oldSubscription;

        public override string TaskName => $"Recreate subscription {oldSubscription.Id}";

        public RecreateSubscriptionTask(Subscription oldSubscription, SubscriptionName subscription, IClientCallbacks callbacks)
            : base(subscription, oldSubscription.MonitoredItems.ToDictionary(item => item.StartNodeId), callbacks)
        {
            this.oldSubscription = oldSubscription;
        }


        public override async Task<bool> ShouldRun(ILogger logger, SessionManager sessionManager, CancellationToken token)
        {
            var session = sessionManager.Session;
            // If the session is currently unset, we will recreate all subscriptions eventually,
            // so no point to doing it now.
            if (session == null) return false;
            if (!oldSubscription.PublishingStopped)
            {
                return false;
            }
            if (!session.Subscriptions.Any(s => s.Id == oldSubscription.Id)) return false;
            try
            {
                var result = await session.ReadAsync(null, 0, TimestampsToReturn.Neither, new ReadValueIdCollection {
                        new ReadValueId {
                            NodeId = Variables.Server_ServerStatus_State,
                            AttributeId = Attributes.Value
                        }
                    }, token);
                var dv = result.Results.First();
                var state = (ServerState)(int)dv.Value;
                // If the server is in a bad state that is why the subscription is failing

                if (state != ServerState.Running)
                {
                    logger.LogWarning("Server is in a non-running state {State}, not recreating subscription {Name}", state, SubscriptionName);
                    return false;
                }
            }
            catch (Exception ex)
            {
                logger.LogError("Failed to obtain server state when checking a failing subscription. Server is likely down: {}", ex.Message);
                return false;
            }

            return true;
        }

        public override async Task Run(ILogger logger, SessionManager sessionManager, FullConfig config, SubscriptionManager subManager, CancellationToken token)
        {
            var session = sessionManager.Session;

            // Should never be the case, but if it is we should just skip doing this.
            if (session == null) return;

            var subState = subManager.Cache.GetSubscriptionState(SubscriptionName);
            if (subState == null) return;

            var grace = config.Subscriptions.RecreateSubscriptionGracePeriodValue.Value;
            if (grace == Timeout.InfiniteTimeSpan) grace = TimeSpan.FromMilliseconds(oldSubscription.CurrentPublishingInterval * 8);

            var diff = DateTime.UtcNow - subState.LastModifiedTime;
            if (diff < TimeSpan.FromMilliseconds(oldSubscription.CurrentPublishingInterval * 8))
            {
                logger.LogWarning("Subscription {Name} was updated {Time} ago. Waiting until {Grace} has passed before recreating",
                    SubscriptionName, diff, grace);
                await Task.Delay(grace - diff, token);
            }

            if (!await ShouldRun(logger, sessionManager, token)) return;

            try
            {
                logger.LogWarning("Server is available, but subscription {Name} is not responding to notifications. Attempting to recreate.", SubscriptionName);
                Callbacks.OnSubscriptionFailure(SubscriptionName);
                await session.RemoveSubscriptionAsync(oldSubscription);
            }
            catch (ServiceResultException serviceEx)
            {
                var symId = StatusCode.LookupSymbolicId(serviceEx.StatusCode);
                logger.LogWarning("Error attempting to remove subscription {Name} from the server: {Err}. It has most likely been dropped. Attempting to recreate...", SubscriptionName, symId);
                // Second attempt shouldn't fail, and doesn't seem to, keeping this here as a backup.
                try
                {
                    await session.RemoveSubscriptionAsync(oldSubscription);
                }
                catch
                {
                    // This is not supposed to be used this way, but this method should be a foolproof way
                    // of ensuring the subscription gets removed properly.
                    session.RemoveTransferredSubscription(oldSubscription);
                }
            }
            finally
            {
                oldSubscription.Dispose();
            }

            await base.Run(logger, sessionManager, config, subManager, token);
        }

        protected override MonitoredItem CreateMonitoredItem(MonitoredItem item, FullConfig config)
        {
            return item;
        }
    }
}

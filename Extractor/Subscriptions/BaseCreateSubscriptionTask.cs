using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Subscriptions
{
    public abstract class BaseCreateSubscriptionTask<T> : PendingSubscriptionTask
    {
        public override SubscriptionName SubscriptionToCreate => SubscriptionName;
        public SubscriptionName SubscriptionName { get; }
        protected IClientCallbacks Callbacks { get; }
        protected Dictionary<NodeId, T> Items { get; }
        private static readonly Gauge numSubscriptions = Metrics
            .CreateGauge("opcua_subscriptions", "Number of active monitored items");

        protected BaseCreateSubscriptionTask(SubscriptionName name, Dictionary<NodeId, T> items, IClientCallbacks callbacks)
        {
            SubscriptionName = name;
            Items = items;
            Callbacks = callbacks;
        }

        protected abstract MonitoredItem CreateMonitoredItem(T item, FullConfig config);


        private async Task CreateItemsWithRetryInner(ILogger logger, int count, UARetryConfig retries, Subscription subscription, CancellationToken token)
        {
            await RetryUtil.RetryAsync($"create monitored items for {SubscriptionName.Name()}", async () =>
            {
                try
                {
                    await subscription.CreateItemsAsync(token);
                    numSubscriptions.Inc(count);
                }
                catch (Exception ex)
                {
                    throw ExtractorUtils.HandleServiceResult(logger, ex, ExtractorUtils.SourceOp.CreateMonitoredItems);
                }
            }, retries, ex => retries.ShouldRetryException(
                ex,
                // Do not retry on certain errors here.
                // If we actually lose connection to the server the subscription will probably be invalidated,
                // so we need to retry it anyway.
                // If it isn't invalid, the retry will actually pick up from where we left of, so we can handle retrying much less here.
                retries.FinalRetryStatusCodes.Except(outerStatusCodes)
            ), logger, token);
        }

        private async Task CreateItemsWithRetry(ILogger logger, UARetryConfig retries, Subscription subscription, FullConfig config, CancellationToken token)
        {
            var numToCreate = subscription.MonitoredItems.Count(m => !m.Created);
            if (numToCreate == 0) return;

            if (numToCreate > config.Source.SubscriptionChunk)
            {
                // This can happen if the subscription is created by the server, synchronized (somehow? still not sure why the SDK does this)
                // then needs to have its items created. The SDK is very dumb, and if this happens and there are thousands of non-created
                // monitored items, it will try to create them all in one request, obviously running into limits somewhere.

                // The only way to make it _not_ do that is to remove the non-created items before proceeding. This might work,
                // I have not been able to reproduce the issue locally.
                logger.LogWarning("Creating more than {Chunk} items, removing and re-adding all non-created monitored items",
                    config.Source.SubscriptionChunk);

                var toAdd = new List<MonitoredItem>();
                toAdd.AddRange(subscription.MonitoredItems.Where(m => !m.Created));
                subscription.RemoveItems(toAdd);

                foreach (var chunk in toAdd.ChunkBy(config.Source.SubscriptionChunk))
                {
                    subscription.AddItems(chunk);

                    await CreateItemsWithRetryInner(logger, numToCreate, retries, subscription, token);
                }
            }
            else
            {
                await CreateItemsWithRetryInner(logger, numToCreate, retries, subscription, token);
            }
        }

        private async Task CreateMonitoredItems(ILogger logger, FullConfig config, Subscription subscription, SubscriptionManager manager, CancellationToken token)
        {
            var hasSubscription = subscription.MonitoredItems.Select(s => s.ResolvedNodeId).ToHashSet();
            var toAdd = Items.Where(i => !hasSubscription.Contains(i.Key)).ToList();

            logger.LogInformation("{Count}/{Count2} monitored items already exist for subscription {Name}", Items.Count - toAdd.Count, Items.Count, SubscriptionName.Name());
            if (toAdd.Count != 0)
            {
                logger.LogInformation("Adding {Count} new monitored items to subscription {Name}", toAdd.Count, SubscriptionName.Name());

                int count = 0;
                foreach (var chunk in toAdd.ChunkBy(config.Source.SubscriptionChunk))
                {
                    token.ThrowIfCancellationRequested();

                    var items = chunk.Select(c => CreateMonitoredItem(c.Value, config)).ToList();
                    count += items.Count;
                    logger.LogDebug("Add {Count} new monitored items to {Name}, {Subscribed} / {Total}",
                        items.Count, SubscriptionName, count, toAdd.Count);
                    subscription.AddItems(items);

                    await CreateItemsWithRetry(logger, config.Source.Retries, subscription, config, token);

                    manager.Cache.IncrementMonitoredItems(SubscriptionName, items.Count);
                }
            }
            else
            {
                logger.LogInformation("All monitored items already exist for subscription {Name}, none added", SubscriptionName);
            }

            await CreateItemsWithRetry(logger, config.Source.Retries, subscription, config, token);
        }

        private async Task<Subscription> EnsureSubscriptionExists(ILogger logger, SessionManager sessionManager, FullConfig config, SubscriptionManager subManager, CancellationToken token)
        {
            var session = await sessionManager.WaitForSession();
            var subscription = session.Subscriptions.FirstOrDefault(sub => sub.DisplayName.StartsWith(SubscriptionName.Name(), StringComparison.InvariantCulture));

            if (subscription == null)
            {
                logger.LogInformation("Creating new subscription with name {Name}", SubscriptionName.Name());
                subscription = new Subscription(session.DefaultSubscription)
                {
                    PublishingInterval = config.Source.PublishingInterval,
                    DisplayName = SubscriptionName.Name(),
                    KeepAliveCount = config.Subscriptions.KeepAliveCount,
                    LifetimeCount = config.Subscriptions.LifetimeCount
                };
                subscription.PublishStatusChanged += subManager.OnSubscriptionPublishStatusChange;
            }
            if (!subscription.Created)
            {
                try
                {
                    session.AddSubscription(subscription);
                    await subscription.CreateAsync(token);
                    subManager.Cache.InitSubscription(SubscriptionName, subscription.Id);
                }
                catch (Exception ex)
                {
                    try
                    {
                        await session.RemoveSubscriptionAsync(subscription);
                    }
                    finally
                    {
                        subscription.PublishStatusChanged -= subManager.OnSubscriptionPublishStatusChange;
                        subscription.Dispose();
                    }
                    throw ExtractorUtils.HandleServiceResult(logger, ex, ExtractorUtils.SourceOp.CreateSubscription);
                }
            }
            return subscription;
        }

        private async Task RunInternal(ILogger logger, SessionManager sessionManager, FullConfig config, SubscriptionManager subManager, CancellationToken token)
        {
            var subscription = await RetryUtil.RetryResultAsync(
                $"ensure subscription {SubscriptionName.Name()}",
                () => EnsureSubscriptionExists(logger, sessionManager, config, subManager, token),
                config.Source.Retries,
                ex => config.Source.Retries.ShouldRetryException(
                    ex,
                    config.Source.Retries.FinalRetryStatusCodes.Except(outerStatusCodes)
                ),
                logger,
                token);

            await CreateMonitoredItems(logger, config, subscription, subManager, token);
        }

        // Only retry a few status codes in the outer scope. If inner retries are exhausted
        // there are only a few cases where we actually want to retry everything again.
        private static readonly uint[] outerStatusCodes = new[]
        {
            StatusCodes.BadSubscriptionIdInvalid,
            StatusCodes.BadNoSubscription,
            StatusCodes.BadSecureChannelClosed,
            StatusCodes.BadSecureChannelIdInvalid,
            StatusCodes.BadConnectionClosed,
            StatusCodes.BadServerNotConnected,
            StatusCodes.BadServerHalted,
            StatusCodes.BadShutdown,
            StatusCodes.BadCommunicationError,
            StatusCodes.BadNotConnected,
            StatusCodes.BadInvalidState
        };

        public override async Task Run(ILogger logger, SessionManager sessionManager, FullConfig config, SubscriptionManager subManager, CancellationToken token)
        {
            await RetryUtil.RetryAsync(
                $"create subscription {SubscriptionName.Name()}",
                () => RunInternal(logger, sessionManager, config, subManager, token),
                config.Source.Retries,
                ex => config.Source.Retries.ShouldRetryException(ex, outerStatusCodes),
                logger,
                token);
            Callbacks.OnCreatedSubscription(SubscriptionName);
            logger.LogDebug("Finished creating subscription {Name}", SubscriptionName);
        }
    }
}

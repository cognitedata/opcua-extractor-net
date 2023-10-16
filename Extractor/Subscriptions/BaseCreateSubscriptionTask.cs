using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Subscriptions
{
    public abstract class BaseCreateSubscriptionTask<T> : PendingSubscriptionTask
    {
        protected string SubscriptionName { get; }
        protected Dictionary<NodeId, T> Items { get; }
        private static readonly Gauge numSubscriptions = Metrics
            .CreateGauge("opcua_subscriptions", "Number of active monitored items");

        protected BaseCreateSubscriptionTask(string subscriptionName, Dictionary<NodeId, T> items)
        {
            SubscriptionName = subscriptionName;
            Items = items;
        }

        protected abstract MonitoredItem CreateMonitoredItem(T item, FullConfig config);


        private async Task CreateItemsWithRetry(ILogger logger, UARetryConfig retries, Subscription subscription, CancellationToken token)
        {
            var numToCreate = subscription.MonitoredItems.Count(m => !m.Created);
            if (numToCreate == 0) return;

            await RetryUtil.RetryAsync("create monitored items", async () =>
            {
                try
                {
                    await subscription.CreateItemsAsync(token);
                    numSubscriptions.Inc(numToCreate);
                }
                catch (Exception ex)
                {
                    throw ExtractorUtils.HandleServiceResult(logger, ex, ExtractorUtils.SourceOp.CreateMonitoredItems);
                }
            }, retries, retries.ShouldRetryException, logger, token);
        }

        private async Task CreateMonitoredItems(ILogger logger, FullConfig config, Subscription subscription, CancellationToken token)
        {
            var hasSubscription = subscription.MonitoredItems.Select(s => s.ResolvedNodeId).ToHashSet();
            var toAdd = Items.Where(i => !hasSubscription.Contains(i.Key)).ToList();
            if (toAdd.Any())
            {
                logger.LogInformation("Adding {Count} new monitored items to subscription {Name}", toAdd.Count, SubscriptionName);

                int count = 0;
                foreach (var chunk in toAdd.ChunkBy(config.Source.SubscriptionChunk))
                {
                    token.ThrowIfCancellationRequested();

                    var items = chunk.Select(c => CreateMonitoredItem(c.Value, config)).ToList();
                    count += items.Count;
                    logger.LogDebug("Add {Count} new monitored items to {Name}, {Subscribed} / {Total}",
                        items.Count, SubscriptionName, count, toAdd.Count);
                    subscription.AddItems(items);

                    await CreateItemsWithRetry(logger, config.Source.Retries, subscription, token);
                }
            }

            await CreateItemsWithRetry(logger, config.Source.Retries, subscription, token);
        }

        private async Task<Subscription> EnsureSubscriptionExists(ILogger logger, ISession session, FullConfig config, SubscriptionManager subManager, CancellationToken token)
        {
            var subscription = session.Subscriptions.FirstOrDefault(sub => sub.DisplayName.StartsWith(SubscriptionName, StringComparison.InvariantCulture));

            if (subscription == null)
            {
                logger.LogInformation("Creating new subscription with name {Name}", SubscriptionName);
                subscription = new Subscription(session.DefaultSubscription)
                {
                    PublishingInterval = config.Source.PublishingInterval,
                    DisplayName = SubscriptionName,
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
            var session = await sessionManager.WaitForSession();

            var subscription = await RetryUtil.RetryResultAsync(
                "ensure subscription",
                () => EnsureSubscriptionExists(logger, session, config, subManager, token),
                config.Source.Retries,
                config.Source.Retries.ShouldRetryException,
                logger,
                token);

            await CreateMonitoredItems(logger, config, subscription, token);
        }

        // Only retry a few status codes in the outer scope. If inner retries are exhausted
        // there are only a few cases where we actually want to retry everything again.
        private static readonly uint[] outerStatusCodes = new[]
        {
            StatusCodes.BadSubscriptionIdInvalid,
            StatusCodes.BadNoSubscription
        };

        public override async Task Run(ILogger logger, SessionManager sessionManager, FullConfig config, SubscriptionManager subManager, CancellationToken token)
        {
            await RetryUtil.RetryAsync(
                "create subscription",
                () => RunInternal(logger, sessionManager, config, subManager, token),
                config.Source.Retries,
                ex => config.Source.Retries.ShouldRetryException(ex, outerStatusCodes),
                logger,
                token);
            logger.LogDebug("Finished creating subscription");
        }
    }
}

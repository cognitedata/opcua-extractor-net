using System;
using System.Collections.Generic;
using Opc.Ua;
using Opc.Ua.Server;

namespace Server
{
    public class ServerIssueConfig
    {
        public int MaxBrowseResults { get; set; }
        public int MaxBrowseNodes { get; set; }
        public int MaxAttributes { get; set; }
        public int MaxMonitoredItems { get; set; }
        public int MaxHistoryNodes { get; set; }

        public int RemainingBrowse { get; set; }
        public int RemainingBrowseNext { get; set; }
        public int RemainingHistoryRead { get; set; }
        public int RemainingRead { get; set; }
        public int RemainingCreateMonitoredItems { get; set; }
        public int RemainingCreateSubscriptions { get; set; }

        public int RandomBrowseFailDenom { get; set; }
        public int RandomBrowseNextFailDenom { get; set; }
        public int RandomReadFailDenom { get; set; }
        public int RandomHistoryReadFailDenom { get; set; }
        public int RandomCreateMonitoredItemsFailDenom { get; set; }
        public int RandomCreateSubscriptionsFailDenom { get; set; }
        public Dictionary<NodeId, StatusCode> HistoryReadStatusOverride { get; } = new();

        public StatusCode SporadicFailureCode { get; set; } = StatusCodes.BadInternalError;
    }

    public class MaxPerRequestCallbacks : IServerRequestCallbacks
    {
        public ServerIssueConfig Issues { get; }

        public MaxPerRequestCallbacks(ServerIssueConfig issues)
        {
            Issues = issues;
        }

        public void OnBrowse(OperationContext context, BrowseDescriptionCollection nodesToBrowse)
        {
            if (Issues.MaxBrowseNodes > 0 && nodesToBrowse.Count > Issues.MaxBrowseNodes)
            {
                throw new ServiceResultException(StatusCodes.BadTooManyOperations);
            }
        }

        public void OnBrowseNext(OperationContext context, ByteStringCollection continuationPoints)
        {
            if (Issues.MaxBrowseNodes > 0 && continuationPoints.Count > Issues.MaxBrowseNodes)
            {
                throw new ServiceResultException(StatusCodes.BadTooManyOperations);
            }
        }

        public void OnHistoryRead(OperationContext context, ExtensionObject historyReadDetails, HistoryReadValueIdCollection nodesToRead)
        {
            if (Issues.MaxHistoryNodes > 0 && nodesToRead.Count > Issues.MaxHistoryNodes)
            {
                throw new ServiceResultException(StatusCodes.BadTooManyOperations);
            }
        }

        public void OnRead(OperationContext context, ReadValueIdCollection nodesToRead)
        {
            if (Issues.MaxAttributes > 0 && nodesToRead.Count > Issues.MaxAttributes)
            {
                throw new ServiceResultException(StatusCodes.BadTooManyOperations);
            }
        }

        public void OnCreateMonitoredItems(OperationContext context, uint subscriptionId, IList<MonitoredItemCreateRequest> itemsToCreate)
        {
            if (Issues.MaxMonitoredItems > 0 && itemsToCreate.Count > Issues.MaxMonitoredItems)
            {
                throw new ServiceResultException(StatusCodes.BadTooManyOperations);
            }
        }
    }

#pragma warning disable CA5394 // Random being used for insecure applications
    // There is no security connected to random here.

    public class RandomFailureCallbacks : IServerRequestCallbacks
    {
        public ServerIssueConfig Issues { get; }
        private readonly Random rand = new();

        public RandomFailureCallbacks(ServerIssueConfig issues)
        {
            Issues = issues;
        }

        private bool Roll(int denom)
        {
            return denom > 0 && rand.NextInt64(0, denom) == 0;
        }

        public void OnBrowse(OperationContext context, BrowseDescriptionCollection nodesToBrowse)
        {
            if (Roll(Issues.RandomBrowseFailDenom))
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnBrowseNext(OperationContext context, ByteStringCollection continuationPoints)
        {
            if (Roll(Issues.RandomBrowseNextFailDenom))
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnHistoryRead(OperationContext context, ExtensionObject historyReadDetails, HistoryReadValueIdCollection nodesToRead)
        {
            if (Roll(Issues.RandomHistoryReadFailDenom))
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnRead(OperationContext context, ReadValueIdCollection nodesToRead)
        {
            if (Roll(Issues.RandomReadFailDenom))
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnCreateMonitoredItems(OperationContext context, uint subscriptionId, IList<MonitoredItemCreateRequest> itemsToCreate)
        {
            if (Roll(Issues.RandomCreateMonitoredItemsFailDenom))
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnCreateSubscription(OperationContext context, double requestedPublishingInterval, uint requestedLifetimeCount, uint requestedMaxKeepAliveCount, uint maxNotificationsPerPublish, bool publishingEnabled, byte priority)
        {
            if (Roll(Issues.RandomCreateSubscriptionsFailDenom))
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }
    }

    public class FailureCountdownCallbacks : IServerRequestCallbacks
    {
        public ServerIssueConfig Issues { get; }

        public FailureCountdownCallbacks(ServerIssueConfig issues)
        {
            Issues = issues;
        }

        public void OnBrowse(OperationContext context, BrowseDescriptionCollection nodesToBrowse)
        {
            if (Issues.RemainingBrowse > 0 && (--Issues.RemainingBrowse) == 0)
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnBrowseNext(OperationContext context, ByteStringCollection continuationPoints)
        {
            if (Issues.RemainingBrowseNext > 0 && (--Issues.RemainingBrowseNext) == 0)
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnHistoryRead(OperationContext context, ExtensionObject historyReadDetails, HistoryReadValueIdCollection nodesToRead)
        {
            if (Issues.RemainingHistoryRead > 0 && (--Issues.RemainingHistoryRead) == 0)
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnRead(OperationContext context, ReadValueIdCollection nodesToRead)
        {
            if (Issues.RemainingRead > 0 && (--Issues.RemainingRead) == 0)
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnCreateMonitoredItems(OperationContext context, uint subscriptionId, IList<MonitoredItemCreateRequest> itemsToCreate)
        {
            if (Issues.RemainingCreateMonitoredItems > 0 && (--Issues.RemainingCreateMonitoredItems) == 0)
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }

        public void OnCreateSubscription(OperationContext context, double requestedPublishingInterval, uint requestedLifetimeCount, uint requestedMaxKeepAliveCount, uint maxNotificationsPerPublish, bool publishingEnabled, byte priority)
        {
            if (Issues.RemainingCreateSubscriptions > 0 && (--Issues.RemainingCreateSubscriptions) == 0)
            {
                throw new ServiceResultException(Issues.SporadicFailureCode);
            }
        }
    }
}
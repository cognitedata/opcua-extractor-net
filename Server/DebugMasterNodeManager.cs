using Opc.Ua;
using Opc.Ua.Server;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Server
{
    public class ServerIssueConfig
    {
        public int MaxBrowseResults { get; set; }
        public int MaxBrowseNodes { get; set; }
        public int MaxAttributes { get; set; }
        public int MaxSubscriptions { get; set; }
        public int MaxHistoryNodes { get; set; }
        public int RemainingBrowseCount { get; set; }
    }
    /// <summary>
    /// The master node manager is called from the server with most "regular" service calls.
    /// It can be extended to override some behavior. Here the reason for overriding is to mock
    /// bad or irregular behavior for some services, for testing.
    /// </summary>
    public class DebugMasterNodeManager : MasterNodeManager
    {
        private readonly ServerIssueConfig issues;
        public DebugMasterNodeManager(
            IServerInternal server,
            ApplicationConfiguration config,
            string dynamicNamespaceUri,
            ServerIssueConfig issueConfig,
            params INodeManager[] nodeManagers) : base(server, config, dynamicNamespaceUri, nodeManagers)
        {
            issues = issueConfig;
        }
        public override void Read(
            OperationContext context,
            double maxAge,
            TimestampsToReturn timestampsToReturn,
            ReadValueIdCollection nodesToRead,
            out DataValueCollection values,
            out DiagnosticInfoCollection diagnosticInfos)
        {
            if (nodesToRead == null) throw new ArgumentNullException(nameof(nodesToRead));
            if (issues.MaxAttributes > 0 && nodesToRead.Count > issues.MaxAttributes)
            {
                values = new DataValueCollection { new DataValue { StatusCode = StatusCodes.BadTooManyOperations } };
                diagnosticInfos = new DiagnosticInfoCollection();
                return;
            }
            base.Read(context, maxAge, timestampsToReturn, nodesToRead, out values, out diagnosticInfos);
        }

        public override void CreateMonitoredItems(
            OperationContext context,
            uint subscriptionId,
            double publishingInterval,
            TimestampsToReturn timestampsToReturn,
            IList<MonitoredItemCreateRequest> itemsToCreate,
            IList<ServiceResult> errors,
            IList<MonitoringFilterResult> filterResults,
            IList<IMonitoredItem> monitoredItems)
        {
            if (itemsToCreate == null) throw new ArgumentNullException(nameof(itemsToCreate));
            if (errors == null) throw new ArgumentNullException(nameof(errors));
            if (issues.MaxSubscriptions > 0 && itemsToCreate.Count > issues.MaxSubscriptions)
            {
                errors.Add(StatusCodes.BadTooManyOperations);
                return;
            }

            base.CreateMonitoredItems(context, subscriptionId, publishingInterval, timestampsToReturn, itemsToCreate, errors, filterResults, monitoredItems);
        }

        public override void HistoryRead(
            OperationContext context,
            ExtensionObject historyReadDetails,
            TimestampsToReturn timestampsToReturn,
            bool releaseContinuationPoints,
            HistoryReadValueIdCollection nodesToRead,
            out HistoryReadResultCollection results,
            out DiagnosticInfoCollection diagnosticInfos)
        {
            if (nodesToRead == null) throw new ArgumentNullException(nameof(nodesToRead));
            if (issues.MaxHistoryNodes > 0 && nodesToRead.Count > issues.MaxHistoryNodes)
            {
                results = new HistoryReadResultCollection { new HistoryReadResult { StatusCode = StatusCodes.BadTooManyOperations } };
                diagnosticInfos = new DiagnosticInfoCollection();
                return;
            }
            base.HistoryRead(context, historyReadDetails, timestampsToReturn, releaseContinuationPoints, nodesToRead, out results, out diagnosticInfos);
        }

        public override void Browse(
            OperationContext context,
            ViewDescription view,
            uint maxReferencesPerNode,
            BrowseDescriptionCollection nodesToBrowse,
            out BrowseResultCollection results,
            out DiagnosticInfoCollection diagnosticInfos)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (nodesToBrowse == null) throw new ArgumentNullException(nameof(nodesToBrowse));
            if (issues.MaxBrowseNodes > 0 && nodesToBrowse.Count > issues.MaxBrowseNodes)
            {
                results = new BrowseResultCollection() { new BrowseResult { StatusCode = StatusCodes.BadTooManyOperations } };
                diagnosticInfos = new DiagnosticInfoCollection();
                return;
            }

            if (issues.RemainingBrowseCount > 0)
            {
                if (issues.RemainingBrowseCount == 1)
                {
                    results = new BrowseResultCollection { new BrowseResult { StatusCode = StatusCodes.BadTooManyOperations } };
                    diagnosticInfos = new DiagnosticInfoCollection();
                    issues.RemainingBrowseCount = 0;
                    return;
                }
                issues.RemainingBrowseCount--;
            }

            base.Browse(context, view, maxReferencesPerNode, nodesToBrowse, out results, out diagnosticInfos);

            if (issues.MaxBrowseResults > 0)
            {
                int count = 0;
                foreach (var result in results)
                {
                    if (result.ContinuationPoint != null)
                    {
                        var cp = context.Session.RestoreContinuationPoint(result.ContinuationPoint);
                        cp.Dispose();
                        result.ContinuationPoint = null;
                    }
                    int remaining = Math.Max(0, issues.MaxBrowseResults - count);
                    if (result.References.Count > remaining)
                    {
                        result.References = new ReferenceDescriptionCollection(result.References.Take(remaining));
                    }
                    count += result.References.Count;
                }
            }
        }
    }
}

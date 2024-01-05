/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

#pragma warning disable CA5394 // Random being used for insecure applications
// There is no security connected to random here.

using Opc.Ua;
using Opc.Ua.Server;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Server
{
    /// <summary>
    /// The master node manager is called from the server with most "regular" service calls.
    /// It can be extended to override some behavior. Here the reason for overriding is to mock
    /// bad or irregular behavior for some services, for testing.
    /// </summary>
    public class DebugMasterNodeManager : MasterNodeManager
    {
        private readonly ServerIssueConfig issues;

        private readonly TestServer server;

        private IServerRequestCallbacks callbacks => server.Callbacks;

        public DebugMasterNodeManager(
            IServerInternal server,
            ApplicationConfiguration config,
            string dynamicNamespaceUri,
            ServerIssueConfig issueConfig,
            TestServer testServer,
            params INodeManager[] nodeManagers) : base(server, config, dynamicNamespaceUri, nodeManagers)
        {
            this.server = testServer;
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
            ArgumentNullException.ThrowIfNull(nodesToRead);

            callbacks.OnRead(context, nodesToRead);

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
            ArgumentNullException.ThrowIfNull(itemsToCreate);
            ArgumentNullException.ThrowIfNull(errors);

            callbacks.OnCreateMonitoredItems(context, subscriptionId, itemsToCreate);

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
            ArgumentNullException.ThrowIfNull(nodesToRead);

            callbacks.OnHistoryRead(context, historyReadDetails, nodesToRead);

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
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(nodesToBrowse);

            callbacks.OnBrowse(context, nodesToBrowse);

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

        public override void BrowseNext(OperationContext context, bool releaseContinuationPoints, ByteStringCollection continuationPoints, out BrowseResultCollection results, out DiagnosticInfoCollection diagnosticInfos)
        {
            callbacks.OnBrowseNext(context, continuationPoints);

            base.BrowseNext(context, releaseContinuationPoints, continuationPoints, out results, out diagnosticInfos);
        }
    }
}

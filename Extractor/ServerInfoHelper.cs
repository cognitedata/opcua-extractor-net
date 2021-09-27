﻿/* Cognite Extractor for OPC-UA
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

using Opc.Ua;
using Serilog;
using System;
using System.Threading;

namespace Cognite.OpcUa
{
    public class ServerInfoHelper
    {
        private readonly UAClient client;
        private readonly ILogger log = Log.Logger.ForContext(typeof(ServerInfoHelper));
        private readonly NodeId[] idsToRead = new[]
        {
            VariableIds.Server_ServerCapabilities_MaxBrowseContinuationPoints,
            VariableIds.Server_ServerCapabilities_MaxHistoryContinuationPoints,
            VariableIds.Server_ServerCapabilities_OperationLimits_MaxMonitoredItemsPerCall,
            VariableIds.Server_ServerCapabilities_OperationLimits_MaxNodesPerBrowse,
            VariableIds.Server_ServerCapabilities_OperationLimits_MaxNodesPerHistoryReadData,
            VariableIds.Server_ServerCapabilities_OperationLimits_MaxNodesPerHistoryReadEvents,
            VariableIds.Server_ServerCapabilities_OperationLimits_MaxNodesPerRead
        };
        public ServerInfoHelper(UAClient client)
        {
            this.client = client;
        }

        /// <summary>
        /// Read from the server configuration to determine upper limits on operations.
        /// Ensures that if the server exposes these values we can avoid exceeding them.
        /// </summary>
        /// <param name="config">Configuration object to modify</param>
        public void LimitConfigValues(FullConfig config, CancellationToken token)
        {
            if (!config.Source.LimitToServerConfig) return;

            log.Information("Reading values from server configuration to determine upper limits");

            var values = client.ReadRawValues(idsToRead, token);

            int SafeValue(int cVal, DataValue sVal, string name)
            {
                if (StatusCode.IsBad(sVal.StatusCode)) return cVal;
                int val = 0;
                try
                {
                    val = Convert.ToInt32(sVal.Value);
                } catch { }
                if ((cVal > val || cVal == 0) && val > 0)
                {
                    log.Information("Max {name} is restricted to {val}", name, val);
                    return val;
                }
                else if (val > 0 && val > cVal)
                {
                    log.Information("Upper limit on {name} is {val}, but configured to {cVal}", name, val, cVal);
                }
                return cVal;
            }

            config.Source.BrowseThrottling.MaxNodeParallelism = SafeValue(
                config.Source.BrowseThrottling.MaxNodeParallelism, values[idsToRead[0]], "browse node parallelism");
            config.History.Throttling.MaxNodeParallelism = SafeValue(
                config.History.Throttling.MaxNodeParallelism, values[idsToRead[1]], "history node parallelism");
            config.Source.SubscriptionChunk = SafeValue(
                config.Source.SubscriptionChunk, values[idsToRead[2]], "subscription chunk");
            config.Source.BrowseNodesChunk = SafeValue(
                config.Source.BrowseNodesChunk, values[idsToRead[3]], "browse nodes chunk");
            config.History.DataNodesChunk = SafeValue(
                config.History.DataNodesChunk, values[idsToRead[4]], "datapoint history nodes chunk");
            config.History.EventNodesChunk = SafeValue(
                config.History.EventNodesChunk, values[idsToRead[5]], "event history nodes chunk");
            config.Source.AttributesChunk = SafeValue(
                config.Source.AttributesChunk, values[idsToRead[6]], "attribute read chunk");
        }
    }
}

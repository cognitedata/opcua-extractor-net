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

using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public class ServerInfoHelper
    {
        private readonly UAClient client;
        private readonly ILogger log;
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
        public ServerInfoHelper(ILogger<ServerInfoHelper> log, UAClient client)
        {
            this.log = log;
            this.client = client;
        }

        /// <summary>
        /// Read from the server configuration to determine upper limits on operations.
        /// Ensures that if the server exposes these values we can avoid exceeding them.
        /// </summary>
        /// <param name="config">Configuration object to modify</param>
        public async Task LimitConfigValues(FullConfig config, CancellationToken token)
        {
            if (!config.Source.LimitToServerConfig) return;

            log.LogInformation("Reading values from server configuration to determine upper limits");

            var values = await client.ReadRawValues(idsToRead, token);

            // Log detailed information about each value read from server
            var serverCapabilityNames = new[]
            {
                "MaxBrowseContinuationPoints",
                "MaxHistoryContinuationPoints", 
                "MaxMonitoredItemsPerCall",
                "MaxNodesPerBrowse",
                "MaxNodesPerHistoryReadData",
                "MaxNodesPerHistoryReadEvents",
                "MaxNodesPerRead"
            };

            log.LogInformation("Server capabilities read results:");
            for (int i = 0; i < idsToRead.Length; i++)
            {
                var nodeId = idsToRead[i];
                var dataValue = values[nodeId];
                var capabilityName = serverCapabilityNames[i];
                
                if (StatusCode.IsBad(dataValue.StatusCode))
                {
                    log.LogWarning("  {CapabilityName}: Failed to read (StatusCode: {StatusCode})", 
                        capabilityName, dataValue.StatusCode);
                }
                else
                {
                    try
                    {
                        var serverValue = Convert.ToInt32(dataValue.Value);
                        if (serverValue > 0)
                        {
                            log.LogInformation("  {CapabilityName}: {ServerValue} (successfully read from server)", 
                                capabilityName, serverValue);
                        }
                        else
                        {
                            log.LogInformation("  {CapabilityName}: {ServerValue} (server returned 0 or null)", 
                                capabilityName, serverValue);
                        }
                    }
                    catch (Exception ex)
                    {
                        log.LogWarning("  {CapabilityName}: Failed to convert value '{Value}' to integer ({Error})", 
                            capabilityName, dataValue.Value, ex.Message);
                    }
                }
            }

            int SafeValue(int cVal, DataValue sVal, string name)
            {
                int val = 0;
                if (!StatusCode.IsBad(sVal.StatusCode))
                {
                    try
                    {
                        val = Convert.ToInt32(sVal.Value);
                    }
                    catch { }
                }

                if ((cVal > val || cVal == 0) && val > 0)
                {
                    log.LogInformation("Max {Name} is restricted to {Val}", name, val);
                    return val;
                }
                else if (val > 0 && val > cVal)
                {
                    log.LogInformation("Upper limit on {Name} is {Val}, but configured to {CVal}", name, val, cVal);
                }
                else if (cVal == 0)
                {
                    log.LogWarning("No upper limit is set on {Name} by the server, the extractor will continue with {CVal}", name, cVal);
                }
                else if (val <= 0)
                {
                    log.LogInformation("Server returned invalid value ({Val}) for {Name}, using configured value {CVal}", val, name, cVal);
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

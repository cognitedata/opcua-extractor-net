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

using Cognite.OpcUa.Utils;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Config
{
    public partial class UAServerExplorer : UAClient
    {
        /// <summary>
        /// Try connecting to the server, and treating it as a discovery server, to list other endpoints on the same server.
        /// </summary>
        public async Task GetEndpoints(CancellationToken token)
        {
            log.LogInformation("Attempting to list endpoints using given url as discovery server");

            if (AppConfig == null)
            {
                await LoadAppConfig();
            }

            var context = AppConfig!.CreateMessageContext();
            var endpointConfig = EndpointConfiguration.Create(AppConfig);
            var endpoints = new EndpointDescriptionCollection();
            using (var channel = DiscoveryChannel.Create(new Uri(Config.Source.EndpointUrl), endpointConfig, context))
            {
                using var disc = new DiscoveryClient(channel);
                try
                {
                    endpoints = await disc.GetEndpointsAsync(null);
                    Summary.Session.Endpoints = endpoints.Select(ep => $"{ep.EndpointUrl}: {ep.SecurityPolicyUri}").ToList();
                }
                catch (Exception e)
                {
                    log.LogWarning("Endpoint discovery failed, the given URL may not be a discovery server.");
                    log.LogDebug(e, "Endpoint discovery failed");
                }
            }


            bool openExists = false;
            bool secureExists = false;

            foreach (var ep in endpoints)
            {
                log.LogInformation("Endpoint: {Url}, Security: {Security}", ep.EndpointUrl, ep.SecurityPolicyUri);
                openExists |= ep.SecurityPolicyUri == SecurityPolicies.None;
                secureExists |= ep.SecurityPolicyUri != SecurityPolicies.None;
                Summary.Session.Secure = secureExists;
            }

            if (Session == null || !Session.Connected)
            {
                try
                {
                    await Run(token, 0);
                    await LimitConfigValues(token);
                }
                catch (Exception ex)
                {
                    log.LogError("Failed to connect to server using initial options");
                    log.LogDebug(ex, "Failed to connect to endpoint");
                }
            }

            if (Session == null || !Session.Connected)
            {
                if (!secureExists && !openExists)
                {
                    log.LogInformation("No endpoint found, make sure the given discovery url is correct");
                }
                else if (!secureExists && Config.Source.Secure)
                {
                    log.LogInformation("No secure endpoint exists, so connection will fail if Secure is true");
                }
                else if (openExists && Config.Source.Secure)
                {
                    log.LogInformation("Secure connection failed, username or password may be wrong, or the client" +
                                    "may need to be added to a trusted list in the server.");
                    log.LogInformation("An open endpoint exists, so if secure is set to false and no username/password is provided" +
                                    "connection may succeed");
                }
                else if (!Config.Source.Secure && !openExists)
                {
                    log.LogInformation("Secure is set to false, but no open endpoint exists. Either set secure to true," +
                                    "or add an open endpoint to the server");
                }

                throw new FatalException("Fatal: Provided configuration failed to connect to the server");
            }

            Session.KeepAliveInterval = Math.Max(Config.Source.KeepAliveInterval, 30000);
        }
    }
}

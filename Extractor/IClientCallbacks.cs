using Cognite.Extractor.Common;
using System;
using System.Collections.Generic;
using System.Text;
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
    }
}

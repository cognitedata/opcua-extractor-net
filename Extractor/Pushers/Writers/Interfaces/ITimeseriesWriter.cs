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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers.Interfaces
{
    public interface ITimeseriesWriter
    {
        /// <summary>
        /// Synchronizes all BaseUANode to CDF Timeseries
        /// </summary>
        /// <param name="extractor">UAExtractor instance<param>
        /// <param name="timeseriesMap">Dictionary of mapping of variables to keys</param>
        /// <param name="nodeToAssetIds">Node to assets to ids</param>
        /// <param name="mismatchedTimeseries">Mismatched timeseries</param>
        /// <param name="update">Type update configuration</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Operation result</returns>
        Task<Result> PushVariables(
            UAExtractor extractor,
            IDictionary<string, UAVariable> timeseriesMap,
            IDictionary<NodeId, long> nodeToAssetIds,
            HashSet<string> mismatchedTimeseries,
            TypeUpdateConfig update,
            CancellationToken token
        );
    }
}

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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.Writers.Dtos;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.Writers
{
    public class MinimalTimeseriesWriter : BaseTimeseriesWriter<MinimalTimeseriesWriter>
    {
        public MinimalTimeseriesWriter(
            ILogger<MinimalTimeseriesWriter> logger,
            CogniteDestination destination,
            FullConfig config
        )
            : base(logger, destination, config) { }

        protected override IEnumerable<TimeSeriesCreate> BuildTimeseries(IDictionary<string, UAVariable> tsMap,
                IEnumerable<string> ids, UAExtractor extractor,  IDictionary<NodeId, long> nodeToAssetIds, Result result)
        {
            var tss = ids.Select(id => tsMap[id]);
                var creates = tss.Select(ts => ts.ToMinimalTimeseries(extractor, config.Cognite?.DataSet?.Id))
                    .Where(ts => ts != null);
                result.Created += creates.Count();
                return creates;
        }

        protected override Task UpdateTimeseries(UAExtractor extractor, IDictionary<string, UAVariable> tsMap,
                IEnumerable<TimeSeries> timeseries, IDictionary<NodeId, long> nodeToAssetIds, TypeUpdateConfig update, Result result, CancellationToken token)
        {
            return Task.CompletedTask;
        }
    }
}

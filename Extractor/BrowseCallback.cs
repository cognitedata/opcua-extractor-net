/* Cognite Extractor for OPC-UA
Copyright (C) 2022 Cognite AS

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

using Cognite.Extractor.Utils;
using Cognite.ExtractorUtils;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public class BrowseReport
    {
        public string? IdPrefix { get; set; }
        public int AssetsUpdated { get; set; }
        public int AssetsCreated { get; set; }
        public int TimeSeriesUpdated { get; set; }
        public int TimeSeriesCreated { get; set; }
        public int RelationshipsCreated { get; set; }
        public int MinimalTimeSeriesCreated { get; set; }
        public int Timestamp { get; set; }
        public bool Rebrowse { get; set; }
    }

    internal class BrowseCallback : FunctionCallWrapper<BrowseReport>
    {
        private readonly bool callOnEmpty;
        public BrowseCallback(CogniteDestination destination, BrowseCallbackConfig config, ILogger log)
            : base(destination, config, log)
        {
            callOnEmpty = config.ReportOnEmpty;
        }

        public async Task Call(BrowseReport report, CancellationToken token)
        {
            if (report == null) return;
            if (!callOnEmpty
                && report.AssetsCreated == 0
                && report.AssetsUpdated == 0
                && report.TimeSeriesCreated == 0
                && report.TimeSeriesUpdated == 0
                && report.RelationshipsCreated == 0
                && report.MinimalTimeSeriesCreated == 0) return;

            await TryCall(report, token);
        }
    }
}

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
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa
{
    public class BrowseReport
    {
        public string? IdPrefix { get; set; }
        public int AssetsUpdated { get; set; }
        public int RawAssetsUpdated { get; set; }
        public int AssetsCreated { get; set; }
        public int RawAssetsCreated { get; set; }
        public int TimeSeriesUpdated { get; set; }
        public int RawTimeseriesUpdated { get; set; }
        public int TimeSeriesCreated { get; set; }
        public int RawTimeseriesCreated { get; set; }
        public int RelationshipsCreated { get; set; }
        public int RawRelationshipsCreated { get; set; }
        public int MinimalTimeSeriesCreated { get; set; }
        public string? RawDatabase { get; set; }
        public string? AssetsTable { get; set; }
        public string? TimeSeriesTable { get; set; }
        public string? RelationshipsTable { get; set; }
    }
}

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

using System;
using System.Collections.Generic;

namespace Cognite.OpcUa.Config
{
    /// <summary>
    /// Contains data about a run of the configuration tool.
    /// </summary>
    public class Summary
    {
        public IList<string> Endpoints { get; set; } = new List<string>();
        public bool Secure { get; set; }
        public int BrowseNodesChunk { get; set; }
        public int BrowseChunk { get; set; }
        public bool BrowseNextWarning { get; set; }
        public int CustomNumTypesCount { get; set; }
        public int MaxArraySize { get; set; }
        public bool StringVariables { get; set; }
        public int AttributeChunkSize { get; set; }
        public bool VariableLimitWarning { get; set; }
        public int SubscriptionChunkSize { get; set; }
        public bool SubscriptionLimitWarning { get; set; }
        public bool SilentSubscriptionsWarning { get; set; }
        public int HistoryChunkSize { get; set; }
        public bool NoHistorizingNodes { get; set; }
        public bool BackfillRecommended { get; set; }
        public bool HistoricalEvents { get; set; }
        public bool AnyEvents { get; set; }
        public int NumEmitters { get; set; }
        public int NumHistEmitters { get; set; }
        public IList<string> NamespaceMap { get; set; } = new List<string>();
        public TimeSpan HistoryGranularity { get; set; }
        public bool Enums { get; set; }
        public bool Auditing { get; set; }
        public bool Subscriptions { get; set; }
        public bool History { get; set; }
        public bool NullDataType { get; set; }
        public bool MissingDataType { get; set; }
    }
}

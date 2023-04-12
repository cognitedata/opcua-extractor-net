/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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

namespace Cognite.OpcUa.Config
{
    public class UAThrottlingConfig
    {
        /// <summary>
        /// Maximum number of requests per minute, approximately.
        /// </summary>
        public int MaxPerMinute { get; set; }
        /// <summary>
        /// Maximum number of parallel requests.
        /// </summary>
        public int MaxParallelism { get; set; }
    }
    public class ContinuationPointThrottlingConfig : UAThrottlingConfig
    {
        /// <summary>
        /// Maximum number of nodes accross all parallel requests.
        /// </summary>
        public int MaxNodeParallelism { get; set; }
    }

    public class ProtoNodeId
    {
        /// <summary>
        /// NamespaceUri of the NodeId.
        /// </summary>
        public string? NamespaceUri { get; set; }
        /// <summary>
        /// Identifier of the NodeId, on the form i=123 or s=string, etc.
        /// </summary>
        public string? NodeId { get; set; }
        public NodeId ToNodeId(UAClient client)
        {
            var node = client.ToNodeId(NodeId, NamespaceUri);
            return node;
        }
    }
    public class ProtoDataType
    {
        /// <summary>
        /// NodeId of the data type.
        /// </summary>
        public ProtoNodeId? NodeId { get; set; }
        /// <summary>
        /// True if is-step should be set on timeseries in CDF.
        /// </summary>
        public bool IsStep { get; set; }
        /// <summary>
        /// True if this is an enum.
        /// </summary>
        public bool Enum { get; set; }
    }
    public interface IPusherConfig
    {
        /// <summary>
        /// True to not write to destination, as a kind of dry-run for this destination.
        /// </summary>
        bool Debug { get; set; }
        /// <summary>
        /// If applicable, read the ranges of extracted variables from the destination.
        /// </summary>
        bool ReadExtractedRanges { get; set; }
        /// <summary>
        /// Replacement for NaN values.
        /// </summary>
        public double? NonFiniteReplacement { get; set; }
    }
}

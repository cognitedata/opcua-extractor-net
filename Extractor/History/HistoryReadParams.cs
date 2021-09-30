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

using Cognite.Extractor.Common;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Cognite.OpcUa.History
{
    public enum HistoryReadType
    {
        FrontfillData,
        BackfillData,
        FrontfillEvents,
        BackfillEvents
    }
    public class HistoryReadNode
    {
        public HistoryReadNode(HistoryReadType type, UAHistoryExtractionState state)
        {
            Type = type;
            State = state;
            Id = state.SourceId;
            if (Id == null || Id.IsNullNodeId) throw new InvalidOperationException("NodeId may not be null");
        }
        /// <summary>
        /// Results in silently uninitilized State, unsafe.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        public HistoryReadNode(HistoryReadType type, NodeId id)
        {
            Type = type;
            Id = id;
            if (Id == null || Id.IsNullNodeId) throw new InvalidOperationException("NodeId may not be null");
        }
        public HistoryReadType Type { get; }
        [NotNull, AllowNull]
        public UAHistoryExtractionState? State { get; set; }
        public DateTime Time =>
            Type == HistoryReadType.BackfillData || Type == HistoryReadType.BackfillEvents
            ? State.SourceExtractedRange.First : State.SourceExtractedRange.Last;
        public NodeId Id { get; }
        public byte[]? ContinuationPoint { get; set; }
        public bool Completed { get; set; }
        public int LastRead { get; set; }
        public int TotalRead { get; set; }
        public IEncodeable? LastResult { get; set; }
    }
    /// <summary>
    /// Parameter class containing the state of a single history read operation.
    /// </summary>
    public class HistoryReadParams : IChunk<HistoryReadNode>
    {
        public HistoryReadDetails Details { get; }
        public IList<HistoryReadNode> Nodes { get; set; }
        public Exception? Exception { get; set; }

        public IEnumerable<HistoryReadNode> Items => Nodes;

        public HistoryReadParams(IEnumerable<HistoryReadNode> nodes, HistoryReadDetails details)
        {
            Nodes = nodes.ToList();
            Details = details;
        }

        public bool Completed(HistoryReadNode item)
        {
            return item.Completed;
        }
    }
}

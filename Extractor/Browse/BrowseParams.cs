﻿/* Cognite Extractor for OPC-UA
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
using System.Linq;

namespace Cognite.OpcUa
{
    public class BrowseParams : IChunk<BrowseNode>
    {
        public Dictionary<NodeId, BrowseNode>? Nodes { get; set; }
        public NodeId ReferenceTypeId { get; set; } = ReferenceTypeIds.HierarchicalReferences;
        public uint NodeClassMask { get; set; }
        public bool IncludeSubTypes { get; set; } = true;
        public BrowseDirection BrowseDirection { get; set; } = BrowseDirection.Forward;
        public uint MaxPerNode { get; set; }
        public uint ResultMask { get; set; } =
            (uint)BrowseResultMask.NodeClass | (uint)BrowseResultMask.DisplayName | (uint)BrowseResultMask.IsForward
            | (uint)BrowseResultMask.ReferenceTypeId | (uint)BrowseResultMask.TypeDefinition | (uint)BrowseResultMask.BrowseName;

        public IEnumerable<BrowseNode> Items => Nodes?.Values ?? Enumerable.Empty<BrowseNode>();

        public Exception? Exception { get; set; }

        public BrowseDescription ToDescription(BrowseNode node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (node.ContinuationPoint != null) throw new ArgumentException("Node has already been read");
            return new BrowseDescription
            {
                BrowseDirection = BrowseDirection,
                NodeClassMask = NodeClassMask,
                IncludeSubtypes = IncludeSubTypes,
                NodeId = node.Id,
                ReferenceTypeId = ReferenceTypeId,
                ResultMask = ResultMask
            };
        }

        public bool Completed(BrowseNode item)
        {
            return item == null || item.ContinuationPoint == null;
        }

        public BrowseParams() { }
        public BrowseParams(BrowseParams other)
        {
            if (other == null) return;
            ReferenceTypeId = other.ReferenceTypeId;
            NodeClassMask = other.NodeClassMask;
            IncludeSubTypes = other.IncludeSubTypes;
            BrowseDirection = other.BrowseDirection;
            MaxPerNode = other.MaxPerNode;
            ResultMask = other.ResultMask;
        }
        public BrowseParams(BrowseParams other, IEnumerable<BrowseNode> nodes) : this(other)
        {
            Nodes = nodes.ToDictionary(node => node.Id);
        }
    }
    public class BrowseNode
    {
        public BrowseNode(NodeId id)
        {
            Id = id;
            Depth = 0;
        }
        public BrowseNode(NodeId id, BrowseNode parent)
        {
            Id = id;
            Depth = parent.Depth + 1;
        }
        public int Depth { get; }
        public NodeId Id { get; }
        public byte[]? ContinuationPoint { get; set; }
        public BrowseResult? Result { get; private set; }
        public void AddReferences(ReferenceDescriptionCollection references)
        {
            if (references == null) references = new ReferenceDescriptionCollection();
            if (Result == null)
            {
                Result = new BrowseResult(this, references);
            }
            else
            {
                Result.References.AddRange(references);
            }
        }
        private int referenceIdx;
        public IEnumerable<ReferenceDescription> GetNextReferences()
        {
            if (Result == null) return Enumerable.Empty<ReferenceDescription>();
            var refs = Result.References.Skip(referenceIdx).ToList();
            referenceIdx += refs.Count;
            return refs;
        }
    }
    public class BrowseResult
    {
        public BrowseNode Parent { get; }
        public ReferenceDescriptionCollection References { get; }
        public BrowseResult(BrowseNode parent, ReferenceDescriptionCollection references)
        {
            Parent = parent;
            References = references;
        }
    }
}

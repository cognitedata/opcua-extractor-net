/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents a non-hierarchical reference between two nodes in the hierarchy
    /// </summary>
    public class UAReference
    {
        /// <summary>
        /// NodeId of the OPC-UA reference type
        /// </summary>
        public UAReferenceType Type { get; }
        /// <summary>
        /// True if this is a forward reference, false otherwise
        /// </summary>
        public bool IsForward { get; }
        /// <summary>
        /// NodeId of the source node
        /// </summary>
        public ReferenceVertex Source { get; }
        /// <summary>
        /// NodeId of the target node
        /// </summary>
        public ReferenceVertex Target { get; }
        // Slight hack here to properly get vertex types without needing the full node objects.
        public UAReference(ReferenceDescription desc, UANode source,
            NodeId target, VariableExtractionState targetState, ReferenceTypeManager manager)
        {
            if (desc == null) throw new ArgumentNullException(nameof(desc));
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (target == null) throw new ArgumentNullException(nameof(target));
            if (manager == null) throw new ArgumentNullException(nameof(manager));
            Type = manager.GetReferenceType(desc.ReferenceTypeId);
            IsForward = desc.IsForward;
            Source = new ReferenceVertex(source.Id, (source is UAVariable variable) && !variable.IsArray);
            Target = new ReferenceVertex(target, desc.NodeClass == NodeClass.Variable && (targetState == null || !targetState.IsArray));
        }
        // For hierarchical references, here the source should always be an object...
        public UAReference(ReferenceDescription desc, NodeId source, UANode target, ReferenceTypeManager manager, bool inverse)
        {
            if (desc == null) throw new ArgumentNullException(nameof(desc));
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (target == null) throw new ArgumentNullException(nameof(target));
            if (manager == null) throw new ArgumentNullException(nameof(manager));
            Type = manager.GetReferenceType(desc.ReferenceTypeId);
            IsForward = !inverse;
            Source = new ReferenceVertex(source, false);
            Target = new ReferenceVertex(target.Id, target is UAVariable variable && !variable.IsArray);
            if (inverse)
            {
                var temp = Source;
                Source = Target;
                Target = temp;
            }
        }
        public string GetName()
        {
            return Type.GetName(!IsForward);
        }
        public override bool Equals(object obj)
        {
            if (!(obj is UAReference other)) return false;
            return other.Source == Source
                && other.Target == Target
                && other.Type.Id == Type.Id;
        }

        public override int GetHashCode()
        {
            return (Source, Target, Type.Id).GetHashCode();
        }
    }
    public class ReferenceVertex
    {
        public NodeId Id { get; }
        public int Index { get; }
        public bool IsTimeSeries { get; }
        public ReferenceVertex(NodeId id, bool isVariable)
        {
            Id = id;
            IsTimeSeries = isVariable;
        }
    }
}

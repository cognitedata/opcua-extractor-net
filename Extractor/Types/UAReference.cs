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
        public UAReference(NodeId type, bool isForward, NodeId source, NodeId target,
            bool sourceTs, bool targetTs, ReferenceTypeManager manager)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (target == null) throw new ArgumentNullException(nameof(target));
            if (manager == null) throw new ArgumentNullException(nameof(manager));
            Type = manager.GetReferenceType(type);
            IsForward = isForward;
            Source = new ReferenceVertex(source, sourceTs);
            Target = new ReferenceVertex(target, targetTs);
        }
        public override string ToString()
        {
            string refName = Type.GetName(!IsForward);
            if (refName == null)
            {
                refName = $"{Type.Id} {(IsForward ? "Forward" : "Inverse")}";
            }

            return $"Reference: {Source} {refName} {Target}";
        }
        public string GetName()
        {
            return Type.GetName(!IsForward);
        }
        public override bool Equals(object obj)
        {
            if (!(obj is UAReference other)) return false;
            return other.Source.Equals(Source)
                && other.Target.Equals(Target)
                && other.Type.Id == Type.Id
                && other.IsForward == IsForward;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Source, Target, Type.Id, IsForward);
        }

        public CogniteSdk.Beta.RelationshipCreate ToRelationship(long? dataSetId, UAExtractor extractor)
        {
            if (extractor == null) throw new ArgumentNullException(nameof(extractor));
            var relationship = new CogniteSdk.Beta.RelationshipCreate
            {
                DataSetId = dataSetId,
                SourceExternalId = extractor.GetUniqueId(Source.Id),
                TargetExternalId = extractor.GetUniqueId(Target.Id),
                SourceType = Source.GetVertexType(),
                TargetType = Target.GetVertexType(),
                ExternalId = extractor.GetRelationshipId(this)
            };
            return relationship;
        }
    }
    public class ReferenceVertex
    {
        public NodeId Id { get; }
        public bool IsTimeSeries { get; }
        public ReferenceVertex(NodeId id, bool isTimeSeries)
        {
            Id = id;
            IsTimeSeries = isTimeSeries;
        }
        public override string ToString()
        {
            return $"{(IsTimeSeries ? "TimeSeries" : "Asset")} {Id}";
        }
        public override bool Equals(object obj)
        {
            if (!(obj is ReferenceVertex other)) return false;
            return other.Id == Id && other.IsTimeSeries == IsTimeSeries; 
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Id, IsTimeSeries);
        }

        public CogniteSdk.Beta.RelationshipVertexType GetVertexType()
        {
            if (IsTimeSeries) return CogniteSdk.Beta.RelationshipVertexType.TimeSeries;
            return CogniteSdk.Beta.RelationshipVertexType.Asset;
        }
    }
}

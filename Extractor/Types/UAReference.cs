using Cognite.OpcUa.Nodes;
using CogniteSdk;
using Opc.Ua;
using System;

namespace Cognite.OpcUa.Types
{
    public class UAReference
    {
        public bool IsHierarchical => Type.IsChildOf(ReferenceTypeIds.HierarchicalReferences);
        public UAReferenceType Type { get; }
        public BaseUANode Source { get; }
        public BaseUANode Target { get; }
        public bool IsForward { get; }


        public UAReference(UAReferenceType referenceType, bool isForward, BaseUANode source, BaseUANode target)
        {
            Type = referenceType;
            Source = source;
            Target = target;
            IsForward = isForward;
        }

        public UAReference CreateInverse()
        {
            return new UAReference(Type, !IsForward, Source, Target);
        }

        public override string ToString()
        {
            string? refName = Type.GetName(!IsForward) ?? $"{Type.Id} {(IsForward ? "Forward" : "Inverse")}";

            return $"Reference {Source} {refName} {Target}";
        }

        public override bool Equals(object obj)
        {
            if (obj is not UAReference other) return false;
            return other.Source.Id == Source.Id
                && other.Target.Id == Target.Id
                && other.Type.Id == Type.Id
                && other.IsForward == IsForward;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Source.Id, Target.Id, Type.Id, IsForward);
        }

        private static RelationshipVertexType VertexType(BaseUANode node)
        {
            if (node is UAVariable vb && !vb.IsProperty && !vb.IsObject)
            {
                return RelationshipVertexType.TimeSeries;
            }
            return RelationshipVertexType.Asset;
        }

        public RelationshipCreate ToRelationship(long? dataSetId, IUAClientAccess client)
        {
            var source = IsForward ? Source : Target;
            var target = IsForward ? Target : Source;

            return new RelationshipCreate
            {
                DataSetId = dataSetId,
                SourceExternalId = client.GetUniqueId(source.Id),
                TargetExternalId = client.GetUniqueId(target.Id),
                SourceType = VertexType(source),
                TargetType = VertexType(target),
                ExternalId = client.GetRelationshipId(this)
            };
        }
    }
}

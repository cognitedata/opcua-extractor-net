using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Nodes
{
    public abstract class BaseUAType : BaseUANode
    {
        public BaseUAType(NodeId id, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
        {
        }

        public void Initialize(string? name, QualifiedName? browseName, BaseUANode? parent, NodeId? parentId)
        {
            Parent = parent;
            FallbackParentId = parentId;
            Attributes.DisplayName = name;
            Attributes.BrowseName = browseName;
        }


        public HashSet<TypeField> AllCollectedFields { get; } = null!;

        public IEnumerable<TypeField> CollectedFields =>
            AllCollectedFields.Where(f => f.Node.NodeClass == NodeClass.Variable);

        public bool IsCollected { get; set; }

        public bool IsChildOf(NodeId id)
        {
            return Id == id || Parent is BaseUAType parentType && parentType.IsChildOf(id);
        }
    }


    public class TypeField
    {
        public QualifiedNameCollection BrowsePath { get; }
        public string Name => BrowsePath.Last().Name;
        public BaseUANode Node { get; }
        public TypeField(BaseUANode node)
        {
            BrowsePath = new QualifiedNameCollection { node.Attributes.BrowseName };
            Node = node;
        }
        public TypeField(BaseUANode node, QualifiedNameCollection previousBrowsePath)
        {
            BrowsePath = new QualifiedNameCollection(previousBrowsePath);
            BrowsePath.Add(node.Attributes.BrowseName);
            Node = node;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 0;
                foreach (var name in BrowsePath)
                {
                    hash *= 31;
                    hash += HashCode.Combine(name.Name, name.NamespaceIndex);
                }
                return hash;
            }
        }
        public override bool Equals(object obj)
        {
            if (obj is not EventField otherField) return false;
            if (BrowsePath.Count != otherField.BrowsePath.Count) return false;

            for (int i = 0; i < BrowsePath.Count; i++)
            {
                if (BrowsePath[i].Name != otherField.BrowsePath[i].Name
                    || BrowsePath[i].NamespaceIndex != otherField.BrowsePath[i].NamespaceIndex) return false;
            }

            return true;
        }
    }
}

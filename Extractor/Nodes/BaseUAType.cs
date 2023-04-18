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


        public HashSet<TypeField> AllCollectedFields { get; set; } = null!;

        public IEnumerable<TypeField> CollectedFields =>
            AllCollectedFields.Where(f => f.Node.NodeClass == NodeClass.Variable);

        public bool IsCollected { get; set; }

        public bool IsChildOf(NodeId id)
        {
            return Id == id || Parent is BaseUAType parentType && parentType.IsChildOf(id);
        }
    }

    public class RawTypeField
    {
        public QualifiedNameCollection BrowsePath { get; }
        public string Name => BrowsePath.Last().Name;
        public RawTypeField(QualifiedName browseName)
        {
            BrowsePath = new QualifiedNameCollection { browseName };
        }
        public RawTypeField(QualifiedNameCollection browsePath)
        {
            BrowsePath = browsePath;
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
            if (obj is not RawTypeField otherField) return false;
            if (BrowsePath.Count != otherField.BrowsePath.Count) return false;

            for (int i = 0; i < BrowsePath.Count; i++)
            {
                if (BrowsePath[i].Name != otherField.BrowsePath[i].Name
                    || BrowsePath[i].NamespaceIndex != otherField.BrowsePath[i].NamespaceIndex) return false;
            }

            return true;
        }
    }

    public class TypeField : RawTypeField
    {
        public BaseUANode Node { get; }
        public TypeField(BaseUANode node) : base(node.Attributes.BrowseName!)
        {
            Node = node;
        }
    }
}

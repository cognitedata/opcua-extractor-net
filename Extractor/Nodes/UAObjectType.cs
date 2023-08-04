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

using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Nodes
{
    public class ObjectTypeAttributes : BaseNodeAttributes
    {
        public bool IsAbstract { get; private set; }
        public ObjectTypeAttributes() : base(NodeClass.ObjectType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.IsAbstract;
            foreach (var attr in base.GetAttributeSet(config)) yield return attr;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.IsAbstract:
                    IsAbstract = value.GetValue(false);
                    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }

        public void LoadFromNodeState(BaseObjectTypeState state)
        {
            IsAbstract = state.IsAbstract;
            LoadFromBaseNodeState(state);
        }
    }

    public class UAObjectType : BaseUAType
    {
        public UAObjectType(NodeId id, string? displayName, QualifiedName? browseName, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
        {
            FullAttributes = new ObjectTypeAttributes();
            Attributes.DisplayName = displayName;
            Attributes.BrowseName = browseName;
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UAObjectType(NodeId id) : this(id, null, null, null, null)
        {
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ObjectTypeAttributes FullAttributes { get; }

        public bool IsEventType()
        {
            if (Id == ObjectTypeIds.BaseEventType) return true;
            return EnumerateTypedAncestors<UAObjectType>().Any(tp => tp.Id == ObjectTypeIds.BaseEventType);
        }

        public override void Format(StringBuilder builder, int indent, bool writeParent = true, bool writeProperties = true)
        {
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}ObjectType: {1}", new string(' ', indent), Name);
            builder.AppendLine();
            base.Format(builder, indent + 4, writeParent);
        }
    }
}

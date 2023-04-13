using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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

using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Xml.Linq;

namespace Cognite.OpcUa.Nodes
{
    public class ReferenceTypeAttributes : BaseNodeAttributes
    {
        public string? InverseName { get; set; }
        public ReferenceTypeAttributes() : base(NodeClass.ReferenceType)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.InverseName;
            foreach (var attr in base.GetAttributeSet(config)) yield return attr;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.InverseName:
                    InverseName = value.GetValue<LocalizedText?>(null)?.Text;
                    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }

        public void LoadFromNodeState(ReferenceTypeState state)
        {
            InverseName = state.InverseName?.Text;
            LoadFromBaseNodeState(state);
        }
    }

    public class UAReferenceType : BaseUAType
    {
        public UAReferenceType(NodeId id, string? displayName, QualifiedName? browseName, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
        {
            FullAttributes = new ReferenceTypeAttributes();
            Attributes.DisplayName = displayName;
            Attributes.BrowseName = browseName;
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UAReferenceType(NodeId id) : this(id, null, null, null, null)
        {
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ReferenceTypeAttributes FullAttributes { get; }

        /// <summary>
        /// Retrieve a descriptor for this reference type.
        /// </summary>
        /// <param name="isInverse">True to get the inverse name of this reference type.</param>
        /// <returns>Descriptor for this reference, if set.</returns>
        public string? GetName(bool isInverse)
        {
            if (isInverse && !string.IsNullOrEmpty(FullAttributes.InverseName)) return FullAttributes.InverseName;
            return Name;
        }

        public override void Format(StringBuilder builder, int indent, bool writeParent = true, bool writeProperties = true)
        {
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}ReferenceType: {1}", new string(' ', indent), Name);
            builder.AppendLine();
            base.Format(builder, indent + 4, writeParent);

            var indt = new string(' ', indent + 4);
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}InverseName: {1}", indt, FullAttributes.InverseName);
            builder.AppendLine();
        }
    }
}

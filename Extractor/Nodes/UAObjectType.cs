﻿using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;

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
            yield return Attributes.Description;
            yield return Attributes.IsAbstract;
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
    }

    public class UAObjectType : BaseUANode
    {
        public UAObjectType(NodeId id, string? displayName, BaseUANode? parent) : base(id, displayName, parent)
        {
            FullAttributes = new ObjectTypeAttributes();
        }

        /// <summary>
        /// Uninitialized constructor, to be used when lazy-initializing
        /// </summary>
        public UAObjectType(NodeId id) : this(id, null, null)
        {
        }

        public void Initialize(ReferenceDescription referenceDesc, BaseUANode parent)
        {
            DisplayName = referenceDesc.DisplayName?.Text;
            Parent = parent;
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ObjectTypeAttributes FullAttributes { get; }
    }
}

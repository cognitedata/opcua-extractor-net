using Cognite.OpcUa.Config;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Nodes
{
    public abstract class BaseNodeAttributes
    {
        /// <summary>
        /// OPC-UA Description attribute
        /// </summary>
        public string? Description { get; private set; }
        /// <summary>
        /// True if this attribute collection has had its data populated at some point.
        /// </summary>
        public bool IsDataRead { get; private set; }
        /// <summary>
        /// List of properties belonging to this node.
        /// </summary>
        public IList<BaseUANode>? Properties { get; private set; }
        /// <summary>
        /// NodeClass of this node
        /// </summary>
        public NodeClass NodeClass { get; }

        public BaseNodeAttributes(NodeClass nodeClass)
        {
            NodeClass = nodeClass;
        }

        public virtual void LoadAttribute(DataValue value, uint attributeId)
        {
            switch (attributeId)
            {
                case Attributes.Description:
                    Description = value.GetValue<LocalizedText?>(null)?.Text;
                    break;
                default:
                    throw new InvalidOperationException($"Got unexpected unmatched attributeId, this is a bug: {attributeId}");
            }
        }

        public abstract IEnumerable<uint> GetAttributeSet(FullConfig config);
    }

    public abstract class BaseUANode
    {
        public abstract BaseNodeAttributes Attributes { get; }

        public NodeClass NodeClass => Attributes.NodeClass;
        public IEnumerable<BaseUANode>? Properties => Attributes.Properties;

        public string DisplayName { get; }
        public NodeId Id { get; }
        public NodeId ParentId { get; }
        public BaseUANode? Parent { get; }

        public IEnumerable<BaseUANode> GetAllProperties()
        {
            if (Properties == null) return Enumerable.Empty<BaseUANode>();
            var result = new List<BaseUANode>();
            result.AddRange(Properties);
            foreach (var prop in Properties)
            {
                result.AddRange(prop.GetAllProperties());
            }
            return result;
        }

        public BaseUANode(NodeId id, string displayName, NodeId parentId)
        {
            Id = id;
            DisplayName = displayName;
            ParentId = parentId;
        }
    }
}

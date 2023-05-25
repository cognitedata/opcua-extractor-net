using System.Collections.Generic;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;

namespace Cognite.OpcUa.Pushers.FDM.Types
{
    public class FullUANodeType : NodeBase
    {
        public Dictionary<string, NodeTypeReference> References { get; }
        public Dictionary<string, DMSReferenceNode> Properties { get; }
        public FullUANodeType? Parent { get; set; }
        public string ExternalId { get; set; }

        public FullUANodeType(BaseUANode node) : base(node)
        {
            References = new();
            Properties = new();
            ExternalId = FDMUtils.SanitizeExternalId(node.Name ?? "");
        }

        public bool IsSimple()
        {
            return Children.Count == 0
                && (Parent == null || Parent.IsSimple());
        }
    }

    public class ChildNode : NodeBase
    {
        public ReferenceNode Reference { get; }
        public ChildNode(BaseUANode node, UAReference reference, string? externalId = null) : base(node)
        {
            Reference = new ReferenceNode(
              node.NodeClass,
              node.Attributes.BrowseName?.Name ?? node.Name ?? "",
              externalId ?? node.Attributes.BrowseName?.Name ?? node.Name ?? "",
              reference
            );
        }

        public IEnumerable<ChildNode> GetAllChildren(bool getParent = true)
        {
            if (getParent)
                yield return this;

            foreach (var child in base.GetAllChildren())
            {
                yield return child;
            }
        }
    }

    public abstract class NodeBase
    {
        public BaseUANode Node { get; }
        public Dictionary<string, ChildNode> Children { get; }

        public NodeBase(BaseUANode node)
        {
            Node = node;
            Children = new();
        }
        public ChildNode AddChild(BaseUANode node, UAReference reference)
        {
            var child = new ChildNode(node, reference);
            Children[child.Reference.BrowseName] = child;
            return child;
        }

        public IEnumerable<ChildNode> GetAllChildren()
        {
            foreach (ChildNode child in Children.Values)
            {
                foreach (ChildNode gc in child.GetAllChildren())
                {
                    yield return gc;
                }
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.FDM.Types
{
    public class FullUANodeType : NodeBase
    {
        public Dictionary<string, NodeTypeReference> References { get; }
        public Dictionary<string, DMSReferenceNode> Properties { get; }
        public FullUANodeType? Parent { get; set; }
        public string ExternalId { get; set; }
        public NodeId NodeId { get; set; }

        public FullUANodeType(BaseUANode node) : base(node)
        {
            References = new();
            Properties = new();
            ExternalId = FDMUtils.SanitizeExternalId(node.Name ?? "");
            NodeId = node.Id;
        }

        public bool IsSimple()
        {
            return Children.Count == 0
                && (Parent == null || Parent.IsSimple());
        }

        public TypeMetadata GetTypeMetadata()
        {
            var properties = new Dictionary<string, IEnumerable<PropertyNode>>();
            foreach (var kvp in Properties)
            {
                var collectedNodes = new List<(BaseUANode node, bool mandatory, UAReference reference)>();
                var node = (BaseUANode)kvp.Value.Node;
                var pathEnum = kvp.Value.Path.Reverse().GetEnumerator();
                pathEnum.MoveNext();
                while (node.Id != NodeId)
                {
                    var pair = pathEnum.Current;
                    collectedNodes.Add((node, kvp.Value.ModellingRule == ModellingRule.Mandatory, pair.Reference));
                    pathEnum.MoveNext();
                    node = node.Parent;
                    if (node == null) throw new InvalidOperationException("Expected property to be proper child of type, followed parents to nothing");
                }
                properties[kvp.Key] = collectedNodes.Select(pair =>
                {
                    var prop = new PropertyNode
                    {
                        TypeDefinition = pair.node.TypeDefinition?.ToString(),
                        BrowseName = $"{pair.node.Attributes.BrowseName?.NamespaceIndex ?? 0}:{pair.node.Attributes.BrowseName?.Name ?? pair.node.Name ?? ""}",
                        NodeId = pair.node.Id.ToString(),
                        NodeClass = (int)pair.node.NodeClass,
                        IsMandatory = pair.mandatory,
                        ReferenceType = pair.reference.Type.Id.ToString(),
                        DisplayName = pair.node.Name ?? "",
                        ExternalId = FDMUtils.SanitizeExternalId(pair.node.Name ?? ""),
                        IsTimeseries = !pair.node.IsRawProperty && pair.node.NodeClass == NodeClass.Variable,
                        Description = pair.node.Attributes.Description
                    };

                    if (pair.node is UAVariable nVar)
                    {
                        prop.ValueRank = nVar.ValueRank;
                        prop.DataType = nVar.FullAttributes.DataType.Id.ToString();
                        prop.ArrayDimensions = nVar.ArrayDimensions;
                    }
                    return prop;
                }).ToList();
            }

            return new TypeMetadata
            {
                IsSimple = IsSimple(),
                NodeId = NodeId.ToString(),
                Parent = Parent?.ExternalId,
                Properties = properties
            };
        }
    }

    public class ChildNode : NodeBase
    {
        public ReferenceNode Reference { get; }
        public ChildNode(BaseUANode node, UAReference reference, string? externalId = null) : base(node)
        {
            Reference = new ReferenceNode(
              node.NodeClass,
              node.Attributes.BrowseName ?? new QualifiedName(node.Name ?? ""),
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
            Children[child.Reference.BrowseName.Name] = child;
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
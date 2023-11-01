using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;

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

        public TypeMetadata GetTypeMetadata(NodeIdContext context)
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
                        NodeId = context.NodeIdToString(pair.node.Id),
                        NodeClass = (int)pair.node.NodeClass,
                        IsMandatory = pair.mandatory,
                        ReferenceType = context.NodeIdToString(pair.reference.Type.Id),
                        DisplayName = pair.node.Name ?? "",
                        ExternalId = FDMUtils.SanitizeExternalId(pair.node.Name ?? ""),
                        IsTimeseries = !pair.node.IsRawProperty && pair.node.NodeClass == NodeClass.Variable,
                        Description = pair.node.Attributes.Description
                    };

                    if (pair.node is UAVariable nVar)
                    {
                        prop.ValueRank = nVar.ValueRank;
                        prop.DataType = context.NodeIdToString(nVar.FullAttributes.DataType.Id);
                        prop.ArrayDimensions = nVar.ArrayDimensions;
                    }
                    return prop;
                }).ToList();
            }

            return new TypeMetadata
            {
                IsSimple = IsSimple(),
                NodeId = context.NodeIdToString(NodeId),
                Parent = Parent?.ExternalId,
                Properties = properties
            };
        }

        public void Build(IReadOnlyDictionary<NodeId, FullUANodeType> types)
        {
            foreach (var child in Children.Values)
            {
                CollectChild(child, Enumerable.Empty<(UAReference, QualifiedName)>(), types);
            }
        }

        private string GetPath(IEnumerable<(UAReference Reference, QualifiedName Name)> path, string name)
        {
            if (path.Any())
            {
                return $"{string.Join('_', path.Select(p => p.Name.Name))}_{name}";
            }
            return name;
        }

        private void CollectChild(ChildNode node, IEnumerable<(UAReference, QualifiedName Name)> path, IReadOnlyDictionary<NodeId, FullUANodeType> types)
        {
            var name = node.Reference.BrowseName;

            FullUANodeType? nodeType = null;
            if (node.Node.TypeDefinition != null)
            {
                nodeType = types.GetValueOrDefault(node.Node.TypeDefinition);
            }

            var isSimple = nodeType != null && nodeType.IsSimple();
            var fullName = GetPath(path, name.Name);
            if (
                isSimple
                && (node.Reference.ModellingRule == ModellingRule.Optional
                    || node.Reference.ModellingRule == ModellingRule.Mandatory)
                && node.Reference.Reference.IsHierarchical
            )
            {
                var nextPath = path.Append((node.Reference.Reference, name));
                if (node.Node is UAVariable variable)
                {
                    Properties[fullName] = new DMSReferenceNode(variable, node.Reference.Reference, fullName, nextPath)
                    {
                        ModellingRule = node.Reference.ModellingRule
                    };
                }

                foreach (var child in node.Children.Values)
                {
                    CollectChild(child, nextPath, types);
                }
            }
            else
            {
                References[fullName] = new NodeTypeReference(node.Reference.NodeClass, node.Reference.BrowseName, fullName, node.Reference.Reference)
                {
                    Type = nodeType,
                    ModellingRule = node.Reference.ModellingRule
                };
            }
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
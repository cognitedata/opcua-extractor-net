using System.Collections.Generic;
using System.Linq;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class NodeTypeCollector
    {
        private readonly ILogger log;
        public Dictionary<NodeId, FullUANodeType> Types { get; }
        private readonly Dictionary<NodeId, ChildNode> properties;
        private readonly FullConfig config;
        private readonly HashSet<NodeId> visitedIds = new();
        public NodeTypeCollector(ILogger log, FullConfig config)
        {
            this.log = log;
            this.config = config;
            Types = new();
            properties = new();
        }

        public void MapNodeTypes(NodeHierarchy nodes)
        {
            var knownNonHierarchical = new List<UAReference>();

            TraverseNode(nodes, knownNonHierarchical, ObjectTypeIds.BaseObjectType, null);
            TraverseNode(nodes, knownNonHierarchical, VariableTypeIds.BaseVariableType, null);

            HandleNonHierarchicalReferences(nodes, knownNonHierarchical);
            BuildNodeTypes();
        }

        private HashSet<NodeId> ignoredReferenceTypes = new HashSet<NodeId>(new[]
        {
            ReferenceTypeIds.HasCondition, ReferenceTypeIds.HasModellingRule, ReferenceTypeIds.HasTypeDefinition
        });

        private void HandleNonHierarchicalReferences(NodeHierarchy nodes, IList<UAReference> knownRefs)
        {
            foreach (var reference in knownRefs)
            {
                // We don't care about references to types here, these are not generally useful.
                if (Types.ContainsKey(reference.Target.Id)) continue;

                // Some reference types may pop up here that we don't want to make into relations
                if (ignoredReferenceTypes.Contains(reference.Type.Id)) continue;

                var node = nodes.Get(reference.Target.Id);
                if (node.NodeClass != NodeClass.Object && node.NodeClass != NodeClass.Variable) continue;

                ChildNode child;
                if (Types.TryGetValue(reference.Source.Id, out var typeSource))
                {
                    child = typeSource.AddChild(node, reference);
                }
                else if (properties.TryGetValue(reference.Source.Id, out var propSource))
                {
                    child = propSource.AddChild(node, reference);
                }
                else
                {
                    continue;
                }
                properties[child.Node.Id] = child;
            }
        }

        private void TraverseNode(NodeHierarchy nodes, IList<UAReference> knownNonHierarchical, NodeId nodeId, UAReference? reference)
        {
            // if (nodeId == ObjectTypeIds.BaseEventType) return;
            var node = nodes.Get(nodeId);
            if (!visitedIds.Add(nodeId)) return;
            bool isType = false;
            if (node.NodeClass == NodeClass.VariableType || node.NodeClass == NodeClass.ObjectType)
            {
                Types[nodeId] = new FullUANodeType(node)
                {
                    Parent = reference == null ? null : Types[reference.Source.Id],
                };
                isType = true;
            }
            else
            {
                if (reference == null) return;
                ChildNode child;
                if (Types.TryGetValue(reference.Source.Id, out var parentNode))
                {
                    child = parentNode.AddChild(node, reference);
                }
                else if (properties.TryGetValue(reference.Source.Id, out var parentProp))
                {
                    child = parentProp.AddChild(node, reference);
                }
                else
                {
                    return;
                }
                properties[nodeId] = child;
            }

            foreach (var subRef in nodes.BySource(nodeId))
            {
                if (subRef.IsHierarchical)
                {
                    TraverseNode(nodes, knownNonHierarchical, subRef.Target.Id, subRef);
                }
                else
                {
                    if (subRef.Type.Id == ReferenceTypeIds.HasModellingRule)
                    {
                        if (isType) continue;
                        ModellingRule modellingRule;
                        if (subRef.Target.Id == ObjectIds.ModellingRule_Mandatory) modellingRule = ModellingRule.Mandatory;
                        else if (subRef.Target.Id == ObjectIds.ModellingRule_Optional) modellingRule = ModellingRule.Optional;
                        else if (subRef.Target.Id == ObjectIds.ModellingRule_MandatoryPlaceholder) modellingRule = ModellingRule.MandatoryPlaceholder;
                        else if (subRef.Target.Id == ObjectIds.ModellingRule_OptionalPlaceholder) modellingRule = ModellingRule.OptionalPlaceholder;
                        else if (subRef.Target.Id == ObjectIds.ModellingRule_ExposesItsArray) modellingRule = ModellingRule.ExposesItsArray;
                        else
                        {
                            modellingRule = ModellingRule.Other;
                            log.LogWarning("Found unknown modelling rule: {Id}", subRef.Target.Id);
                        }

                        properties[nodeId].Reference.ModellingRule = modellingRule;
                    }
                    else
                    {
                        knownNonHierarchical.Add(subRef);
                    }
                }
            }
        }

        private void BuildNodeTypes()
        {
            foreach (var type in Types.Values)
            {
                foreach (var child in type.Children.Values)
                {
                    CollectChild(type, child, Enumerable.Empty<string>());
                }
            }
        }

        private string GetPath(IEnumerable<string> path, string name)
        {
            if (path.Any())
            {
                return $"{string.Join('_', path)}_{name}";
            }
            return name;
        }

        private void CollectChild(FullUANodeType type, ChildNode node, IEnumerable<string> path)
        {
            var name = node.Reference.BrowseName;
            var nodeType = Types[node.Node.TypeDefinition!];
            var fullName = GetPath(path, name);
            if (!nodeType.IsSimple()
                || node.Reference.ModellingRule != ModellingRule.Optional && node.Reference.ModellingRule != ModellingRule.Mandatory
                || !node.Reference.Reference.IsHierarchical)
            {
                type.References[fullName] = new NodeTypeReference(node.Reference.NodeClass, node.Reference.BrowseName, fullName, node.Reference.Reference)
                {
                    Type = nodeType,
                    ModellingRule = node.Reference.ModellingRule,
                };
                return;
            }

            if (node.Node is UAVariable variable)
            {
                type.Properties[fullName] = new NodeTypeProperty(variable, node.Reference.Reference, fullName)
                {
                    ModellingRule = node.Reference.ModellingRule
                };
            }

            var nextPath = path.Append(name);
            foreach (var child in node.Children.Values)
            {
                CollectChild(type, child, nextPath);
            }
        }
    }

    public enum ModellingRule
    {
        // Either 1:1 relationship to other type, or an embedded property
        Mandatory,
        // Either 1:0-1 relationship to other type, or an optional property
        Optional,
        // Skipped? We don't really need to store these.
        ExposesItsArray,
        // 1:0-n relationship
        OptionalPlaceholder,
        // 1:1-n relationship
        MandatoryPlaceholder,
        // Modelling rules are extensible, we have no clue what to do with these.
        Other
    }

    public class BaseNodeTypeReference
    {
        public NodeClass NodeClass { get; }
        public string BrowseName { get; }
        public string ExternalId { get; }

        public BaseNodeTypeReference(NodeClass nodeClass, string browseName, string externalId)
        {
            BrowseName = browseName;
            NodeClass = nodeClass;
            ExternalId = FDMUtils.SanitizeExternalId(externalId);
        }
    }
    public class EdgeNodeTypeReference : BaseNodeTypeReference
    {
        public UAReference Reference { get; }
        public ModellingRule ModellingRule { get; set; } = ModellingRule.Optional;

        public EdgeNodeTypeReference(NodeClass nodeClass, string browseName, string externalId, UAReference uaReference) : base(nodeClass, browseName, externalId)
        {
            Reference = uaReference;
        }
    }
    public class NodeTypeReference : EdgeNodeTypeReference
    {
        public NodeTypeReference(NodeClass nodeClass, string browseName, string externalId, UAReference uaReference)
            : base(nodeClass, browseName, externalId, uaReference)
        {
        }

        public FullUANodeType? Type { get; set; }
    }


    public class NodeTypeProperty : EdgeNodeTypeReference
    {
        public UAVariable Node { get; set; }
        public BasePropertyType? DMSType { get; set; }
        public NodeTypeProperty(UAVariable node, UAReference reference, string externalId)
            : base(node.NodeClass, node.Attributes.BrowseName?.Name ?? node.Name ?? "", externalId, reference)
        {
            Node = node;
        }
    }

    public class FullUANodeType : BaseChildNode
    {
        public Dictionary<string, NodeTypeReference> References { get; }
        public Dictionary<string, NodeTypeProperty> Properties { get; }
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

    public class BaseChildNode
    {
        public BaseUANode Node { get; }
        public Dictionary<string, ChildNode> Children { get; }

        public BaseChildNode(BaseUANode node)
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

    public class ChildNode : BaseChildNode
    {
        public EdgeNodeTypeReference Reference { get; }
        public ChildNode(BaseUANode node, UAReference reference, string? externalId = null) : base(node)
        {
            Reference = new EdgeNodeTypeReference(
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
}

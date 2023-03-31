using Cognite.OpcUa.Config;
using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class NodeTypeCollector
    {
        private readonly ILogger log;
        public Dictionary<NodeId, FullUANodeType> Types { get; }
        private readonly Dictionary<NodeId, FullChildNode> properties;
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

                FullChildNode child;
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
            log.LogTrace("Traverse node {Id}", nodeId);
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
                FullChildNode child;
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

                        

                        properties[nodeId].ModellingRule = modellingRule;
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
                BuildType(type);
            }
        }

        private void BuildType(FullUANodeType type)
        {
            foreach (var child in type.RawChildren.Values)
            {
                CollectChild(type, child, Enumerable.Empty<string>());
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

        private void CollectChild(FullUANodeType type, FullChildNode node, IEnumerable<string> path)
        {
            var name = node.BrowseName;
            var nodeType = Types[node.Node.NodeType!.Id];
            var fullName = GetPath(path, name);
            if (!nodeType.IsSimple()
                || node.ModellingRule != ModellingRule.Optional && node.ModellingRule != ModellingRule.Mandatory
                || !node.Reference.IsHierarchical)
            {
                type.References[fullName] = new NodeTypeReference(node.NodeClass, node.BrowseName, fullName, node.Reference)
                {
                    Type = nodeType,
                    ModellingRule = node.ModellingRule,
                };
                return;
            }

            if (node.Node is UAVariable variable)
            {
                type.Properties[fullName] = new NodeTypeProperty(variable, node.Reference, fullName)
                {
                    ModellingRule = node.ModellingRule
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
        public UAReference Reference { get; }
        public string BrowseName { get; }
        public string ExternalId { get; }
        public ModellingRule ModellingRule { get; set; } = ModellingRule.Optional;

        public BaseNodeTypeReference(NodeClass nodeClass, string browseName, string externalId, UAReference uaReference)
        {
            Reference = uaReference;
            BrowseName = browseName;
            NodeClass = nodeClass;
            ExternalId = FDMUtils.SanitizeExternalId(externalId);
        }
    }
    public class NodeTypeReference : BaseNodeTypeReference
    {
        public NodeTypeReference(NodeClass nodeClass, string browseName, string externalId, UAReference uaReference)
            : base(nodeClass, browseName, externalId, uaReference)
        {
        }

        public FullUANodeType? Type { get; set; }
    }


    public class NodeTypeProperty : BaseNodeTypeReference
    {
        public UAVariable Node { get; set; }
        public PropertyTypeVariant TypeVariant { get; set; } = PropertyTypeVariant.json;
        public NodeTypeProperty(UAVariable node, UAReference reference, string externalId)
            : base(node.NodeClass, node.BrowseName, externalId, reference)
        {
            Node = node;
        }
    }

    public class FullChildNode : BaseNodeTypeReference
    {
        public UANode Node { get; }
        public Dictionary<string, FullChildNode> Children { get; }
        public FullChildNode(UANode node, UAReference reference)
            : base(node.NodeClass, node.BrowseName, node.BrowseName, reference)
        {
            Node = node;
            Children = new Dictionary<string, FullChildNode>();
        }

        public FullChildNode AddChild(UANode node, UAReference reference)
        {
            var child = new FullChildNode(node, reference);
            Children[child.BrowseName] = child;
            return child;
        }
    }

    public class FullUANodeType
    {
        public UANode Node { get; }
        public Dictionary<string, NodeTypeReference> References { get; }
        public Dictionary<string, NodeTypeProperty> Properties { get; }
        public Dictionary<string, FullChildNode> RawChildren { get; }
        public FullUANodeType? Parent { get; set; }
        public string ExternalId { get; set; }

        public FullUANodeType(UANode node)
        {
            Node = node;
            References = new();
            Properties = new();
            RawChildren = new();
            ExternalId = FDMUtils.SanitizeExternalId(node.DisplayName);
        }

        public bool IsSimple()
        {
            return RawChildren.Count == 0
                && (Parent == null || Parent.IsSimple());
        }

        public FullChildNode AddChild(UANode node, UAReference reference)
        {
            var child = new FullChildNode(node, reference);
            RawChildren[child.BrowseName] = child;
            return child;
        }
    }
}

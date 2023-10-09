using System.Collections.Generic;
using System.Linq;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.FDM.Types;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class NodeTypeCollector
    {
        private readonly ILogger log;
        public Dictionary<NodeId, FullUANodeType> Types { get; }
        private readonly Dictionary<NodeId, ChildNode> properties;
        private readonly HashSet<NodeId> visitedIds = new();
        public NodeTypeCollector(ILogger log)
        {
            this.log = log;
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
                    knownNonHierarchical.Add(subRef);
                }
            }
        }

        private void BuildNodeTypes()
        {
            foreach (var type in Types.Values)
            {
                foreach (var child in type.Children.Values)
                {
                    CollectChild(type, child, Enumerable.Empty<(UAReference Reference, QualifiedName Name)>());
                }
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

        private void CollectChild(FullUANodeType type, ChildNode node, IEnumerable<(UAReference Reference, QualifiedName Name)> path)
        {
            var name = node.Reference.BrowseName;
            var nodeType = Types[node.Node.TypeDefinition!];
            var fullName = GetPath(path, name.Name);
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

            var nextPath = path.Append((node.Reference.Reference, name));
            if (node.Node is UAVariable variable)
            {
                type.Properties[fullName] = new DMSReferenceNode(variable, node.Reference.Reference, fullName, nextPath)
                {
                    ModellingRule = node.Reference.ModellingRule
                };
            }

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
}

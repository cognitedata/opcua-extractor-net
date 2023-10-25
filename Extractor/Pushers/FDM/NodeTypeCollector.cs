using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.FDM.Types;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class TypeTraverseResult : TraverseResult
    {
        public IReadOnlyDictionary<NodeId, FullUANodeType> Types { get; }

        public TypeTraverseResult(IReadOnlyDictionary<NodeId, FullUANodeType> types, IReadOnlyDictionary<NodeId, BaseUANode> mappedNodes, IEnumerable<UAReference> mappedReferences)
            : base(mappedNodes, mappedReferences)
        {
            Types = types;
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

    public class NodeTypeCollector
    {
        private readonly ILogger log;
        private readonly Dictionary<NodeId, FullUANodeType> types = new();
        private readonly Dictionary<NodeId, ChildNode> properties = new();

        private static readonly HashSet<NodeId> ignoredReferenceTypes = new HashSet<NodeId>(new[]
        {
            ReferenceTypeIds.HasCondition, ReferenceTypeIds.HasModellingRule, ReferenceTypeIds.HasTypeDefinition
        });

        private readonly HashSet<NodeId> knownTypeDefinitions;

        private readonly NodeHierarchy typeHierarchy;

        private readonly Dictionary<NodeId, BaseUANode> mappedNodes = new();
        private readonly HashSet<UAReference> mappedReferences = new();

        public NodeTypeCollector(ILogger log, HashSet<NodeId> knownTypeDefinitions, NodeHierarchy typeHierarchy)
        {
            this.log = log;
            this.knownTypeDefinitions = knownTypeDefinitions;
            this.typeHierarchy = typeHierarchy;
        }

        public TypeTraverseResult MapTypes()
        {
            // Starting from each known reference type
            // 1. Browse any outgoing references to non-types. Further including of types happens later, so this is
            //    just to include any type fields
            // 2. Browse up to any parent types, until we reach one of the root types.
            //    This is fully recursive, so any parent type will get the full treatment.
            foreach (var type in knownTypeDefinitions)
            {
                TraverseNodeTypeUp(type, null);
            }

            return new TypeTraverseResult(types, mappedNodes, mappedReferences);
        }

        private void TraverseNodeTypeUp(NodeId nodeId, UAReference? reference)
        {
            var node = typeHierarchy.Get(nodeId);

            if (!node.IsType) return;

            if (!TraverseNode(node, reference)) return;

            if (nodeId == ObjectTypeIds.BaseObjectType || nodeId == VariableTypeIds.BaseVariableType) return;

            var parentRef = typeHierarchy.ByTarget(node.Id).FirstOrDefault(n => n.Type.Id == ReferenceTypeIds.HasSubtype);

            if (parentRef != null)
            {
                TraverseNodeTypeUp(parentRef.Source.Id, parentRef);
                if (types.TryGetValue(parentRef.Source.Id, out var parentType))
                {
                    types[nodeId].Parent = parentType;
                }
            }
        }

        private bool TraverseNode(BaseUANode node, UAReference? reference)
        {
            if (reference != null) mappedReferences.Add(reference);
            if (!mappedNodes.TryAdd(node.Id, node)) return false;

            StoreNode(node, reference);

            foreach (var subRef in typeHierarchy.BySource(node.Id))
            {
                if (subRef.Target.IsType && subRef.IsHierarchical) continue;

                HandleOutReference(node, subRef);
            }
            return true;
        }

        private void StoreNode(BaseUANode node, UAReference? reference)
        {
            mappedNodes[node.Id] = node;
            if (node.NodeClass == NodeClass.VariableType || node.NodeClass == NodeClass.ObjectType)
            {
                types[node.Id] = new FullUANodeType(node);
            }
            else
            {
                if (reference == null) throw new InvalidOperationException("Node types must be collected from types as root");
                ChildNode child;
                if (types.TryGetValue(reference.Source.Id, out var parentNode))
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
                properties[node.Id] = child;
            }
        }

        private void HandleOutReference(BaseUANode parent, UAReference subRef)
        {
            if (subRef.IsHierarchical)
            {
                TraverseNode(subRef.Target, subRef);
            }
            else
            {
                if (subRef.Type.Id == ReferenceTypeIds.HasModellingRule && !parent.IsType)
                {
                    ModellingRule modellingRule;
                    if (subRef.Target.Id == ObjectIds.ModellingRule_Mandatory) modellingRule = ModellingRule.Mandatory;
                    else if (subRef.Target.Id == ObjectIds.ModellingRule_Optional) modellingRule = ModellingRule.Optional;
                    else if (subRef.Target.Id == ObjectIds.ModellingRule_MandatoryPlaceholder) modellingRule = ModellingRule.MandatoryPlaceholder;
                    else if (subRef.Target.Id == ObjectIds.ModellingRule_OptionalPlaceholder) modellingRule = ModellingRule.OptionalPlaceholder;
                    else if (subRef.Target.Id == ObjectIds.ModellingRule_ExposesItsArray) modellingRule = ModellingRule.ExposesItsArray;
                    else
                    {
                        modellingRule = ModellingRule.Other;
                        // The standard isn't very clear on whether or not this is possible.
                        // We've never seen it, but the standard might technically allow it.
                        // For now, just log a warning. We'll have to handle it if it comes up.
                        log.LogWarning("Found unknown modelling rule: {Id}", subRef.Target.Id);
                    }

                    properties[parent.Id].Reference.ModellingRule = modellingRule;
                }

                var target = subRef.Target;
                // If this is a type, or in the list of exceptions, we don't want to treat this
                // node as a connection. Typically because it has special meaning in the
                // OPC-UA type hierarchy.
                // We do want to ingest it as an edge, though.
                if (target.IsType || ignoredReferenceTypes.Contains(subRef.Type.Id))
                {
                    mappedReferences.Add(subRef);
                    return;
                }

                StoreNode(target, subRef);
            }
        }
    }
}

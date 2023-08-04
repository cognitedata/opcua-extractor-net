/* Cognite Extractor for OPC-UA
Copyright (C) 2022 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Pushers.FDM
{
    internal class NodeTrimmer
    {
        private NodeHierarchy nodes;
        private readonly HashSet<NodeId> visitedIds = new();
        private FullConfig config;
        private ILogger log;
        public NodeTrimmer(NodeHierarchy nodes, FullConfig config, ILogger log)
        {
            this.nodes = nodes;
            this.config = config;
            this.log = log;
        }

        private bool IsExcludedNode(BaseUANode node)
        {
            if (node.TypeDefinition == null) return false;
            // Is this one of the type system nodes?
            if (node.Id == ObjectIds.XmlSchema_TypeSystem || node.Id == ObjectIds.OPCBinarySchema_TypeSystem) return true;

            // Is the type defintion of the node from the type system?
            if (node.TypeDefinition == ObjectTypeIds.DataTypeSystemType || node.TypeDefinition == VariableTypeIds.DataTypeDescriptionType
                || node.TypeDefinition == VariableTypeIds.DataTypeDictionaryType) return true;
            return false;
        }

        private void TraverseNode(List<BaseUANode> result, List<UAReference> refResult, UAReference? reference, BaseUANode node)
        {
            if (IsExcludedNode(node)) return;
            if (reference != null) refResult.Add(reference);
            if (!visitedIds.Add(node.Id)) return;
            result.Add(node);
            var bySource = nodes.BySource(node.Id);
            var byTarget = nodes.ByTarget(node.Id);

            if (!bySource.Any() && !byTarget.Any())
            {
                log.LogWarning("Orphaned node: {Name} {Id}", node.Name, node.Id);
            }

            NodeId? dataTypeId = null;
            if (node is UAVariable variable)
            {
                dataTypeId = variable.FullAttributes.DataType.Id;
            }
            else if (node is UAVariableType varType)
            {
                dataTypeId = varType.FullAttributes.DataType.Id;
            }
            if (dataTypeId != null && !dataTypeId.IsNullNodeId) TraverseNode(result, refResult, null, nodes.Get(dataTypeId));
            if (node.TypeDefinition != null && !node.TypeDefinition.IsNullNodeId)
            {
                TraverseNode(result, refResult, null, nodes.Get(node.TypeDefinition));
            }

            if (node.Id.NamespaceIndex != 0)
            {
                // We explore all references for custom nodes
                foreach (var rf in bySource)
                {
                    TraverseNode(result, refResult, rf, nodes.Get(rf.Target.Id));
                }
                foreach (var rf in byTarget)
                {
                    TraverseNode(result, refResult, rf, nodes.Get(rf.Source.Id));
                }
            }
            else
            {
                // We explore hierarchically up, and down to non-types for base nodes,
                // we also follow non-hierarchical references, but only outward.
                foreach (var rf in bySource)
                {
                    var target = nodes.Get(rf.Target.Id);
                    if (rf.IsHierarchical)
                    {
                        // These nodes in particular we skip here, as they are huge, and we only need custom members.
                        // They contain the binary schema for every OPC-UA type.
                        if (target.NodeClass == NodeClass.Object || target.NodeClass == NodeClass.Variable)
                        {
                            TraverseNode(result, refResult, rf, target);
                        }
                    }
                    else
                    {
                        TraverseNode(result, refResult, rf, target);
                    }
                }
                foreach (var rf in byTarget)
                {
                    if (rf.IsHierarchical)
                    {
                        TraverseNode(result, refResult, rf, nodes.Get(rf.Source.Id));
                    }
                }
            }
        }

        private void TraverseHierarchy(List<BaseUANode> result, List<UAReference> refResult, UAReference? reference, BaseUANode node)
        {
            if (IsExcludedNode(node)) return;
            if (reference != null) refResult.Add(reference);
            if (visitedIds.Add(node.Id)) result.Add(node);
            var bySource = nodes.BySource(node.Id);

            NodeId? dataTypeId = null;
            if (node is UAVariable variable)
            {
                dataTypeId = variable.FullAttributes.DataType.Id;
            }
            else if (node is UAVariableType varType)
            {
                dataTypeId = varType.FullAttributes.DataType.Id;
            }
            if (dataTypeId != null && !dataTypeId.IsNullNodeId) TraverseNode(result, refResult, null, nodes.Get(dataTypeId));

            if (node.TypeDefinition != null && !node.TypeDefinition.IsNullNodeId)
            {
                TraverseNode(result, refResult, null, nodes.Get(node.TypeDefinition));
            }

            // For hierarchical nodes we just follow all outgoing references.
            foreach (var rf in bySource)
            {
                if (rf.IsHierarchical)
                {
                    TraverseHierarchy(result, refResult, rf, nodes.Get(rf.Target.Id));
                }
                else
                {
                    TraverseNode(result, refResult, rf, nodes.Get(rf.Target.Id));
                }
            }
        }



        public NodeHierarchy Filter()
        {
            var result = new List<BaseUANode>();
            var refResult = new List<UAReference>();
            var roots = nodes.NodeMap.Values.Where(nd => nd.Id.NamespaceIndex != 0).ToList();

            foreach (var node in roots)
            {
                TraverseNode(result, refResult, null, node);
            }

            // If we have enabled all events we need to explore the event hierarchy
            if (config.Events.Enabled && config.Events.AllEvents && nodes.NodeMap.TryGetValue(ObjectTypeIds.BaseEventType, out var baseEvt))
            {
                TraverseNode(result, refResult, null, baseEvt);
                TraverseHierarchy(result, refResult, null, baseEvt);
            }

            // Make sure we get all used reference types
            foreach (var type in refResult.Select(rf => rf.Type.Id).Distinct().ToList())
            {
                TraverseNode(result, refResult, null, nodes.Get(type));
            }

            // Make sure the modelling rule type definition is added
            TraverseNode(result, refResult, null, nodes.Get(ObjectTypeIds.ModellingRuleType));

            refResult = refResult.DistinctBy(rf => (rf.Source.Id, rf.Target.Id, rf.Type.Id)).ToList();

            return new NodeHierarchy(refResult, result);
        }
    }
}

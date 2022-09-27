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
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Pushers.FDM
{
    internal class NodeTrimmer
    {
        private Dictionary<NodeId, IEnumerable<UAReference>> referencesByTargetId;
        private Dictionary<NodeId, IEnumerable<UAReference>> referencesBySourceId;
        private HashSet<NodeId> visitedIds = new HashSet<NodeId>();
        private Dictionary<NodeId, UANode> nodeMap;
        private FullConfig config;
        private ILogger log;
        public NodeTrimmer(IEnumerable<UANode> nodes, IEnumerable<UAReference> references, FullConfig config, ILogger log)
        {
            referencesByTargetId = references.GroupBy(rf => rf.Target.Id).ToDictionary(group => group.Key, group => (IEnumerable<UAReference>)group);
            referencesBySourceId = references.GroupBy(rf => rf.Source.Id).ToDictionary(group => group.Key, group => (IEnumerable<UAReference>)group);
            nodeMap = nodes.ToDictionary(node => node.Id);
            this.config = config;
            this.log = log;
        }

        private void TraverseNode(List<UANode> result, List<UAReference> refResult, UAReference? reference, UANode node)
        {
            if (reference != null) refResult.Add(reference);
            if (!visitedIds.Add(node.Id)) return;
            result.Add(node);
            var bySource = referencesBySourceId.GetValueOrDefault(node.Id) ?? Enumerable.Empty<UAReference>();
            var byTarget = referencesByTargetId.GetValueOrDefault(node.Id) ?? Enumerable.Empty<UAReference>();

            if (!bySource.Any() && !byTarget.Any())
            {
                log.LogWarning("Orphaned node: {Name} {Id}", node.DisplayName, node.Id);
            }

            if (node.NodeClass == NodeClass.Variable || node.NodeClass == NodeClass.VariableType)
            {
                var dataTypeId = (node as UAVariable)!.VariableAttributes.DataType?.Raw;
                if (dataTypeId != null && !dataTypeId.IsNullNodeId) TraverseNode(result, refResult, null, nodeMap[dataTypeId]);
            }

            if (node.Id.NamespaceIndex != 0)
            {
                // We explore all references for custom nodes
                foreach (var rf in bySource)
                {
                    TraverseNode(result, refResult, rf, nodeMap[rf.Target.Id]);
                }
                foreach (var rf in byTarget)
                {
                    TraverseNode(result, refResult, rf, nodeMap[rf.Source.Id]);
                }
            }
            else
            {
                // We explore hierarchically up, and down to non-types for base nodes,
                // we also follow non-hierarchical references, but only outward.
                foreach (var rf in bySource)
                {
                    var target = nodeMap[rf.Target.Id];
                    if (rf.IsHierarchical)
                    {
                        // These nodes in particular we skip here, as they are huge, and we only need custom members.
                        // They contain the binary schema for every OPC-UA type.
                        if (node.Id == ObjectIds.XmlSchema_TypeSystem || node.Id == ObjectIds.OPCBinarySchema_TypeSystem) continue;
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
                        TraverseNode(result, refResult, rf, nodeMap[rf.Source.Id]);
                    }
                }
            }
        }

        private void TraverseHierarchy(List<UANode> result, List<UAReference> refResult, UAReference? reference, UANode node)
        {
            if (reference != null) refResult.Add(reference);
            if (visitedIds.Add(node.Id)) result.Add(node);
            var bySource = referencesBySourceId.GetValueOrDefault(node.Id) ?? Enumerable.Empty<UAReference>();

            if (node.NodeClass == NodeClass.Variable || node.NodeClass == NodeClass.VariableType)
            {
                var dataTypeId = (node as UAVariable)!.VariableAttributes.DataType?.Raw;
                if (dataTypeId != null && !dataTypeId.IsNullNodeId) TraverseNode(result, refResult, null, nodeMap[dataTypeId]);
            }

            // For hierarchical nodes we just follow all outgoing references.
            foreach (var rf in bySource)
            {
                if (rf.IsHierarchical)
                {
                    TraverseHierarchy(result, refResult, rf, nodeMap[rf.Target.Id]);
                }
                else
                {
                    TraverseNode(result, refResult, rf, nodeMap[rf.Target.Id]);
                }
            }
        }



        public (List<UANode>, List<UAReference>) Filter()
        {
            var result = new List<UANode>();
            var refResult = new List<UAReference>();
            var roots = nodeMap.Values.Where(nd => nd.Id.NamespaceIndex != 0).ToList();

            foreach (var node in roots)
            {
                TraverseNode(result, refResult, null, node);
            }

            // If we have enabled all events we need to explore the event hierarchy
            if (config.Events.Enabled && config.Events.AllEvents && nodeMap.TryGetValue(ObjectTypeIds.BaseEventType, out var baseEvt))
            {
                TraverseNode(result, refResult, null, baseEvt);
                TraverseHierarchy(result, refResult, null, baseEvt);
            }

            // Make sure we get all used reference types
            foreach (var type in refResult.Select(rf => rf.Type.Id).Distinct().ToList())
            {
                TraverseNode(result, refResult, null, nodeMap[type]);
            }

            refResult = refResult.DistinctBy(rf => (rf.Source.Id, rf.Target.Id, rf.Type.Id)).ToList();

            return (result, refResult);
        }
    }
}

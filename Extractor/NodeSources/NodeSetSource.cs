﻿using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    class NodeSetConfig
    {
        public string? FileName { get; set; }
        public Uri? URL { get; set; }
    }

    internal class BasicReference : IReference
    {
        public NodeId? ReferenceTypeId { get; set; }
        public bool IsInverse { get; set; }
        public ExpandedNodeId? TargetId { get; set; }
    }

    internal class PlainType
    {
        public NodeId NodeId { get; set; }
        public PlainType? Parent { get; set; }
        public NodeClass NodeClass { get; set; }
        public string? DisplayName { get; set; }

        public PlainType(NodeId id, string? displayName)
        {
            NodeId = id;
            DisplayName = displayName;
        }

        public bool IsOfType(NodeId type)
        {
            PlainType? node = this;
            do
            {
                if (node.NodeId == type) return true;
                node = node.Parent;
            } while (node != null);
            return false;
        }
    }

    internal class PlainEventType : PlainType
    {
        public IList<NodeState> Properties { get; } = new List<NodeState>();
        public PlainEventType(PlainType other) : base(other.NodeId, other.DisplayName)
        {
            Parent = other.Parent;
            NodeClass = other.NodeClass;
        }
    }

    public class NodeSetSource : BaseNodeSource, IEventFieldSource
    {
        private readonly NodeStateCollection nodes = new NodeStateCollection();
        private readonly Dictionary<NodeId, NodeState> nodeDict = new Dictionary<NodeId, NodeState>();

        private Dictionary<NodeId, PlainType> types = new Dictionary<NodeId, PlainType>();

        private Dictionary<NodeId, IList<IReference>> references = new Dictionary<NodeId, IList<IReference>>();
        private List<UAVariable> rawVariables = new List<UAVariable>();
        private List<UANode> rawObjects = new List<UANode>();
        public NodeSetSource(FullConfig config, UAExtractor extractor, UAClient client) : base(config, extractor, client)
        {
            LoadNodeSet(new NodeSetConfig
            {
                URL = new Uri("https://files.opcfoundation.org/schemas/UA/1.04/Opc.Ua.NodeSet2.xml")
            });
            LoadNodeSet(new NodeSetConfig
            {
                FileName = "TestServer.NodeSet2.xml"
            });
        }

        private void LoadNodeSet(NodeSetConfig set)
        {
            if (set.URL != null)
            {
                string fileName = set.FileName ?? set.URL.Segments.Last();
                if (!File.Exists(fileName))
                {
                    using (var client = new WebClient())
                    {
                        client.DownloadFile(set.URL, fileName);
                    }
                }
                set.FileName = fileName;
            }
            LoadNodeSet(set.FileName!);
        }

        private void LoadNodeSet(string file)
        {
            using var stream = new FileStream(file, FileMode.Open, FileAccess.Read);
            var set = Opc.Ua.Export.UANodeSet.Read(stream);
            set.Import(Client.SystemContext, nodes);
            /* foreach (var node in nodes)
            {
                Console.WriteLine($"{node.NodeId}: {node.DisplayName}, {node.NodeClass}");
                var references = new List<IReference>();
                node.GetReferences(Client.SystemContext, references);
                var children = new List<BaseInstanceState>();
                node.GetChildren(Client.SystemContext, children);
                if (references.Any()) Console.WriteLine(references.Any() + ", " + string.Join(',', references.Select(reference => reference.TargetId)));
                if (children.Any()) Console.WriteLine(children.Any() + ", " + string.Join(',', children.Select(child => child.DisplayName)));
                if (node is BaseTypeState type) Console.WriteLine(type.SuperTypeId);
                if (node is BaseInstanceState instance) Console.WriteLine(instance.ModellingRuleId + ", " + instance.TypeDefinitionId);
                
            }
            foreach (var ns in Client.NamespaceTable.ToArray())
            {
                Console.WriteLine(ns);
            } */
        }

        private void LoadReferences()
        {
            int cnt = 0;
            // First, extract all references and group them by nodeId
            foreach (var node in nodes)
            {
                if (!references.TryGetValue(node.NodeId, out var refs))
                {
                    references[node.NodeId] = refs = new List<IReference>();
                }
                node.GetReferences(Client.SystemContext, refs);
                if (node is BaseTypeState type) refs.Add(new BasicReference
                {
                    IsInverse = true,
                    ReferenceTypeId = ReferenceTypeIds.HasSubtype,
                    TargetId = type.SuperTypeId
                });
                if (node is BaseInstanceState instance)
                {
                    if (instance.ModellingRuleId != null && !instance.ModellingRuleId.IsNullNodeId) refs.Add(new BasicReference
                    {
                        IsInverse = false,
                        ReferenceTypeId = ReferenceTypeIds.HasModellingRule,
                        TargetId = instance.ModellingRuleId
                    });
                    if (instance.TypeDefinitionId != null && !instance.TypeDefinitionId.IsNullNodeId) refs.Add(new BasicReference
                    {
                        IsInverse = false,
                        ReferenceTypeId = ReferenceTypeIds.HasTypeDefinition,
                        TargetId = instance.TypeDefinitionId
                    });
                }
                cnt += refs.Count;
            }
            Console.WriteLine($"Found {cnt} references");
            // Create all inverse references
            foreach (var node in nodes)
            {
                foreach (var reference in references[node.NodeId])
                {
                    var targetId = Client.ToNodeId(reference.TargetId);
                    if (!references.TryGetValue(targetId, out var targetRefs))
                    {

                        references[targetId] = targetRefs = new List<IReference>();
                    }
                    if (!targetRefs.Any(targetRef =>
                        Client.ToNodeId(targetRef.TargetId) == node.NodeId
                        && targetRef.ReferenceTypeId == reference.ReferenceTypeId
                        && targetRef.IsInverse == !reference.IsInverse))
                    {
                        targetRefs.Add(new BasicReference
                        {
                            IsInverse = !reference.IsInverse,
                            TargetId = node.NodeId,
                            ReferenceTypeId = reference.ReferenceTypeId
                        });
                        cnt++;
                    }
                }
            }
            Console.WriteLine($"Found or created {cnt} references");
        }


        private void LoadTypeTree()
        {
            // Needs two passes since the order is not guaranteed.
            foreach (var node in nodes)
            {
                if (node.NodeClass != NodeClass.VariableType
                    && node.NodeClass != NodeClass.ObjectType
                    && node.NodeClass != NodeClass.ReferenceType
                    && node.NodeClass != NodeClass.DataType) continue;
                types[node.NodeId] = new PlainType(node.NodeId, node.DisplayName?.Text) { NodeClass = node.NodeClass,
                    NodeId = node.NodeId, DisplayName = node.DisplayName?.Text };
                
            }
            foreach (var type in types)
            {
                var parentRef = references[type.Key].FirstOrDefault(rf =>
                    rf.ReferenceTypeId == ReferenceTypeIds.HasSubtype
                    && rf.IsInverse);
                if (parentRef != null)
                {
                    type.Value.Parent = types.GetValueOrDefault(Client.ToNodeId(parentRef.TargetId));
                }
                if (type.Value.NodeClass == NodeClass.DataType)
                {
                    PropertyState? enumVarNode = null;
                    foreach (var rf in references[type.Key])
                    {
                        if (rf.ReferenceTypeId != ReferenceTypeIds.HasProperty || rf.IsInverse) continue;
                        if (!nodeDict.TryGetValue(Client.ToNodeId(rf.TargetId), out var node)) continue;
                        if (node.BrowseName?.Name != "EnumStrings" && node.BrowseName?.Name != "EnumValues") continue;
                        enumVarNode = node as PropertyState;
                        break;
                    }                       
                    

                    Client.DataTypeManager.RegisterType(type.Value.NodeId,
                        type.Value.Parent?.NodeId ?? NodeId.Null, type.Value.DisplayName);

                    if (enumVarNode != null)
                    {
                        Client.DataTypeManager.SetEnumStrings(type.Value.NodeId, enumVarNode.Value);
                    }
                }
            }
        }

        private bool IsOfType(NodeId source, NodeId parent)
        {
            if (!types.TryGetValue(source, out var type)) return false;
            return type.IsOfType(parent);
        }

        private IEnumerable<IReference> Browse(NodeId node, NodeId referenceTypeId, BrowseDirection direction, bool allowSubTypes)
        {
            var refs = references[node];
            foreach (var reference in refs)
            {
                if (!allowSubTypes && referenceTypeId != reference.ReferenceTypeId) continue;
                else if (allowSubTypes && !IsOfType(reference.ReferenceTypeId, referenceTypeId)) continue;

                if (reference.IsInverse && direction != BrowseDirection.Inverse
                    && direction != BrowseDirection.Both) continue;
                else if (!reference.IsInverse && direction != BrowseDirection.Forward
                    && direction != BrowseDirection.Both) continue;

                yield return reference;
            }
        }

        private bool BuildNode(NodeId id, NodeId parent)
        {
            var node = nodeDict[id];
            if (node.NodeClass == NodeClass.Variable
                || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.VariableType)
            {
                var variable = new UAVariable(id, node.DisplayName?.Text ?? "", parent, node.NodeClass);
                if (node is BaseVariableState varState)
                {
                    variable.VariableAttributes.AccessLevel = varState.AccessLevel;
                    variable.VariableAttributes.ArrayDimensions =
                        varState.ArrayDimensions == null
                        ? null
                        : varState.ArrayDimensions.Select(val => (int)val).ToArray();
                    variable.VariableAttributes.ValueRank = varState.ValueRank;
                    variable.VariableAttributes.Description = varState.Description?.Text;
                    variable.VariableAttributes.Historizing = varState.Historizing;
                    variable.SetNodeType(Client, varState.TypeDefinitionId);
                    variable.VariableAttributes.DataType = Client.DataTypeManager.GetDataType(varState.DataType);
                }
                else if (node is BaseVariableTypeState typeState)
                {
                    variable.VariableAttributes.ArrayDimensions =
                        typeState.ArrayDimensions == null
                        ? null
                        : typeState.ArrayDimensions.Select(val => (int)val).ToArray();
                    variable.VariableAttributes.ValueRank = typeState.ValueRank;
                    variable.VariableAttributes.Description = typeState.Description?.Text;
                    variable.SetDataPoint(new Variant(typeState.Value));
                    variable.ValueRead = true;
                    variable.VariableAttributes.DataType = Client.DataTypeManager.GetDataType(typeState.DataType);
                }
                rawVariables.Add(variable);
                return true;
            }
            else if (node.NodeClass == NodeClass.Object
                || Config.Extraction.NodeTypes.AsNodes && node.NodeClass == NodeClass.ObjectType)
            {
                if (id == ObjectIds.Server || id == ObjectIds.Aliases) return false;
                var obj = new UANode(id, node.DisplayName?.Text ?? "", parent, node.NodeClass);
                if (node is BaseObjectState objState)
                {
                    obj.Attributes.Description = objState.Description?.Text;
                    obj.Attributes.EventNotifier = objState.EventNotifier;
                    obj.SetNodeType(Client, objState.TypeDefinitionId);
                }
                else if (node is BaseObjectTypeState typeState)
                {
                    obj.Attributes.Description = typeState.Description?.Text;
                }
                rawObjects.Add(obj);
                return true;
            }
            return false;
        }


        /// <summary>
        /// Construct 
        /// </summary>
        public void BuildNodes(IEnumerable<NodeId> rootNodes)
        {
            foreach (var node in nodes)
            {
                nodeDict[node.NodeId] = node;
            }

            // Load all references into dictionary
            LoadReferences();
            // Build internal type tree for browsing, and insert types into type managers where relevant.
            LoadTypeTree();

            var visitedNodes = new HashSet<NodeId>();

            // Simulate browsing the node hierarchy. We do it this way to ensure that we visit the correct nodes.
            var nextIds = rootNodes.ToList();

            foreach (var id in rootNodes)
            {
                visitedNodes.Add(id);
                BuildNode(id, NodeId.Null);
            }

            while (nextIds.Any())
            {
                var refs = new List<(IReference Node, NodeId ParentId)>();

                foreach (var id in nextIds)
                {
                    var children = Browse(id, ReferenceTypeIds.HierarchicalReferences, BrowseDirection.Forward, true);
                    refs.AddRange(children.Select(child => (child, id)));
                }

                nextIds.Clear();
                foreach (var (child, parent) in refs)
                {
                    var childId = Client.ToNodeId(child.TargetId);
                    if (visitedNodes.Add(childId) && BuildNode(childId, parent))
                    {
                        nextIds.Add(childId);
                    }
                }
            }


            foreach (var node in rawObjects)
            {
                Console.WriteLine(node.ToString());
            }

            foreach (var node in rawVariables)
            {
                Console.WriteLine(node.ToString());
            }
        }


        public override Task<BrowseResult> ParseResults(CancellationToken token)
        {
            throw new NotImplementedException();
        }

        private IEnumerable<EventField> ToFields(NodeState state)
        {
            var refs = references[state.NodeId];
            var children = refs
                .Where(rf => !rf.IsInverse && IsOfType(rf.ReferenceTypeId, ReferenceTypeIds.HierarchicalReferences))
                .Select(rf => nodeDict.GetValueOrDefault(Client.ToNodeId(rf.TargetId)))
                .Where(node => node != null && (node.NodeClass == NodeClass.Object || node.NodeClass == NodeClass.Variable))
                .ToList();
            if (state.NodeClass == NodeClass.Object && !children.Any()) yield break;
            else if (state.NodeClass != NodeClass.Variable) yield break;
        }

        public Dictionary<NodeId, HashSet<EventField>> GetEventIdFields(CancellationToken token)
        {
            var evtTypes = types.Where(type => type.Value.NodeClass == NodeClass.ObjectType
                && IsOfType(type.Key, ObjectTypeIds.BaseEventType)).ToDictionary(kvp => kvp.Key, kvp => new PlainEventType(kvp.Value));

            HashSet<NodeId>? whitelist = null;
            if (Config.Events.EventIds != null && Config.Events.EventIds.Any())
            {
                whitelist = new HashSet<NodeId>(Config.Events.EventIds.Select(proto => proto.ToNodeId(Client, ObjectTypeIds.BaseEventType)));
            }

            foreach (var (id, type) in evtTypes)
            {

            }



            throw new NotImplementedException();
        }
    }
}
